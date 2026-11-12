// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestNulArguments rejects embedded NUL before a path, table or SQL argument
// can be truncated by the C API, and keeps existing data unchanged.
func TestNulArguments(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "db")
		db, err := Open(dir+"\x00ignored", nil)
		if db != nil {
			db.Close()
		}

		if db != nil || !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Open with NUL: db %v, err %v", db, err)
		}

		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("invalid path created prefix directory: %v", err)
		}
	})
	for _, method := range []string{"write", "query", "query_context", "explain"} {
		t.Run(method, func(t *testing.T) {
			opts := DefaultOptions()

			db, dir := open(t, &opts)
			p := Point{Time: t0, Fields: []Field{{"v", 1}}}
			if err := db.Write("cpu", p); err != nil {
				t.Fatal(err)
			}

			before := db.Stats().VisibleSeq
			var err error
			switch method {
			case "write":
				err = db.Write("cpu\x00suffix", p)
			case "query", "query_context":
				var rows *Rows
				if method == "query" {
					rows, err = db.Query("SELECT 7\x00; DELETE FROM cpu")
				} else {
					rows, err = db.QueryContext(context.Background(), "SELECT 7\x00; DELETE FROM cpu")
				}

				if rows != nil {
					rows.Close()
					t.Error("invalid SQL returned rows")
				}
			case "explain":
				var plan string
				plan, err = db.Explain("SELECT 7\x00; DELETE FROM cpu")
				if plan != "" {
					t.Error("invalid SQL returned a plan")
				}
			}

			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("%s with NUL: %v", method, err)
			}

			if db.Stats().VisibleSeq != before {
				t.Error("invalid input advanced visible sequence")
			}

			// Reopen through WAL recovery, so both the table and its exact
			// m_row count must survive a refused operation.
			db.Close()
			db, err = Open(dir, &opts)
			if err != nil {
				t.Fatal(err)
			}

			defer db.Close()
			got := queryAll(t, db, "SELECT count(*) FROM cpu")
			if got[0][0] != int64(1) {
				t.Fatalf("prefix table changed: %v", got)
			}
		})
	}
}

// TestInvalidOptionsDoNotCreateDirectory rejects options before filesystem work.
func TestInvalidOptionsDoNotCreateDirectory(t *testing.T) {
	for _, name := range []string{"interval", "partition", "retention"} {
		t.Run(name, func(t *testing.T) {
			opts := DefaultOptions()
			switch name {
			case "interval":
				opts.FlushInterval = 0
			case "partition":
				opts.PartitionDurationDays = 0
			case "retention":
				opts.RetentionDays = 110001
			}

			dir := filepath.Join(t.TempDir(), "invalid")
			db, err := Open(dir, &opts)
			if db != nil {
				db.Close()
			}

			if db != nil || !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("invalid options: db %v, err %v", db, err)
			}

			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("invalid options created directory: %v", err)
			}
		})
	}
}

// TestNulVarcharValues preserves NUL in length-delimited values while the
// C-string arguments reject it, for memtable, flushed and reopened queries.
func TestNulVarcharValues(t *testing.T) {
	opts := DefaultOptions()

	db, dir := open(t, &opts)
	tag, field := "host\x00suffix", "value\x00suffix"
	if err := db.Write("cpu", Point{Time: t0, Tags: []Tag{{"host", tag}}, Fields: []Field{{"v", field}}}); err != nil {
		t.Fatal(err)
	}

	for stage := 0; stage < 3; stage++ {
		got := queryAll(t, db, "SELECT host, v FROM cpu")
		if len(got) != 1 || got[0][0] != tag || got[0][1] != field {
			t.Fatalf("stage %d: %v", stage, got)
		}

		if stage == 0 {
			waitUntilFlushed(t, db)
		} else if stage == 1 {
			db.Close()
			var err error
			db, err = Open(dir, &opts)
			if err != nil {
				t.Fatal(err)
			}

			defer db.Close()
		}
	}
}

// TestPointTimeRange rejects the original out-of-range instant before its
// microsecond conversion can wrap; no point of a refused batch survives.
func TestPointTimeRange(t *testing.T) {
	opts := DefaultOptions()

	db, dir := open(t, &opts)
	p := Point{Time: t0, Fields: []Field{{"v", 1}}}
	if err := db.Write("m", p); err != nil {
		t.Fatal(err)
	}

	min := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	max := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	// A full uint64 microsecond period wraps onto t0 in either direction.
	farFuture := time.Unix(t0.Unix()+18446744073709, 551616000)
	farPast := time.Unix(t0.Unix()-18446744073710, 448384000)
	cases := []time.Time{min.Add(-time.Nanosecond), max, max.Add(time.Nanosecond), time.Time{}, farFuture, farPast,
		time.Date(1900, 1, 1, 0, 0, 0, 0, time.FixedZone("east", 14*3600))}
	before := db.Stats().VisibleSeq
	for _, instant := range cases {
		bad := p
		bad.Time = instant
		err := db.Write("m", p, bad)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("time %v: err %v", instant, err)
		}
	}

	if db.Stats().VisibleSeq != before {
		t.Error("refused batch advanced visible sequence")
	}

	db.Close()
	var err error
	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	got := queryAll(t, db, "SELECT count(*) FROM m")
	if got[0][0] != int64(1) {
		t.Fatalf("refused batch survived recovery: %v", got)
	}
}

// TestPointTimeBoundaries accepts instants just inside the UTC limits,
// keeps microsecond precision and handles time-zone offsets.
func TestPointTimeBoundaries(t *testing.T) {
	opts := DefaultOptions()

	db, dir := open(t, &opts)
	min := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	max := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	instants := []time.Time{min, min.Add(time.Nanosecond), time.Unix(0, -1), t0.In(time.FixedZone("east", 9*3600)), max.Add(-time.Nanosecond)}
	points := make([]Point, len(instants))
	for i, instant := range instants {
		points[i] = Point{Time: instant, Fields: []Field{{"id", i}}}
	}

	if err := db.Write("m", points...); err != nil {
		t.Fatal(err)
	}

	waitUntilFlushed(t, db)
	db.Close()
	var err error
	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	got := queryAll(t, db, "SELECT time FROM m ORDER BY id")
	if len(got) != len(instants) {
		t.Fatalf("got %d rows, want %d", len(got), len(instants))
	}

	for i, instant := range instants {
		want := instant.UTC().Truncate(time.Microsecond)
		if got[i][0] != want {
			t.Errorf("time %v: got %v, want %v", instant, got[i][0], want)
		}
	}
}

// TestWriteAllocationLimits rejects oversized inputs in the sizing pass,
// before allocating the C arena or duplicating their string contents.
func TestWriteAllocationLimits(t *testing.T) {
	large := strings.Repeat("x", MaxStringLen+1)
	cases := []Point{
		{Time: t0, Tags: []Tag{{Name: "tag", Value: large}}, Fields: []Field{{Name: "v", Value: 1}}},
		{Time: t0, Fields: []Field{{Name: "v", Value: large}}},
	}
	for _, point := range cases {
		if _, err := writeArenaSize([]Point{point}); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("oversized string: %v", err)
		}
	}
	value := strings.Repeat("x", MaxStringLen)
	points := make([]Point, 8)
	for i := range points {
		points[i] = Point{Time: t0, Fields: []Field{{Name: "v", Value: value}}}
	}
	if _, err := writeArenaSize(points); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("oversized batch: %v", err)
	}
	// One maximum-size value remains valid; the allocation includes C headers.
	if n, err := writeArenaSize(points[:1]); err != nil || n <= uintptr(len(value)) {
		t.Fatalf("valid boundary: size=%d error=%v", n, err)
	}
}
