// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestBufferedRowsStop checks requests made after a chunk was fetched but
// before its remaining rows were returned.
func TestBufferedRowsStop(t *testing.T) {
	for _, mode := range []string{"interrupt", "context"} {
		t.Run(mode, func(t *testing.T) {
			db, _ := open(t, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rows, err := db.QueryContext(ctx, "SELECT i FROM (VALUES (1), (2), (3)) AS v(i)")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() || rows.Int64(0) != 1 {
				t.Fatalf("first row: %v", rows.Err())
			}
			want := error(ErrInterrupted)
			if mode == "context" {
				cancel()
				want = context.Canceled
			} else {
				rows.Interrupt()
			}
			for i := 0; i < 3; i++ {
				if rows.Next() || !errors.Is(rows.Err(), want) {
					t.Fatalf("Next after %s: err %v, want %v", mode, rows.Err(), want)
				}
			}
		})
	}
}

// TestRowsNormalEnd preserves successful EOF across later cancellation and
// interruption; reading the last nonempty chunk alone is not EOF.
func TestRowsNormalEnd(t *testing.T) {
	for _, eof := range []bool{false, true} {
		t.Run(strconv.FormatBool(eof), func(t *testing.T) {
			db, _ := open(t, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rows, err := db.QueryContext(ctx, "SELECT 1")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			if eof && (rows.Next() || rows.Err() != nil) {
				t.Fatalf("normal end: %v", rows.Err())
			}
			cancel()
			rows.Interrupt()
			for i := 0; i < 3; i++ {
				if rows.Next() {
					t.Fatal("row after stop")
				}
				if eof {
					if rows.Err() != nil {
						t.Fatalf("EOF changed: %v", rows.Err())
					}
				} else if !errors.Is(rows.Err(), context.Canceled) {
					t.Fatalf("last chunk is not EOF: %v", rows.Err())
				}
			}
		})
	}
}

// TestInterruptWhileFetching checks that a native fetch can be interrupted
// from another goroutine without holding the result-destruction lock.
func TestInterruptWhileFetching(t *testing.T) {
	db, _ := open(t, nil)
	rows, err := db.Query("WITH RECURSIVE t(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM t WHERE i < 1000000000) SELECT sum(i) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	interrupted := make(chan struct{})
	timer := time.AfterFunc(25*time.Millisecond, func() { rows.Interrupt(); close(interrupted) })
	defer timer.Stop()
	if rows.Next() || !errors.Is(rows.Err(), ErrInterrupted) {
		t.Fatalf("running fetch: %v", rows.Err())
	}
	<-interrupted
}

// TestContextWhileFetching checks that QueryContext stops a running native
// fetch, as well as rows already in the Go-facing chunk.
func TestContextWhileFetching(t *testing.T) {
	db, _ := open(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, err := db.QueryContext(ctx, "WITH RECURSIVE t(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM t WHERE i < 1000000000) SELECT sum(i) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	timer := time.AfterFunc(25*time.Millisecond, cancel)
	defer timer.Stop()
	if rows.Next() || !errors.Is(rows.Err(), context.Canceled) {
		t.Fatalf("running fetch: %v", rows.Err())
	}
}

// TestUnsignedFields accepts uint and uint64 values through MaxInt64 and
// rejects larger ones without accepting any point of the batch.
func TestUnsignedFields(t *testing.T) {
	db, _ := open(t, nil)
	max := uint64(1<<63 - 1)
	values := []struct {
		// m_value is the unsigned input passed as a field value.
		m_value any

		// m_want is the signed value expected when reading it back.
		m_want int64
	}{
		{uint(1), 1},
		{uint8(255), 255},
		{uint16(65535), 65535},
		{uint32(1<<32 - 1), 1<<32 - 1},
		{uint64(max), int64(max)},
	}
	tooLarge := uint64(1) << 63
	rejected := []any{tooLarge}
	if strconv.IntSize == 64 {
		values = append(values, struct {
			// m_value is the unsigned input passed as a field value.
			m_value any

			// m_want is the signed value expected when reading it back.
			m_want int64
		}{uint(max), int64(max)})
		rejected = append(rejected, uint(tooLarge))
	}
	for i, tc := range values {
		err := db.Write("unsigned", Point{Time: t0.Add(time.Duration(i) * time.Microsecond), Fields: []Field{{"v", tc.m_value}}})
		if err != nil {
			t.Fatalf("write %T: %v", tc.m_value, err)
		}
	}
	got := queryAll(t, db, "SELECT v FROM unsigned ORDER BY time")
	if len(got) != len(values) {
		t.Fatalf("got %d rows", len(got))
	}
	for i, tc := range values {
		if got[i][0] != tc.m_want {
			t.Fatalf("value %T: %v, want %d", tc.m_value, got[i][0], tc.m_want)
		}
	}
	before := db.Stats().VisibleSeq
	for _, v := range rejected {
		err := db.Write("unsigned", Point{Time: t0, Fields: []Field{{"v", 1}}}, Point{Time: t0, Fields: []Field{{"v", v}}})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("oversize %T: %v", v, err)
		}
	}
	if db.Stats().VisibleSeq != before {
		t.Fatal("rejected batch advanced visible sequence")
	}
	got = queryAll(t, db, "SELECT v FROM unsigned ORDER BY time")
	if len(got) != len(values) {
		t.Fatal("rejected batch stored a row")
	}
}

// TestFlushIntervalBounds checks conversion boundaries before directory
// creation, including settings that previously wrapped to valid C values.
func TestFlushIntervalBounds(t *testing.T) {
	const max = time.Duration(1<<32-1) * time.Millisecond
	cases := []struct {
		// m_name identifies the conversion boundary under test.
		m_name string

		// m_interval is the requested background flush interval.
		m_interval time.Duration

		// m_invalid reports whether opening must reject these options.
		m_invalid bool
	}{
		{"negative", -time.Millisecond, true},
		{"submillisecond", time.Nanosecond, true},
		{"fractional", time.Millisecond + time.Nanosecond, true},
		{"overflow", max + time.Millisecond, true},
		{"wrap_to_200ms", max + 201*time.Millisecond, true},
		{"zero", 0, true},
		{"minimum", time.Millisecond, false},
		{"maximum", max, false},
	}
	for _, tc := range cases {
		t.Run(tc.m_name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.FlushInterval = tc.m_interval
			dir := filepath.Join(t.TempDir(), "db")
			db, err := Open(dir, &opts)
			if db != nil {
				defer db.Close()
			}
			if tc.m_invalid {
				if db != nil || !errors.Is(err, ErrInvalidArgument) {
					t.Fatalf("Open(%v): db %v, err %v", tc.m_interval, db, err)
				}
				if _, err := os.Stat(dir); !os.IsNotExist(err) {
					t.Fatalf("invalid options created a directory: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
