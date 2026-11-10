// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// t0 is a fixed time for the points of the tests.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// open opens a database in a new directory, closed when the test ends.
func open(t *testing.T, opts *Options) (*DB, string) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(func() { db.Close() })

	return db, dir
}

// waitUntilFlushed waits for the background thread to store visible writes.
func waitUntilFlushed(t *testing.T, db *DB) {
	t.Helper()
	target := db.Stats().VisibleSeq
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats := db.Stats()
		if stats.FlushedSeq >= target {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("background flush did not reach sequence %d: %+v", target, stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// queryAll runs a query and reads every row with Value.
func queryAll(t *testing.T, db *DB, sql string) [][]any {
	t.Helper()

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rows.Close()

	var all [][]any

	for rows.Next() {
		row := make([]any, len(rows.Columns()))
		for c := range row {
			row[c] = rows.Value(c)
		}
		all = append(all, row)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}

	return all
}

// TestWALSyncOptions checks the default and both C option conversions, then
// reopens unflushed writes with the other sync mode.
func TestWALSyncOptions(t *testing.T) {
	if DefaultOptions().WALSync {
		t.Fatal("WAL sync must be disabled by default")
	}
	for _, sync := range []bool{false, true} {
		t.Run(fmt.Sprint(sync), func(t *testing.T) {
			opts := DefaultOptions()
			opts.WALSync = sync
			opts.FlushInterval = time.Hour
			c, err := opts.toC()
			if err != nil {
				t.Fatal(err)
			}
			if (c.m_wal_sync != 0) != sync {
				t.Fatal("WALSync was not converted to the C option")
			}
			db, dir := open(t, &opts)
			if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			opts.WALSync = !sync
			db, err = Open(dir, &opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if got := queryAll(t, db, "SELECT v FROM m"); len(got) != 1 || got[0][0] != int64(1) {
				t.Fatalf("recovered rows: %v", got)
			}
		})
	}
}

// TestWriteQuery writes every field type and reads it back, NULLs
// included.
func TestWriteQuery(t *testing.T) {
	db, _ := open(t, nil)

	err := db.Write("cpu",
		Point{
			Time:   t0,
			Tags:   []Tag{{"host", "web-1"}},
			Fields: []Field{{"usage", 12.5}, {"cores", 8}, {"up", true}, {"note", "ok"}},
		},
		Point{
			Time:   t0.Add(time.Second),
			Tags:   []Tag{{"host", "web-2"}},
			Fields: []Field{{"usage", 30.0}},
		})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := queryAll(t, db,
		"SELECT time, host, usage, cores, up, note FROM cpu ORDER BY time")
	want := [][]any{
		{t0, "web-1", 12.5, int64(8), true, "ok"},
		{t0.Add(time.Second), "web-2", 30.0, nil, nil, nil},
	}

	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows %v, want %v", got, want)
	}
}

// TestReopen finds the writes again after a close, flushed or not.
func TestReopen(t *testing.T) {
	opts := DefaultOptions()

	db, dir := open(t, &opts)

	for i := 0; i < 3; i++ {
		if err := db.Write("m", Point{Time: t0.Add(time.Duration(i) * time.Second),
			Fields: []Field{{"v", i}}}); err != nil {
			t.Fatal(err)
		}

		if i == 1 {
			waitUntilFlushed(t, db)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := queryAll(t, db, "SELECT sum(v) FROM m"); fmt.Sprint(got) != "[[3]]" {
		t.Fatalf("sum %v, want [[3]]", got)
	}
}

// TestErrors checks error codes, messages and errors.Is.
func TestErrors(t *testing.T) {
	db, dir := open(t, nil)

	// An empty batch.
	err := db.Write("m")
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "empty batch") {
		t.Fatalf("empty batch: %v", err)
	}

	// A value type the C API has no column type for.
	err = db.Write("m", Point{Time: t0, Fields: []Field{{"v", []int{1}}}})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unsupported value: %v", err)
	}

	// A column changing type.
	if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}

	err = db.Write("m", Point{Time: t0, Fields: []Field{{"v", "x"}}})
	if !errors.Is(err, ErrSchemaConflict) {
		t.Fatalf("schema conflict: %v", err)
	}

	// A second handle on the same directory.
	if _, err := Open(dir, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open: %v", err)
	}

	// SQL errors and refused statements.
	if _, err := db.Query("SELECT nope FROM m"); err == nil {
		t.Fatal("unknown column accepted")
	}

	if _, err := db.Query("DELETE FROM m"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("DELETE: %v", err)
	}

	if err := db.DropTable("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("drop missing table: %v", err)
	}

	// Calls on a closed database.
	db.Close()

	if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err == nil {
		t.Fatal("write after close accepted")
	}
}

// TestTables lists tables and verifies that managed tables cannot be dropped.
func TestTables(t *testing.T) {
	opts := DefaultOptions()

	db, dir := open(t, &opts)

	for _, name := range []string{"b", "a"} {
		if err := db.Write(name, Point{Time: t0, Tags: []Tag{{"host", "h"}},
			Fields: []Field{{"v", 1.5}}}); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := db.Tables()
	if err != nil {
		t.Fatal(err)
	}

	want := "[{a [{time 0 4} {host 1 5} {v 2 3}]} {b [{time 0 4} {host 1 5} {v 2 3}]}]"
	if fmt.Sprint(tables) != want {
		t.Fatalf("tables %v, want %v", tables, want)
	}

	before := db.Stats()
	if err := db.DropTable("A"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("drop managed table: %v", err)
	}
	after := db.Stats()
	if after.FlushedSeq != before.FlushedSeq || after.VisibleSeq != before.VisibleSeq {
		t.Fatalf("rejected drop changed sequence boundaries: before %+v, after %+v", before, after)
	}

	tables, err = db.Tables()
	if err != nil || fmt.Sprint(tables) != want {
		t.Fatalf("after rejected drop: %v, %v", tables, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, name := range []string{"a", "b"} {
		got := queryAll(t, reopened, "SELECT count(*) FROM "+name)
		if got[0][0] != int64(1) {
			t.Fatalf("%s after recovery: %v", name, got)
		}
	}
}

// TestOrdinaryDDL covers mixed-table queries, persistence and protected names.
func TestOrdinaryDDL(t *testing.T) {
	opts := DefaultOptions()

	db, dir := open(t, &opts)
	if err := db.Write("cpu", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}
	waitUntilFlushed(t, db)
	if err := db.Write("cpu", Point{Time: t0.Add(time.Second), Fields: []Field{{"v", 2}}}); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()
	if err := db.ExecDDL("CREATE TABLE copied AS SELECT * FROM cpu"); err != nil {
		t.Fatal(err)
	}
	if after := db.Stats(); after.VisibleSeq != before.VisibleSeq || after.FlushedSeq < before.FlushedSeq || after.FlushedSeq > after.VisibleSeq {
		t.Fatalf("DDL changed time-series sequence boundaries: before %+v, after %+v", before, after)
	}
	got := queryAll(t, db, "SELECT sum(c.v) FROM copied c JOIN cpu t ON c.v = t.v")
	if got[0][0] != int64(3) {
		t.Fatalf("mixed-table JOIN: %v", got)
	}
	for _, sql := range []string{"DROP TABLE cpu", "DROP TABLE IF EXISTS cpu", "CREATE OR REPLACE TABLE cpu AS SELECT 1 AS v", "DROP TABLE _pt_state", "CREATE TABLE x AS SELECT * FROM _pt_tables"} {
		if err := db.ExecDDL(sql); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("protected statement %q: %v", sql, err)
		}
	}
	if err := db.Write("copied", Point{Time: t0, Fields: []Field{{"v", 3}}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("time-series write to ordinary table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got = queryAll(t, reopened, "SELECT count(*) FROM copied")
	if got[0][0] != int64(2) {
		t.Fatalf("ordinary table after recovery: %v", got)
	}
	if err := reopened.DropTable("COPIED"); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DropTable("copied"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("drop missing ordinary table: %v", err)
	}
	if err := reopened.DropTable("cpu"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("drop managed table: %v", err)
	}
}

// TestExplainStats checks Explain, Stats and Version.
func TestExplainStats(t *testing.T) {
	db, _ := open(t, nil)

	err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}})
	if err != nil {
		t.Fatal(err)
	}

	plan, err := db.Explain("SELECT count(*) FROM m")
	if err != nil || plan == "" {
		t.Fatalf("Explain: %q, %v", plan, err)
	}

	if s := db.Stats(); s.VisibleSeq != 1 {
		t.Fatalf("visible seq %d, want 1", s.VisibleSeq)
	}

	if Version() == "" {
		t.Fatal("empty version")
	}
}

// TestManyRows reads a result over several chunks.
func TestManyRows(t *testing.T) {
	db, _ := open(t, nil)

	points := make([]Point, 5000)
	for i := range points {
		points[i] = Point{Time: t0.Add(time.Duration(i) * time.Millisecond),
			Fields: []Field{{"v", i}}}
	}

	if err := db.Write("m", points...); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT v FROM m ORDER BY v")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		if rows.Int64(0) != int64(n) {
			t.Fatalf("row %d is %d", n, rows.Int64(0))
		}
		n++
	}

	if rows.Err() != nil || n != len(points) {
		t.Fatalf("read %d rows, %v", n, rows.Err())
	}
}

// TestContext stops a query whose context is cancelled.
func TestContext(t *testing.T) {
	db, _ := open(t, nil)

	if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	rows, err := db.QueryContext(ctx, "SELECT v FROM m")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cancel()

	for rows.Next() {
	}

	if !errors.Is(rows.Err(), context.Canceled) {
		t.Fatalf("err %v, want context.Canceled", rows.Err())
	}

	// A context that is already done refuses the query.
	if _, err := db.QueryContext(ctx, "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("done context: %v", err)
	}
}

// TestCloseWithRows closes a database while a result is still open.
func TestCloseWithRows(t *testing.T) {
	db, _ := open(t, nil)

	if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT v FROM m")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if rows.Next() || rows.Err() == nil {
		t.Fatal("rows still readable after the database closed")
	}

	rows.Close()
}

// TestConcurrent writes and queries from several goroutines at once.
func TestConcurrent(t *testing.T) {
	db, _ := open(t, nil)

	const writers = 4
	const batches = 50

	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func(w int) {
			defer wg.Done()

			for b := 0; b < batches; b++ {
				err := db.Write("m", Point{Time: t0.Add(time.Duration(b) * time.Second),
					Tags: []Tag{{"w", fmt.Sprint(w)}}, Fields: []Field{{"v", 1}}})
				if err != nil {
					t.Error(err)

					return
				}

				// Errors on this goroutine must not mix with the others'.
				if _, err := db.Query("SELECT nope FROM m"); err == nil ||
					!strings.Contains(err.Error(), "nope") {
					t.Errorf("query error %v", err)

					return
				}
			}
		}(w)
	}

	wg.Wait()

	if got := queryAll(t, db, "SELECT count(*) FROM m"); fmt.Sprint(got) !=
		fmt.Sprint([][]any{{int64(writers * batches)}}) {
		t.Fatalf("count %v", got)
	}
}

func TestBackgroundError(t *testing.T) {
	db, _ := open(t, nil)

	message, err := db.BackgroundError()
	if err != nil {
		t.Fatal(err)
	}
	if message != "" {
		t.Fatalf("message = %q, want none", message)
	}
}
