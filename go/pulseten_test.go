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

// TestWriteQuery writes every field type and reads it back, NULLs
// included.
func TestWriteQuery(t *testing.T) {
	db, _ := open(t, nil)

	seq, err := db.Write("cpu",
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
	if err != nil || seq == 0 {
		t.Fatalf("Write: seq %d, %v", seq, err)
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
	opts.BackgroundFlush = false

	db, dir := open(t, &opts)

	for i := 0; i < 3; i++ {
		if _, err := db.Write("m", Point{Time: t0.Add(time.Duration(i) * time.Second),
			Fields: []Field{{"v", i}}}); err != nil {
			t.Fatal(err)
		}

		if i == 1 {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
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
	_, err := db.Write("m")
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "empty batch") {
		t.Fatalf("empty batch: %v", err)
	}

	// A value type the C API has no column type for.
	_, err = db.Write("m", Point{Time: t0, Fields: []Field{{"v", []int{1}}}})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unsupported value: %v", err)
	}

	// A column changing type.
	if _, err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}

	_, err = db.Write("m", Point{Time: t0, Fields: []Field{{"v", "x"}}})
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

	if _, err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err == nil {
		t.Fatal("write after close accepted")
	}
}

// TestTables lists tables and drops one.
func TestTables(t *testing.T) {
	db, _ := open(t, nil)

	for _, name := range []string{"b", "a"} {
		if _, err := db.Write(name, Point{Time: t0, Tags: []Tag{{"host", "h"}},
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

	if err := db.DropTable("A"); err != nil {
		t.Fatal(err)
	}

	tables, err = db.Tables()
	if err != nil || len(tables) != 1 || tables[0].Name != "b" {
		t.Fatalf("after drop: %v, %v", tables, err)
	}
}

// TestExplainStats checks Explain, Stats and Version.
func TestExplainStats(t *testing.T) {
	db, _ := open(t, nil)

	seq, err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}})
	if err != nil {
		t.Fatal(err)
	}

	plan, err := db.Explain("SELECT count(*) FROM m")
	if err != nil || plan == "" {
		t.Fatalf("Explain: %q, %v", plan, err)
	}

	if s := db.Stats(); s.VisibleSeq != seq {
		t.Fatalf("visible seq %d, want %d", s.VisibleSeq, seq)
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

	if _, err := db.Write("m", points...); err != nil {
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

	if _, err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
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

	if _, err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
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
				_, err := db.Write("m", Point{Time: t0.Add(time.Duration(b) * time.Second),
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
