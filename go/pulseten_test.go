// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// t0 is a fixed time for the points of the tests.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// openTestDB opens a database in a new directory, closed when the test ends.
func openTestDB(t *testing.T, opts *Options) (*DB, string) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(func() { db.Close() })

	return db, dir
}

func TestOpenWithoutCreation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	options := DefaultOptions()
	if !options.CreateIfMissing {
		t.Fatal("creation must be enabled by default")
	}

	options.CreateIfMissing = false
	db, err := Open(dir, &options)
	if db != nil {
		db.Close()
	}
	if db != nil || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open without creation: %v %v", db, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Open without creation left a directory: %v", err)
	}

	db, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db, err = Open(dir, &options)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
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

			db, dir := openTestDB(t, &opts)
			if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
				t.Fatal(err)
			}

			db.Close()
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
	db, _ := openTestDB(t, nil)

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

	db, dir := openTestDB(t, &opts)

	for i := 0; i < 3; i++ {
		if err := db.Write("m", Point{Time: t0.Add(time.Duration(i) * time.Second),
			Fields: []Field{{"v", i}}}); err != nil {
			t.Fatal(err)
		}

		if i == 1 {
			waitUntilFlushed(t, db)
		}
	}

	db.Close()

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
	db, dir := openTestDB(t, nil)

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

	if _, err := db.Query("DROP TABLE missing"); !errors.Is(err, ErrInvalidArgument) {
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

	db, dir := openTestDB(t, &opts)

	if tables, err := db.Tables(); err != nil || len(tables) != 0 {
		t.Fatalf("empty catalog: tables %v, err %v", tables, err)
	}

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

	want := "[{a true 24 <nil> [{time 0 4} {host 1 5} {v 2 3}]} {b true 24 <nil> [{time 0 4} {host 1 5} {v 2 3}]}]"
	if fmt.Sprint(tables) != want {
		t.Fatalf("tables %v, want %v", tables, want)
	}

	before := db.Stats()
	if _, err := db.Query("DROP TABLE A"); !errors.Is(err, ErrInvalidArgument) {
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

	db.Close()
	if fmt.Sprint(tables) != want {
		t.Fatalf("metadata after close: %v, want %v", tables, want)
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

// TestQueryReadOnly verifies rejected statements leave data and schemas intact.
func TestQueryReadOnly(t *testing.T) {
	db, _ := openTestDB(t, nil)
	if err := db.Write("cpu", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}

	for _, sql := range []string{
		"CREATE TABLE copied AS SELECT * FROM cpu",
		"DROP TABLE cpu", "DELETE FROM cpu", "UPDATE cpu SET v = 2",
		"ALTER TABLE cpu ADD COLUMN extra BIGINT", "SELECT 1; DROP TABLE cpu",
	} {
		rows, err := db.Query(sql)
		if rows != nil {
			rows.Close()
			t.Errorf("%s returned rows", sql)
		}

		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", sql, err)
		}
	}

	if got := queryAll(t, db, "SELECT v FROM cpu"); len(got) != 1 || got[0][0] != int64(1) {
		t.Fatalf("rejected statements changed rows: %v", got)
	}
}

// TestExec joins ordinary SQL tables with unflushed data and preserves SQL
// state.
func TestExec(t *testing.T) {
	opts := DefaultOptions()
	opts.FlushInterval = time.Hour
	db, dir := openTestDB(t, &opts)
	for _, sql := range []string{
		"CREATE SCHEMA app",
		"CREATE TABLE app.devices (host VARCHAR, label VARCHAR)",
		"INSERT INTO app.devices VALUES ('a', 'first')",
		"UPDATE app.devices SET label = 'lab'",
		"BEGIN; DELETE FROM app.devices; ROLLBACK",
	} {
		if err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q): %v", sql, err)
		}
	}

	if err := db.Write("cpu", Point{Time: t0, Tags: []Tag{{"host", "a"}}, Fields: []Field{{"v", 7}}}); err != nil {
		t.Fatal(err)
	}
	join := "SELECT d.label, c.v FROM cpu c JOIN app.devices d ON c.host = d.host"
	if got := queryAll(t, db, join); len(got) != 1 || got[0][0] != "lab" || got[0][1] != int64(7) {
		t.Fatalf("mixed query: %v", got)
	}
	if stats := db.Stats(); stats.VisibleSeq != 1 || stats.FlushedSeq != 0 {
		t.Fatalf("SQL changed time-series progress: %+v", stats)
	}

	if err := db.Exec("CREATE TABLE taken (v BIGINT)"); err != nil {
		t.Fatal(err)
	}
	p := Point{Time: t0, Fields: []Field{{"v", 1}}}
	if err := db.Write("taken", p); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ordinary table accepted time-series write: %v", err)
	}
	if err := db.Exec("DROP TABLE taken"); err != nil {
		t.Fatal(err)
	}
	if err := db.Write("taken", p); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("SELECT 1\x00; DROP TABLE app.devices"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("embedded NUL: %v", err)
	}
	if err := db.Exec("INSERT INTO missing_exec_table VALUES (1)"); err == nil || !strings.Contains(err.Error(), "missing_exec_table") {
		t.Fatalf("missing SQL diagnostic: %v", err)
	}

	db.Close()
	if err := db.Exec("SELECT 1"); !errors.Is(err, errClosed) {
		t.Fatalf("closed Exec: %v", err)
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := queryAll(t, db, join); len(got) != 1 || got[0][0] != "lab" || got[0][1] != int64(7) {
		t.Fatalf("reopened mixed query: %v", got)
	}
}

// TestExecContext separates native DML concurrency from namespace lock waits.
func TestExecContext(t *testing.T) {
	const slowSelect = "SELECT sum(sqrt(i)) FROM range(1000000000000) r(i)"
	const slowInsert = "INSERT INTO jobs " + slowSelect
	for _, test := range []struct {
		m_name            string
		m_sql             string
		m_locks_namespace bool
	}{
		{"query", slowSelect, false},
		{"insert", slowInsert, false},
		{"set", "SET threads = 4; " + slowInsert, false},
		{"reset", "RESET threads; " + slowInsert, false},
		{"use", "USE main; " + slowInsert, false},
		{"search_path", "SET search_path = 'main'; " + slowInsert, false},
		{"variable", "SET VARIABLE batch_size = 4; " + slowInsert, false},
		{"setting_before_ddl", "SET threads = 4; " + slowInsert + "; CREATE TABLE ddl (v BIGINT)", true},
		{"prepared_insert", "PREPARE work AS " + slowInsert + "; EXECUTE work", false},
		{"explained_insert", "EXPLAIN ANALYZE " + slowInsert, false},
		{"transaction", "BEGIN; " + slowInsert + "; COMMIT", false},
		{"ddl_transaction", "BEGIN; CREATE TABLE ddl (v BIGINT); " + slowInsert + "; COMMIT", true},
		{"later_ddl", slowInsert + "; CREATE TABLE ddl (v BIGINT)", true},
		{"explained_ddl", "EXPLAIN ANALYZE CREATE TABLE ddl AS " + slowSelect, true},
	} {
		t.Run(test.m_name, func(t *testing.T) {
			options := DefaultOptions()
			options.WriteLockTimeout = 100 * time.Millisecond
			db, _ := openTestDB(t, &options)
			if err := db.Exec("CREATE TABLE jobs (v DOUBLE)"); err != nil {
				t.Fatal(err)
			}
			p := Point{Time: t0, Fields: []Field{{"v", 1}}}
			if err := db.Write("metrics", p); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			finished := make(chan struct{})
			var executionErr error
			go func() {
				executionErr = db.ExecContext(ctx, "INSERT INTO jobs VALUES (1); "+test.m_sql)
				close(finished)
			}()
			// Join before the database cleanup, including after an assertion fails.
			defer func() {
				cancel()
				<-finished
			}()

			deadline := time.Now().Add(3 * time.Second)
			for queryAll(t, db, "SELECT count(*) FROM jobs")[0][0] != int64(1) {
				if time.Now().After(deadline) {
					t.Fatal("first SQL statement did not commit")
				}
				time.Sleep(time.Millisecond)
			}
			if err := db.Write("metrics", p); err != nil {
				t.Fatalf("existing time-series write blocked by SQL: %v", err)
			}
			writeErr := db.Write("fresh", p)
			if test.m_locks_namespace {
				if !errors.Is(writeErr, ErrLockTimeout) {
					t.Fatalf("first time-series write must wait for DDL: %v", writeErr)
				}
			} else if writeErr != nil {
				t.Fatalf("first time-series write blocked by DML: %v", writeErr)
			}

			insertCtx, stopInsert := context.WithTimeout(context.Background(), time.Second)
			defer stopInsert()
			if err := db.ExecContext(insertCtx, "INSERT INTO jobs VALUES (2)"); err != nil {
				t.Fatalf("independent DML blocked by SQL: %v", err)
			}
			if err := db.ExecContext(insertCtx, "SELECT count(*) FROM jobs"); err != nil {
				t.Fatalf("ordinary query blocked by SQL: %v", err)
			}
			waitCtx, stopWait := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer stopWait()
			waitErr := db.ExecContext(waitCtx, "CREATE TABLE waiter (v BIGINT)")
			if test.m_locks_namespace {
				if !errors.Is(waitErr, context.DeadlineExceeded) {
					t.Fatalf("namespace lock cancellation: %v", waitErr)
				}
			} else if waitErr != nil {
				t.Fatalf("DDL blocked by ordinary DML: %v", waitErr)
			}

			cancel()
			select {
			case <-finished:
				if !errors.Is(executionErr, context.Canceled) {
					t.Fatalf("SQL execution cancellation: %v", executionErr)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("SQL cancellation did not finish")
			}
			if got := queryAll(t, db, "SELECT count(*) FROM jobs")[0][0]; got != int64(2) {
				t.Fatalf("cancellation changed committed rows: %v", got)
			}
			if err := db.ExecContext(ctx, "DROP TABLE jobs"); !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-cancelled SQL: %v", err)
			}
			// Cancellation must roll back DDL before releasing the namespace lock.
			if test.m_locks_namespace {
				if err := db.Write("ddl", p); err != nil {
					t.Fatalf("unfinished ordinary table survived cancellation: %v", err)
				}
			}
			if err := db.Exec("INSERT INTO jobs VALUES (3)"); err != nil {
				t.Fatalf("execution after cancellation: %v", err)
			}
		})
	}
}

// TestConcurrentExec checks independent native connections and Go error copies.
func TestConcurrentExec(t *testing.T) {
	db, _ := openTestDB(t, nil)
	if err := db.Exec("CREATE TABLE devices (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			if err := db.Exec(fmt.Sprintf("INSERT INTO devices VALUES (%d)", id)); err != nil {
				t.Error(err)
			}
			name := fmt.Sprintf("missing_exec_%d", id)
			if err := db.Exec("INSERT INTO " + name + " VALUES (1)"); err == nil || !strings.Contains(err.Error(), name) {
				t.Errorf("diagnostic for %s: %v", name, err)
			}
		}(i)
	}
	workers.Wait()
	if got := queryAll(t, db, "SELECT count(*) FROM devices"); got[0][0] != int64(4) {
		t.Fatalf("concurrent inserts: %v", got)
	}
}

// TestExplainStats checks query plans, visible write counts and version
// reporting.
func TestExplainStats(t *testing.T) {
	db, _ := openTestDB(t, nil)

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

// TestQueryAcrossChunks reads a result over several chunks.
func TestQueryAcrossChunks(t *testing.T) {
	db, _ := openTestDB(t, nil)

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

// TestQueryContextCancellation stops a query whose context is cancelled.
func TestQueryContextCancellation(t *testing.T) {
	db, _ := openTestDB(t, nil)

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
	db, _ := openTestDB(t, nil)

	if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT v FROM m")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	if rows.Next() || rows.Err() == nil {
		t.Fatal("rows still readable after the database closed")
	}

	rows.Close()
}

// TestCloseDuringExec closes a database while ExecContext is running: Close
// waits for the call instead of freeing its handle, and later calls fail.
func TestCloseDuringExec(t *testing.T) {
	db, _ := openTestDB(t, nil)
	if err := db.Exec("CREATE TABLE jobs (v BIGINT)"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	executed := make(chan error, 1)
	go func() {
		executed <- db.ExecContext(ctx, "INSERT INTO jobs VALUES (1); "+
			"SELECT sum(sqrt(i)) FROM range(1000000000000) r(i)")
	}()
	closed := make(chan struct{})
	// Join both goroutines before the database cleanup, even after a failure.
	defer func() {
		cancel()
		<-executed
		<-closed
	}()

	deadline := time.Now().Add(3 * time.Second)
	for queryAll(t, db, "SELECT count(*) FROM jobs")[0][0] != int64(1) {
		if time.Now().After(deadline) {
			close(closed)
			t.Fatal("first SQL statement did not commit")
		}
		time.Sleep(time.Millisecond)
	}

	go func() {
		db.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while ExecContext was running")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-executed:
		executed <- err
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SQL execution cancellation: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SQL cancellation did not finish")
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish after ExecContext returned")
	}

	if err := db.Exec("SELECT 1"); !errors.Is(err, errClosed) {
		t.Fatalf("Exec after Close: %v", err)
	}
}

// TestConcurrentWritesAndQueries writes and queries from several goroutines at
// once.
func TestConcurrentWritesAndQueries(t *testing.T) {
	db, _ := openTestDB(t, nil)

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

// TestNoBackgroundErrorOnOpen checks that a fresh database has no background
// failure.
func TestNoBackgroundErrorOnOpen(t *testing.T) {
	db, _ := openTestDB(t, nil)

	message, err := db.BackgroundError()
	if err != nil {
		t.Fatal(err)
	}

	if message != "" {
		t.Fatalf("message = %q, want none", message)
	}
}

// Native errors remain owned after other C calls and later interruption.
func TestFetchErrorRetained(t *testing.T) {
	db, _ := openTestDB(t, nil)
	if err := db.Write("m", Point{Time: t0, Fields: []Field{{"v", 1}}}); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT CAST('bad-' || CAST(v AS VARCHAR) AS BIGINT) FROM m")
	if err != nil {
		t.Fatal(err)
	}

	defer rows.Close()
	if rows.Next() || rows.Err() == nil {
		t.Fatal("expected execution error")
	}

	cause := rows.Err().Error()
	_ = db.Stats()
	rows.Interrupt()
	if rows.Next() || rows.Err() == nil || rows.Err().Error() != cause {
		t.Fatalf("terminal error changed: %v", rows.Err())
	}
}

// A budget belongs to one open database, not the Go options or each connection.
func TestDuckDBMemoryLimit(t *testing.T) {
	if DefaultOptions().DuckDBMemoryLimitBytes != 0 {
		t.Fatal("default must be automatic")
	}

	opts := DefaultOptions()
	opts.DuckDBMemoryLimitBytes = 128 << 20
	first, _ := openTestDB(t, &opts)
	opts.DuckDBMemoryLimitBytes = 3 << 30
	second, _ := openTestDB(t, &opts)
	if first.Stats().DuckDBMemoryLimitBytes != 128<<20 || second.Stats().DuckDBMemoryLimitBytes != 3<<30 {
		t.Fatal("budgets were capped or shared between instances")
	}

	for i := 0; i < 6; i++ {
		rows, err := first.Query("SELECT current_setting('memory_limit')")
		if err != nil {
			t.Fatal(err)
		}

		if !rows.Next() {
			t.Fatal(rows.Err())
		}

		value := rows.Value(0)
		if value != "128.0 MiB" {
			t.Fatalf("limit: %v, %v", value, err)
		}

		defer rows.Close()
	}

	opts.DuckDBMemoryLimitBytes = ^uint64(0)
	if db, err := Open(filepath.Join(t.TempDir(), "invalid"), &opts); !errors.Is(err, ErrInvalidArgument) {
		if db != nil {
			db.Close()
		}

		t.Fatalf("invalid limit: %v", err)
	}
}

// Thread settings are per open, while stats retain the initial effective value.
func TestDuckDBThreads(t *testing.T) {
	if DefaultOptions().DuckDBThreads != 0 {
		t.Fatal("default must use DuckDB's platform default")
	}

	readThreads := func(db *DB) int64 {
		t.Helper()
		rows, err := db.Query("SELECT current_setting('threads')")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("thread setting: %v", rows.Err())
		}
		return rows.Int64(0)
	}

	automatic, _ := openTestDB(t, nil)
	if threads := automatic.Stats().DuckDBThreads; threads == 0 || int64(threads) != readThreads(automatic) {
		t.Fatal("automatic thread setting was not resolved")
	}
	automatic.Close()

	opts := DefaultOptions()
	opts.DuckDBThreads = 1
	first, dir := openTestDB(t, &opts)
	opts.DuckDBThreads = 2
	second, _ := openTestDB(t, &opts)
	if readThreads(first) != 1 || first.Stats().DuckDBThreads != 1 ||
		readThreads(second) != 2 || second.Stats().DuckDBThreads != 2 {
		t.Fatal("thread settings were ignored or shared between instances")
	}
	if err := first.Exec("SET threads = 2"); err != nil {
		t.Fatal(err)
	}
	if readThreads(first) != 2 || first.Stats().DuckDBThreads != 1 {
		t.Fatal("stats did not retain the initial thread setting")
	}
	first.Close()
	if first.Stats().DuckDBThreads != 0 {
		t.Fatal("closed database has nonzero stats")
	}
	reopened, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if readThreads(reopened) != 2 || reopened.Stats().DuckDBThreads != 2 {
		t.Fatal("reopen did not apply the new thread setting")
	}

	opts.DuckDBThreads = ^uint32(0)
	path := filepath.Join(t.TempDir(), "invalid")
	if db, err := Open(path, &opts); !errors.Is(err, ErrInvalidArgument) {
		if db != nil {
			db.Close()
		}
		t.Fatalf("invalid thread count: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid threads touched the database directory: %v", err)
	}
}
