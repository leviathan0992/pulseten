// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// queryInt runs a query with arguments that must return one BIGINT.
func queryInt(t *testing.T, db *DB, sql string, args ...any) int64 {
	t.Helper()

	rows, err := db.Query(sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}

	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("Query(%q) returned no row: %v", sql, rows.Err())
	}

	return rows.Int64(0)
}

// TestQueryParams passes every type of argument to queries over rows in memory
// and in partitions.
func TestQueryParams(t *testing.T) {
	db, _ := openTestDB(t, nil)

	hosts := []string{"a", "O'Brien", "' OR 1=1 --"}
	for i, host := range hosts {
		err := db.Write("cpu", Point{
			Time:   t0.AddDate(0, 0, i),
			Tags:   []Tag{{"host", host}},
			Fields: []Field{{"n", i}, {"v", float64(i) + 0.5}, {"up", i > 0}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, stored := range []bool{false, true} {
		if stored {
			waitUntilFlushed(t, db)
		}

		for _, host := range hosts {
			if got := queryInt(t, db, "SELECT count(*) FROM cpu WHERE host = $1", host); got != 1 {
				t.Fatalf("host %q: %d rows", host, got)
			}
		}

		for _, c := range []struct {
			sql  string
			args []any
			want int64
		}{
			{"SELECT count(*) FROM cpu WHERE time >= $1", []any{t0.AddDate(0, 0, 1)}, 2},
			{"SELECT count(*) FROM cpu WHERE time = $1 OR time > $1", []any{t0.AddDate(0, 0, 1)}, 2},
			{"SELECT count(*) FROM cpu WHERE time < ? AND n >= ?", []any{t0.AddDate(0, 0, 2), uint8(1)}, 1},
			{"SELECT count(*) FROM cpu WHERE v > $1", []any{1.0}, 2},
			{"SELECT count(*) FROM cpu WHERE v < $1", []any{math.Inf(1)}, 3},
			{"SELECT count(*) FROM cpu WHERE up = $1", []any{true}, 2},
			{"SELECT count(*) FROM cpu WHERE n = $1", []any{nil}, 0},
			{"SELECT count(*) FROM cpu WHERE host IN ($2, $1)", []any{"a", "none"}, 1},
			{"SELECT count(*) FROM (SELECT * FROM cpu LIMIT $1)", []any{int32(2)}, 2},
		} {
			if got := queryInt(t, db, c.sql, c.args...); got != c.want {
				t.Errorf("stored=%v %s %v: got %d, want %d", stored, c.sql, c.args, got, c.want)
			}
		}
	}

	// Values come back unchanged; times keep microseconds.
	at := t0.Add(1234567 * time.Nanosecond)
	rows, err := db.Query("SELECT $1, $2, $3, $4", "", 0.1, at, "a\x00b")
	if err != nil {
		t.Fatal(err)
	}

	defer rows.Close()

	if !rows.Next() {
		t.Fatal(rows.Err())
	}

	if rows.String(0) != "" || rows.Float64(1) != 0.1 ||
		!rows.Time(2).Equal(at.Truncate(time.Microsecond)) {
		t.Fatalf("values changed: %q %v %v", rows.String(0), rows.Float64(1), rows.Time(2))
	}

	// EXPLAIN plans an argument like the literal it stands for.
	plan, err := db.Explain("SELECT * FROM cpu WHERE host = $1", "O'Brien")
	if err != nil {
		t.Fatal(err)
	}

	literal, err := db.Explain("SELECT * FROM cpu WHERE host = 'O''Brien'")
	if err != nil || plan != literal {
		t.Fatalf("Explain: %v\n%s\n%s", err, plan, literal)
	}
}

// TestParamErrors rejects arguments that do not match the parameters, and
// values that cannot be passed.
func TestParamErrors(t *testing.T) {
	db, _ := openTestDB(t, nil)

	for _, c := range []struct {
		sql  string
		args []any
		want error
	}{
		{"SELECT $1", nil, ErrInvalidArgument},
		{"SELECT 1", []any{1}, ErrInvalidArgument},
		{"SELECT $1, $2", []any{1}, ErrInvalidArgument},
		{"SELECT $name", []any{1}, ErrInvalidArgument},
		{"SELECT $1", []any{struct{}{}}, ErrInvalidArgument},
		{"SELECT $1", []any{[]byte("x")}, ErrInvalidArgument},
		{"SELECT $1", []any{time.Second}, ErrInvalidArgument},
		{"SELECT $1", []any{uint64(math.MaxInt64) + 1}, ErrInvalidArgument},
		{"SELECT $1", []any{"\xff"}, ErrInvalidArgument},
		{"SELECT $1", []any{strings.Repeat("x", MaxStringBytes+1)}, ErrLimitExceeded},
		{"SELECT $1", []any{time.UnixMicro(math.MinInt64)}, ErrInvalidArgument},
		{"SELECT $1", []any{time.UnixMicro(math.MaxInt64)}, ErrInvalidArgument},
		{"SELECT $1", []any{time.Unix(math.MaxInt64, 0)}, ErrInvalidArgument},
	} {
		rows, err := db.Query(c.sql, c.args...)
		if rows != nil || !errors.Is(err, c.want) {
			t.Errorf("Query(%q, %.20v): %v", c.sql, c.args, err)
		}

		if _, err := db.Explain(c.sql, c.args...); !errors.Is(err, c.want) {
			t.Errorf("Explain(%q, %.20v): %v", c.sql, c.args, err)
		}
	}

	// The limits themselves are accepted.
	for _, at := range []time.Time{minParamTime, maxParamTime.Add(-time.Nanosecond), {}} {
		if got := queryInt(t, db, "SELECT count($1::VARCHAR)", at); got != 1 {
			t.Errorf("time %v: %d", at, got)
		}
	}

	if got := queryInt(t, db, "SELECT length($1)", strings.Repeat("x", MaxStringBytes)); got != MaxStringBytes {
		t.Errorf("longest string: %d", got)
	}
}

// TestExecParams writes to a SQL table with arguments.
func TestExecParams(t *testing.T) {
	db, _ := openTestDB(t, nil)

	if err := db.Exec("CREATE TABLE s (id BIGINT, name VARCHAR, ts TIMESTAMP)"); err != nil {
		t.Fatal(err)
	}

	name := "O'Brien'); DROP TABLE s; --"
	if err := db.Exec("INSERT INTO s (name, ts, id) VALUES ($2, $3, $1)", 1, name, t0); err != nil {
		t.Fatal(err)
	}

	if err := db.ExecContext(context.Background(), "INSERT INTO s VALUES (?, ?, NULL)", "2", nil); err != nil {
		t.Fatal(err)
	}

	if got := queryInt(t, db, "SELECT count(*) FROM s WHERE id = $1 AND name = $2 AND ts = $3", 1, name, t0); got != 1 {
		t.Fatalf("inserted rows: %d", got)
	}

	if got := queryInt(t, db, "SELECT id FROM s WHERE name IS NULL"); got != 2 {
		t.Fatalf("converted id: %d", got)
	}

	for _, sql := range []string{
		"INSERT INTO s (id) VALUES ($1); INSERT INTO s (id) VALUES (3)",
		"CREATE TABLE t AS SELECT $1",
		"INSERT INTO s (id) VALUES ($1), ($2)",
	} {
		if err := db.Exec(sql, 3); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Exec(%q): %v", sql, err)
		}
	}

	if got := queryInt(t, db, "SELECT count(*) FROM s"); got != 2 {
		t.Fatalf("a rejected statement ran: %d rows", got)
	}
}
