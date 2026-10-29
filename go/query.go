// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

package pulseten

/*
#include <stdlib.h>
#include "shim.h"
*/
import "C"

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Type is the type of a column.
type Type int

// The column types.
const (
	// TypeBoolean is true or false.
	TypeBoolean Type = C.PULSETEN_TYPE_BOOLEAN

	// TypeBigint is a signed 64-bit integer.
	TypeBigint Type = C.PULSETEN_TYPE_BIGINT

	// TypeDouble is an IEEE 754 double.
	TypeDouble Type = C.PULSETEN_TYPE_DOUBLE

	// TypeTimestamp is a time in microseconds since 1970-01-01 UTC.
	TypeTimestamp Type = C.PULSETEN_TYPE_TIMESTAMP

	// TypeVarchar is a UTF-8 string.
	TypeVarchar Type = C.PULSETEN_TYPE_VARCHAR
)

// Role is the role of a table column.
type Role int

// The column roles.
const (
	// RoleTime is column 0, "time".
	RoleTime Role = C.PULSETEN_ROLE_TIME

	// RoleTag is a VARCHAR tag.
	RoleTag Role = C.PULSETEN_ROLE_TAG

	// RoleField is a BOOLEAN, BIGINT, DOUBLE or VARCHAR field.
	RoleField Role = C.PULSETEN_ROLE_FIELD
)

// Column is a column of a table.
type Column struct {
	// Name is the lower-case name.
	Name string

	// Role is time, tag or field.
	Role Role

	// Type is the value type.
	Type Type
}

// Table is a table and its newest columns.
type Table struct {
	// Name is the lower-case name.
	Name string

	// Columns are in the order they were added, "time" first.
	Columns []Column
}

// Tables returns every table and its newest columns, whether its writes are
// flushed or not, sorted by name.
func (db *DB) Tables() ([]Table, error) {
	h, release, err := db.handle()
	if err != nil {
		return nil, err
	}
	defer release()

	var catalog *C.pulseten_catalog_t
	var msg *C.char

	if err := toError(C.pt_go_catalog_get(h, &catalog, &msg), msg); err != nil {
		return nil, err
	}
	defer C.pulseten_catalog_destroy(catalog)

	tables := make([]Table, C.pulseten_catalog_table_count(catalog))

	for t := range tables {
		ct := C.uint32_t(t)
		tables[t].Name = C.GoString(C.pulseten_catalog_table_name(catalog, ct))
		tables[t].Columns =
			make([]Column, C.pulseten_catalog_column_count(catalog, ct))

		for c := range tables[t].Columns {
			col := C.pulseten_catalog_column(catalog, ct, C.uint32_t(c))
			tables[t].Columns[c] = Column{
				Name: C.GoString(col.name),
				Role: Role(col.role),
				Type: Type(col._type),
			}
		}
	}

	return tables, nil
}

// DropTable drops a table: its stored and unflushed rows and its columns.
// Its name may then be written again. Writes to every table wait while it
// runs; queries started before it still read the table.
func (db *DB) DropTable(table string) error {
	ctable, err := checkedCString(table, "table name")
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(ctable))

	h, release, err := db.handle()
	if err != nil {
		return err
	}
	defer release()

	var msg *C.char

	return toError(C.pt_go_drop_table(h, ctable, &msg), msg)
}

// Explain returns DuckDB's plan for a SELECT, as Query would run it.
func (db *DB) Explain(sql string) (string, error) {
	csql, err := checkedCString(sql, "SQL statement")
	if err != nil {
		return "", err
	}
	defer C.free(unsafe.Pointer(csql))

	h, release, err := db.handle()
	if err != nil {
		return "", err
	}
	defer release()

	var plan *C.char
	var msg *C.char

	if err := toError(C.pt_go_explain(h, csql, &plan, &msg), msg); err != nil {
		return "", err
	}
	defer C.pulseten_free(unsafe.Pointer(plan))

	return C.GoString(plan), nil
}

// ResultColumn is a column of a query result.
type ResultColumn struct {
	// Name is the column name.
	Name string

	// Type is the type its values are read as.
	Type Type
}

// Rows is the result of a query, read row by row with Next. Until it is
// closed it holds the query's snapshot, so close it promptly.
type Rows struct {
	// db is the database the query reads.
	db *DB

	// mu keeps Interrupt from running while the result is destroyed.
	mu sync.Mutex

	// result is the C result; nil once destroyed.
	result *C.pulseten_result_t

	// columns are the result columns.
	columns []ResultColumn

	// n is the number of rows in the current chunk.
	n C.uint32_t

	// row is the current row of the chunk.
	row C.uint32_t

	// err is the error that ended the rows, if any.
	err error

	// done is set by Next when a fetch reports normal end.
	done bool

	// interrupted lets Next observe Interrupt before serving a cached row.
	interrupted atomic.Bool

	// ctx is checked by Next before returning a row.
	ctx context.Context

	// stop ends the goroutine that watches ctx; nil without one.
	stop chan struct{}
}

// Query starts a SELECT over one consistent snapshot of every write so far.
// The statement is checked and planned here; errors found while it runs are
// returned by Rows.Err.
func (db *DB) Query(sql string) (*Rows, error) {
	return db.QueryContext(context.Background(), sql)
}

// QueryContext is Query, interrupting an active query when ctx is done.
// Next checks ctx before returning a row, including rows already fetched.
// Cancellation after normal end leaves Err nil.
func (db *DB) QueryContext(ctx context.Context, sql string) (*Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	csql, err := checkedCString(sql, "SQL statement")
	if err != nil {
		return nil, err
	}
	defer C.free(unsafe.Pointer(csql))

	h, release, err := db.handle()
	if err != nil {
		return nil, err
	}
	defer release()

	var result *C.pulseten_result_t
	var msg *C.char

	if err := toError(C.pt_go_query(h, csql, &result, &msg), msg); err != nil {
		return nil, err
	}

	r := &Rows{db: db, result: result, ctx: ctx}

	r.columns = make([]ResultColumn, C.pulseten_column_count(result))
	for c := range r.columns {
		r.columns[c] = ResultColumn{
			Name: C.GoString(C.pulseten_column_name(result, C.uint32_t(c))),
			Type: Type(C.pulseten_column_type(result, C.uint32_t(c))),
		}
	}

	db.rowsMu.Lock()
	db.rows[r] = struct{}{}
	db.rowsMu.Unlock()

	// Interrupt the query once ctx is done, until the rows are closed.
	if ctx.Done() != nil {
		r.stop = make(chan struct{})

		go func() {
			select {
			case <-ctx.Done():
				r.Interrupt()
			case <-r.stop:
			}
		}()
	}

	return r, nil
}

// Columns returns the result columns.
func (r *Rows) Columns() []ResultColumn {
	return r.columns
}

// Next moves to the next row, fetching the next chunk when needed. It
// checks cancellation and interruption before returning a cached row, too.
// It returns false after the last row or on an error; see Err. Once normal
// end is reported, later cancellation or interruption leaves Err nil.
func (r *Rows) Next() bool {
	if r.err != nil || r.done {
		return false
	}

	if r.result == nil {
		r.err = errRowsClosed

		return false
	}

	if err := r.stopError(); err != nil {
		r.err = err
		r.n = 0

		return false
	}

	if r.row+1 < r.n {
		r.row++

		return true
	}

	// Fetch under the database's shared lock, so that DB.Close waits.
	_, release, err := r.db.handle()
	if err != nil {
		r.err = err

		return false
	}

	var n C.uint32_t
	var msg *C.char

	err = toError(C.pt_go_fetch(r.result, &n, &msg), msg)
	release()

	if err != nil {
		if ctxErr := r.ctx.Err(); ctxErr != nil && errors.Is(err, ErrInterrupted) {
			err = ctxErr
		}

		r.err = err
		r.n = 0

		return false
	}

	// The request may have arrived while the native fetch was running.
	if err := r.stopError(); err != nil {
		r.err = err
		r.n = 0

		return false
	}

	r.n = n
	r.row = 0
	r.done = n == 0

	return n > 0
}

// stopError reports a request to stop an active result. Next owns err and
// done; Interrupt only sets the atomic request and interrupts the C query.
func (r *Rows) stopError() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if r.interrupted.Load() {
		return ErrInterrupted
	}

	return nil
}

// errRowsClosed is returned by Next after Close.
var errRowsClosed = errors.New("pulseten: rows are closed")

// Err returns the error that ended the rows, or nil after the last row.
func (r *Rows) Err() error {
	return r.err
}

// Interrupt requests that an active query stop from another goroutine.
// Next observes the request before returning a cached row or after a running
// fetch, then returns false and Err matches ErrInterrupted. With QueryContext,
// a done context makes Err its error instead. After Next has reported normal
// end (false with Err nil), interruption leaves later calls at normal end.
func (r *Rows) Interrupt() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.result != nil {
		r.interrupted.Store(true)
		C.pulseten_interrupt(r.result)
	}
}

// destroy frees the C result; it may be called more than once.
func (r *Rows) destroy() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.result == nil {
		return
	}

	C.pulseten_result_destroy(r.result)
	r.result = nil
	r.n = 0

	if r.stop != nil {
		close(r.stop)
	}
}

// Close stops the query and frees the result. Closing twice does nothing.
func (r *Rows) Close() error {
	r.db.rowsMu.Lock()
	delete(r.db.rows, r)
	r.db.rowsMu.Unlock()

	r.destroy()

	return nil
}

// IsNull tells whether a value of the current row is NULL.
func (r *Rows) IsNull(col int) bool {
	return C.pulseten_is_null(r.result, C.uint32_t(col), r.row) != 0
}

// Bool reads a BOOLEAN value of the current row; false if NULL.
func (r *Rows) Bool(col int) bool {
	return C.pulseten_get_bool(r.result, C.uint32_t(col), r.row) != 0
}

// Int64 reads a BIGINT value, or a TIMESTAMP in microseconds, of the
// current row; 0 if NULL.
func (r *Rows) Int64(col int) int64 {
	return int64(C.pulseten_get_int64(r.result, C.uint32_t(col), r.row))
}

// Float64 reads a DOUBLE value of the current row; 0 if NULL.
func (r *Rows) Float64(col int) float64 {
	return float64(C.pulseten_get_double(r.result, C.uint32_t(col), r.row))
}

// String reads a VARCHAR value of the current row; "" if NULL.
func (r *Rows) String(col int) string {
	var n C.size_t
	s := C.pulseten_get_varchar(r.result, C.uint32_t(col), r.row, &n)

	if s == nil {
		return ""
	}

	return C.GoStringN(s, C.int(n))
}

// Time reads a TIMESTAMP value of the current row, in UTC; the zero Time if
// NULL.
func (r *Rows) Time(col int) time.Time {
	if r.IsNull(col) {
		return time.Time{}
	}

	return time.UnixMicro(r.Int64(col)).UTC()
}

// Value reads a value of the current row as a bool, int64, float64, string
// or time.Time by its column type; nil if NULL.
func (r *Rows) Value(col int) any {
	if col < 0 || col >= len(r.columns) || r.IsNull(col) {
		return nil
	}

	switch r.columns[col].Type {
	case TypeBoolean:
		return r.Bool(col)
	case TypeBigint:
		return r.Int64(col)
	case TypeDouble:
		return r.Float64(col)
	case TypeTimestamp:
		return r.Time(col)
	case TypeVarchar:
		return r.String(col)
	}

	return nil
}
