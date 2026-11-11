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

// ColumnKind is what a table column is used for.
type ColumnKind int

// The column kinds.
const (
	// ColumnTime is column 0, "time".
	ColumnTime ColumnKind = C.PULSETEN_COLUMN_TIME

	// ColumnTag is a VARCHAR tag.
	ColumnTag ColumnKind = C.PULSETEN_COLUMN_TAG

	// ColumnField is a BOOLEAN, BIGINT, DOUBLE or VARCHAR field.
	ColumnField ColumnKind = C.PULSETEN_COLUMN_FIELD
)

// Column is a column of a table.
type Column struct {
	// Name is the lower-case name.
	Name string

	// Kind is time, tag or field.
	Kind ColumnKind

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

// Tables returns every managed time-series table and its newest columns,
// whether its writes are flushed or not, sorted by name.
func (db *DB) Tables() ([]Table, error) {
	h, release, err := db.handle()
	if err != nil {
		return nil, err
	}
	defer release()

	var catalog *C.pulseten_catalog_t
	var msg *C.char

	if err := toError(C.pt_go_catalog(h, &catalog, &msg), msg); err != nil {
		return nil, err
	}
	defer C.pulseten_catalog_destroy(catalog)

	ctables := unsafe.Slice(catalog.m_tables, int(catalog.m_n_tables))
	tables := make([]Table, len(ctables))

	for t, table := range ctables {
		tables[t].Name = C.GoString(table.m_name)
		columns := unsafe.Slice(table.m_columns, int(table.m_n_columns))
		tables[t].Columns = make([]Column, len(columns))

		for c, col := range columns {
			tables[t].Columns[c] = Column{
				Name: C.GoString(col.m_name),
				Kind: ColumnKind(col.m_kind),
				Type: Type(col.m_type),
			}
		}
	}

	return tables, nil
}

// ExecDDL executes one CREATE TABLE or DROP TABLE for an ordinary table in main.
// CREATE TABLE AS SELECT may read ordinary and time-series tables in one snapshot.
// Managed tables and internal objects are protected. Temporary tables, foreign
// keys and CASCADE are unsupported. A nil error means the change is durable.
func (db *DB) ExecDDL(sql string) error {
	csql, err := checkedCString(sql, "SQL statement")
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(csql))
	h, release, err := db.handle()
	if err != nil {
		return err
	}
	defer release()
	var msg *C.char

	return toError(C.pt_go_exec_ddl(h, csql, &msg), msg)
}

// DropTable drops an ordinary table. It rejects managed time-series tables with
// ErrInvalidArgument without flushing or changing their data or schemas.
// A missing table returns ErrNotFound.
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
	// m_db is the database the query reads.
	m_db *DB

	// m_mu keeps Interrupt from running while the result is destroyed.
	m_mu sync.Mutex

	// m_result is the C result; nil once destroyed.
	m_result *C.pulseten_result_t

	// m_columns are the result columns.
	m_columns []ResultColumn

	// m_n is the number of rows in the current chunk.
	m_n C.uint32_t

	// m_row is the current row of the chunk.
	m_row C.uint32_t

	// m_err is the error that ended the rows, if any.
	m_err error

	// m_done is set by Next when a fetch reports normal end.
	m_done bool

	// m_interrupted lets Next observe Interrupt before serving a cached row.
	m_interrupted atomic.Bool

	// m_ctx is checked by Next before returning a row.
	m_ctx context.Context

	// m_stop ends the goroutine that watches ctx; nil without one.
	m_stop chan struct{}
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

	r := &Rows{m_db: db, m_result: result, m_ctx: ctx}

	r.m_columns = make([]ResultColumn, C.pulseten_result_column_count(result))
	for c := range r.m_columns {
		r.m_columns[c] = ResultColumn{
			Name: C.GoString(C.pulseten_result_column_name(result, C.uint32_t(c))),
			Type: Type(C.pulseten_result_column_type(result, C.uint32_t(c))),
		}
	}

	db.m_rowsMu.Lock()
	db.m_rows[r] = struct{}{}
	db.m_rowsMu.Unlock()

	// Interrupt the query once ctx is done, until the rows are closed.
	if ctx.Done() != nil {
		r.m_stop = make(chan struct{})

		go func() {
			select {
			case <-ctx.Done():
				r.Interrupt()
			case <-r.m_stop:
			}
		}()
	}

	return r, nil
}

// Columns returns the result columns.
func (r *Rows) Columns() []ResultColumn {
	return r.m_columns
}

// Next moves to the next row, fetching the next chunk when needed. It
// checks cancellation and interruption before returning a cached row, too.
// It returns false after the last row or on an error; see Err. Once normal
// end is reported, later cancellation or interruption leaves Err nil.
func (r *Rows) Next() bool {
	if r.m_err != nil || r.m_done {
		return false
	}

	if r.m_result == nil {
		r.m_err = errRowsClosed

		return false
	}

	if err := r.stopError(); err != nil {
		r.m_err = err
		r.m_n = 0

		return false
	}

	if r.m_row+1 < r.m_n {
		r.m_row++

		return true
	}

	// Fetch under the database's shared lock, so that DB.Close waits.
	_, release, err := r.m_db.handle()
	if err != nil {
		r.m_err = err

		return false
	}

	var n C.uint32_t
	var msg *C.char

	err = toError(C.pt_go_result_fetch(r.m_result, &n, &msg), msg)
	release()

	if err != nil {
		if ctxErr := r.m_ctx.Err(); ctxErr != nil && errors.Is(err, ErrInterrupted) {
			err = ctxErr
		}

		r.m_err = err
		r.m_n = 0

		return false
	}

	// The request may have arrived while the native fetch was running.
	if err := r.stopError(); err != nil {
		r.m_err = err
		r.m_n = 0

		return false
	}

	r.m_n = n
	r.m_row = 0
	r.m_done = n == 0

	return n > 0
}

// stopError reports a request to stop an active result. Next owns err and
// done; Interrupt only sets the atomic request and interrupts the C query.
func (r *Rows) stopError() error {
	if err := r.m_ctx.Err(); err != nil {
		return err
	}
	if r.m_interrupted.Load() {
		return ErrInterrupted
	}

	return nil
}

// errRowsClosed is returned by Next after Close.
var errRowsClosed = errors.New("pulseten: rows are closed")

// Err returns the error that ended the rows, or nil after the last row.
func (r *Rows) Err() error {
	return r.m_err
}

// Interrupt requests that an active query stop from another goroutine.
// Next observes the request before returning a cached row or after a running
// fetch, then returns false and Err matches ErrInterrupted. With QueryContext,
// a done context makes Err its error instead. After Next has reported normal
// end (false with Err nil), interruption leaves later calls at normal end.
func (r *Rows) Interrupt() {
	r.m_mu.Lock()
	defer r.m_mu.Unlock()

	if r.m_result != nil {
		r.m_interrupted.Store(true)
		C.pulseten_result_interrupt(r.m_result)
	}
}

// destroy frees the C result; it may be called more than once.
func (r *Rows) destroy() {
	r.m_mu.Lock()
	defer r.m_mu.Unlock()

	if r.m_result == nil {
		return
	}

	C.pulseten_result_destroy(r.m_result)
	r.m_result = nil
	r.m_n = 0

	if r.m_stop != nil {
		close(r.m_stop)
	}
}

// Close stops the query and frees the result. Closing twice does nothing.
func (r *Rows) Close() error {
	r.m_db.m_rowsMu.Lock()
	delete(r.m_db.m_rows, r)
	r.m_db.m_rowsMu.Unlock()

	r.destroy()

	return nil
}

// IsNull tells whether a value of the current row is NULL.
func (r *Rows) IsNull(col int) bool {
	return C.pulseten_result_is_null(r.m_result, C.uint32_t(col), r.m_row) != 0
}

// Bool reads a BOOLEAN value of the current row; false if NULL.
func (r *Rows) Bool(col int) bool {
	return C.pulseten_result_get_bool(r.m_result, C.uint32_t(col), r.m_row) != 0
}

// Int64 reads a BIGINT value, or a TIMESTAMP in microseconds, of the
// current row; 0 if NULL.
func (r *Rows) Int64(col int) int64 {
	return int64(C.pulseten_result_get_int64(r.m_result, C.uint32_t(col), r.m_row))
}

// Float64 reads a DOUBLE value of the current row; 0 if NULL.
func (r *Rows) Float64(col int) float64 {
	return float64(C.pulseten_result_get_double(r.m_result, C.uint32_t(col), r.m_row))
}

// String reads a VARCHAR value of the current row; "" if NULL.
func (r *Rows) String(col int) string {
	var n C.size_t
	s := C.pulseten_result_get_varchar(r.m_result, C.uint32_t(col), r.m_row, &n)

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
	if col < 0 || col >= len(r.m_columns) || r.IsNull(col) {
		return nil
	}

	switch r.m_columns[col].Type {
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
