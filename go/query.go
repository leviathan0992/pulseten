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

// Exec passes SQL directly to DuckDB and discards result rows. Each call uses a
// new connection; transactions must begin and end within the same call.
// Unfinished transactions are rolled back on return. Session settings and
// temporary objects do not carry across calls. References to _pt_ objects,
// including reads, and changes to time-series names or the main schema are
// rejected. Pure queries, DML and built-in SET/RESET/USE do not take the
// namespace lock. DDL and unclassified control or extension statements hold it
// through commit or rollback, coordinating with first time-series writes.
// Database imports and whole-database copies into PulseTen storage are
// rejected. PRAGMA copy_database requires two string literal arguments. Use
// Query to read unflushed time-series data. Setting fsync_mode is rejected:
// WALSync controls it. SET memory_limit changes the shared budget, and fails
// with CodeOutOfMemory if DuckDB already holds more than the new budget; FORCE
// CHECKPOINT can wait for writers and delay new transactions. Use ExecContext
// for interruptible calls.
func (db *DB) Exec(sql string) error {
	return db.ExecContext(context.Background(), sql)
}

// ExecContext cancels SQL execution or its lock wait when ctx is done.
// Cancellation does not undo statements already committed. Successful execution
// remains success even if cancellation races the return.
func (db *DB) ExecContext(ctx context.Context, sql string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	h, release, err := db.acquireHandle()
	if err != nil {
		return err
	}
	defer release()

	csql, err := checkedCString(sql, "SQL")
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(csql))

	var cancel *C.pulseten_cancel_t
	var msg *C.char
	if ctx.Done() != nil {
		if err := toError(C.pt_go_cancel_create(&cancel, &msg), msg); err != nil {
			return err
		}
		defer C.pulseten_cancel_destroy(cancel)

		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			select {
			case <-ctx.Done():
				C.pulseten_cancel_request(cancel)
			case <-stop:
			}
		}()
		defer func() {
			close(stop)
			<-done
		}()
	}

	err = toError(C.pt_go_exec(h, csql, cancel, &msg), msg)
	if errors.Is(err, ErrInterrupted) && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

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

// Table is a declared or initialized time-series table and its newest columns.
type Table struct {
	// Name is the lower-case name.
	Name string

	// Initialized becomes true after the first accepted write, before flush.
	Initialized bool
	// PartitionDurationHours is the table's fixed partition duration.
	PartitionDurationHours uint32
	// RetentionHours is the override. Nil inherits Open's default; zero keeps all
	// data.
	RetentionHours *uint32
	// Columns are in creation order, "time" first; empty before initialization.
	Columns []Column
}

// Tables returns every managed time-series table and its newest columns,
// including uninitialized declarations and unflushed writes, sorted by name.
func (db *DB) Tables() ([]Table, error) {
	h, release, err := db.acquireHandle()
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

	ctables := unsafe.Slice(catalog.m_tables, int(catalog.m_table_count))
	tables := make([]Table, len(ctables))

	for t, table := range ctables {
		tables[t].Name = C.GoString(table.m_name)
		tables[t].Initialized = table.m_initialized != 0
		tables[t].PartitionDurationHours = uint32(table.m_partition_duration_hours)
		if table.m_retention_hours != C.PULSETEN_USE_DEFAULT_RETENTION {
			retention := uint32(table.m_retention_hours)
			tables[t].RetentionHours = &retention
		}
		columns := unsafe.Slice(table.m_columns, int(table.m_column_count))
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

// Explain returns DuckDB's plan for a SELECT, as Query would run it.
func (db *DB) Explain(sql string) (string, error) {
	csql, err := checkedCString(sql, "SQL statement")
	if err != nil {
		return "", err
	}

	defer C.free(unsafe.Pointer(csql))

	h, release, err := db.acquireHandle()
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
// Reading and closing must be serialized by the caller; Interrupt may run
// concurrently with either operation.
type Rows struct {
	/* ==================== Fixed query metadata ===================== */

	// m_db is the database the query reads.
	m_db *DB

	// m_columns are the result columns, initialized before returning Rows.
	m_columns []ResultColumn

	// m_context is checked by Next before returning a row.
	m_context context.Context

	// m_stop is fixed before the watcher starts; nil without one.
	// destroy closes it under m_mutex after destroying the result.
	m_stop chan struct{}

	/* ====================== Result lifetime ======================== */

	// m_mutex serializes Interrupt and destroy. Readers may use m_result
	// without this lock because the caller excludes concurrent Close.
	m_mutex sync.Mutex

	// m_result is the C result; nil once destroyed.
	m_result *C.pulseten_result_t

	/* ================= Caller-serialized reading =================== */

	// m_row_count is the number of rows in the current chunk.
	m_row_count C.uint32_t

	// m_row_index is the current row of the chunk.
	m_row_index C.uint32_t

	// m_error is the error that ended the rows, if any.
	m_error error

	// m_done is set by Next when a fetch reports normal end.
	m_done bool

	/* ===================== Atomic interruption ===================== */

	// m_interrupted lets Next observe Interrupt before serving a cached row.
	m_interrupted atomic.Bool
}

// Query starts a SELECT over one consistent snapshot of every write so far.
// The statement is checked and planned here; errors found while it runs are
// returned by Rows.Err.
func (db *DB) Query(sql string) (*Rows, error) {
	return db.QueryContext(context.Background(), sql)
}

// openQuery keeps cancellation alive until the native open and its watcher
// both finish. The caller holds the database handle for the entire call.
func openQuery(ctx context.Context, db *C.pulseten_db_t, sql *C.char) (*C.pulseten_result_t, error) {
	var cancel *C.pulseten_cancel_t
	var msg *C.char
	if ctx.Done() != nil {
		if err := toError(C.pt_go_cancel_create(&cancel, &msg), msg); err != nil {
			return nil, err
		}

		defer C.pulseten_cancel_destroy(cancel)

		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			select {
			case <-ctx.Done():
				C.pulseten_cancel_request(cancel)
			case <-stop:
			}
		}()
		// Join before destroying the handle; cancellation may race the return.
		defer func() {
			close(stop)
			<-done
		}()
	}

	var result *C.pulseten_result_t
	err := toError(C.pt_go_query(db, sql, cancel, &result, &msg), msg)
	if ctxErr := ctx.Err(); ctxErr != nil && (err == nil || errors.Is(err, ErrInterrupted)) {
		C.pulseten_result_destroy(result)

		return nil, ctxErr
	}

	return result, err
}

// QueryContext is Query, cancelling opening or execution when ctx is done.
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

	h, release, err := db.acquireHandle()
	if err != nil {
		return nil, err
	}

	defer release()

	result, err := openQuery(ctx, h, csql)
	if err != nil {
		return nil, err
	}

	r := &Rows{m_db: db, m_result: result, m_context: ctx}

	r.m_columns = make([]ResultColumn, C.pulseten_result_column_count(result))
	for c := range r.m_columns {
		r.m_columns[c] = ResultColumn{
			Name: C.GoString(C.pulseten_result_column_name(result, C.uint32_t(c))),
			Type: Type(C.pulseten_result_column_type(result, C.uint32_t(c))),
		}
	}

	db.m_rows_mutex.Lock()
	db.m_rows[r] = struct{}{}

	db.m_rows_mutex.Unlock()

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
// It returns false after the last row or on an error; see Err. A terminal
// error is retained across later calls and interruption. Once normal
// end is reported, later cancellation or interruption leaves Err nil.
func (r *Rows) Next() bool {
	if r.m_error != nil || r.m_done {
		return false
	}

	if r.m_result == nil {
		r.m_error = errRowsClosed

		return false
	}

	if err := r.cancellationError(); err != nil {
		r.m_error = err
		r.m_row_count = 0

		return false
	}

	if r.m_row_index+1 < r.m_row_count {
		r.m_row_index++

		return true
	}

	// Fetch under the database's shared lock, so that DB.Close waits.
	_, release, err := r.m_db.acquireHandle()
	if err != nil {
		r.m_error = err

		return false
	}

	var n C.uint32_t
	var msg *C.char

	err = toError(C.pt_go_result_fetch(r.m_result, &n, &msg), msg)
	release()

	if err != nil {
		if ctxErr := r.m_context.Err(); ctxErr != nil && errors.Is(err, ErrInterrupted) {
			err = ctxErr
		}

		r.m_error = err
		r.m_row_count = 0

		return false
	}

	// The request may have arrived while the native fetch was running.
	if err := r.cancellationError(); err != nil {
		r.m_error = err
		r.m_row_count = 0

		return false
	}

	r.m_row_count = n
	r.m_row_index = 0
	r.m_done = n == 0

	return n > 0
}

// cancellationError reports a request to stop an active result. Next owns err
// and done; Interrupt only sets the atomic request and interrupts the C query.
func (r *Rows) cancellationError() error {
	if err := r.m_context.Err(); err != nil {
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
	return r.m_error
}

// Interrupt requests that an active query stop from another goroutine.
// Next observes the request before returning a cached row or after a running
// fetch, then returns false and Err matches ErrInterrupted. With QueryContext,
// a done context makes Err its error instead. After Next has reported normal
// end (false with Err nil), interruption leaves later calls at normal end.
func (r *Rows) Interrupt() {
	r.m_mutex.Lock()
	defer r.m_mutex.Unlock()

	if r.m_result != nil {
		r.m_interrupted.Store(true)
		C.pulseten_result_interrupt(r.m_result)
	}
}

// destroy frees the C result; it may be called more than once.
func (r *Rows) destroy() {
	r.m_mutex.Lock()
	defer r.m_mutex.Unlock()

	if r.m_result == nil {
		return
	}

	C.pulseten_result_destroy(r.m_result)
	r.m_result = nil
	r.m_row_count = 0

	if r.m_stop != nil {
		close(r.m_stop)
	}
}

// Close stops the query and frees the result. Closing twice does nothing.
func (r *Rows) Close() {
	r.m_db.m_rows_mutex.Lock()
	delete(r.m_db.m_rows, r)
	r.m_db.m_rows_mutex.Unlock()

	r.destroy()

	return
}

// IsNull tells whether a value of the current row is NULL.
func (r *Rows) IsNull(col int) bool {
	return C.pulseten_result_is_null(r.m_result, C.uint32_t(col), r.m_row_index) != 0
}

// Bool reads a BOOLEAN value of the current row; false if NULL.
func (r *Rows) Bool(col int) bool {
	return C.pulseten_result_bool(r.m_result, C.uint32_t(col), r.m_row_index) != 0
}

// Int64 reads a BIGINT value, or a TIMESTAMP in microseconds, of the
// current row; 0 if NULL.
func (r *Rows) Int64(col int) int64 {
	return int64(C.pulseten_result_int64(r.m_result, C.uint32_t(col), r.m_row_index))
}

// Float64 reads a DOUBLE value of the current row; 0 if NULL.
func (r *Rows) Float64(col int) float64 {
	return float64(C.pulseten_result_double(r.m_result, C.uint32_t(col), r.m_row_index))
}

// String reads a VARCHAR value of the current row; "" if NULL.
func (r *Rows) String(col int) string {
	var n C.size_t
	s := C.pulseten_result_varchar(r.m_result, C.uint32_t(col), r.m_row_index, &n)

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
