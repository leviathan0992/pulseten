// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

// Package pulseten is the Go interface to PulseTen, an embedded time-series
// database built on DuckDB.
//
// A database is a directory opened by one process at a time. Points are
// written to tables in batches; a batch is logged and visible to queries
// when Write returns a nil error. WALSync is off by default; enable it to
// make each successful write durable before it returns. Tables can be declared
// without columns or inferred by writes; the first accepted write defines their
// columns. Exec runs ordinary table SQL directly in DuckDB. Query runs SELECT
// statements over ordinary tables and time-series data, including unflushed
// writes.
//
// Directory paths, table names and SQL statements must not contain NUL.
// VARCHAR tag and field values are length-delimited and may contain NUL.
//
// The package calls the C library through cgo and finds it with
// pkg-config: install PulseTen and put its lib/pkgconfig directory on
// PKG_CONFIG_PATH.
//
// A DB may be used by any number of goroutines at once. Rows are read by one
// goroutine at a time; Rows.Interrupt may be called from any goroutine.
package pulseten

/*
#cgo pkg-config: pulseten
#include <stdlib.h>

#include "shim.h"
*/
import "C"

import (
	"errors"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// Limits of the C library; see pulseten.h.
const (
	// MaxPoints is the most points one Write takes.
	MaxPoints = C.PULSETEN_MAX_POINTS

	// MaxColumns is the most columns of a table, "time" included.
	MaxColumns = C.PULSETEN_MAX_COLUMNS

	// MaxNameBytes is the most bytes of a table or column name.
	MaxNameBytes = C.PULSETEN_MAX_NAME_BYTES

	// MaxStringBytes is the most bytes of a VARCHAR tag or field value.
	MaxStringBytes = C.PULSETEN_MAX_STRING_BYTES
)

// Version returns the version of the linked C library, e.g. "0.1.0".
func Version() string {
	return C.GoString(C.pulseten_version())
}

// Options are the settings of Open; start from DefaultOptions.
type Options struct {
	// CreateIfMissing allows Open to create a database. Default: true.
	CreateIfMissing bool

	// FlushInterval is the idle wait between background passes. It must be
	// a whole number of milliseconds from 1 to 4294967295. Writers waiting
	// for memtable capacity may wake the background thread sooner. Safely
	// rolled-back flush OOM failures retry after a full interval.
	FlushInterval time.Duration

	// WriteLockTimeout bounds table and namespace lock waits for Write,
	// CreateTable, SetTableOptions and DropTable. Zero waits indefinitely
	// (default). Use whole milliseconds from 0 to 4294967295.
	WriteLockTimeout time.Duration

	// MemtableLimitBytes makes new writes wait while unflushed column data
	// is at or above this many bytes; 0 disables waiting. Default: 128 MiB.
	MemtableLimitBytes uint64

	// WALFileSizeBytes is the size at which the write-ahead log starts a new
	// file.
	WALFileSizeBytes uint64

	// PartitionDurationHours is the number of hours one time partition covers.
	// Used when the database is created; an existing one keeps its own.
	PartitionDurationHours uint32

	// RetentionHours keeps this many hours before the current time; 0 keeps
	// everything. Tables without an override inherit this value for this open.
	RetentionHours uint32

	// MaxFutureSkew bounds future timestamps against the database clock. Zero
	// disables the check. Use whole seconds up to 4294967295 seconds. Applies to
	// this open only; WAL recovery ignores the bound. Default: zero.
	MaxFutureSkew time.Duration

	// WALSync syncs each WAL group before acknowledging its writes. The
	// default, false, writes to the operating system's page cache without
	// waiting for a sync; a system crash or power loss may lose acknowledged
	// writes or corrupt unsynced WAL data. Applies to this open only.
	// File creation, rotation, recovery and background flushes may still
	// sync files.
	WALSync bool

	// DuckDBMemoryLimitBytes is the shared DuckDB query/storage budget.
	// 0 uses DuckDB's default. Positive values are exact bytes up to 2^53.
	// The budget covers DuckDB's buffer manager for this open.
	DuckDBMemoryLimitBytes uint64

	// DuckDBThreads sets DuckDB's shared query/storage execution threads.
	// 0 keeps DuckDB's platform default; positive values are exact, at most 1024.
	// Applies to this open only. It does not bound total process threads or
	// DuckDB's separate async threads.
	DuckDBThreads uint32
}

// DefaultOptions returns the library's default settings.
func DefaultOptions() Options {
	var c C.pulseten_options_t
	C.pulseten_options_init(&c)

	return Options{
		CreateIfMissing:        c.m_create_if_missing != 0,
		FlushInterval:          time.Duration(c.m_flush_interval_ms) * time.Millisecond,
		WriteLockTimeout:       time.Duration(c.m_write_lock_timeout_ms) * time.Millisecond,
		MemtableLimitBytes:     uint64(c.m_memtable_limit_bytes),
		WALFileSizeBytes:       uint64(c.m_wal_file_size_bytes),
		PartitionDurationHours: uint32(c.m_partition_duration_hours),
		RetentionHours:         uint32(c.m_retention_hours),
		MaxFutureSkew:          time.Duration(c.m_max_future_skew_seconds) * time.Second,
		WALSync:                c.m_wal_sync != 0,
		DuckDBMemoryLimitBytes: uint64(c.m_duckdb_memory_limit_bytes),
		DuckDBThreads:          uint32(c.m_duckdb_threads),
	}
}

// toC checks the duration before converting the options for pulseten_open().
func (o *Options) toC() (C.pulseten_options_t, error) {
	var c C.pulseten_options_t

	const maxInterval = time.Duration(1<<32-1) * time.Millisecond
	if o.FlushInterval < time.Millisecond || o.FlushInterval > maxInterval ||
		o.FlushInterval%time.Millisecond != 0 {
		return c, &Error{
			Code: CodeInvalidArgument,
			Msg:  "flush interval must be whole milliseconds from 1 to 4294967295",
		}
	}

	if o.WriteLockTimeout < 0 || o.WriteLockTimeout > maxInterval ||
		o.WriteLockTimeout%time.Millisecond != 0 {
		return c, &Error{Code: CodeInvalidArgument,
			Msg: "write lock timeout must be whole milliseconds from 0 to 4294967295"}
	}

	c.m_flush_interval_ms = C.uint32_t(o.FlushInterval / time.Millisecond)
	if o.MaxFutureSkew < 0 || o.MaxFutureSkew%time.Second != 0 ||
		o.MaxFutureSkew/time.Second > time.Duration(1<<32-1) {
		return c, &Error{Code: CodeInvalidArgument,
			Msg: "future skew must be whole seconds from 0 to 4294967295"}
	}
	c.m_max_future_skew_seconds = C.uint32_t(o.MaxFutureSkew / time.Second)
	c.m_write_lock_timeout_ms = C.uint32_t(o.WriteLockTimeout / time.Millisecond)
	c.m_memtable_limit_bytes = C.uint64_t(o.MemtableLimitBytes)
	c.m_duckdb_memory_limit_bytes = C.uint64_t(o.DuckDBMemoryLimitBytes)
	c.m_duckdb_threads = C.uint32_t(o.DuckDBThreads)
	c.m_wal_file_size_bytes = C.uint64_t(o.WALFileSizeBytes)
	c.m_partition_duration_hours = C.uint32_t(o.PartitionDurationHours)
	c.m_retention_hours = C.uint32_t(o.RetentionHours)
	if o.CreateIfMissing {
		c.m_create_if_missing = 1
	}
	if o.WALSync {
		c.m_wal_sync = 1
	}

	return c, nil
}

// DB is an open database.
type DB struct {
	/* ====================== Database lifetime ====================== */

	// m_mutex protects this group. Calls hold it shared; Close holds it
	// exclusively. Acquired before m_rows_mutex when both are needed.
	m_mutex sync.RWMutex

	// m_db is the C handle; nil once closed.
	m_db *C.pulseten_db_t

	/* ======================== Open results ========================= */

	// m_rows_mutex protects this group; acquired before a result's m_mutex.
	m_rows_mutex sync.Mutex

	// m_rows are the open results, which Close closes first.
	m_rows map[*Rows]struct{}
}

// errClosed is returned by calls on a closed DB.
var errClosed = errors.New("pulseten: database is closed")

// checkedCString converts a NUL-free argument to a C string. The caller
// frees the result with C.free.
func checkedCString(value, name string) (*C.char, error) {
	if strings.IndexByte(value, 0) >= 0 {
		return nil, &Error{Code: CodeInvalidArgument, Msg: name + " must not contain NUL"}
	}

	return C.CString(value), nil
}

// Open opens a database directory and creates it if missing by default.
// Set Options.CreateIfMissing to false to require an existing database.
// If the database is missing and creation is disabled, Open returns
// ErrInvalidArgument.
// The parent directory must exist. A nil opts selects the defaults.
//
// Recovery discards the newest WAL file from its first invalid record
// onward, including previously acknowledged writes if damaged. Invalid file
// headers, earlier damaged files, or a prefix shorter than stored data fail
// opening.
func Open(dir string, opts *Options) (*DB, error) {
	cdir, err := checkedCString(dir, "directory path")
	if err != nil {
		return nil, err
	}

	defer C.free(unsafe.Pointer(cdir))

	var copts *C.pulseten_options_t
	if opts != nil {
		c, err := opts.toC()
		if err != nil {
			return nil, err
		}

		copts = &c
	}

	var db *C.pulseten_db_t
	var msg *C.char

	if err := toError(C.pt_go_open(cdir, copts, &db, &msg), msg); err != nil {
		return nil, err
	}

	return &DB{m_db: db, m_rows: make(map[*Rows]struct{})}, nil
}

// Close closes the database, and first every Rows still open, which must not
// be in use by another goroutine. Close waits for calls already running, such
// as Write or ExecContext, and does not cancel them: cancel their contexts
// first, or Close waits as long as they run. Later calls fail. Unflushed
// writes remain in the write-ahead log; closing does not force unsynced writes
// to disk. Closing twice does nothing.
func (db *DB) Close() {
	db.m_mutex.Lock()
	defer db.m_mutex.Unlock()

	if db.m_db == nil {
		return
	}

	db.m_rows_mutex.Lock()
	for r := range db.m_rows {
		r.destroy()
	}

	db.m_rows = nil
	db.m_rows_mutex.Unlock()

	C.pulseten_close(db.m_db)
	db.m_db = nil

	return
}

// acquireHandle returns the C handle with db.m_mutex held shared; call the
// returned function to release it.
func (db *DB) acquireHandle() (*C.pulseten_db_t, func(), error) {
	db.m_mutex.RLock()

	if db.m_db == nil {
		db.m_mutex.RUnlock()

		return nil, nil, errClosed
	}

	return db.m_db, db.m_mutex.RUnlock, nil
}

// Stats are the counters of a database.
type Stats struct {
	// FlushedSeq is the sequence number of the newest stored write.
	FlushedSeq uint64

	// VisibleSeq is the sequence number of the newest visible write.
	VisibleSeq uint64

	// MemtableBytes is the bytes of unflushed writes held in memory.
	MemtableBytes uint64

	// WALFileCount is the number of write-ahead log files.
	WALFileCount uint64

	// BackgroundFlushes counts background flushes that stored something.
	BackgroundFlushes uint64

	// BackgroundErrors counts passes with a failed flush, WAL cleanup or
	// expiry, once per pass.
	BackgroundErrors uint64

	// ThrottledWrites counts writes that had to wait for memory.
	ThrottledWrites uint64

	// ExpiredPartitions counts partitions dropped for being past the
	// retention period.
	ExpiredPartitions uint64

	// DuckDBMemoryLimitBytes is the shared budget selected at open.
	// Later SQL configuration changes are not reflected here.
	DuckDBMemoryLimitBytes uint64

	// DuckDBThreads is the effective execution thread setting selected at open.
	// Later SQL configuration changes are not reflected here.
	DuckDBThreads uint32
}

// BackgroundError returns the failures from the latest failed background pass
// (flush, WAL cleanup or expiry), or "" if none failed. Successful passes
// retain the previous failure message.
func (db *DB) BackgroundError() (string, error) {
	h, release, err := db.acquireHandle()
	if err != nil {
		return "", err
	}

	defer release()

	var message *C.char
	var msg *C.char

	if err := toError(C.pt_go_copy_background_error(h, &message, &msg), msg); err != nil {
		return "", err
	}

	if message == nil {
		return "", nil
	}

	defer C.pulseten_free(unsafe.Pointer(message))

	return C.GoString(message), nil
}

// Stats reads the counters; all zero on a closed DB.
func (db *DB) Stats() Stats {
	h, release, err := db.acquireHandle()
	if err != nil {
		return Stats{}
	}

	defer release()

	var c C.pulseten_stats_t
	C.pulseten_stats(h, &c)

	return Stats{
		FlushedSeq:             uint64(c.m_flushed_seq),
		VisibleSeq:             uint64(c.m_visible_seq),
		MemtableBytes:          uint64(c.m_memtable_bytes),
		WALFileCount:           uint64(c.m_wal_file_count),
		BackgroundFlushes:      uint64(c.m_background_flushes),
		BackgroundErrors:       uint64(c.m_background_errors),
		ThrottledWrites:        uint64(c.m_throttled_writes),
		ExpiredPartitions:      uint64(c.m_expired_partitions),
		DuckDBMemoryLimitBytes: uint64(c.m_duckdb_memory_limit_bytes),
		DuckDBThreads:          uint32(c.m_duckdb_threads),
	}
}

// TableOptions replaces a table's mutable settings as a whole.
type TableOptions struct {
	// RetentionHours overrides Open's default. Nil inherits; a pointer to zero
	// keeps all data. Queries filter expired rows even before physical cleanup.
	RetentionHours *uint32
}

// SetTableOptions durably replaces retention without flushing data. The table
// must exist. Results hold table locks until closed; table lock
// waits use WriteLockTimeout. Extending retention may reveal rows that physical
// cleanup has not deleted yet.
func (db *DB) SetTableOptions(name string, options TableOptions) error {
	handle, release, err := db.acquireHandle()
	if err != nil {
		return err
	}
	defer release()
	cname, err := checkedCString(name, "table name")
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(cname))
	var c C.pulseten_table_options_t
	C.pulseten_table_options_init(&c)
	if options.RetentionHours != nil {
		c.m_has_retention_hours = 1
		c.m_retention_hours = C.uint32_t(*options.RetentionHours)
	}
	var msg *C.char
	return toError(C.pt_go_set_table_options(handle, cname, &c, &msg), msg)
}

// CreateTableOptions declares a table without specifying its columns.
type CreateTableOptions struct {
	// PartitionDurationHours is fixed at creation. Zero selects the durable
	// database default.
	PartitionDurationHours uint32
	// RetentionHours overrides Open's default. Nil inherits; zero keeps all data.
	RetentionHours *uint32
}

// CreateTable durably declares a new table. The first accepted write defines
// its columns; queries before that return ErrTableNotInitialized. Existing
// names are rejected without changing settings. No data is flushed.
func (db *DB) CreateTable(name string, options CreateTableOptions) error {
	handle, release, err := db.acquireHandle()
	if err != nil {
		return err
	}
	defer release()
	cname, err := checkedCString(name, "table name")
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(cname))
	var c C.pulseten_create_table_options_t
	C.pulseten_create_table_options_init(&c)
	c.m_partition_duration_hours = C.uint32_t(options.PartitionDurationHours)
	if options.RetentionHours != nil {
		c.m_has_retention_hours = 1
		c.m_retention_hours = C.uint32_t(*options.RetentionHours)
	}
	var msg *C.char
	return toError(C.pt_go_create_table(handle, cname, &c, &msg), msg)
}

// DropTable durably removes a managed table, including an uninitialized one.
// Close its Rows first: table lock waits use WriteLockTimeout. Drop performs a
// global flush before deletion, so latency depends on all buffered data and a
// failed deletion may still have completed the flush. The name can be reused
// with new columns and options. Pending Write, Set or Drop requests that cross
// a deletion fail with ErrInvalidArgument; retry by name only if intended.
func (db *DB) DropTable(name string) error {
	handle, release, err := db.acquireHandle()
	if err != nil {
		return err
	}
	defer release()
	cname, err := checkedCString(name, "table name")
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(cname))
	var msg *C.char
	return toError(C.pt_go_drop_table(handle, cname, &msg), msg)
}

// DropDatabase removes the closed database currently at dir. Close all DB
// handles first. Unknown entries and links are rejected before removal.
// Missing and empty directories are successful cleanup. The main file is
// removed and the directory synced before WAL cleanup. After
// ErrOutcomeUnknown, retry only if no new database has been created at the
// path and none can be created during the retry.
func DropDatabase(dir string) error {
	cdir, err := checkedCString(dir, "directory path")
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(cdir))
	var msg *C.char
	return toError(C.pt_go_drop_database(cdir, &msg), msg)
}
