// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

// Package pulseten is the Go interface to PulseTen, an embedded real-time
// analytical database.
//
// A database is a directory opened by one process at a time. Points are
// written to tables in batches; a batch is logged and visible to queries
// when Write returns a nil error. WALSync is off by default; enable it to
// make each successful write durable before it returns. Tables and columns
// are created by the writes themselves. Queries are SQL SELECT statements
// over one consistent snapshot.
// Ordinary tables are created and dropped through ExecDDL; CREATE TABLE AS SELECT
// can materialize query results, including time-series data.
// The managed time-series tables remain protected.
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

	// MaxNameLen is the most bytes of a table or column name.
	MaxNameLen = C.PULSETEN_MAX_NAME_LEN

	// MaxStringLen is the most bytes of a VARCHAR tag or field value.
	MaxStringLen = C.PULSETEN_MAX_STRING_LEN
)

// Version returns the version of the linked C library, e.g. "0.1.0".
func Version() string {
	return C.GoString(C.pulseten_version())
}

// Options are the settings of Open; start from DefaultOptions.
type Options struct {
	// FlushInterval is the idle wait between background passes. It must be
	// a whole number of milliseconds from 1 to 4294967295. Writers waiting
	// for memtable capacity may wake the background thread sooner.
	FlushInterval time.Duration

	// MemtableLimitBytes makes new writes wait while unflushed column data
	// is at or above this many bytes; 0 disables waiting.
	MemtableLimitBytes uint64

	// WALSegmentSizeBytes is the size at which the write-ahead log starts a new
	// file.
	WALSegmentSizeBytes uint64

	// PartitionDurationDays is the number of days one time partition covers. Used
	// when the database is created; an existing one keeps its own.
	PartitionDurationDays uint32

	// RetentionDays keeps this many days before the current time; 0 keeps
	// everything. Applies to this open only.
	RetentionDays uint32

	// WALSync syncs each WAL group before acknowledging its writes. The
	// default, false, writes to the operating system's page cache without
	// waiting for a sync; a system crash or power loss may lose acknowledged
	// writes or corrupt unsynced WAL data. Applies to this open only.
	// Segment creation, rotation, recovery and background flushes may still
	// sync files.
	WALSync bool
}

// DefaultOptions returns the library's default settings.
func DefaultOptions() Options {
	var c C.pulseten_options_t
	C.pulseten_options_init(&c)

	return Options{
		FlushInterval:         time.Duration(c.m_flush_interval_ms) * time.Millisecond,
		MemtableLimitBytes:    uint64(c.m_memtable_limit_bytes),
		WALSegmentSizeBytes:   uint64(c.m_wal_segment_size_bytes),
		PartitionDurationDays: uint32(c.m_partition_duration_days),
		RetentionDays:         uint32(c.m_retention_days),
		WALSync:               c.m_wal_sync != 0,
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

	c.m_flush_interval_ms = C.uint32_t(o.FlushInterval / time.Millisecond)
	c.m_memtable_limit_bytes = C.uint64_t(o.MemtableLimitBytes)
	c.m_wal_segment_size_bytes = C.uint64_t(o.WALSegmentSizeBytes)
	c.m_partition_duration_days = C.uint32_t(o.PartitionDurationDays)
	c.m_retention_days = C.uint32_t(o.RetentionDays)
	if o.WALSync {
		c.m_wal_sync = 1
	}

	return c, nil
}

// DB is an open database.
type DB struct {
	// m_mu lets Close wait for calls in progress, fetches included: calls hold
	// it shared, Close holds it exclusively.
	m_mu sync.RWMutex

	// m_db is the C handle; nil once closed.
	m_db *C.pulseten_db_t

	// m_rowsMu protects m_rows.
	m_rowsMu sync.Mutex

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

// Open opens a database directory, creating it if it does not exist; its
// parent must exist. Every write that was durable is recovered. A nil opts
// selects the defaults.
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
// be in use by another goroutine. Unflushed writes remain in the write-ahead
// log; closing does not force unsynced writes to disk. Closing twice does
// nothing.
func (db *DB) Close() error {
	db.m_mu.Lock()
	defer db.m_mu.Unlock()

	if db.m_db == nil {
		return nil
	}

	db.m_rowsMu.Lock()
	for r := range db.m_rows {
		r.destroy()
	}
	db.m_rows = nil
	db.m_rowsMu.Unlock()

	var msg *C.char
	err := toError(C.pt_go_close(db.m_db, &msg), msg)
	db.m_db = nil

	return err
}

// handle returns the C handle with db.m_mu held shared; call the returned
// function to release it.
func (db *DB) handle() (*C.pulseten_db_t, func(), error) {
	db.m_mu.RLock()

	if db.m_db == nil {
		db.m_mu.RUnlock()

		return nil, nil, errClosed
	}

	return db.m_db, db.m_mu.RUnlock, nil
}

// Stats are the counters of a database.
type Stats struct {
	// FlushedSeq is the sequence number of the newest stored write.
	FlushedSeq uint64

	// VisibleSeq is the sequence number of the newest visible write.
	VisibleSeq uint64

	// MemtableBytes is the bytes of unflushed writes held in memory.
	MemtableBytes uint64

	// WALSegmentCount is the number of write-ahead log files.
	WALSegmentCount uint64

	// BackgroundFlushes counts background flushes that stored something.
	BackgroundFlushes uint64

	// BackgroundErrors counts passes with a failed flush or expiry, once per pass.
	BackgroundErrors uint64

	// ThrottledWrites counts writes that had to wait for memory.
	ThrottledWrites uint64

	// ExpiredPartitions counts partitions dropped for being past the
	// retention period.
	ExpiredPartitions uint64
}

// BackgroundError returns the message of the latest failed background flush or
// expiry, or "" if none failed.
func (db *DB) BackgroundError() (string, error) {
	h, release, err := db.handle()
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
	h, release, err := db.handle()
	if err != nil {
		return Stats{}
	}
	defer release()

	var c C.pulseten_stats_t
	C.pulseten_read_stats(h, &c)

	return Stats{
		FlushedSeq:        uint64(c.m_flushed_seq),
		VisibleSeq:        uint64(c.m_visible_seq),
		MemtableBytes:     uint64(c.m_memtable_bytes),
		WALSegmentCount:   uint64(c.m_wal_segment_count),
		BackgroundFlushes: uint64(c.m_background_flushes),
		BackgroundErrors:  uint64(c.m_background_errors),
		ThrottledWrites:   uint64(c.m_throttled_writes),
		ExpiredPartitions: uint64(c.m_expired_partitions),
	}
}
