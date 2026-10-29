// Copyright (c) 2026, The PulseTen Authors
// SPDX-License-Identifier: Apache-2.0

// Package pulseten is the Go interface to PulseTen, an embedded real-time
// analytical database.
//
// A database is a directory opened by one process at a time. Points are
// written to tables in batches; a batch is durable and visible to queries
// when Write returns a nil error. Tables and columns are created by the
// writes themselves. Queries are SQL SELECT statements over one consistent
// snapshot.
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
	// BackgroundFlush flushes in a background thread. Without it, call
	// DB.Flush; dropping a table also flushes first.
	BackgroundFlush bool

	// FlushInterval is the idle wait between background passes. It must be
	// a whole number of milliseconds from 0 to 4294967295, and greater than
	// zero when BackgroundFlush is set.
	FlushInterval time.Duration

	// FlushBytes wakes the background flusher when unflushed column data
	// reaches this many bytes.
	FlushBytes uint64

	// MemtableLimitBytes makes new writes wait, with background flushing,
	// while unflushed column data is at or above this many bytes; 0 disables
	// waiting.
	MemtableLimitBytes uint64

	// WALSegmentBytes is the size at which the write-ahead log starts a new
	// file.
	WALSegmentBytes uint64

	// PartitionDays is the number of days one time partition covers. Used
	// when the database is created; an existing one keeps its own.
	PartitionDays uint32

	// RetentionDays keeps this many days before the current time; 0 keeps
	// everything. Applies to this open only.
	RetentionDays uint32
}

// DefaultOptions returns the library's default settings.
func DefaultOptions() Options {
	var c C.pulseten_options_t
	C.pulseten_options_init(&c)

	return Options{
		BackgroundFlush:    c.background_flush != 0,
		FlushInterval:      time.Duration(c.flush_interval_ms) * time.Millisecond,
		FlushBytes:         uint64(c.flush_bytes),
		MemtableLimitBytes: uint64(c.memtable_limit_bytes),
		WALSegmentBytes:    uint64(c.wal_segment_bytes),
		PartitionDays:      uint32(c.partition_days),
		RetentionDays:      uint32(c.retention_days),
	}
}

// toC checks the duration before converting the options for pulseten_open().
func (o *Options) toC() (C.pulseten_options_t, error) {
	var c C.pulseten_options_t

	const maxInterval = time.Duration(1<<32-1) * time.Millisecond
	if o.FlushInterval < 0 || o.FlushInterval > maxInterval ||
		o.FlushInterval%time.Millisecond != 0 {
		return c, &Error{
			Code: CodeInvalidArgument,
			Msg:  "flush interval must be whole milliseconds from 0 to 4294967295",
		}
	}
	if o.BackgroundFlush && o.FlushInterval == 0 {
		return c, &Error{
			Code: CodeInvalidArgument,
			Msg:  "background flush interval must be greater than zero",
		}
	}

	if o.BackgroundFlush {
		c.background_flush = 1
	}

	c.flush_interval_ms = C.uint32_t(o.FlushInterval / time.Millisecond)
	c.flush_bytes = C.uint64_t(o.FlushBytes)
	c.memtable_limit_bytes = C.uint64_t(o.MemtableLimitBytes)
	c.wal_segment_bytes = C.uint64_t(o.WALSegmentBytes)
	c.partition_days = C.uint32_t(o.PartitionDays)
	c.retention_days = C.uint32_t(o.RetentionDays)

	return c, nil
}

// DB is an open database.
type DB struct {
	// mu lets Close wait for calls in progress, fetches included: calls hold
	// it shared, Close holds it exclusively.
	mu sync.RWMutex

	// db is the C handle; nil once closed.
	db *C.pulseten_db_t

	// rowsMu protects rows.
	rowsMu sync.Mutex

	// rows are the open results, which Close closes first.
	rows map[*Rows]struct{}
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

	return &DB{db: db, rows: make(map[*Rows]struct{})}, nil
}

// Close closes the database, and first every Rows still open, which must not
// be in use by another goroutine. Unflushed writes stay durable in the
// write-ahead log. Closing twice does nothing.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.db == nil {
		return nil
	}

	db.rowsMu.Lock()
	for r := range db.rows {
		r.destroy()
	}
	db.rows = nil
	db.rowsMu.Unlock()

	var msg *C.char
	err := toError(C.pt_go_close(db.db, &msg), msg)
	db.db = nil

	return err
}

// handle returns the C handle with db.mu held shared; call the returned
// function to release it.
func (db *DB) handle() (*C.pulseten_db_t, func(), error) {
	db.mu.RLock()

	if db.db == nil {
		db.mu.RUnlock()

		return nil, nil, errClosed
	}

	return db.db, db.mu.RUnlock, nil
}

// Flush stores every unflushed write in the analytical storage now. It is
// not needed for durability.
func (db *DB) Flush() error {
	h, release, err := db.handle()
	if err != nil {
		return err
	}
	defer release()

	var msg *C.char

	return toError(C.pt_go_flush(h, &msg), msg)
}

// Expire drops the stored time partitions whose days have all passed the
// retention period, and returns how many it dropped. The background flush
// does this after each flush.
func (db *DB) Expire() (uint64, error) {
	h, release, err := db.handle()
	if err != nil {
		return 0, err
	}
	defer release()

	var removed C.uint64_t
	var msg *C.char

	err = toError(C.pt_go_expire(h, &removed, &msg), msg)

	return uint64(removed), err
}

// Stats are the counters of a database.
type Stats struct {
	// FlushedSeq is the sequence number of the newest stored write.
	FlushedSeq uint64

	// VisibleSeq is the sequence number of the newest visible write.
	VisibleSeq uint64

	// MemtableBytes is the bytes of unflushed writes held in memory.
	MemtableBytes uint64

	// WALSegments is the number of write-ahead log files.
	WALSegments uint64

	// BackgroundFlushes counts background flushes that stored something.
	BackgroundFlushes uint64

	// BackgroundErrors counts background flushes or expiries that failed.
	BackgroundErrors uint64

	// ThrottledWrites counts writes that had to wait for memory.
	ThrottledWrites uint64

	// ExpiredPartitions counts partitions dropped for being past the
	// retention period.
	ExpiredPartitions uint64
}

// Stats reads the counters; all zero on a closed DB.
func (db *DB) Stats() Stats {
	h, release, err := db.handle()
	if err != nil {
		return Stats{}
	}
	defer release()

	var c C.pulseten_stats_t
	C.pulseten_get_stats(h, &c)

	return Stats{
		FlushedSeq:        uint64(c.flushed_seq),
		VisibleSeq:        uint64(c.visible_seq),
		MemtableBytes:     uint64(c.memtable_bytes),
		WALSegments:       uint64(c.wal_segments),
		BackgroundFlushes: uint64(c.background_flushes),
		BackgroundErrors:  uint64(c.background_errors),
		ThrottledWrites:   uint64(c.throttled_writes),
		ExpiredPartitions: uint64(c.expired_partitions),
	}
}
