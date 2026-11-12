/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_db_access.h
 *    White-box database inspection and failure controls.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_db_access_h
#define test_db_access_h

#ifndef PT_TESTING
#error "test_db_access.h requires the PulseTen test library"
#endif

#include "db.h"
#include "db_table.h"
#include "memtable.h"
#include "storage.h"
#include "wal.h"

/** Test-only access to database resources and synchronization points. */
class db_test_access final {
 public:
  /** This access class contains only static operations. */
  db_test_access() = delete;

  /** Borrow the memtable while the database remains open. */
  static memtable &memtable_handle(db &database);

  /** Borrow storage; callers exclude concurrent flushes and expiry. */
  static storage &storage_handle(db &database);

  /** Borrow table objects to coordinate S/X lock tests while the db is open. */
  static db_tables &tables_handle(db &database);

  /** Hold snapshot creation while a test arranges table-lock interleavings. */
  static ut_shared_mutex_t &snapshot_mutex(db &database);

  /** Wake background maintenance after preparing a failure. */
  static void wake_flusher(db &database);

  /** Set a one-shot capacity checkpoint with no writes in progress. */
  static void before_space_wait(db &database, void (*callback)(db &));

  /** Pause selected groups outside the queue lock; set only with no active
   * writes. */
  static void set_before_wal(db &database, void (*callback)(void *),
                             void *context);

  /** Inspect group I/O counters without exposing WAL coordination in db. */
  static void read_wal_stats(db &database, wal_stats_t &stats);

  /** Fail the next WAL group sync after its data has been written. */
  static void fail_next_wal_sync(db &database);

  /** Fail the next flush while excluding concurrent storage mutations. */
  static void fail_next_flush(db &database);

  /** Enable persistent flush failure, serialized with background maintenance.
   */
  static void set_flush_failure(db &database, bool fail);

  /** Run expiry at a test-controlled clock value. */
  static db_status_t expire(db &database, int64_t now_us, uint64_t &removed);
};

#endif /* test_db_access_h */
