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

#include <vector>

#include "db.h"
#include "db_table.h"
#include "memtable.h"
#include "store.h"
#include "wal.h"

/** Test-only access to database resources and synchronization points. */
class db_test_access final {
 public:
  db_test_access() = delete;

  /** Return the memtable; valid while the database remains open. */
  static memtable &memtable_handle(db &database);

  /** Return storage; the test must exclude concurrent flushes and expiry. */
  static store &store_handle(db &database);

  /** Return the table directory for lock tests while the database is open. */
  static db_tables &tables_handle(db &database);

  /** Retain each initialized table's immutable schema in name order. */
  static void collect_schemas(db &database, std::vector<schema_ref_t> &schemas);

  /** Hold snapshot creation while a test arranges table-lock interleavings. */
  static ut_shared_mutex_t &snapshot_mutex(db &database);

  /** Hold namespace X to reproduce a long namespace-changing exec. */
  static ut_rwlock &namespace_lock(db &database);

  /** Stop maintenance so tests can inspect Set without a wakeup race. */
  static void stop_flusher(db &database);

  /** Wake background maintenance after preparing a failure. */
  static void wake_flusher(db &database);

  /** Inspect group I/O counters without exposing WAL coordination in db. */
  static void wal_stats(db &database, wal::stats_t &stats);

  /** Run WAL cleanup independently of flushing. */
  static db_status_t purge_wal(db &database);

  /** Run expiry at a test-controlled clock value. */
  static db_status_t expire(db &database, int64_t now_us);
};

#endif /* test_db_access_h */
