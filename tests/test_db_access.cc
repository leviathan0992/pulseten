/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_db_access.cc
 *    Database test access, kept outside production objects.
 *
 *-------------------------------------------------------------------------
 */

#include "test_db_access.h"

#include <mutex>

#include "test_storage_access.h"
#include "test_wal_access.h"
#include "ut_mutex.h"

/** Borrow the database's memtable. */
memtable &db_test_access::memtable_handle(db &database) {
  return (*database.m_mt);
}

/** Borrow the database's storage. */
storage &db_test_access::storage_handle(db &database) {
  return (*database.m_storage);
}

/** Coordinate table-lock tests without exposing the cache in production. */
db_tables &db_test_access::tables_handle(db &database) {
  return (database.m_tables);
}

/** Coordinate snapshot timing without adding a production callback. */
ut_shared_mutex_t &db_test_access::snapshot_mutex(db &database) {
  return (database.m_snapshot_mutex);
}

/** Wake background maintenance. */
void db_test_access::wake_flusher(db &database) {
  std::lock_guard<std::mutex> lock(database.m_flusher_mutex);
  database.m_flusher_wake = true;
  database.m_flusher_wake_cv.notify_one();

  return;
}

/** Install a callback captured by each group leader before releasing the lock.
 */
void db_test_access::set_before_wal(db &database, void (*callback)(void *),
                                    void *context) {
  ut_lock_guard_t lock(database.m_write_mutex);
  database.m_test_before_wal = callback;
  database.m_test_wal_context = context;

  return;
}

/** Inspect the WAL groups selected by database leaders. */
void db_test_access::read_wal_stats(db &database, wal_stats_t &stats) {
  database.m_wal->read_stats(stats);

  return;
}

/** Inject a WAL sync failure. */
void db_test_access::fail_next_wal_sync(db &database) {
  wal_test_access::fail_next_sync(*database.m_wal);

  return;
}

/** Inject one flush failure while excluding storage mutations. */
void db_test_access::fail_next_flush(db &database) {
  ut_lock_guard_t lock(database.m_flush_mutex);
  storage_test_access::fail_next_flush(*database.m_storage);

  return;
}

/** Control persistent flush failure while excluding storage mutations. */
void db_test_access::set_flush_failure(db &database, bool fail) {
  ut_lock_guard_t lock(database.m_flush_mutex);
  storage_test_access::set_flush_failure(*database.m_storage, fail);

  return;
}

/** Test-only clock control; ordinary callers rely on background retention. */
db_status_t db_test_access::expire(db &database, int64_t now_us,
                                   uint64_t &removed) {
  return (database.expire(now_us, removed));
}

void db_test_access::before_space_wait(db &database, void (*callback)(db &)) {
  database.m_test_before_space_wait = callback;
}
