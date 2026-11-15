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

#include <pthread.h>

#include "ut.h"

/** Return the database's memtable. */
memtable &db_test_access::memtable_handle(db &database) {
  return (*database.m_memtable);
}

/** Return the database's storage. */
store &db_test_access::store_handle(db &database) {
  return (*database.m_store);
}

/** Coordinate table-lock tests without exposing the cache in production. */
db_tables &db_test_access::tables_handle(db &database) {
  return (database.m_tables);
}

/** Coordinate snapshot timing without adding a production callback. */
ut_shared_mutex_t &db_test_access::snapshot_mutex(db &database) {
  return (database.m_snapshot_mutex);
}

/** Coordinate namespace wait deadlines without a long-running SQL fixture. */
ut_rwlock &db_test_access::namespace_lock(db &database) {
  return (database.m_namespace_lock);
}

/** Wake background maintenance. */
void db_test_access::wake_flusher(db &database) {
  database.m_flusher_event.set();

  return;
}

/** Inspect the WAL groups selected by database leaders. */
void db_test_access::wal_stats(db &database, wal::stats_t &stats) {
  database.m_wal->stats(stats);

  return;
}

/** Test-only clock control; ordinary callers rely on background retention. */
db_status_t db_test_access::expire(db &database, int64_t now_us,
                                   uint64_t &removed) {
  return (database.expire(now_us, removed));
}

/** Run WAL cleanup directly so tests can inspect its result. */
db_status_t db_test_access::purge_wal(db &database) {
  return (database.purge_wal());
}

/** Stop background work before a test inspects or injects storage state. */
void db_test_access::stop_flusher(db &database) {
  if (!database.m_flusher_started) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(database.m_flusher_mutex);
    database.m_flusher_stop = true;
    database.m_flusher_event.set();
  }

  pthread_join(database.m_flusher_thread, nullptr);
  database.m_flusher_started = false;

  return;
}
