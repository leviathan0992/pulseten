/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_query.cc
 *    Read-only query cursors and consistent snapshots.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "db_table.h"
#include "duckdb_database.h"
#include "memtable.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_new.h"
#include "ut_noncopyable.h"
#include "ut_shared_mutex.h"

/** Build what a query sees from the stored catalog at F and the pinned
memtable snapshot of writes after F. */
void db_cursor::build_tables(const storage_catalog_t &catalog) {
  m_snapshot.m_flushed_seq = catalog.m_flushed_seq;
  m_snapshot.m_partition_duration_days = catalog.m_partition_duration_days;

  /* Partition keys are ordered by table id, then day. Visit only this table's
  range instead of scanning the full directory for every table. */
  for (const auto &[name, stored] : catalog.m_tables) {
    ut_a(!stored.m_schema.m_cols.empty());

    query_table_t &table = m_snapshot.m_tables[name];
    table.m_schema = stored.m_schema;

    auto partition =
        catalog.m_partitions.lower_bound({stored.m_table_id, INT64_MIN});
    while (partition != catalog.m_partitions.end() &&
           partition->first.first == stored.m_table_id) {
      table.m_partitions.push_back(partition->second);
      ++partition;
    }
  }

  /* Unflushed batches; a wider visible directory replaces the stored one. */
  std::map<std::string, memtable_table_view_t>::const_iterator h;

  for (h = m_memtable_snapshot.m_tables.begin();
       h != m_memtable_snapshot.m_tables.end(); ++h) {
    ut_a(h->second.m_schema != nullptr);

    query_table_t &table = m_snapshot.m_tables[h->first];
    table.m_batches = h->second.m_batches;

    if (h->second.m_schema->m_cols.size() > table.m_schema.m_cols.size()) {
      table.m_schema = *h->second.m_schema;
    }
  }

  return;
}

/** Request interruption of an active cursor. */
void db_cursor::interrupt() {
  m_query->interrupt();

  return;
}

/** Stop a query and release its transaction, snapshot and table locks. */
void db_cursor::close() {
  /* Stop cancellation requests before releasing any connection resources. */
  if (m_cancel != nullptr) {
    m_cancel->detach();
    m_cancel = nullptr;
  }

  /* The statement stops before the snapshot it reads goes away. */
  query_cursor::destroy(m_query);

  /* Disconnect rolls back the read transaction. Do not run fresh SQL here:
  an application global may close its cursor during process teardown. */
  if (m_conn != nullptr) {
    duckdb_disconnect(&m_conn);
  }

  /* An empty snapshot is safe to release, including after an open failure. */
  if (m_db != nullptr) {
    m_db->m_mt->release_snapshot(m_memtable_snapshot);
  }

  m_query = nullptr;
  m_table_locks.release();

  return;
}

db_cursor::~db_cursor() {
  close();
}

void db_cursor::destroy(db_cursor *cursor) {
  ut_delete(cursor);

  return;
}

/** Release a partially opened cursor, including its snapshot pin. */
class db::cursor_guard final : private ut_noncopyable {
 public:
  /** Cursor to close; NULL once handed over. */
  db_cursor *m_cursor{nullptr};

  /** Close a cursor that was not transferred to the caller. */
  ~cursor_guard() { db_cursor::destroy(m_cursor); }
};

/** Reuse metadata for F, or read it in the cursor's own transaction.
@return DB_SUCCESS or the catalog query/validation error */
db_status_t db::query_catalog(duckdb_connection conn, batch_seq_t flushed_seq,
                              std::shared_ptr<const storage_catalog_t> &out) {
  ut_ad(m_snapshot_mutex.is_owned_shared());

  {
    std::lock_guard<std::mutex> lock(m_catalog_snapshot_mutex);
    if (m_catalog_snapshot != nullptr &&
        m_catalog_snapshot->m_flushed_seq == flushed_seq) {
      out = m_catalog_snapshot;
      return (DB_SUCCESS);
    }
  }

  auto loaded = std::make_shared<storage_catalog_t>();
  if (const db_status_t err = storage_catalog_reader(conn, *loaded).read();
      !err.ok()) {
    return (err);
  }

  if (loaded->m_flushed_seq != flushed_seq) {
    return (DB_CORRUPTION);
  }

  out = loaded;
  std::lock_guard<std::mutex> lock(m_catalog_snapshot_mutex);
  /* A slower reader must not replace a newer cached snapshot. */
  if (m_catalog_snapshot == nullptr ||
      m_catalog_snapshot->m_flushed_seq <= flushed_seq) {
    m_catalog_snapshot = out;
  }

  return (DB_SUCCESS);
}

db_status_t db_cursor::begin_snapshot(batch_seq_t &flushed_seq) {
  /* Borrow the connection; the cursor still owns it. */
  duckdb_session session(m_conn);

  if (const db_status_t err = session.begin(); !err.ok()) {
    return (err);
  }

  /* Reading F also fixes the transaction's snapshot here and now. */
  duckdb_rows_t rows;
  if (const db_status_t err =
          duckdb_exec(m_conn, "SELECT flushed_seq FROM _pt_state", rows);
      !err.ok()) {
    return (err);
  }

  if (rows.size() != 1 || rows[0][0].m_is_null) {
    return (db_status_t(DB_CORRUPTION, "bad _pt_state"));
  }

  flushed_seq = static_cast<batch_seq_t>(rows[0][0].m_i);

  return (DB_SUCCESS);
}

/** Wait for shared snapshot access, allowing a query to cancel the wait. */
db_status_t db_cursor::lock_snapshot(
    std::shared_lock<ut_shared_mutex_t> &lock) {
  ut_ad(!lock.owns_lock());
  ut_ad(lock.mutex() == &m_db->m_snapshot_mutex);

  if (m_cancel == nullptr) {
    lock.lock();

    return (DB_SUCCESS);
  }

  /* Retention may hold this lock. Check cancellation while it remains held,
  so abandoning a query does not have to wait for partition deletion. */
  while (!lock.try_lock()) {
    if (m_cancel->requested()) {
      return (DB_INTERRUPTED);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  /* Cancellation may arrive just as the lock becomes available. */
  if (m_cancel->requested()) {
    lock.unlock();

    return (DB_INTERRUPTED);
  }

  return (DB_SUCCESS);
}

/** Take the snapshot of a cursor: the DuckDB transaction at F and the
memtable writes after F. A deletion of stored data waits until it is fixed.
@return DB_SUCCESS, DB_FENCED, DB_CORRUPTION or a DuckDB snapshot error */
db_status_t db_cursor::take_snapshot() {
  db &database = *m_db;

  std::shared_lock<ut_shared_mutex_t> snapshots(database.m_snapshot_mutex,
                                                std::defer_lock);
  if (const db_status_t err = lock_snapshot(snapshots); !err.ok()) {
    return (err);
  }

  if (const db_status_t err = database.check_status(); !err.ok()) {
    return (err);
  }

  /* Pin BEFORE fixing the stored snapshot. A flush can then commit and
  retire at any point without making the reader lose memtable rows. */
  database.m_mt->pin_snapshot(m_memtable_snapshot);

  batch_seq_t flushed_seq = 0;
  if (const db_status_t err = begin_snapshot(flushed_seq); !err.ok()) {
    return (err);
  }

  std::shared_ptr<const storage_catalog_t> stored;
  if (const db_status_t err =
          database.query_catalog(m_conn, flushed_seq, stored);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err =
          database.m_mt->complete_snapshot(flushed_seq, m_memtable_snapshot);
      !err.ok()) {
    return (err);
  }

  build_tables(*stored);

  return (database.check_status());
}

/** Allocate a cursor with a DuckDB connection of its own, owned by a guard.
@return DB_SUCCESS, DB_OUT_OF_MEMORY or DB_ERROR */
db_status_t db::cursor_create(cursor_guard &owner, query_cancel *cancel) {
  owner.m_cursor = ut_new<db_cursor>();
  if (owner.m_cursor == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  db_cursor &cursor = *owner.m_cursor;
  cursor.m_db = this;

  if (duckdb_connect(m_storage->database(), &cursor.m_conn) != DuckDBSuccess) {
    cursor.m_conn = nullptr;
    return (db_status_t(DB_ERROR, "cannot connect to DuckDB"));
  }

  cursor.m_cancel = cancel;
  if (cancel != nullptr) {
    cancel->attach(cursor.m_conn);
  }

  return (DB_SUCCESS);
}

/** Select known time-series sources, then acquire their shared locks in order.
 */
db_status_t db::lock_query_tables(const std::vector<std::string> &names,
                                  db_table_locks &locks,
                                  const query_cancel *cancel) {
  ut_a(locks.m_locks.empty());

  /* Collect the full set before locking. CTE names and ordinary tables do not
  need time-series locks; the set sorts and deduplicates the remaining names. */
  std::set<std::string> requests;
  for (const std::string &name : names) {
    const db_table_t *table = m_tables.find(name);
    if (table != nullptr && table->schema() != nullptr) {
      requests.insert(name);
    }
  }

  /* Every query takes table locks in the same order to avoid deadlocks. */
  for (const std::string &name : requests) {
    locks.m_locks.emplace_back();
    if (const db_status_t err = lock_table(name, db_table_lock_mode_t::SHARED,
                                           locks.m_locks.back(), cancel);
        !err.ok()) {
      locks.m_locks.pop_back();

      return (err);
    }
  }

  return (DB_SUCCESS);
}

/** Detect newly visible sources without taking locks out of order.
A missing mode requires releasing the old snapshot and reacquiring all modes. */
bool db_cursor::tables_locked(const std::vector<std::string> &names) const {
  for (const auto &name : names) {
    if (m_snapshot.m_tables.count(name) == 0) {
      continue;
    }

    const db_table_t *table = m_db->m_tables.find(name);
    ut_a(table != nullptr);
    if (!m_table_locks.holds(table)) {
      return (false);
    }
  }

  return (true);
}

/** Start a SELECT, or EXPLAIN of it, over one consistent snapshot, for reading
its rows chunk by chunk. */
db_status_t db::cursor_open(const std::string &sql, bool explain,
                            db_cursor **out, query_cancel *cancel) {
  *out = nullptr;

  if (cancel != nullptr && cancel->requested()) {
    return (DB_INTERRUPTED);
  }

  cursor_guard owner;

  if (const db_status_t err = cursor_create(owner, cancel); !err.ok()) {
    return (err);
  }

  db_cursor *cursor = owner.m_cursor;

  /* Check the statement before taking a snapshot for it. */
  query_ranges_t ranges;
  std::vector<std::string> tables;
  if (const db_status_t err =
          query_analyze(cursor->m_conn, sql, ranges, tables);
      !err.ok()) {
    return (err);
  }

  for (;;) {
    if (cancel != nullptr && cancel->requested()) {
      return (DB_INTERRUPTED);
    }

    if (const db_status_t err = check_status(); !err.ok()) {
      return (err);
    }

    if (const db_status_t err =
            lock_query_tables(tables, cursor->m_table_locks, cancel);
        !err.ok()) {
      return (err);
    }

    if (const db_status_t err = cursor->take_snapshot(); !err.ok()) {
      return (err);
    }

    if (cursor->tables_locked(tables)) {
      break;
    }

    /* Drop the transaction, pin and old modes before taking a new sorted set.
     */
    db_cursor::destroy(owner.m_cursor);

    owner.m_cursor = nullptr;
    if (const db_status_t err = cursor_create(owner, cancel); !err.ok()) {
      return (err);
    }

    cursor = owner.m_cursor;
  }

  if (cancel != nullptr && cancel->requested()) {
    return (DB_INTERRUPTED);
  }

  if (const db_status_t err =
          query_cursor::open(cursor->m_conn, cursor->m_snapshot, sql, explain,
                             ranges, &cursor->m_query);
      !err.ok()) {
    return (err);
  }

  if (cancel != nullptr) {
    cancel->detach();

    cursor->m_cancel = nullptr;

    if (cancel->requested()) {
      return (DB_INTERRUPTED);
    }
  }

  *out = cursor;
  owner.m_cursor = nullptr;

  return (DB_SUCCESS);
}

/** Read the column metadata of a cursor. */
const std::vector<query_column_t> &db_cursor::columns() const {
  return (m_query->columns());
}

/** Read the next rows of a cursor, at most QUERY_CHUNK_ROWS. */
db_status_t db_cursor::fetch(query_rows_t &rows) {
  return (m_query->fetch(rows));
}

/** Show DuckDB's plan for a SELECT, as a cursor would run it. */
db_status_t db::explain(const std::string &sql, std::string &plan) {
  plan.clear();

  db_cursor *cursor = nullptr;

  db_status_t err = cursor_open(sql, true, &cursor);
  if (!err.ok()) {
    return (err);
  }

  query_rows_t rows;

  for (;;) {
    err = cursor->fetch(rows);
    if (!err.ok() || rows.empty()) {
      break;
    }

    for (size_t r = 0; r < rows.size(); ++r) {
      for (size_t c = 0; c < rows[r].size(); ++c) {
        plan += rows[r][c].m_s + "\n";
      }
    }
  }

  db_cursor::destroy(cursor);

  if (!err.ok()) {
    plan.clear();
  }

  return (err);
}
