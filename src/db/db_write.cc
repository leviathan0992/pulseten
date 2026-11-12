/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_write.cc
 *    Write planning, WAL commit and memtable publication.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "db_table.h"
#include "duckdb_database.h"
#include "ingest.h"
#include "ingest_codec.h"
#include "memtable.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_noncopyable.h"
#include "ut_time.h"
#include "wal.h"

/** Context of one write, owned until commit and publication finish. */
class db::write_context final : private ut_noncopyable {
 private:
  /** The database coordinates this write with WAL and memtable state. */
  friend class db;

  /** Retain the database that owns queueing and table-lock policy. */
  explicit write_context(db &database) : m_db(database) {}

  /** Owner, alive until this write returns. */
  db &m_db;

  /** Plan under S, taking X and replanning if the schema must change. */
  db_status_t plan();

  /** Database-owned table, valid until database close. */
  db_table_t *m_target{nullptr};

  /** Shared for existing columns, exclusive for schema changes, through
  WAL commit and publication. */
  db_table_lock m_table_lock;

  /** Normalized table name. */
  std::string m_table_name;

  /** The input points. */
  const ingest_point_t *m_points{nullptr};

  /** Number of input points. */
  uint32_t m_n_points{0};

  /** Validated schema change and owned column batch. */
  ingest_plan_t m_plan;

  /** Encoded WAL record; stays valid until the WAL wait returns. */
  std::string m_payload;

  /** Ordered sequence, reset to zero if this write is certainly not logged. */
  batch_seq_t m_seq{0};

  /** The leader has completed or rejected this write. */
  bool m_done{false};

  /** Completion result, protected by db.m_write_mutex. */
  db_status_t m_status;

  /** Wakes this writer on completion or when it becomes group leader. */
  ut_condition_variable_t m_ready;
};

/** Keep the schema stable while planning and encoding the WAL payload.
@return DB_SUCCESS, or the write validation error; no WAL record is queued
  here */
db_status_t db::write_context::plan() {
  schema_ref_t schema = m_target->schema();

  if (const db_status_t err =
          ingest_plan(schema.get(), m_table_name, m_points, m_n_points, m_plan);
      !err.ok()) {
    return (err);
  }

  if (!m_plan.m_new_cols.empty()) {
    /* Drop S before waiting for X so concurrent creators can progress. */
    m_table_lock.release();
    if (const db_status_t err = m_db.lock_table(
            m_table_name, db_table_lock_mode_t::EXCLUSIVE, m_table_lock);
        !err.ok()) {
      return (err);
    }

    /* Replan against changes committed while switching from S to X. */
    schema = m_target->schema();
    if (const db_status_t err = ingest_plan(schema.get(), m_table_name,
                                            m_points, m_n_points, m_plan);
        !err.ok()) {
      return (err);
    }
  }

  ut_a(m_plan.m_table_name == m_table_name);

  ingest_encode(m_plan, m_payload);
  ut_a(!m_payload.empty());

  return (DB_SUCCESS);
}

/** Reject a user table using DuckDB's current catalog while the name is
S-locked. The common existing time-series path needs no catalog connection.
@return DB_SUCCESS, DB_INVALID_ARGUMENT or a connection/catalog error */
db_status_t db::check_write_target(const db_table_t &table,
                                   const std::string &name) {
  if (table.schema() != nullptr) {
    return (DB_SUCCESS);
  }

  duckdb_connection conn = nullptr;
  if (duckdb_connect(m_storage->database(), &conn) != DuckDBSuccess) {
    return (db_status_t(DB_ERROR, "cannot inspect the write target"));
  }

  duckdb_rows_t rows;
  const db_status_t err = duckdb_exec(
      conn,
      "SELECT count(*) FROM information_schema.tables "
      "WHERE table_catalog = current_database() AND table_schema = 'main' "
      "AND lower(table_name) = " +
          duckdb_literal(name),
      rows);

  duckdb_disconnect(&conn);
  if (!err.ok()) {
    return (err);
  }

  if (rows.size() != 1 || rows[0].size() != 1 || rows[0][0].m_is_null) {
    return (DB_CORRUPTION);
  }

  if (rows[0][0].m_i != 0) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "user tables cannot receive time-series writes"));
  }

  return (DB_SUCCESS);
}

/** Wait for memtable capacity, waking the background flusher as needed.
@return DB_SUCCESS if admission is allowed, DB_FENCED, or the last flush error
  if the memtable is still full */
db_status_t db::wait_for_memtable_space() {
  bool waited = false;

  for (;;) {
    uint64_t epoch;
    {
      std::lock_guard<std::mutex> lock(m_flusher_mutex);
      epoch = m_progress_epoch;
    }

    if (const db_status_t err = check_status(); !err.ok()) {
      return (err);
    }

    if (m_mt->has_space()) {
      return (DB_SUCCESS);
    }

#ifdef PT_TESTING
    if (m_test_before_space_wait != nullptr) {
      auto callback = m_test_before_space_wait;
      m_test_before_space_wait = nullptr;
      callback(*this);
    }
#endif /* PT_TESTING */

    std::unique_lock<std::mutex> lock(m_flusher_mutex);
    if (epoch != m_progress_epoch) {
      continue;
    }

    m_flusher_wake = true;
    m_flusher_wake_cv.notify_one();
    /* A failed attempt may be retried, but this caller must not be trapped. */
    if (!m_last_flush_status.ok()) {
      return (m_last_flush_status);
    }

    if (!waited) {
      m_stats.m_throttled_writes++;
      waited = true;
    }

    m_memtable_space_cv.wait(lock, [&] { return (epoch != m_progress_epoch); });
  }
}

/** Refuse points older than the retention period; they would only be
dropped again.
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
db_status_t db::check_retention(const ingest_point_t *points,
                                uint32_t n_points) const {
  const uint32_t days = m_options.m_retention_days;

  /* 0 keeps all data. */
  if (days == 0) {
    return (DB_SUCCESS);
  }

  const int64_t cutoff = ut_now_us() - static_cast<int64_t>(days) * UT_DAY_US;

  for (uint32_t p = 0; p < n_points; ++p) {
    if (points[p].m_time_us < cutoff) {
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "point " + std::to_string(p) +
                              " is older than the retention period of " +
                              std::to_string(days) + " days"));
    }
  }

  return (DB_SUCCESS);
}

/** Allocate the batch before enqueueing it in the database write stream.
Every accepted write must reach commit_write() to complete its queue lifetime.
@return DB_SUCCESS, a validation/allocation error before enqueue, or DB_FENCED
*/
db_status_t db::reserve_write(write_context &w) {
  ut_lock_guard_t lock(m_write_mutex);

  if (const db_status_t err = check_status_low(); !err.ok()) {
    return (err);
  }

  ut_ad(w.m_plan.m_new_cols.empty() || w.m_table_lock.exclusive());
  if (m_next_seq == UINT64_MAX) {
    return (DB_LIMIT_EXCEEDED);
  }

  /* Allocate and validate before accepting a record into the WAL queue. */
  if (const db_status_t err = m_mt->reserve(m_next_seq, w.m_plan); !err.ok()) {
    return (err);
  }

  if (!w.m_plan.m_new_cols.empty()) {
    /* Share the memtable's updated schema with the table cache. */
    w.m_target->set_schema(m_mt->find_schema(w.m_table_name));
  }

  w.m_seq = m_next_seq++;
  m_writes.push_back(&w);

  return (DB_SUCCESS);
}

/** Complete a selected prefix after its leader has stopped borrowing payloads.
 */
void db::complete_writes(size_t count, const db_status_t &status,
                         bool keep_sequence) {
  ut_ad(m_write_mutex.is_owned());
  ut_a(count <= m_writes.size());

  for (size_t i = 0; i < count; ++i) {
    write_context *writer = m_writes.front();
    m_writes.pop_front();

    writer->m_status = status;
    writer->m_done = true;

    if (!keep_sequence) {
      writer->m_seq = 0;
    }

    writer->m_ready.notify_one();
  }

  return;
}

/** Publish or fail the active group, then release its borrowed queue nodes.
The WAL no longer borrows the group. */
void db::finish_write_group(size_t count, batch_seq_t last_seq,
                            const db_status_t &status) {
  ut_ad(m_write_mutex.is_owned());
  ut_a(m_write_active);

  if (!status.ok()) {
    /* Even a pre-I/O error leaves reserved sequences that cannot be skipped. */
    fail_low(status);
  }

  const bool may_be_logged = status.ok() || status.code() == DB_OUTCOME_UNKNOWN;

  db_status_t result = status;

  if (m_db_status.ok()) {
    /* No fatal error occurred during WAL I/O; make this group visible. */
    m_mt->publish(last_seq);
  } else if (status.ok()) {
    /* Another operation stopped publication while this WAL group was in I/O. */
    result = m_db_status.with_code(DB_OUTCOME_UNKNOWN);
  }

  complete_writes(count, result, may_be_logged);

  m_write_active = false;

  return;
}

/** Select a bounded queue prefix; a single large write still makes progress.
The records borrow payloads until finish_write_group() completes the group. */
void db::build_write_group(std::vector<wal_record_t> &records) {
  ut_ad(m_write_mutex.is_owned());
  ut_a(!m_write_active);
  ut_a(!m_writes.empty());

  records.clear();

  size_t bytes = 0;
  for (write_context *next : m_writes) {
    const size_t record_bytes = WAL_RECORD_HEADER_SIZE + next->m_payload.size();
    if (!records.empty() && (bytes >= DB_WRITE_GROUP_BYTES ||
                             record_bytes > DB_WRITE_GROUP_BYTES - bytes)) {
      break;
    }

    wal_record_t record;
    record.m_seq = next->m_seq;
    record.m_payload =
        reinterpret_cast<const unsigned char *>(next->m_payload.data());
    record.m_len = next->m_payload.size();
    records.push_back(record);
    bytes += record_bytes;
  }

  return;
}

/** Wait for the group commit of a reserved write, then make it visible.
@param[in,out]  seq  sequence number of the write; reset to 0 if the write
                     was certainly not written
@retval DB_SUCCESS logged and visible
@retval DB_OUTCOME_UNKNOWN the write may or may not be durable; the database is
  stopped until reopen
@return otherwise the error of a write that was certainly not written */
db_status_t db::commit_write(write_context &w, batch_seq_t &seq) {
  ut_unique_lock_t lock(m_write_mutex);

  /* Wait until another leader completes this write or it becomes queue head. */
  while (!w.m_done && m_writes.front() != &w) {
    w.m_ready.wait(lock);
  }

  if (!w.m_done) {
    /* Only the queue head decides whether to submit. A failed database makes
    this writer leave the queue without touching the WAL. */
    if (const db_status_t err = check_status_low(); !err.ok()) {
      complete_writes(1, err, false);
    } else {
      /* This writer is now the leader and selects the next group to commit. */
      std::vector<wal_record_t> records;
      build_write_group(records);

      m_write_active = true;

#ifdef PT_TESTING
      void (*before_wal)(void *) = m_test_before_wal;
      void *context = m_test_wal_context;
#endif /* PT_TESTING */

      /* The leader owns this queue prefix while WAL I/O runs without the mutex.
       */
      lock.unlock();

#ifdef PT_TESTING
      if (before_wal != nullptr) {
        before_wal(context);
      }
#endif /* PT_TESTING */

      /* Queue nodes and payloads remain owned until the leader completes I/O.
       */
      const db_status_t status = m_wal->append(records.data(), records.size());

      lock.lock();

      finish_write_group(records.size(), records.back().m_seq, status);
    }

    /* Hand off on both success and failure. Pending writers check the database
    when they reach the head; followers already completed by a group just
    return. */
    if (!m_writes.empty()) {
      m_writes.front()->m_ready.notify_one();
    }
  }

  seq = w.m_seq;

  return (w.m_status);
}

/** Write a batch of points to a table; thread-safe. */
db_status_t db::write(const char *table_name, size_t table_name_length,
                      const ingest_point_t *points, uint32_t n_points,
                      batch_seq_t &seq) {
  seq = 0;

  write_context w(*this);

  /* Validate and normalize the name before looking up its table entry. */
  if (!schema_normalize_table_name(table_name, table_name_length,
                                   w.m_table_name)
           .ok()) {
    return (db_status_t(DB_INVALID_ARGUMENT, "invalid table name"));
  }

  /* Reject expired points before waiting for memtable capacity. */
  if (const db_status_t err = check_retention(points, n_points); !err.ok()) {
    return (err);
  }

  /* Reject a stopped database before creating table state or waiting on locks.
   */
  if (const db_status_t err = check_status(); !err.ok()) {
    return (err);
  }

  /* Allow concurrent writes to existing columns while blocking schema changes.
  If new columns are needed, plan() releases this shared lock and takes an
  exclusive lock. */
  if (const db_status_t err = lock_table(
          w.m_table_name, db_table_lock_mode_t::SHARED, w.m_table_lock);
      !err.ok()) {
    return (err);
  }

  w.m_target = w.m_table_lock.table();

  /* Reject SQL-created user tables before capacity waits, with DDL excluded. */
  if (const db_status_t err = check_write_target(*w.m_target, w.m_table_name);
      !err.ok()) {
    return (err);
  }

  /* Wait for flush progress to free capacity before preparing this write. */
  if (const db_status_t err = wait_for_memtable_space(); !err.ok()) {
    return (err);
  }

  w.m_points = points;
  w.m_n_points = n_points;

  /* Schema changes take X here, before acquiring the global write lock. */
  if (const db_status_t err = w.plan(); !err.ok()) {
    return (err);
  }

  /* Hold the write lock to keep WAL and memtable writes in the same order. */
  if (const db_status_t err = reserve_write(w); !err.ok()) {
    return (err);
  }

  /* The database leader commits and publishes a group without locking I/O
   * waits. */
  return (commit_write(w, seq));
}
