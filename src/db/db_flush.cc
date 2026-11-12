/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_flush.cc
 *    Flush, retention expiry and background maintenance.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "memtable.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_shared_mutex.h"
#include "ut_time.h"
#include "wal.h"

/** Reclaim stored WAL segments even when a flush has no new batches. A failed
deletion or directory sync is retried on the next flush. */
void db::purge_wal(db_flush_info_t &info) {
  ut_ad(m_flush_mutex.is_owned());

  info.m_cleanup_status =
      m_wal->purge(info.m_flushed_seq, info.m_segments_removed);

  return;
}

/** Store every visible, unflushed write.
@return DB_SUCCESS, a rolled-back preparation error, DB_CORRUPTION, DB_FENCED,
  or DB_OUTCOME_UNKNOWN after commit or rollback cannot be confirmed */
db_status_t db::flush_low(db_flush_info_t &info) {
  ut_ad(m_flush_mutex.is_owned());

  if (const db_status_t err = check_status(); !err.ok()) {
    return (err);
  }

  /* Everything visible now; writes that become visible
  meanwhile wait for the next flush. The batches stay valid until they are
  retired below, which only this flush does. */
  std::vector<memtable_flush_item_t> items;
  m_mt->collect_unflushed(items);

  /* Nothing to store, but an earlier flush may have left segments to purge. */
  if (items.empty()) {
    info.m_flushed_seq = m_storage->catalog().m_flushed_seq;
    purge_wal(info);

    return (DB_SUCCESS);
  }

  /* Make the WAL prefix durable before DuckDB can durably advance F. */
  if (!m_options.m_wal_sync) {
    if (const db_status_t err = m_wal->sync(); !err.ok()) {
      return (err.with_context("cannot sync WAL before flush"));
    }
  }

  /* The memtable and the storage must agree on F. */
  ut_a(items.front().m_seq == m_storage->catalog().m_flushed_seq + 1);

  std::vector<storage_write_t> writes;
  writes.resize(items.size());
  uint64_t rows = 0;

  for (size_t i = 0; i < items.size(); ++i) {
    writes[i].m_seq = items[i].m_seq;
    writes[i].m_batch = items[i].m_batch;
    writes[i].m_schema = items[i].m_schema.get();
    rows += items[i].m_batch->m_n_rows;
  }

  /* Readers pin before fixing their stored snapshot, so storage can commit
  without a PulseTen reader lock. Publish no memtable retirement until the
  complete storage transaction succeeds. */
  if (const db_status_t err = m_storage->flush(writes.data(), writes.size());
      !err.ok()) {
    if (err.code() == DB_OUTCOME_UNKNOWN || err.code() == DB_CORRUPTION) {
      fail(err);
    }

    return (err);
  }

  ut_a(m_storage->catalog().m_flushed_seq == items.back().m_seq);

  /* Trimming may free the batches. */
  m_mt->trim(items.back().m_seq);

  info.m_rows = rows;
  info.m_writes = items.size();
  info.m_flushed_seq = items.back().m_seq;

  purge_wal(info);

  return (DB_SUCCESS);
}

/** Store every visible, unflushed write in DuckDB in one transaction and retire
it from the memtable. */
db_status_t db::flush(db_flush_info_t &info) {
  info = db_flush_info_t();

  ut_lock_guard_t flush_lock(m_flush_mutex);

  const db_status_t err = flush_low(info);
  {
    std::lock_guard<std::mutex> lock(m_flusher_mutex);

    m_last_flush_status = err;
    ++m_progress_epoch;
    m_memtable_space_cv.notify_all();
  }

  return (err);
}

/** Drop the stored partitions whose days have all passed the retention period,
measured back from a given time. */
db_status_t db::expire(int64_t now_us, uint64_t &removed) {
  removed = 0;

  /* 0 keeps all data. */
  if (m_options.m_retention_days == 0) {
    return (DB_SUCCESS);
  }

  /* Serialize expiry with the complete flush transaction. */
  ut_lock_guard_t flush_lock(m_flush_mutex);

  if (const db_status_t err = check_status(); !err.ok()) {
    return (err);
  }

  /* Partitions ending on or before this day hold only expired times. */
  const int64_t cutoff =
      now_us - static_cast<int64_t>(m_options.m_retention_days) * UT_DAY_US;
  const int64_t end_day = ut_utc_day(cutoff);

  /* No logical change: do not invalidate snapshots on every timer tick. */
  const storage_catalog_t &catalog = m_storage->catalog();
  bool has_expired = false;
  for (const std::pair<const std::pair<uint64_t, int64_t>, storage_partition_t>
           &part : catalog.m_partitions) {
    if (part.second.m_first_day + catalog.m_partition_duration_days <=
        end_day) {
      has_expired = true;
      break;
    }
  }

  if (!has_expired) {
    return (DB_SUCCESS);
  }

  /* F does not change on expiry. No snapshot is fixed while the deletion
  runs, so none combines the old stored data with writes after it; cursors
  fixed earlier keep their DuckDB MVCC view. */
  std::unique_lock<ut_shared_mutex_t> snapshots(m_snapshot_mutex);

  /* Recheck after waiting for the snapshot lock. */
  if (const db_status_t err = check_status(); !err.ok()) {
    return (err);
  }

  const db_status_t err = m_storage->expire(end_day, removed);
  /* A rolled-back expiry can simply run again; an uncertain commit cannot. */
  if (err.code() == DB_OUTCOME_UNKNOWN) {
    fail(err);
  }

  if (err.ok() && removed > 0) {
    {
      std::lock_guard<std::mutex> lock(m_catalog_snapshot_mutex);
      m_catalog_snapshot.reset();
    }

    std::lock_guard<std::mutex> lock(m_flusher_mutex);
    m_stats.m_expired_partitions += removed;
  }

  return (err);
}

/** Wait until the next round is due.
@param[in,out]  lock  holds db.m_flusher_mutex
@return false if the flusher must stop */
bool db::flusher_wait(std::unique_lock<std::mutex> &lock) {
  ut_ad(lock.owns_lock());

  const std::chrono::milliseconds interval(m_options.m_flush_interval_ms);

  /* A wake-up during the last round must not wait a whole interval. */
  if (!m_flusher_stop && !m_flusher_wake) {
    m_flusher_wake_cv.wait_for(lock, interval);
  }

  if (m_flusher_stop) {
    return (false);
  }

  m_flusher_wake = false;

  return (true);
}

/** Add what one task failed with to the message of the round.
@param[in,out] result accumulated errors from this round
@param[in]  task   name of the task
@param[in]  status  outcome of the task */
static void db_append_background_error(db_status_t &result, const char *task,
                                       const db_status_t &status) {
  if (status.ok() || status.code() == DB_FENCED) {
    return;
  }

  const db_status_t failure = status.with_context(task);
  if (result.ok()) {
    result = failure;
    return;
  }

  result =
      db_status_t(result.code(), result.message() + "; " + failure.message());

  return;
}

/** Flush, expire old partitions, and record the round's result.
REQUIRES: db.m_flusher_mutex is not held. */
void db::flusher_flush_and_expire() {
  db_status_t result;
  db_flush_info_t info;

  const db_status_t flushed = flush(info);
  const bool stored = flushed.ok() && info.m_writes > 0;
  db_append_background_error(result, "flush", flushed);
  db_append_background_error(result, "WAL cleanup", info.m_cleanup_status);

  /* Flush returns with no prepared transaction. An unsafe storage outcome
  has already stopped the database; otherwise expiry can run independently. */
  if (check_status().ok()) {
    uint64_t removed = 0;

    const db_status_t expired = expire(ut_now_us(), removed);
    db_append_background_error(result, "expire", expired);
  }

  /* I/O is finished; lock only to update shared counters and the last error. */
  std::lock_guard<std::mutex> lock(m_flusher_mutex);

  /* An expiry failure does not undo the completed flush. */
  if (stored) {
    m_stats.m_background_flushes++;
  }

  if (!result.ok()) {
    m_stats.m_background_errors++;

    m_last_background_error = std::move(result);
  }

  return;
}

/** Background flusher: flush every interval, or when woken.
@return NULL */
void *db::flusher_main(void *arg) {
  db &database = *static_cast<db *>(arg);

  std::unique_lock<std::mutex> lock(database.m_flusher_mutex);

  while (database.flusher_wait(lock)) {
    /* Flush and expiry also acquire the flusher mutex internally. */
    lock.unlock();

    if (!database.check_status().ok()) {
      /* A stopped instance cannot perform maintenance. Only close wakes it. */
      lock.lock();
      database.m_flusher_wake_cv.wait(
          lock, [&] { return (database.m_flusher_stop); });
      break;
    }

    database.flusher_flush_and_expire();

    /* Reacquire the mutex to check stop and wait for the next round. */
    lock.lock();
  }

  return (nullptr);
}

/** Read the message of the latest background failure. */
db_status_t db::background_status() {
  std::lock_guard<std::mutex> lock(m_flusher_mutex);

  return (m_last_background_error);
}

/** Read the database counters. */
void db::read_stats(db_stats_t &stats) {
  {
    std::lock_guard<std::mutex> lock(m_flusher_mutex);
    stats = m_stats;
    stats.m_duckdb_memory_limit_bytes = m_options.m_duckdb_memory_limit_bytes;
  }

  memtable_stats_t mt_stats;
  m_mt->read_stats(mt_stats);
  stats.m_flushed_seq = mt_stats.m_flushed_seq;
  stats.m_visible_seq = mt_stats.m_visible_seq;
  stats.m_memtable_bytes = mt_stats.m_live_bytes;

  wal_stats_t log;
  m_wal->read_stats(log);
  stats.m_wal_segment_count = log.m_segments;

  return;
}
