/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine.cc
 *    The write path and WAL replay.
 *
 *-------------------------------------------------------------------------
 */

#include "engine.h"

#include <pthread.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "duckdb_database.h"
#include "ingest.h"
#include "ingest_codec.h"
#include "memtable.h"
#include "os_file.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_new.h"
#include "ut_time.h"
#include "wal.h"
#include "wal_recv.h"

/** File locked while a directory is open, so that only one engine uses
it at a time. */
#define ENGINE_LOCK_FILE "LOCK"

/** An open database directory. */
struct engine_t {
  /* Fixed once engine_open() returns. */

  /** Holds the directory's lock. */
  os_file_t m_lock_file{OS_FILE_CLOSED};

  /** The write-ahead log. */
  wal_t *m_wal{nullptr};

  /** Writes after F and every table's schema. */
  memtable_t *m_mt{nullptr};

  /** The DuckDB file with the writes up to F. */
  storage_t *m_storage{nullptr};

  /** Settings given to engine_open(), checked. */
  engine_options_t m_options;

  /** Background flusher thread. */
  pthread_t m_flusher_thread{};

  /** m_flusher_thread runs. */
  bool m_flusher_started{false};

  /* Protected by m_write_mutex. */

  /** Orders writes: queuing in the WAL and reserving in the memtable
  happen together under it. Taken before the catalog, WAL and memtable locks,
  never while holding them, and never held while waiting for a group
  commit. */
  ut_mutex_t m_write_mutex;

  /** What fenced the engine; the first failure wins. Success until the WAL, the
  memtable and the storage may disagree; after that every write and flush is
  rejected until the directory is reopened. */
  db_status_t m_fence_status;

  /* Protected by m_catalog_mutex. */

  /** Allows concurrent name checks; DDL changes take it exclusively.
  Taken after m_write_mutex when both locks are needed. */
  std::shared_mutex m_catalog_mutex;

  /** Ordinary table names, separate from the storage catalog that flushes
  replace without holding the write lock. */
  std::set<std::string> m_user_tables;

  /** Target reserved by the active ordinary DDL; empty when none is running.
  DDL also holds m_write_mutex while reserving the name. */
  std::string m_ddl_table;

  /* Protected by m_query_catalog_mutex. */

  /** Guards the cached reader catalog; never held across DuckDB calls.
  Taken after m_snapshot_mutex when both are needed. */
  std::mutex m_query_catalog_mutex;

  /** Immutable metadata read in a query transaction, matched by F.
  Expiry clears it because deleting partitions does not change F. */
  std::shared_ptr<const storage_catalog_t> m_query_catalog;

  /* Locks that protect work, not fields. */

  /** Serializes ordinary DDL, including its target-name reservation.
  Taken before snapshot creation and the short flush/write commit locks. */
  std::mutex m_ddl_mutex;

  /** Allows one flush, expiry or ordinary DDL commit at a time. Taken before
  m_snapshot_mutex and m_write_mutex. */
  ut_mutex_t m_flush_mutex;

  /** Held shared while a cursor fixes its snapshot, and exclusively during
  partition deletion and ordinary DDL commit. Taken after m_flush_mutex and
  before m_write_mutex. */
  std::shared_mutex m_snapshot_mutex;

  /* Protected by m_flusher_mutex, a leaf lock. */

  /** Protects the fields below. */
  std::mutex m_flusher_mutex;

  /** Wakes the flusher early. */
  std::condition_variable m_flusher_wake_cv;

  /** Wakes writers waiting for room after a flush. */
  std::condition_variable m_memtable_room_cv;

  /** engine_close() asks the flusher to exit. */
  bool m_flusher_stop{false};

  /** Someone asked the flusher to flush now. */
  bool m_flusher_wake{false};

  /** Message of the latest background failure that was not a fence. */
  db_status_t m_background_status;

  /** Write rejection after the last failed flush; a successful retry clears
  it. An uncertain flush commit fences writes that have not been queued. */
  db_status_t m_flush_error{DB_SUCCESS};

  /** Counters; m_wal_segments is read from the WAL when asked for. */
  engine_stats_t m_stats;

#ifdef PT_DEBUG
  /** Optional test callback before DDL execution, protected by m_ddl_mutex. */
  void (*m_dbg_before_ddl)(void *){nullptr};

  /** Borrowed context passed to m_dbg_before_ddl while the DDL is running. */
  void *m_dbg_ddl_context{nullptr};

  /** Optional test callback after pinning a cursor's memtable snapshot.
  Set only while no cursor is opening. */
  void (*m_dbg_after_snapshot_pin)(void *){nullptr};

  /** Borrowed context passed to m_dbg_after_snapshot_pin. */
  void *m_dbg_snapshot_context{nullptr};
#endif
};

/** State of a WAL replay. */
struct engine_replay_t {
  /** Memtable the records are replayed into. */
  memtable_t *m_mt{nullptr};

  /** Writes up to here are in storage and are skipped. */
  batch_seq_t m_flushed_seq{0};

  /** Records replayed so far. */
  uint64_t m_replayed{0};
};

/** Replay one WAL record into the memtable, unless it is already stored.
Records after F must be consecutive.
@return DB_SUCCESS, DB_CORRUPTION, or the error of the failed step */
static db_status_t engine_replay(void *ctx, batch_seq_t seq,
                                 const unsigned char *payload, size_t len) {
  engine_replay_t *replay = static_cast<engine_replay_t *>(ctx);

  /* Already in DuckDB; its segment just has not been purged yet. */
  if (seq <= replay->m_flushed_seq) {
    return (DB_SUCCESS);
  }

  /* Every write after F must be in the WAL, without a gap. */
  if (seq != replay->m_flushed_seq + replay->m_replayed + 1) {
    return (DB_CORRUPTION);
  }

  ingest_plan_t plan;

  db_status_t err = ingest_decode(payload, len, plan);
  if (!err.ok()) {
    return (err);
  }

  err = memtable_reserve(*replay->m_mt, seq, plan);
  if (!err.ok()) {
    return (err);
  }

  replay->m_replayed++;

  /* Replay is in order, so this never waits. */
  return (memtable_publish(*replay->m_mt, seq));
}

/** Take the directory lock.
@return DB_SUCCESS, DB_LOCKED if another opener holds the lock, or
  DB_IO_ERROR */
static db_status_t engine_lock_dir(const char *dir, os_file_t &file) {
  const std::string path = std::string(dir) + "/" + ENGINE_LOCK_FILE;

  db_status_t err = os_file_open(path.c_str(), OS_FILE_OPEN_OR_CREATE, file);
  if (!err.ok()) {
    return (err);
  }

  err = os_file_lock(file);
  if (!err.ok()) {
    static_cast<void>(os_file_close(file));
  }

  return (err);
}

/** Load the stored tables into the memtable, then replay the WAL records
after F, or create the WAL of a new directory.
@return DB_SUCCESS, or the memtable/WAL recovery error; DB_CORRUPTION if
  storage and WAL boundaries disagree */
static db_status_t engine_recover(engine_t &engine, const char *dir) {
  const storage_catalog_t &catalog = storage_catalog(*engine.m_storage);

  db_status_t err = memtable_create(catalog.m_flushed_seq, &engine.m_mt);
  if (!err.ok()) {
    return (err);
  }

  /* WAL records after F may follow any stored schema version. */
  std::map<std::string, storage_table_t>::const_iterator it;

  for (it = catalog.m_tables.begin(); it != catalog.m_tables.end(); ++it) {
    err = memtable_add_table(*engine.m_mt, it->second.m_versions);
    if (!err.ok()) {
      return (err);
    }
  }

  bool exists = false;
  err = wal_exists(dir, exists);
  if (!err.ok()) {
    return (err);
  }

  if (!exists) {
    /* Stored writes without a WAL: the WAL was lost. */
    if (catalog.m_flushed_seq > 0) {
      return (DB_CORRUPTION);
    }

    err = wal_create(dir, 0, &engine.m_wal, catalog.m_wal_id,
                     engine.m_options.m_wal_sync);
    if (!err.ok()) {
      return (err);
    }

    wal_set_segment_bytes(*engine.m_wal, engine.m_options.m_wal_segment_bytes);

    return (DB_SUCCESS);
  }

  engine_replay_t replay;
  replay.m_mt = engine.m_mt;
  replay.m_flushed_seq = catalog.m_flushed_seq;

  wal_recv_t wal_recv;
  err = wal_open(dir, 0, engine_replay, &replay, &engine.m_wal, wal_recv,
                 catalog.m_wal_id, engine.m_options.m_wal_sync);
  if (!err.ok()) {
    return (err);
  }

  wal_set_segment_bytes(*engine.m_wal, engine.m_options.m_wal_segment_bytes);

  /* Storage cannot hold writes the WAL never had, and the WAL must still
  have every write after F. */
  if (wal_recv.m_last_seq < catalog.m_flushed_seq ||
      wal_recv.m_first_seq > catalog.m_flushed_seq + 1) {
    return (DB_CORRUPTION);
  }

  /* Replay made every write after F visible, in order. */
  memtable_stats_t stats;
  memtable_get_stats(*engine.m_mt, stats);

  ut_a(stats.m_flushed_seq == catalog.m_flushed_seq);
  ut_a(stats.m_visible_seq == wal_recv.m_last_seq);
  ut_a(stats.m_live_writes == replay.m_replayed);

  return (DB_SUCCESS);
}

/** Background flusher: flush every interval, or when woken.
@return NULL */
static void *engine_flusher_main(void *arg);

/** Open an engine's resources in order. engine_open() closes the partially
opened engine on failure.
@return DB_SUCCESS, or the lock, storage, recovery, registration or thread-start
  error */
static db_status_t engine_open_low(const char *dir, engine_t &engine) {
  db_status_t err = engine_lock_dir(dir, engine.m_lock_file);
  if (!err.ok()) {
    return (err);
  }

  err = storage_open(dir, engine.m_options.m_partition_days, &engine.m_storage);
  if (!err.ok()) {
    return (err);
  }

  /* Track ordinary DuckDB tables separately; flush replaces the catalog. */
  engine.m_user_tables = storage_catalog(*engine.m_storage).m_user_tables;

  /* Restore schemas and replay unflushed WAL writes into the memtable. */
  err = engine_recover(engine, dir);
  if (!err.ok()) {
    return (err);
  }

  /* A restored file may conflict with a time-series table present only in
  the PulseTen WAL, so check again after replay and before exposing queries. */
  for (const std::string &name : engine.m_user_tables) {
    if (memtable_get_schema(*engine.m_mt, name) != nullptr) {
      return (db_status_t(
          DB_CORRUPTION,
          "ordinary table conflicts with a recovered time-series table: " +
              name));
    }
  }

  err = query_install_scans(storage_connection(*engine.m_storage));
  if (!err.ok()) {
    return (err);
  }

  if (pthread_create(&engine.m_flusher_thread, nullptr, engine_flusher_main,
                     &engine) != 0) {
    return (db_status_t(DB_ERROR, "cannot start the background flush thread"));
  }

  engine.m_flusher_started = true;

  return (DB_SUCCESS);
}

/** Open a database directory: create its WAL and storage if there are none,
otherwise load the storage and replay the WAL records after F. */
db_status_t engine_open(const char *dir, const engine_options_t *options,
                        engine_t **out) {
  *out = nullptr;

  if (dir == nullptr || dir[0] == '\0') {
    return (db_status_t(DB_INVALID_ARGUMENT, "dir must be a path"));
  }

  const engine_options_t checked_options =
      options != nullptr ? *options : engine_options_t();

  if (checked_options.m_flush_interval_ms == 0) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "background flush interval must be greater than zero"));
  }

  /* Longer than any time a write may have; also keeps the cutoff from
  overflowing. */
  if (checked_options.m_retention_days > ENGINE_MAX_RETENTION_DAYS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "retention period must be at most " +
                            std::to_string(ENGINE_MAX_RETENTION_DAYS) +
                            " days"));
  }

  if (checked_options.m_partition_days < 1 ||
      checked_options.m_partition_days > STORAGE_MAX_PARTITION_DAYS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "partition span must be 1 to " +
                            std::to_string(STORAGE_MAX_PARTITION_DAYS) +
                            " days"));
  }

  db_status_t err = os_dir_create(dir);
  if (!err.ok()) {
    return (err.with_context("cannot prepare database directory '" +
                             std::string(dir) + "'"));
  }

  std::string directory;
  err = os_dir_resolve(dir, directory);
  if (!err.ok()) {
    return (err);
  }
  dir = directory.c_str();

  engine_t *engine = ut_new<engine_t>();
  if (engine == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }
  engine->m_options = checked_options;

  err = engine_open_low(dir, *engine);
  if (!err.ok()) {
    /* Release every resource acquired before the open failed. */
    static_cast<void>(engine_close(engine));

    return (err);
  }

  *out = engine;

  return (DB_SUCCESS);
}

/** Close an engine. */
db_status_t engine_close(engine_t *engine) {
  if (engine == nullptr) {
    return (DB_SUCCESS);
  }

  /* Stop the flusher first; it uses everything below. */
  if (engine->m_flusher_started) {
    {
      std::lock_guard<std::mutex> lock(engine->m_flusher_mutex);
      engine->m_flusher_stop = true;
      engine->m_flusher_wake_cv.notify_one();
    }

    pthread_join(engine->m_flusher_thread, nullptr);
  }

  const db_status_t err = wal_close(engine->m_wal);

  memtable_free(engine->m_mt);
  storage_close(engine->m_storage);
  static_cast<void>(os_file_close(engine->m_lock_file));

  ut_delete(engine);

  return (err);
}

/** One write on its way through the engine. */
struct engine_write_t {
  /** Canonical table name, used for schema lookups. */
  std::string m_table;

  /** The input points. */
  const ingest_point_t *m_points{nullptr};

  /** Number of input points. */
  uint32_t m_n_points{0};

  /** Validated schema change and owned column batch. */
  ingest_plan_t m_plan;

  /** Encoded WAL record; stays valid until the WAL wait returns. */
  std::string m_payload;

  /** Queue entry in the WAL. */
  wal_writer_t m_writer;
};

/** Plan a write against the newest schema and encode its WAL payload.
@return DB_SUCCESS, or the write validation error; no WAL record is queued
  here */
static db_status_t engine_plan_write(engine_t &engine, engine_write_t &w) {
  const schema_t *schema = memtable_get_schema(*engine.m_mt, w.m_table);

  const db_status_t err =
      ingest_plan(schema, w.m_table, w.m_points, w.m_n_points, w.m_plan);
  if (!err.ok()) {
    return (err);
  }

  ut_a(w.m_plan.m_table == w.m_table);

  ingest_encode(w.m_plan, w.m_payload);
  ut_a(!w.m_payload.empty());

  return (DB_SUCCESS);
}

/** A plan that adds no columns fits any later schema, because columns never
change. A schema change must be based on the newest version.
@return true if the plan must be made again */
static bool engine_plan_is_stale(engine_t &engine, const ingest_plan_t &plan) {
  ut_ad(engine.m_write_mutex.is_owned());

  if (plan.m_new_cols.empty()) {
    return (false);
  }

  const schema_t *schema = memtable_get_schema(*engine.m_mt, plan.m_table);
  const uint64_t version = schema != nullptr ? schema->m_version : 0;

  return (version != plan.m_base_version);
}

/** Check whether an earlier failure fenced the engine.
REQUIRES: engine.m_write_mutex is held.
@return true if an earlier failure fenced the engine */
static bool engine_is_fenced_low(const engine_t &engine) {
  ut_ad(engine.m_write_mutex.is_owned());

  return (!engine.m_fence_status.ok());
}

/** Stop all writes after the WAL and the memtable may have diverged.
REQUIRES: engine.m_write_mutex is held.
@param[in]  status  what failed; ignored if the engine is already fenced */
static void engine_fence_low(engine_t &engine, const db_status_t &status) {
  ut_ad(engine.m_write_mutex.is_owned());
  ut_a(!status.ok());

  /* Preserve the first cause; later failures can be its consequences. */
  if (engine.m_fence_status.ok()) {
    engine.m_fence_status = status;
  }

  memtable_fail(*engine.m_mt);
}

/** Stop all writes after the WAL and the memtable may have diverged.
@param[in]  status  what failed; ignored if the engine is already fenced */
static void engine_fence(engine_t &engine, const db_status_t &status) {
  ut_lock_guard_t lock(engine.m_write_mutex);

  engine_fence_low(engine, status);
}

/** Check whether an earlier failure fenced the engine.
@return true if an earlier failure fenced the engine */
static bool engine_is_fenced(engine_t &engine) {
  ut_lock_guard_t lock(engine.m_write_mutex);

  return (engine_is_fenced_low(engine));
}

/** Check whether a table name is available for time-series writes.
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
static db_status_t engine_check_write_target(engine_t &engine,
                                             const std::string &table) {
  std::shared_lock<std::shared_mutex> lock(engine.m_catalog_mutex);

  /* DDL reserves its target before releasing the write lock. */
  if (engine.m_user_tables.count(table) != 0 || engine.m_ddl_table == table) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "ordinary tables cannot receive time-series writes"));
  }

  return (DB_SUCCESS);
}

/** Wait for memtable capacity, waking the background flusher as needed.
@return DB_SUCCESS if admission is allowed, DB_FENCED, or the last flush error
  if the memtable is still full */
static db_status_t engine_wait_for_memtable_room(engine_t &engine) {
  const uint64_t limit = engine.m_options.m_memtable_limit_bytes;

  if (limit == 0) {
    return (DB_SUCCESS);
  }

  bool waited = false;

  for (;;) {
    memtable_stats_t stats;
    memtable_get_stats(*engine.m_mt, stats);

    if (stats.m_live_bytes < limit) {
      return (DB_SUCCESS);
    }

    /* A fenced engine no longer flushes, so room would never come. */
    if (engine_is_fenced(engine)) {
      return (engine_fence_status(engine).with_code(DB_FENCED));
    }

    std::unique_lock<std::mutex> lock(engine.m_flusher_mutex);

    engine.m_flusher_wake = true;
    engine.m_flusher_wake_cv.notify_one();

    /* A rolled-back flush can be retried, but must not trap this caller. */
    if (!engine.m_flush_error.ok()) {
      return (engine.m_flush_error);
    }

    /* Count a throttled write once, however long it waits. */
    if (!waited) {
      engine.m_stats.m_throttled_writes++;
      waited = true;
    }

    /* Woken after every flush; the timeout covers a wake-up that came
    before this wait started. */
    engine.m_memtable_room_cv.wait_for(lock, std::chrono::milliseconds(10));
  }
}

/** Refuse points older than the retention period; they would only be
dropped again.
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
static db_status_t engine_check_retention(const engine_t &engine,
                                          const ingest_point_t *points,
                                          uint32_t n_points) {
  const uint32_t days = engine.m_options.m_retention_days;

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

/** Give up a write that is queued in the WAL but that the memtable refused.
The record will still be written, so memory no longer matches the WAL, and only
recovery can make them agree again. The queue entry lives on the caller's stack
and may have to lead the next group, so it must still go through the WAL.
REQUIRES: lock holds engine.m_write_mutex.
@return DB_OUTCOME_UNKNOWN */
static db_status_t engine_abandon_write(engine_t &engine, engine_write_t &w,
                                        ut_unique_lock_t &lock,
                                        db_status_t err) {
  const db_status_t failure = err.with_context("the memtable refused a write");
  engine_fence_low(engine, failure);

  lock.unlock();

  static_cast<void>(wal_append_commit(*engine.m_wal, w.m_writer));

  return (failure.with_code(DB_OUTCOME_UNKNOWN));
}

/** Fix a write's place: queue it in the WAL and reserve it in the memtable,
in the same order for every write. On DB_SUCCESS the caller must call
engine_commit_write(); on any other result the WAL no longer refers to the
write.
@retval DB_SUCCESS queued and reserved
@retval DB_OUTCOME_UNKNOWN queued, but the memtable refused it; the engine is
  now fenced
@return otherwise the validation or fencing error; nothing queued */
static db_status_t engine_reserve_write(engine_t &engine, engine_write_t &w,
                                        batch_seq_t &seq) {
  ut_unique_lock_t lock(engine.m_write_mutex);

  if (engine_is_fenced_low(engine)) {
    return (engine.m_fence_status.with_code(DB_FENCED));
  }

  /* DDL may have reserved the table while this write was being planned. */
  db_status_t err = engine_check_write_target(engine, w.m_table);
  if (!err.ok()) {
    return (err);
  }

  /* Another write changed the schema after this write was planned. */
  if (engine_plan_is_stale(engine, w.m_plan)) {
    err = engine_plan_write(engine, w);
    if (!err.ok()) {
      return (err);
    }
  }

  err = wal_append_enqueue(*engine.m_wal, w.m_payload.data(),
                           w.m_payload.size(), w.m_writer, seq);
  if (!err.ok()) {
    return (err);
  }

  ut_a(seq != 0);

  err = memtable_reserve(*engine.m_mt, seq, w.m_plan);
  if (!err.ok()) {
    return (engine_abandon_write(engine, w, lock, err));
  }

  return (DB_SUCCESS);
}

/** Wait for the group commit of a reserved write, then make it visible.
@param[in,out]  seq  sequence number of the write; reset to 0 if the write
                     was certainly not written
@retval DB_SUCCESS logged and visible
@retval DB_OUTCOME_UNKNOWN the write may or may not be durable; the engine is
  fenced
@return otherwise the error of a write that was certainly not written */
static db_status_t engine_commit_write(engine_t &engine, engine_write_t &w,
                                       batch_seq_t &seq) {
  const db_status_t status = wal_append_commit(*engine.m_wal, w.m_writer);
  if (!status.ok()) {
    engine_fence(engine, status);

    if (status.code() == DB_IO_ERROR) {
      return (status.with_code(DB_OUTCOME_UNKNOWN));
    }

    seq = 0;

    return (status);
  }

  const db_status_t published = memtable_publish(*engine.m_mt, seq);
  if (!published.ok()) {
    return (engine_fence_status(engine).with_code(DB_OUTCOME_UNKNOWN));
  }

  return (DB_SUCCESS);
}

/** Write a batch of points to a table; thread-safe. */
db_status_t engine_write(engine_t &engine, const char *table, size_t table_len,
                         const ingest_point_t *points, uint32_t n_points,
                         batch_seq_t &seq) {
  seq = 0;

  engine_write_t w;
  if (!schema_canonical_table_name(table, table_len, w.m_table).ok()) {
    return (db_status_t(DB_INVALID_ARGUMENT, "invalid table name"));
  }

  /* Reject expired points before waiting for memtable capacity. */
  db_status_t err = engine_check_retention(engine, points, n_points);
  if (!err.ok()) {
    return (err);
  }

  /* Reject table conflicts before waiting for capacity or planning. */
  err = engine_check_write_target(engine, w.m_table);
  if (!err.ok()) {
    return (err);
  }

  err = engine_wait_for_memtable_room(engine);
  if (!err.ok()) {
    return (err);
  }

  w.m_points = points;
  w.m_n_points = n_points;

  /* Plan writes concurrently without holding the write lock. */
  err = engine_plan_write(engine, w);
  if (!err.ok()) {
    return (err);
  }

  /* Hold the write lock to keep WAL and memtable writes in the same order. */
  err = engine_reserve_write(engine, w, seq);
  if (!err.ok()) {
    return (err);
  }

  /* Wait for the group commit without the lock. */
  return (engine_commit_write(engine, w, seq));
}

/** Reclaim stored WAL segments even when a flush has no new batches. A failed
deletion or directory sync is retried on the next flush. */
static void engine_purge_wal(engine_t &engine, engine_flush_info_t &info) {
  ut_ad(engine.m_flush_mutex.is_owned());

  uint64_t removed = 0;
  if (wal_purge(*engine.m_wal, info.m_flushed_seq, removed).ok()) {
    info.m_segments_removed = removed;
  }
}

/** Store every visible, unflushed write.
@return DB_SUCCESS, a rolled-back preparation error, DB_CORRUPTION, DB_FENCED,
  or DB_OUTCOME_UNKNOWN after commit or rollback cannot be confirmed */
static db_status_t engine_flush_low(engine_t &engine,
                                    engine_flush_info_t &info) {
  ut_ad(engine.m_flush_mutex.is_owned());

  if (engine_is_fenced(engine)) {
    return (engine_fence_status(engine).with_code(DB_FENCED));
  }

  /* Everything visible now; writes that become visible
  meanwhile wait for the next flush. The batches stay valid until they are
  retired below, which only this flush does. */
  std::vector<memtable_flush_item_t> items;
  memtable_collect_unflushed(*engine.m_mt, items);

  /* Nothing to store, but an earlier flush may have left segments to purge. */
  if (items.empty()) {
    info.m_flushed_seq = storage_catalog(*engine.m_storage).m_flushed_seq;
    engine_purge_wal(engine, info);

    return (DB_SUCCESS);
  }

  /* Make the WAL prefix durable before DuckDB can durably advance F. */
  if (!engine.m_options.m_wal_sync) {
    const db_status_t err = wal_sync(*engine.m_wal);
    if (!err.ok()) {
      return (err.with_context("cannot sync WAL before flush"));
    }
  }

  /* The memtable and the storage must agree on F. */
  ut_a(items.front().m_seq ==
       storage_catalog(*engine.m_storage).m_flushed_seq + 1);

  std::vector<storage_write_t> writes;
  writes.resize(items.size());
  uint64_t rows = 0;

  for (size_t i = 0; i < items.size(); ++i) {
    writes[i].m_seq = items[i].m_seq;
    writes[i].m_batch = items[i].m_batch;
    writes[i].m_schema = items[i].m_schema;
    rows += items[i].m_batch->m_n_rows;
  }

  /* Writes and queries continue while the storage works. */
  db_status_t err =
      storage_flush_prepare(*engine.m_storage, &writes[0], writes.size());
  if (!err.ok()) {
    if (err.code() == DB_OUTCOME_UNKNOWN || err.code() == DB_CORRUPTION) {
      engine_fence(engine, err);
    }

    return (err);
  }

  /* Readers register before BEGIN and retain retired versions. A reader
  seeing the old F keeps these batches; a reader seeing the new F filters
  them out. COMMIT does not hold any PulseTen reader lock. */
  err = storage_flush_commit(*engine.m_storage);
  /* After a failed commit the stored F is unknown. */
  if (!err.ok()) {
    engine_fence(engine, err);

    return (err);
  }

  ut_a(storage_catalog(*engine.m_storage).m_flushed_seq == items.back().m_seq);

  /* Trimming may free the batches. */
  memtable_trim(*engine.m_mt, items.back().m_seq);

  info.m_rows = rows;
  info.m_writes = items.size();
  info.m_flushed_seq = items.back().m_seq;

  engine_purge_wal(engine, info);

  return (DB_SUCCESS);
}

/** Store every visible, unflushed write in DuckDB in one transaction and retire
it from the memtable. */
db_status_t engine_flush(engine_t &engine, engine_flush_info_t &info) {
  info = engine_flush_info_t();

  ut_lock_guard_t flush_lock(engine.m_flush_mutex);

  const db_status_t err = engine_flush_low(engine, info);
  {
    std::lock_guard<std::mutex> lock(engine.m_flusher_mutex);
    engine.m_flush_error =
        err.code() == DB_OUTCOME_UNKNOWN ? err.with_code(DB_FENCED) : err;
    engine.m_memtable_room_cv.notify_all();
  }

  return (err);
}

/** Drop the stored partitions whose days have all passed the retention period,
measured back from a given time. */
db_status_t engine_expire(engine_t &engine, int64_t now_us, uint64_t &removed) {
  removed = 0;

  /* 0 keeps all data. */
  if (engine.m_options.m_retention_days == 0) {
    return (DB_SUCCESS);
  }

  /* No flush is prepared while the flush lock is held. */
  ut_lock_guard_t flush_lock(engine.m_flush_mutex);

  if (engine_is_fenced(engine)) {
    return (engine_fence_status(engine).with_code(DB_FENCED));
  }

  /* Partitions ending on or before this day hold only expired times. */
  const int64_t cutoff =
      now_us -
      static_cast<int64_t>(engine.m_options.m_retention_days) * UT_DAY_US;
  const int64_t end_day = ut_utc_day(cutoff);

  /* No logical change: do not invalidate snapshots on every timer tick. */
  const storage_catalog_t &catalog = storage_catalog(*engine.m_storage);
  bool has_expired = false;
  for (const std::pair<const std::pair<uint64_t, int64_t>, storage_partition_t>
           &part : catalog.m_partitions) {
    if (part.second.m_first_day + catalog.m_partition_days <= end_day) {
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
  std::unique_lock<std::shared_mutex> snapshots(engine.m_snapshot_mutex);

  /* A fence may have come while waiting for the lock. */
  if (engine_is_fenced(engine)) {
    return (engine_fence_status(engine).with_code(DB_FENCED));
  }

  db_status_t err = storage_expire(*engine.m_storage, end_day, removed);
  /* A rolled-back expiry can simply run again; an uncertain commit cannot. */
  if (err.code() == DB_OUTCOME_UNKNOWN) {
    engine_fence(engine, err);
  }

  if (err.ok() && removed > 0) {
    {
      std::lock_guard<std::mutex> lock(engine.m_query_catalog_mutex);
      engine.m_query_catalog.reset();
    }
    std::lock_guard<std::mutex> lock(engine.m_flusher_mutex);
    engine.m_stats.m_expired_partitions += removed;
  }

  return (err);
}

/** Drop an ordinary table, rejecting managed time-series tables. */
db_status_t engine_drop_table(engine_t &engine, const char *table,
                              size_t table_len) {
  std::string canonical;
  if (!schema_canonical_table_name(table, table_len, canonical).ok()) {
    return (db_status_t(DB_INVALID_ARGUMENT, "invalid table name"));
  }

  return (engine_exec_ddl(engine, "DROP TABLE \"" + canonical + "\""));
}

/** Copy the newest schema of every table, flushed or not, by name. */
void engine_list_schemas(engine_t &engine, std::vector<schema_t> &tables) {
  memtable_list_schemas(*engine.m_mt, tables);
}

/** What one round of the flusher did. */
struct engine_flusher_result_t {
  /** Result of the flush. */
  db_status_t m_flush_err{DB_SUCCESS};

  /** Result of the expiry. */
  db_status_t m_expire_err{DB_SUCCESS};

  /** The flush stored at least one write. */
  bool m_stored{false};

  /** What failed; empty when the round only repeated a fence. */
  db_status_t m_status;
};

/** Wait until the next round is due.
@param[in,out]  lock  holds engine.m_flusher_mutex
@return false if the flusher must stop */
static bool engine_flusher_wait(engine_t &engine,
                                std::unique_lock<std::mutex> &lock) {
  ut_ad(lock.owns_lock());

  const std::chrono::milliseconds interval(
      engine.m_options.m_flush_interval_ms);

  /* A wake-up during the last round must not wait a whole interval. */
  if (!engine.m_flusher_stop && !engine.m_flusher_wake) {
    engine.m_flusher_wake_cv.wait_for(lock, interval);
  }

  if (engine.m_flusher_stop) {
    return (false);
  }

  engine.m_flusher_wake = false;

  return (true);
}

/** Add what one task failed with to the message of the round.
@param[in]  task   name of the task
@param[in]  status  outcome of the task */
static void engine_flusher_append_error(engine_flusher_result_t &result,
                                        const char *task,
                                        const db_status_t &status) {
  if (status.ok() || status.code() == DB_FENCED) {
    return;
  }

  const db_status_t failure = status.with_context(task);
  if (result.m_status.ok()) {
    result.m_status = failure;
    return;
  }

  result.m_status =
      db_status_t(result.m_status.code(),
                  result.m_status.message() + "; " + failure.message());
}

/** Flush, then drop what has passed the retention period. */
static void engine_flusher_flush_and_expire(engine_t &engine,
                                            engine_flusher_result_t &result) {
  engine_flush_info_t info;

  result.m_flush_err = engine_flush(engine, info);
  result.m_stored = result.m_flush_err.ok() && info.m_writes > 0;

  /* A failed flush may still leave expired partitions to drop. Only
  retryable failures leave the storage safe for another transaction. */
  const bool can_expire = result.m_flush_err.ok() ||
                          result.m_flush_err.code() == DB_ERROR ||
                          result.m_flush_err.code() == DB_OUT_OF_MEMORY;

  if (can_expire) {
    uint64_t removed = 0;

    result.m_expire_err = engine_expire(engine, ut_now_us(), removed);
  }

  engine_flusher_append_error(result, "flush", result.m_flush_err);
  engine_flusher_append_error(result, "expire", result.m_expire_err);
}

/** Count a finished result.
REQUIRES: engine.m_flusher_mutex is held. */
static void engine_flusher_record_result(engine_t &engine,
                                         engine_flusher_result_t &result) {
  /* An expiry failure does not undo the completed flush. */
  if (result.m_stored) {
    engine.m_stats.m_background_flushes++;
  }

  if (!result.m_flush_err.ok() || !result.m_expire_err.ok()) {
    engine.m_stats.m_background_errors++;
  }

  if (!result.m_status.ok()) {
    engine.m_background_status = std::move(result.m_status);
  }
}

/** Background flusher: flush every interval, or when woken.
@return NULL */
static void *engine_flusher_main(void *arg) {
  engine_t &engine = *static_cast<engine_t *>(arg);
  std::unique_lock<std::mutex> lock(engine.m_flusher_mutex);

  while (engine_flusher_wait(engine, lock)) {
    lock.unlock();

    engine_flusher_result_t result;
    engine_flusher_flush_and_expire(engine, result);

    lock.lock();

    engine_flusher_record_result(engine, result);
    engine.m_memtable_room_cv.notify_all();
  }

  return (nullptr);
}

/** Read what fenced the engine. */
db_status_t engine_fence_status(engine_t &engine) {
  {
    ut_lock_guard_t lock(engine.m_write_mutex);
    if (!engine.m_fence_status.ok()) {
      return (engine.m_fence_status);
    }
  }

  /* The WAL can fail before its writer records the engine fence. */
  return (wal_status(*engine.m_wal));
}

/** Read the message of the latest background failure. */
db_status_t engine_background_status(engine_t &engine) {
  std::lock_guard<std::mutex> lock(engine.m_flusher_mutex);

  return (engine.m_background_status);
}

/** Read the engine counters. */
void engine_get_stats(engine_t &engine, engine_stats_t &stats) {
  {
    std::lock_guard<std::mutex> lock(engine.m_flusher_mutex);
    stats = engine.m_stats;
  }

  wal_stats_t wal;
  wal_get_stats(*engine.m_wal, wal);
  stats.m_wal_segments = wal.m_segments;
}

/** Build what a query sees from the stored catalog at F and a memtable snapshot
of the writes after F.
@param[in]  memtable_snap  pinned memtable snapshot after F */
static void engine_build_query_tables(const storage_catalog_t &catalog,
                                      const memtable_snapshot_t &memtable_snap,
                                      query_snapshot_t &snap) {
  snap.m_flushed_seq = catalog.m_flushed_seq;
  snap.m_partition_days = catalog.m_partition_days;

  /* Stored tables, with the newest stored version and their partitions. */
  std::map<std::string, storage_table_t>::const_iterator t;

  for (t = catalog.m_tables.begin(); t != catalog.m_tables.end(); ++t) {
    ut_a(!t->second.m_versions.empty());

    query_table_t &table = snap.m_tables[t->first];
    table.m_schema = t->second.m_versions.back();

    std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
        p;
    for (p = catalog.m_partitions.begin(); p != catalog.m_partitions.end();
         ++p) {
      if (p->first.first == t->second.m_table_id) {
        table.m_partitions.push_back(p->second);
      }
    }
  }

  /* Unflushed batches; their schema replaces the stored one if newer. */
  std::map<std::string, memtable_table_view_t>::const_iterator h;

  for (h = memtable_snap.m_tables.begin(); h != memtable_snap.m_tables.end();
       ++h) {
    ut_a(h->second.m_schema != nullptr);

    query_table_t &table = snap.m_tables[h->first];
    table.m_batches = h->second.m_batches;

    if (h->second.m_schema->m_version > table.m_schema.m_version) {
      table.m_schema = *h->second.m_schema;
    }
  }
}

/** A query reading a consistent snapshot. */
struct engine_cursor_t {
  /** Engine the cursor reads. */
  engine_t *m_engine{nullptr};

  /** Connection running the snapshot transaction. */
  duckdb_connection m_conn{nullptr};

  /** Memtable writes after F. */
  memtable_snapshot_t m_memtable_snap;

  /** m_memtable_snap is pinned or taken and must be released. */
  bool m_has_memtable_snap{false};

  /** What the query sees; the query registry points here. */
  query_snapshot_t m_snap;

  /** The running statement; NULL until it is opened. */
  query_cursor_t *m_query{nullptr};
};

/** Request interruption of an active cursor. */
void engine_cursor_interrupt(engine_cursor_t &cursor) {
  query_cursor_interrupt(*cursor.m_query);
}

/** Stop a query and free its cursor. */
void engine_cursor_close(engine_cursor_t *cursor) {
  if (cursor == nullptr) {
    return;
  }

  /* The statement stops before the snapshot it reads goes away. */
  query_cursor_close(cursor->m_query);

  /* Disconnect rolls back the read transaction. Do not run fresh SQL here:
  an application global may close its cursor during process teardown. */
  if (cursor->m_conn != nullptr) {
    duckdb_disconnect(&cursor->m_conn);
  }

  if (cursor->m_has_memtable_snap) {
    memtable_snapshot_release(*cursor->m_engine->m_mt, cursor->m_memtable_snap);
  }

  ut_delete(cursor);
}

/** Release a partially opened cursor, including its snapshot pin. */
struct engine_cursor_guard_t {
  /** Cursor to close; NULL once handed over. */
  engine_cursor_t *m_cursor{nullptr};

  /** Close a cursor that was not transferred to the caller. */
  ~engine_cursor_guard_t() {
    engine_cursor_close(m_cursor);
  }
};

/** Reuse metadata for F, or read it in the cursor's own transaction.
REQUIRES: engine.m_snapshot_mutex is held shared, excluding partition expiry.
@return DB_SUCCESS or the catalog query/validation error */
static db_status_t engine_query_catalog(
    engine_t &engine, duckdb_connection conn, batch_seq_t flushed_seq,
    std::shared_ptr<const storage_catalog_t> &out) {
  {
    std::lock_guard<std::mutex> lock(engine.m_query_catalog_mutex);
    if (engine.m_query_catalog != nullptr &&
        engine.m_query_catalog->m_flushed_seq == flushed_seq) {
      out = engine.m_query_catalog;
      return (DB_SUCCESS);
    }
  }

  auto loaded = std::make_shared<storage_catalog_t>();
  const db_status_t err = storage_read_catalog(conn, *loaded);
  if (!err.ok()) {
    return (err);
  }
  if (loaded->m_flushed_seq != flushed_seq) {
    return (DB_CORRUPTION);
  }

  out = loaded;
  std::lock_guard<std::mutex> lock(engine.m_query_catalog_mutex);
  /* A slower reader must not replace a newer cached snapshot. */
  if (engine.m_query_catalog == nullptr ||
      engine.m_query_catalog->m_flushed_seq <= flushed_seq) {
    engine.m_query_catalog = out;
  }
  return (DB_SUCCESS);
}

/** Take the snapshot of a cursor: the DuckDB transaction at F and the
memtable writes after F. A deletion of stored data waits until it is fixed.
@return DB_SUCCESS, DB_FENCED, DB_CORRUPTION or a DuckDB snapshot error */
static db_status_t engine_cursor_take_snapshot(engine_cursor_t &cursor) {
  engine_t &engine = *cursor.m_engine;

  ut_a(!cursor.m_has_memtable_snap);

  std::shared_lock<std::shared_mutex> snapshots(engine.m_snapshot_mutex);

  if (engine_is_fenced(engine)) {
    return (engine_fence_status(engine).with_code(DB_FENCED));
  }

  /* Pin BEFORE fixing the stored snapshot. A flush can then commit and
  retire at any point without making the reader lose memtable rows. */
  memtable_snapshot_pin(*engine.m_mt, cursor.m_memtable_snap);
  cursor.m_has_memtable_snap = true;
#ifdef PT_DEBUG
  if (engine.m_dbg_after_snapshot_pin != nullptr) {
    engine.m_dbg_after_snapshot_pin(engine.m_dbg_snapshot_context);
  }
#endif
  batch_seq_t flushed_seq = 0;
  db_status_t err = query_begin_snapshot(cursor.m_conn, flushed_seq);
  if (!err.ok()) {
    return (err);
  }

  std::shared_ptr<const storage_catalog_t> stored;
  err = engine_query_catalog(engine, cursor.m_conn, flushed_seq, stored);
  if (!err.ok()) {
    return (err);
  }
  err = memtable_snapshot_complete(*engine.m_mt, flushed_seq,
                                   cursor.m_memtable_snap);
  if (!err.ok()) {
    return (err);
  }
  engine_build_query_tables(*stored, cursor.m_memtable_snap, cursor.m_snap);

  if (engine_is_fenced(engine)) {
    return (engine_fence_status(engine).with_code(DB_FENCED));
  }

  return (DB_SUCCESS);
}

/** Allocate a cursor with a DuckDB connection of its own, owned by a guard.
@return DB_SUCCESS, DB_OUT_OF_MEMORY or DB_ERROR */
static db_status_t engine_cursor_create(engine_t &engine,
                                        engine_cursor_guard_t &owner) {
  owner.m_cursor = ut_new<engine_cursor_t>();
  if (owner.m_cursor == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  engine_cursor_t &cursor = *owner.m_cursor;
  cursor.m_engine = &engine;

  if (duckdb_connect(storage_database(*engine.m_storage), &cursor.m_conn) !=
      DuckDBSuccess) {
    cursor.m_conn = nullptr;
    return (db_status_t(DB_ERROR, "cannot connect to DuckDB"));
  }

  return (DB_SUCCESS);
}

/** Check that a DDL statement may change its table now.
REQUIRES: engine.m_write_mutex is held.
@return DB_SUCCESS, DB_FENCED, DB_INVALID_ARGUMENT or DB_NOT_FOUND */
static db_status_t engine_check_ddl(engine_t &engine, const query_ddl_t &ddl) {
  ut_ad(engine.m_write_mutex.is_owned());

  if (engine_is_fenced_low(engine)) {
    return (engine.m_fence_status.with_code(DB_FENCED));
  }

  if (memtable_get_schema(*engine.m_mt, ddl.m_table) != nullptr) {
    return (db_status_t(
        DB_INVALID_ARGUMENT,
        ddl.m_is_create
            ? "ordinary table name conflicts with a time-series table"
            : "time-series tables cannot be dropped"));
  }

  std::shared_lock<std::shared_mutex> catalog_lock(engine.m_catalog_mutex);
  if (!ddl.m_is_create && !ddl.m_if_exists &&
      engine.m_user_tables.count(ddl.m_table) == 0) {
    return (
        db_status_t(DB_NOT_FOUND, "table " + ddl.m_table + " does not exist"));
  }

  return (DB_SUCCESS);
}

/** Record a committed DDL statement in the ordinary table names.
REQUIRES: engine.m_write_mutex is held. */
static void engine_record_ddl(engine_t &engine, const query_ddl_t &ddl) {
  ut_ad(engine.m_write_mutex.is_owned());

  storage_user_table_committed(*engine.m_storage, ddl.m_table, ddl.m_is_create);

  std::unique_lock<std::shared_mutex> catalog_lock(engine.m_catalog_mutex);
  if (ddl.m_is_create) {
    engine.m_user_tables.insert(ddl.m_table);
  } else {
    engine.m_user_tables.erase(ddl.m_table);
  }
}

/** Keep a DDL target reserved until its transaction has ended. */
struct engine_ddl_guard_t {
  /** Engine whose target must be released; NULL before reservation. */
  engine_t *m_engine{nullptr};

  /** Release the target after the cursor has committed or rolled back. */
  ~engine_ddl_guard_t() {
    if (m_engine != nullptr) {
      ut_lock_guard_t lock(m_engine->m_write_mutex);
      std::unique_lock<std::shared_mutex> catalog_lock(
          m_engine->m_catalog_mutex);
      m_engine->m_ddl_table.clear();
    }
  }
};

/** Execute ordinary-table DDL over one consistent snapshot. */
db_status_t engine_exec_ddl(engine_t &engine, const std::string &sql) {
  std::lock_guard<std::mutex> ddl_lock(engine.m_ddl_mutex);
  engine_ddl_guard_t reservation;
  /* Close the transaction before releasing its reserved name. */
  engine_cursor_guard_t owner;

  db_status_t err = engine_cursor_create(engine, owner);
  if (!err.ok()) {
    return (err);
  }

  engine_cursor_t &cursor = *owner.m_cursor;

  query_ddl_t ddl;
  err = query_check_ddl(cursor.m_conn, sql, ddl);
  if (!err.ok()) {
    return (err);
  }

  {
    ut_lock_guard_t write_lock(engine.m_write_mutex);
    err = engine_check_ddl(engine, ddl);
    if (!err.ok()) {
      return (err);
    }
    std::unique_lock<std::shared_mutex> catalog_lock(engine.m_catalog_mutex);
    engine.m_ddl_table = ddl.m_table;
    reservation.m_engine = &engine;
  }

  err = engine_cursor_take_snapshot(cursor);
  if (!err.ok()) {
    return (err);
  }

#ifdef PT_DEBUG
  if (engine.m_dbg_before_ddl != nullptr) {
    engine.m_dbg_before_ddl(engine.m_dbg_ddl_context);
  }
#endif

  /* DuckDB retains the ordinary-table MVCC view; the pinned memtable keeps
  the memtable rows valid while writes, flushes and new queries continue. */
  err = query_execute_ddl(cursor.m_conn, cursor.m_snap, sql);
  if (!err.ok()) {
    return (err);
  }

  /* Keep DDL commit outside the gap between the two halves of a snapshot. */
  ut_lock_guard_t flush_lock(engine.m_flush_mutex);
  std::unique_lock<std::shared_mutex> snapshots(engine.m_snapshot_mutex);
  ut_lock_guard_t write_lock(engine.m_write_mutex);

  if (engine_is_fenced_low(engine)) {
    return (engine.m_fence_status.with_code(DB_FENCED));
  }

  err = query_commit(cursor.m_conn);
  if (!err.ok()) {
    /* After a failed commit the stored catalog is unknown. */
    engine_fence_low(engine, err);

    return (err);
  }

  engine_record_ddl(engine, ddl);

  return (DB_SUCCESS);
}

/** Start a SELECT, or EXPLAIN of it, over one consistent snapshot, for reading
its rows chunk by chunk. */
db_status_t engine_cursor_open(engine_t &engine, const std::string &sql,
                               bool explain, engine_cursor_t **out) {
  *out = nullptr;

  engine_cursor_guard_t owner;

  db_status_t err = engine_cursor_create(engine, owner);
  if (!err.ok()) {
    return (err);
  }

  engine_cursor_t *cursor = owner.m_cursor;

  /* Check the statement before taking a snapshot for it. */
  query_ranges_t ranges;
  err = query_validate_select(cursor->m_conn, sql, ranges);
  if (!err.ok()) {
    return (err);
  }

  err = engine_cursor_take_snapshot(*cursor);
  if (!err.ok()) {
    return (err);
  }

  err = query_cursor_open(cursor->m_conn, cursor->m_snap, sql, explain, ranges,
                          &cursor->m_query);
  if (!err.ok()) {
    return (err);
  }

  *out = cursor;
  owner.m_cursor = nullptr;

  return (DB_SUCCESS);
}

/** Read the column metadata of a cursor. */
const std::vector<query_column_t> &engine_cursor_columns(
    const engine_cursor_t &cursor) {
  return (query_cursor_columns(*cursor.m_query));
}

/** Read the next rows of a cursor, at most QUERY_CHUNK_ROWS. */
db_status_t engine_cursor_fetch(engine_cursor_t &cursor, query_rows_t &rows) {
  return (query_cursor_fetch(*cursor.m_query, rows));
}

/** Show DuckDB's plan for a SELECT, as a cursor would run it. */
db_status_t engine_explain(engine_t &engine, const std::string &sql,
                           std::string &plan) {
  plan.clear();

  engine_cursor_t *cursor = nullptr;

  db_status_t err = engine_cursor_open(engine, sql, true, &cursor);
  if (!err.ok()) {
    return (err);
  }

  query_rows_t rows;

  for (;;) {
    err = engine_cursor_fetch(*cursor, rows);
    if (!err.ok() || rows.empty()) {
      break;
    }

    for (size_t r = 0; r < rows.size(); ++r) {
      for (size_t c = 0; c < rows[r].size(); ++c) {
        plan += rows[r][c].m_s + "\n";
      }
    }
  }

  engine_cursor_close(cursor);

  if (!err.ok()) {
    plan.clear();
  }

  return (err);
}

/** Read the memtable counters. */
void engine_get_memtable_stats(engine_t &engine, memtable_stats_t &stats) {
  memtable_get_stats(*engine.m_mt, stats);
}

/** Borrow the engine's memtable for diagnostics. */
memtable_t &engine_memtable(engine_t &engine) {
  return (*engine.m_mt);
}

/** Borrow the engine's storage for diagnostics. */
storage_t &engine_storage(engine_t &engine) {
  return (*engine.m_storage);
}

#ifdef PT_DEBUG
/** Wake the background flusher after a test has prepared a failure. */
void engine_dbg_wake_flusher(engine_t &engine) {
  std::lock_guard<std::mutex> lock(engine.m_flusher_mutex);

  engine.m_flusher_wake = true;
  engine.m_flusher_wake_cv.notify_one();
}

/** Make the next WAL sync fail. */
void engine_dbg_fail_next_wal_sync(engine_t &engine) {
  wal_dbg_fail_next_sync(*engine.m_wal);
}

/** Make the next storage flush fail before it commits. */
void engine_dbg_fail_next_flush(engine_t &engine) {
  ut_lock_guard_t lock(engine.m_flush_mutex);

  storage_dbg_fail_next_flush(*engine.m_storage);
}

/** Enable or disable persistent pre-commit flush failures. */
void engine_dbg_set_flush_failure(engine_t &engine, bool fail) {
  ut_lock_guard_t lock(engine.m_flush_mutex);

  storage_dbg_set_flush_failure(*engine.m_storage, fail);
}

/** Set a callback before DDL execution, without holding the operation locks. */
void engine_dbg_set_ddl_callback(engine_t &engine, void (*callback)(void *),
                                 void *context) {
  std::lock_guard<std::mutex> lock(engine.m_ddl_mutex);

  engine.m_dbg_before_ddl = callback;
  engine.m_dbg_ddl_context = context;
}

/** Set a callback after pinning the memtable and before fixing stored data. */
void engine_dbg_set_snapshot_callback(engine_t &engine,
                                      void (*callback)(void *), void *context) {
  engine.m_dbg_after_snapshot_pin = callback;
  engine.m_dbg_snapshot_context = context;
}
#endif
