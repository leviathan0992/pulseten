/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine0engine.cc
 *    The write path and WAL replay.
 *
 *-------------------------------------------------------------------------
 */

#include "engine0engine.h"

#include <pthread.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <new>

#include "ingest0codec.h"
#include "os0file.h"
#include "query0query.h"
#include "ut0new.h"
#include "ut0time.h"

/** File locked while a directory is open, so that only one engine uses
it at a time. */
#define ENGINE_LOCK_FILE "LOCK"

/** An open database directory. */
struct engine_t {
  /** Holds the directory's lock. */
  os_file_t lock_file{OS_FILE_CLOSED};

  /** The write-ahead log. */
  wal_t *wal{nullptr};

  /** Writes after F and every table's schema. */
  memtable_t *mt{nullptr};

  /** The DuckDB file with the writes up to F. */
  storage_t *storage{nullptr};

  /** Orders writes: queuing in the WAL and reserving in the memtable
  happen together under it. Taken before the WAL and memtable mutexes,
  never while holding them, and never held while waiting for a flush. */
  std::mutex write_mutex;

  /** Set when the WAL, the memtable and the storage may disagree; every
  later write and flush is rejected until the directory is reopened.
  Protected by write_mutex. */
  bool fenced{false};

  /** Even while idle, odd while partitions or tables are dropped. A
  snapshot must see the same even value before BEGIN and after selecting
  its hot batches. Protected by write_mutex. */
  uint64_t drop_epoch{0};

  /** Signalled when drop_epoch becomes even or the engine is fenced. */
  std::condition_variable drop_cv;

  /** Set while a table is dropped: new writes wait before they are
  queued. Protected by write_mutex; pause_cv signals its end. */
  bool writes_paused{false};

  /** Signalled when writes_paused is cleared or the engine is fenced. */
  std::condition_variable pause_cv;

  /** Tables dropped. A write planned before a drop is planned again,
  since its table may be gone or another one. Changed under
  write_mutex. */
  std::atomic<uint64_t> drops{0};

  /** Allows one flush at a time. Taken before write_mutex. */
  std::mutex flush_mutex;

  /** Settings given to engine_open(), checked. */
  engine_options_t options;

  /** Background flusher thread. */
  pthread_t flusher_thread{};

  /** flusher_thread runs. Set by engine_open() before any write. */
  bool flusher_started{false};

  /** Protects the fields below it; a leaf lock. */
  std::mutex flusher_mutex;

  /** Wakes the flusher early. */
  std::condition_variable flusher_wake_cv;

  /** Wakes writers waiting for room after a flush. */
  std::condition_variable room_cv;

  /** engine_close() asks the flusher to exit. */
  bool flusher_stop{false};

  /** Someone asked the flusher to flush now. */
  bool flusher_wake{false};

  /** Counters; wal_segments is read from the WAL when asked for. */
  engine_stats_t stats;
};

/** State of a WAL replay. */
struct engine_replay_t {
  /** Memtable the records are replayed into. */
  memtable_t *mt{nullptr};

  /** Writes up to here are in storage and are skipped. */
  batch_seq_t flushed_seq{0};

  /** Records replayed so far. */
  uint64_t replayed{0};
};

/** Replay one WAL record into the memtable, unless it is already stored.
@param[in,out]  ctx      recovery state containing the memtable and stored
                         sequence boundary
@param[in]      seq      sequence number of this WAL record
@param[in]      payload  encoded record payload
@param[in]      len      input length in bytes
@return DB_SUCCESS, or the decode, reservation or publication error;
  DB_CORRUPTION if records after the stored boundary are not consecutive */
static dberr_t engine_replay(void *ctx, batch_seq_t seq,
                             const unsigned char *payload, size_t len) {
  engine_replay_t *replay = static_cast<engine_replay_t *>(ctx);

  if (seq <= replay->flushed_seq) {
    return (DB_SUCCESS);
  }

  if (seq != replay->flushed_seq + replay->replayed + 1) {
    return (DB_CORRUPTION);
  }

  ingest_plan_t plan;

  dberr_t err = ingest_decode(payload, len, plan);
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = memtable_reserve(*replay->mt, seq, plan);
  if (err != DB_SUCCESS) {
    return (err);
  }

  replay->replayed++;

  /* Replay is in order, so this never waits. */
  return (memtable_publish(*replay->mt, seq));
}

/** Take the directory lock.
@param[in]   dir   database or WAL directory
@param[out]  file  descriptor owning the directory lock; closed on failure
@return DB_SUCCESS, DB_LOCKED if another opener holds the lock, or DB_IO_ERROR
*/
static dberr_t engine_lock(const char *dir, os_file_t &file) {
  const std::string path = std::string(dir) + "/" + ENGINE_LOCK_FILE;

  dberr_t err = os_file_open(path.c_str(), OS_FILE_OPEN_OR_CREATE, file);
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = os_file_lock(file);
  if (err != DB_SUCCESS) {
    (void)os_file_close(file);
  }

  return (err);
}

/** Load the stored tables into the memtable, then replay the WAL records
after F, or create the WAL of a new directory.
@param[in,out]  engine  database engine
@param[in]      dir     database or WAL directory
@param[out]     recv    stored boundary and WAL replay counters
@return DB_SUCCESS, or the memtable/WAL recovery error; DB_CORRUPTION if
  storage and WAL boundaries disagree */
static dberr_t engine_recover(engine_t &engine, const char *dir,
                              engine_recv_t &recv) {
  const storage_state_t *state = storage_get_state(*engine.storage);
  recv.flushed_seq = state->flushed_seq;

  dberr_t err = memtable_create(state->flushed_seq, &engine.mt);
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* WAL records after F may follow any stored schema version. */
  std::map<std::string, storage_table_t>::const_iterator it;

  for (it = state->tables.begin(); it != state->tables.end(); ++it) {
    err = memtable_add_table(*engine.mt, it->second.versions);
    if (err != DB_SUCCESS) {
      return (err);
    }
  }

  bool exists = false;
  err = wal_exists(dir, exists);
  if (err != DB_SUCCESS) {
    return (err);
  }

  if (!exists) {
    /* Stored writes without a WAL: the WAL was lost. */
    if (state->flushed_seq > 0) {
      return (DB_CORRUPTION);
    }

    err = wal_create(dir, 0, &engine.wal, state->wal_id);
    if (err == DB_SUCCESS) {
      wal_set_segment_bytes(*engine.wal, engine.options.wal_segment_bytes);
    }

    return (err);
  }

  engine_replay_t replay;
  replay.mt = engine.mt;
  replay.flushed_seq = state->flushed_seq;

  wal_recv_t wal_recv;
  err = wal_open(dir, 0, engine_replay, &replay, &engine.wal, wal_recv,
                 state->wal_id);
  if (err != DB_SUCCESS) {
    return (err);
  }

  wal_set_segment_bytes(*engine.wal, engine.options.wal_segment_bytes);

  recv.wal_records = wal_recv.records;
  recv.replayed = replay.replayed;

  /* Storage cannot hold writes the WAL never had, and the WAL must still
  have every write after F. */
  if (wal_recv.last_seq < state->flushed_seq ||
      wal_recv.first_seq > state->flushed_seq + 1) {
    return (DB_CORRUPTION);
  }

  return (DB_SUCCESS);
}

/** Background flusher: flush every interval, or when woken.
@param[in]  arg  the engine
@return NULL */
static void *engine_flusher_main(void *arg);

/** Keep a partially opened engine owned until it is handed to the caller. */
struct engine_open_guard_t {
  /** Engine to close; NULL once handed over. */
  engine_t *engine{nullptr};

  /** Close an engine that was not transferred to the caller. */
  ~engine_open_guard_t() { (void)engine_close(engine); }
};

/** Open a database directory: create its WAL and storage if there are none,
otherwise load the storage and replay the WAL records after F.
@param[in]   dir      existing directory
@param[in]   options  settings, or NULL for the defaults
@param[out]  out      open engine, or NULL on failure
@param[out]  recv     what recovery found; may be NULL
@param[out]  error    message on failure
@return DB_SUCCESS, DB_LOCKED, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR,
  DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
dberr_t engine_open(const char *dir, const engine_options_t *options,
                    engine_t **out, engine_recv_t *recv, std::string &error) {
  *out = nullptr;

  engine_recv_t local_recv;
  if (recv == nullptr) {
    recv = &local_recv;
  }
  *recv = engine_recv_t();

  error.clear();

  engine_t *engine = ut_new<engine_t>();
  if (engine == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  engine_open_guard_t owner;
  owner.engine = engine;
  if (options != nullptr) {
    engine->options = *options;
  }

  if (engine->options.background_flush &&
      engine->options.flush_interval_ms == 0) {
    error = "background flush interval must be greater than zero";

    return (DB_INVALID_ARGUMENT);
  }

  /* Longer than any time a write may have; also keeps the cutoff from
  overflowing. */
  if (engine->options.retention_days > ENGINE_MAX_RETENTION_DAYS) {
    error = "retention period must be at most " +
            std::to_string(ENGINE_MAX_RETENTION_DAYS) + " days";

    return (DB_INVALID_ARGUMENT);
  }

  std::string directory;
  dberr_t err = os_dir_resolve(dir, directory);
  if (err != DB_SUCCESS) {
    return (err);
  }
  dir = directory.c_str();

  err = engine_lock(dir, engine->lock_file);

  if (err == DB_SUCCESS) {
    err = storage_open(dir, engine->options.partition_days, &engine->storage,
                       error);
  }

  if (err == DB_SUCCESS) {
    err = engine_recover(*engine, dir, *recv);
  }

  if (err == DB_SUCCESS) {
    err = query_install(storage_get_database(*engine->storage));
  }

  if (err == DB_SUCCESS && engine->options.background_flush) {
    if (pthread_create(&engine->flusher_thread, nullptr, engine_flusher_main,
                       engine) == 0) {
      engine->flusher_started = true;
    } else {
      error = "cannot start the background flush thread";
      err = DB_ERROR;
    }
  }

  if (err != DB_SUCCESS) {
    if (error.empty()) {
      error = ut_strerr(err);
    }

    return (err);
  }

  *out = engine;
  owner.engine = nullptr;

  return (DB_SUCCESS);
}

/** Close an engine.
@param[in]  engine  engine; may be NULL
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t engine_close(engine_t *engine) {
  if (engine == nullptr) {
    return (DB_SUCCESS);
  }

  /* Stop the flusher first; it uses everything below. */
  if (engine->flusher_started) {
    {
      std::lock_guard<std::mutex> lock(engine->flusher_mutex);
      engine->flusher_stop = true;
      engine->flusher_wake_cv.notify_one();
    }

    pthread_join(engine->flusher_thread, nullptr);
  }

  const dberr_t err = wal_close(engine->wal);

  memtable_free(engine->mt);
  storage_close(engine->storage);
  (void)os_file_close(engine->lock_file);

  ut_delete(engine);

  return (err);
}

/** Keep a planner's schema alive through concurrent table drops. */
struct engine_schema_guard_t {
  /** Memtable owning the reference. */
  memtable_t *mt{nullptr};

  /** Released on every return. */
  memtable_schema_ref_t ref;

  /** Release the write planner's hold on the schema. */
  ~engine_schema_guard_t() { memtable_schema_release(*mt, ref); }
};

/** Plan a write against the newest schema and encode its WAL payload.
@param[in,out]  engine    database engine
@param[in]      table     canonical table name used for schema lookup
@param[in]      name      original table name supplied by the caller
@param[in]      name_len  length of name in bytes
@param[in]      points    input points
@param[in]      n_points  number of points in the input batch
@param[out]     plan      validated schema change and owned column batch
@param[out]     payload   encoded WAL payload for the plan
@return DB_SUCCESS, or the write validation error; no WAL record is queued
  here */
static dberr_t engine_plan(engine_t &engine, const std::string &table,
                           const char *name, size_t name_len,
                           const ingest_point_t *points, uint32_t n_points,
                           ingest_plan_t &plan, std::string &payload) {
  engine_schema_guard_t owner;
  owner.mt = engine.mt;
  memtable_schema_acquire(*engine.mt, table, owner.ref);

  const dberr_t err =
      ingest_plan(owner.ref.schema, name, name_len, points, n_points, plan);
  if (err != DB_SUCCESS) {
    return (err);
  }

  ingest_encode(plan, payload);

  return (DB_SUCCESS);
}

/** A plan that adds no columns fits any later schema, because columns never
change. A schema change must be based on the newest version.
@param[in,out]  engine  database engine
@param[in]      plan    plan whose base schema version is checked
@return true if the plan can still be reserved */
static bool engine_plan_is_current(engine_t &engine,
                                   const ingest_plan_t &plan) {
  if (plan.new_cols.empty()) {
    return (true);
  }

  const schema_t *schema = memtable_get_schema(*engine.mt, plan.table);
  const uint64_t version = schema != nullptr ? schema->version : 0;

  return (version == plan.base_version);
}

/** Stop all writes after the WAL and the memtable may have diverged.
@param[in,out]  engine  database engine */
static void engine_fence(engine_t &engine) {
  {
    std::lock_guard<std::mutex> lock(engine.write_mutex);
    engine.fenced = true;
    engine.drop_cv.notify_all();
    engine.pause_cv.notify_all();
  }

  memtable_fail(*engine.mt);
}

/** Check whether an earlier failure fenced the engine.
@param[in,out]  engine  database engine
@return true if an earlier failure fenced the engine */
static bool engine_is_fenced(engine_t &engine) {
  std::lock_guard<std::mutex> lock(engine.write_mutex);

  return (engine.fenced);
}

/** Ask the background flusher to flush now.
@param[in,out]  engine  database engine */
static void engine_wake_flusher(engine_t &engine) {
  std::lock_guard<std::mutex> lock(engine.flusher_mutex);

  engine.flusher_wake = true;
  engine.flusher_wake_cv.notify_one();
}

/** Wait while the memtable holds more than its limit, so that writes cannot
outrun flushing without bound.
@param[in,out]  engine  database engine
@return DB_SUCCESS once there is room, or DB_FENCED */
static dberr_t engine_wait_for_room(engine_t &engine) {
  const uint64_t limit = engine.options.memtable_limit_bytes;

  if (limit == 0 || !engine.flusher_started) {
    return (DB_SUCCESS);
  }

  bool waited = false;

  for (;;) {
    memtable_stats_t stats;
    memtable_get_stats(*engine.mt, stats);

    if (stats.live_bytes < limit) {
      return (DB_SUCCESS);
    }

    if (engine_is_fenced(engine)) {
      return (DB_FENCED);
    }

    std::unique_lock<std::mutex> lock(engine.flusher_mutex);

    engine.flusher_wake = true;
    engine.flusher_wake_cv.notify_one();

    if (!waited) {
      engine.stats.throttled_writes++;
      waited = true;
    }

    /* Woken after every flush; the timeout covers a wake-up that came
    before this wait started. */
    engine.room_cv.wait_for(lock, std::chrono::milliseconds(10));
  }
}

/** Write a batch of points to a table; thread-safe.
@param[in]   engine     engine
@param[in]   table      table name as given by the user
@param[in]   table_len  length of table
@param[in]   points     the points
@param[in]   n_points   number of points
@param[out]  seq        sequence number of the write; set on DB_SUCCESS and
                        DB_OUTCOME_UNKNOWN, otherwise 0
@param[out]  error      why a write was rejected
@retval DB_SUCCESS durable and visible
@retval DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT, DB_LIMIT_EXCEEDED,
  DB_OUT_OF_MEMORY rejected; nothing written
@retval DB_FENCED rejected; an earlier write failed, reopen
@retval DB_OUTCOME_UNKNOWN the write may or may not be durable; do not retry
  blindly; the engine is now fenced */
dberr_t engine_write(engine_t &engine, const char *table, size_t table_len,
                     const ingest_point_t *points, uint32_t n_points,
                     batch_seq_t &seq, std::string &error) {
  seq = 0;

  error.clear();

  std::string canonical;
  if (schema_table_name(table, table_len, canonical) != DB_SUCCESS) {
    error = "invalid table name";

    return (DB_INVALID_ARGUMENT);
  }

  if (engine_is_fenced(engine)) {
    return (DB_FENCED);
  }

  /* Nothing older than the retention period is accepted; it would only
  be dropped again. */
  if (engine.options.retention_days > 0) {
    const int64_t cutoff =
        ut_now_us() - (int64_t)engine.options.retention_days * UT_DAY_US;

    for (uint32_t p = 0; p < n_points; ++p) {
      if (points[p].time_us < cutoff) {
        error = "point " + std::to_string(p) +
                " is older than the retention period of " +
                std::to_string(engine.options.retention_days) + " days";

        return (DB_INVALID_ARGUMENT);
      }
    }
  }

  dberr_t err = engine_wait_for_room(engine);
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* Plan without the lock, so that concurrent writes plan in parallel. */
  ingest_plan_t plan;
  std::string payload;
  const uint64_t drops = engine.drops.load();

  err = engine_plan(engine, canonical, table, table_len, points, n_points, plan,
                    payload);
  if (err != DB_SUCCESS) {
    error.swap(plan.error);

    return (err);
  }

  /* Fix the write's place: queue it in the WAL and reserve it in the
  memtable, in the same order for every write. */
  wal_writer_t w;
  bool reserved = false;

  {
    std::unique_lock<std::mutex> lock(engine.write_mutex);

    while (engine.writes_paused && !engine.fenced) {
      engine.pause_cv.wait(lock);
    }

    if (engine.fenced) {
      return (DB_FENCED);
    }

    if (engine.drops.load() != drops || !engine_plan_is_current(engine, plan)) {
      err = engine_plan(engine, canonical, table, table_len, points, n_points,
                        plan, payload);
      if (err != DB_SUCCESS) {
        error.swap(plan.error);

        return (err);
      }
    }

    err =
        wal_append_enqueue(*engine.wal, payload.data(), payload.size(), w, seq);
    if (err != DB_SUCCESS) {
      return (err);
    }

    err = memtable_reserve(*engine.mt, seq, plan);

    if (err == DB_SUCCESS) {
      reserved = true;
    } else {
      /* The record is queued and will be written, but memory does not
      hold it. Recovery from the WAL is the only way to agree again. */
      engine.fenced = true;
    }
  }

  if (!reserved) {
    memtable_fail(*engine.mt);
  }

  /* Wait for the group commit without the lock. */
  const dberr_t wait_err = wal_append_wait(*engine.wal, w);

  if (!reserved) {
    return (DB_OUTCOME_UNKNOWN);
  }

  if (wait_err != DB_SUCCESS) {
    engine_fence(engine);

    if (wait_err == DB_IO_ERROR) {
      return (DB_OUTCOME_UNKNOWN);
    }

    /* Certainly not written. */
    seq = 0;

    return (wait_err);
  }

  /* Durable. If an earlier write failed meanwhile, this one cannot become
  visible now; it will be after the directory is reopened. */
  err = memtable_publish(*engine.mt, seq);

  if (err != DB_SUCCESS) {
    return (DB_OUTCOME_UNKNOWN);
  }

  /* Enough unflushed data: flush without waiting for the interval. */
  if (engine.flusher_started) {
    memtable_stats_t stats;
    memtable_get_stats(*engine.mt, stats);

    if (stats.live_bytes >= engine.options.flush_bytes) {
      engine_wake_flusher(engine);
    }
  }

  return (DB_SUCCESS);
}

/** Reclaim stored WAL segments even when a flush has no new batches. A failed
deletion or directory sync is retried on the next flush.
@param[in,out]  engine  database engine
@param[in,out]  info    flush boundary and count of WAL segments successfully
                        removed */
static void engine_recycle_wal(engine_t &engine, engine_flush_info_t &info) {
  uint64_t removed = 0;
  if (wal_recycle(*engine.wal, info.flushed_seq, removed) == DB_SUCCESS) {
    info.segments_removed = removed;
  }
}

/** engine_flush() with flush_mutex held.
@param[in,out]  engine  database engine
@param[out]     info    flush counts and stored sequence boundary
@param[out]     error   reason for failure
@return DB_SUCCESS, a preparation error with nothing committed, DB_FENCED, or
  DB_OUTCOME_UNKNOWN after an uncertain commit */
static dberr_t engine_flush_low(engine_t &engine, engine_flush_info_t &info,
                                std::string &error) {
  if (engine_is_fenced(engine)) {
    return (DB_FENCED);
  }

  /* Everything visible now; writes that become visible meanwhile wait for
  the next flush. The batches stay valid until they are retired below,
  which only this flush does. */
  std::vector<memtable_flush_item_t> items;

  memtable_collect(*engine.mt, items);
  if (items.empty()) {
    info.flushed_seq = storage_get_state(*engine.storage)->flushed_seq;
    engine_recycle_wal(engine, info);

    return (DB_SUCCESS);
  }

  std::vector<storage_write_t> writes;
  writes.resize(items.size());
  uint64_t rows = 0;

  for (size_t i = 0; i < items.size(); ++i) {
    writes[i].seq = items[i].seq;
    writes[i].batch = items[i].batch;
    writes[i].schema = items[i].schema;
    rows += items[i].batch->n_rows;
  }

  /* Writes and queries continue while the storage works. */
  dberr_t err =
      storage_flush_prepare(*engine.storage, &writes[0], writes.size(), error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* Readers register before BEGIN and retain retired versions. A reader
  seeing the old F keeps these batches; a reader seeing the new F filters
  them out. COMMIT does not hold any PulseTen reader lock. */
  err = storage_flush_commit(*engine.storage, error);
  if (err != DB_SUCCESS) {
    engine_fence(engine);

    return (err);
  }

  /* Retiring may free the batches. */
  memtable_retire(*engine.mt, items.back().seq);

  info.rows = rows;
  info.writes = items.size();
  info.flushed_seq = items.back().seq;

  engine_recycle_wal(engine, info);

  return (DB_SUCCESS);
}

/** Store every visible, unflushed write in DuckDB in one transaction and
retire it from the memtable.
@param[in]   engine  engine
@param[out]  info    what was stored
@param[out]  error   message on failure
@retval DB_SUCCESS stored, or nothing to store
@retval DB_ERROR, DB_OUT_OF_MEMORY rolled back; nothing changed; may be
  retried
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN the commit failed; the engine is fenced and
  reopening finds out what was stored */
dberr_t engine_flush(engine_t &engine, engine_flush_info_t &info,
                     std::string &error) {
  info = engine_flush_info_t();

  error.clear();

  std::lock_guard<std::mutex> flush_lock(engine.flush_mutex);

  return (engine_flush_low(engine, info, error));
}

/** Start a drop: make drop_epoch odd, so that a snapshot taken across the
drop is taken again. End it with engine_drop_guard_t.
@param[in,out]  engine  database engine
@return DB_SUCCESS, or DB_FENCED with drop_epoch unchanged */
static dberr_t engine_drop_begin(engine_t &engine) {
  std::lock_guard<std::mutex> lock(engine.write_mutex);

  if (engine.fenced) {
    return (DB_FENCED);
  }

  ++engine.drop_epoch;

  return (DB_SUCCESS);
}

/** End a drop: make drop_epoch even again on every return. */
struct engine_drop_guard_t {
  /** Engine whose odd epoch to make even again. */
  engine_t *engine{nullptr};

  /** Finish the drop epoch and wake readers waiting for a stable snapshot. */
  ~engine_drop_guard_t() {
    std::lock_guard<std::mutex> lock(engine->write_mutex);
    ++engine->drop_epoch;
    engine->drop_cv.notify_all();
  }
};

/** Drop the stored partitions whose days have all passed the retention
period, measured back from a given time.
@param[in]   engine   engine
@param[in]   now_us   the time the retention period ends at
@param[out]  removed  partitions dropped
@param[out]  error    message on failure
@retval DB_SUCCESS dropped, or nothing to drop
@retval DB_ERROR rolled back; nothing changed
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN the commit failed; the engine is fenced */
dberr_t engine_expire(engine_t &engine, int64_t now_us, uint64_t &removed,
                      std::string &error) {
  removed = 0;

  error.clear();

  if (engine.options.retention_days == 0) {
    return (DB_SUCCESS);
  }

  /* No flush is prepared while the flush lock is held. */
  std::lock_guard<std::mutex> flush_lock(engine.flush_mutex);

  if (engine_is_fenced(engine)) {
    return (DB_FENCED);
  }

  /* Partitions ending on or before this day hold only expired times. */
  const int64_t cutoff =
      now_us - (int64_t)engine.options.retention_days * UT_DAY_US;
  const int64_t end_day = ut_utc_day(cutoff);

  /* No logical change: do not invalidate snapshots on every timer tick. */
  const storage_state_t *state = storage_get_state(*engine.storage);
  bool has_expired = false;
  for (const std::pair<const std::pair<uint64_t, int64_t>, storage_partition_t>
           &part : state->partitions) {
    if (part.second.first_day + state->partition_days <= end_day) {
      has_expired = true;
      break;
    }
  }
  if (!has_expired) {
    return (DB_SUCCESS);
  }

  /* F does not change on expiry. Mark the entire deletion so a reader
  cannot combine the old cold snapshot with writes after the deletion. */
  dberr_t err = engine_drop_begin(engine);
  if (err != DB_SUCCESS) {
    return (err);
  }

  engine_drop_guard_t drop_guard;
  drop_guard.engine = &engine;

  /* Existing cursors retain their DuckDB MVCC snapshot. Only a cursor
  still establishing its snapshot retries if this epoch changes. */
  err = storage_expire(*engine.storage, end_day, removed, error);

  if (err == DB_OUTCOME_UNKNOWN) {
    engine_fence(engine);
  }

  if (err == DB_SUCCESS && removed > 0) {
    std::lock_guard<std::mutex> lock(engine.flusher_mutex);
    engine.stats.expired_partitions += removed;
  }

  return (err);
}

/** Let writes go on after a drop, whether it succeeded or failed. */
struct engine_pause_guard_t {
  /** Engine whose writes to resume. */
  engine_t *engine{nullptr};

  /** Resume writers and notify those waiting for the table drop. */
  ~engine_pause_guard_t() {
    std::lock_guard<std::mutex> lock(engine->write_mutex);
    engine->writes_paused = false;
    engine->pause_cv.notify_all();
  }
};

/** Drop a table: every write to it, stored or not, and its schema.
@param[in]   engine     engine
@param[in]   table      table name as given by the user
@param[in]   table_len  length of table
@param[out]  error      message on failure
@retval DB_SUCCESS dropped and durable
@retval DB_INVALID_ARGUMENT not a valid table name
@retval DB_NOT_FOUND no such table
@retval DB_ERROR, DB_OUT_OF_MEMORY not dropped; may be retried
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN the commit failed; the engine is fenced */
dberr_t engine_drop_table(engine_t &engine, const char *table, size_t table_len,
                          std::string &error) {
  error.clear();

  std::string canonical;
  if (schema_table_name(table, table_len, canonical) != DB_SUCCESS) {
    error = "invalid table name";

    return (DB_INVALID_ARGUMENT);
  }

  /* No flush runs while the flush lock is held. */
  std::lock_guard<std::mutex> flush_lock(engine.flush_mutex);

  /* Stop new writes, then let the queued ones become visible: the flush
  below then stores every write to the table, so none is left in the
  memtable or replayed from the WAL. */
  {
    std::lock_guard<std::mutex> lock(engine.write_mutex);
    if (engine.fenced) {
      return (DB_FENCED);
    }
    engine.writes_paused = true;
  }
  engine_pause_guard_t pause;
  pause.engine = &engine;

  dberr_t err = memtable_wait_visible(*engine.mt);
  if (err != DB_SUCCESS) {
    return (err);
  }

  if (memtable_get_schema(*engine.mt, canonical) == nullptr) {
    error = "table " + canonical + " does not exist";

    return (DB_NOT_FOUND);
  }

  engine_flush_info_t info;
  err = engine_flush_low(engine, info, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* A reader whose snapshot crosses the drop starts again, so it never
  combines the stored table with writes to a new one. */
  err = engine_drop_begin(engine);
  if (err != DB_SUCCESS) {
    return (err);
  }

  engine_drop_guard_t drop_guard;
  drop_guard.engine = &engine;

  err = storage_drop_table(*engine.storage, canonical, error);

  if (err == DB_OUTCOME_UNKNOWN) {
    engine_fence(engine);
  }
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* Every write to the table was retired by the flush. */
  {
    std::lock_guard<std::mutex> lock(engine.write_mutex);
    memtable_drop_table(*engine.mt, canonical);
    engine.drops++;
  }

  return (DB_SUCCESS);
}

/** Copy the newest schema of every table, flushed or not, by name.
@param[in]   engine  engine
@param[out]  tables  the schemas */
void engine_list_tables(engine_t &engine, std::vector<schema_t> &tables) {
  memtable_list_schemas(*engine.mt, tables);
}

/** Background flusher: flush every interval, or when woken.
@param[in]  arg  the engine
@return NULL */
static void *engine_flusher_main(void *arg) {
  engine_t *engine = static_cast<engine_t *>(arg);
  const std::chrono::milliseconds interval(engine->options.flush_interval_ms);
  std::unique_lock<std::mutex> lock(engine->flusher_mutex);

  for (;;) {
    if (!engine->flusher_stop && !engine->flusher_wake) {
      engine->flusher_wake_cv.wait_for(lock, interval);
    }

    if (engine->flusher_stop) {
      break;
    }

    engine->flusher_wake = false;
    lock.unlock();

    engine_flush_info_t info;
    std::string error;
    dberr_t err = engine_flush(*engine, info, error);
    const bool stored = err == DB_SUCCESS && info.writes > 0;

    /* Then drop what has passed the retention period. */
    if (err == DB_SUCCESS) {
      uint64_t removed = 0;
      err = engine_expire(*engine, ut_now_us(), removed, error);
    }

    lock.lock();

    /* An expiry failure does not undo the completed flush. */
    if (stored) {
      engine->stats.background_flushes++;
    }
    if (err != DB_SUCCESS) {
      engine->stats.background_errors++;
    }

    engine->room_cv.notify_all();
  }

  return (nullptr);
}

/** Read the engine counters.
@param[in,out]  engine  database engine
@param[out]     stats   counter snapshot */
void engine_get_stats(engine_t &engine, engine_stats_t &stats) {
  {
    std::lock_guard<std::mutex> lock(engine.flusher_mutex);
    stats = engine.stats;
  }

  wal_stats_t wal;
  wal_get_stats(*engine.wal, wal);
  stats.wal_segments = wal.segments;
}

/** Build what a query sees from the stored state at F and a memtable snapshot
of the writes after F.
@param[in]   state  cold catalog read from the query transaction
@param[in]   hot    pinned memtable snapshot after the cold boundary
@param[out]  snap   query tables combining the cold catalog and pinned hot
                    batches */
static void engine_query_tables(const storage_state_t &state,
                                const memtable_snapshot_t &hot,
                                query_snapshot_t &snap) {
  snap.flushed_seq = state.flushed_seq;
  snap.partition_days = state.partition_days;

  /* Stored tables, with the newest stored version and their partitions. */
  std::map<std::string, storage_table_t>::const_iterator t;

  for (t = state.tables.begin(); t != state.tables.end(); ++t) {
    query_table_t &table = snap.tables[t->first];
    table.schema = t->second.versions.back();

    std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
        p;
    for (p = state.partitions.begin(); p != state.partitions.end(); ++p) {
      if (p->first.first == t->second.table_id) {
        table.partitions.push_back(p->second);
      }
    }
  }

  /* Unflushed batches; their schema replaces the stored one if newer. */
  std::map<std::string, memtable_table_view_t>::const_iterator h;

  for (h = hot.tables.begin(); h != hot.tables.end(); ++h) {
    query_table_t &table = snap.tables[h->first];
    table.batches = h->second.batches;

    if (h->second.schema->version > table.schema.version) {
      table.schema = *h->second.schema;
    }
  }
}

/** A query reading a consistent snapshot. */
struct engine_cursor_t {
  /** Engine the cursor reads. */
  engine_t *engine{nullptr};

  /** Connection running the snapshot transaction. */
  duckdb_connection conn{nullptr};

  /** The transaction was started and must be ended. */
  bool in_transaction{false};

  /** Memtable writes after F. */
  memtable_snapshot_t hot;

  /** hot is pinned or taken and must be released. */
  bool has_hot{false};

  /** What the query sees; the query registry points here. */
  query_snapshot_t snap;

  /** The running statement; NULL until it is opened. */
  query_cursor_t *query{nullptr};
};

/** Request interruption of an active cursor. The running or next fetch
observes the request; after it returns DB_INTERRUPTED, every later fetch does
too. Normal end is reached by a successful fetch with empty rows; interrupting
after that leaves later fetches successful and empty.
@param[in]  cursor  cursor */
void engine_cursor_interrupt(engine_cursor_t &cursor) {
  query_interrupt(*cursor.query);
}

/** Stop a query and free its cursor.
@param[in]  cursor  cursor; may be NULL */
void engine_cursor_close(engine_cursor_t *cursor) {
  if (cursor == nullptr) {
    return;
  }

  /* The statement stops before the snapshot it reads goes away. */
  query_close(cursor->query);

  /* Disconnect rolls back the read transaction. Do not run fresh SQL here:
  an application global may close its cursor during process teardown. */
  if (cursor->conn != nullptr) {
    duckdb_disconnect(&cursor->conn);
  }

  if (cursor->has_hot) {
    memtable_snapshot_release(*cursor->engine->mt, cursor->hot);
  }

  ut_delete(cursor);
}

/** Release a partially opened cursor, including its snapshot pin. */
struct engine_cursor_guard_t {
  /** Cursor to close; NULL once handed over. */
  engine_cursor_t *cursor{nullptr};

  /** Close a cursor that was not transferred to the caller. */
  ~engine_cursor_guard_t() { engine_cursor_close(cursor); }
};

/** Take the snapshot of a cursor: the DuckDB transaction at F and the
memtable writes after F.
@param[in,out]  cursor  cursor whose cold transaction and hot pin are
                        established
@param[out]     retry   true if a concurrent drop requires a new snapshot
@param[out]     error   reason for failure
@return DB_SUCCESS, DB_FENCED, DB_CORRUPTION or a DuckDB snapshot error; retry
  reports a crossed drop on success */
static dberr_t engine_cursor_snapshot_low(engine_cursor_t &cursor, bool &retry,
                                          std::string &error) {
  engine_t *engine = cursor.engine;
  retry = false;
  uint64_t drop_epoch = 0;

  {
    std::unique_lock<std::mutex> lock(engine->write_mutex);
    while (!engine->fenced && engine->drop_epoch % 2 != 0) {
      engine->drop_cv.wait(lock);
    }
    if (engine->fenced) {
      return (DB_FENCED);
    }
    drop_epoch = engine->drop_epoch;
  }
  /* Register BEFORE fixing the cold snapshot. A flush can then commit
  and retire at any point without making an old reader lose hot rows. */
  memtable_snapshot_pin(*engine->mt, cursor.hot);
  cursor.has_hot = true;
  batch_seq_t flushed_seq = 0;
  dberr_t err = query_begin(cursor.conn, flushed_seq, error);
  cursor.in_transaction = true;
  if (err != DB_SUCCESS) {
    return (err);
  }

  storage_state_t cold;
  err = storage_snapshot(cursor.conn, cold, error);
  if (err != DB_SUCCESS) {
    return (err);
  }
  if (cold.flushed_seq != flushed_seq) {
    return (DB_CORRUPTION);
  }
  err = memtable_snapshot_at(*engine->mt, flushed_seq, cursor.hot);
  if (err != DB_SUCCESS) {
    return (err);
  }
  engine_query_tables(cold, cursor.hot, cursor.snap);
  {
    std::lock_guard<std::mutex> lock(engine->write_mutex);
    if (engine->fenced) {
      return (DB_FENCED);
    }
    retry = engine->drop_epoch != drop_epoch;
  }

  return (DB_SUCCESS);
}

/** Take a cursor's snapshot, again if it crossed a drop, without blocking
flushes or holding a reader lock during DuckDB work.
@param[in,out]  cursor  query cursor
@param[out]     error   reason for failure
@return DB_SUCCESS after a stable snapshot is fixed, or the snapshot/rollback
  error */
static dberr_t engine_cursor_snapshot(engine_cursor_t &cursor,
                                      std::string &error) {
  for (;;) {
    bool retry = false;
    const dberr_t err = engine_cursor_snapshot_low(cursor, retry, error);
    if (err != DB_SUCCESS || !retry) {
      return (err);
    }

    duckdb_result result{};
    const bool ok =
        duckdb_query(cursor.conn, "ROLLBACK", &result) == DuckDBSuccess;
    duckdb_destroy_result(&result);
    if (!ok) {
      error = "cannot restart the query snapshot after a drop";

      return (DB_ERROR);
    }
    cursor.in_transaction = false;
    cursor.snap = query_snapshot_t();
    memtable_snapshot_release(*cursor.engine->mt, cursor.hot);
    cursor.has_hot = false;
  }
}

/** Start a SELECT, or EXPLAIN of it, over one consistent snapshot, for
reading its rows chunk by chunk.
@param[in]   engine   engine
@param[in]   sql      a single SELECT; see query0query.h
@param[in]   explain  explain it instead of running it
@param[out]  out      open cursor, or NULL on failure
@param[out]  error    reason on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT for a refused statement or result
  type, DB_ERROR for a SQL error, or DB_OUT_OF_MEMORY */
dberr_t engine_cursor_open(engine_t &engine, const std::string &sql,
                           bool explain, engine_cursor_t **out,
                           std::string &error) {
  *out = nullptr;

  error.clear();

  engine_cursor_t *cursor = ut_new<engine_cursor_t>();
  if (cursor == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  engine_cursor_guard_t owner;
  owner.cursor = cursor;
  cursor->engine = &engine;

  if (duckdb_connect(storage_get_database(*engine.storage), &cursor->conn) !=
      DuckDBSuccess) {
    cursor->conn = nullptr;
    error = "cannot connect to DuckDB";

    return (DB_ERROR);
  }

  /* Check the statement before taking a snapshot for it. */
  dberr_t err = query_check(cursor->conn, sql, error);

  if (err == DB_SUCCESS) {
    err = engine_cursor_snapshot(*cursor, error);
  }

  if (err == DB_SUCCESS) {
    err = query_open(cursor->conn, cursor->snap, sql, explain, &cursor->query,
                     error);
  }

  if (err != DB_SUCCESS) {
    return (err);
  }

  *out = cursor;
  owner.cursor = nullptr;

  return (DB_SUCCESS);
}

/** Read the column metadata of a cursor.
@param[in]  cursor  cursor whose result metadata is read
@return the result columns of a cursor */
const std::vector<query_column_t> &engine_cursor_columns(
    const engine_cursor_t &cursor) {
  return (query_cursor_columns(*cursor.query));
}

/** Read the next rows of a cursor, at most QUERY_CHUNK_ROWS.
@param[in,out]  cursor  query cursor
@param[out]     rows    the rows; empty once every row was read
@param[out]     error   reason for failure
@return as query_fetch() */
dberr_t engine_cursor_fetch(engine_cursor_t &cursor, query_rows_t &rows,
                            std::string &error) {
  return (query_fetch(*cursor.query, rows, error));
}

/** Show DuckDB's plan for a SELECT, as a cursor would run it.
@param[in,out]  engine  database engine
@param[in]      sql     a single SELECT
@param[out]     plan    plan text; empty on failure
@param[out]     error   reason for failure
@return DB_SUCCESS or the error from planning or fetching the EXPLAIN result */
dberr_t engine_explain(engine_t &engine, const std::string &sql,
                       std::string &plan, std::string &error) {
  plan.clear();

  engine_cursor_t *cursor = nullptr;

  dberr_t err = engine_cursor_open(engine, sql, true, &cursor, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  query_rows_t rows;

  for (;;) {
    err = engine_cursor_fetch(*cursor, rows, error);
    if (err != DB_SUCCESS || rows.empty()) {
      break;
    }

    for (size_t r = 0; r < rows.size(); ++r) {
      for (size_t c = 0; c < rows[r].size(); ++c) {
        plan += rows[r][c].s + "\n";
      }
    }
  }

  engine_cursor_close(cursor);

  if (err != DB_SUCCESS) {
    plan.clear();
  }

  return (err);
}

/** Read the memtable counters.
@param[in,out]  engine  database engine
@param[out]     stats   counter snapshot */
void engine_get_memtable_stats(engine_t &engine, memtable_stats_t &stats) {
  memtable_get_stats(*engine.mt, stats);
}

/** Borrow the engine's memtable for diagnostics.
@param[in,out]  engine  database engine
@return the memtable, for tests and diagnostics */
memtable_t *engine_get_memtable(engine_t &engine) {
  return (engine.mt);
}

/** Borrow the engine's storage for diagnostics.
@param[in,out]  engine  database engine
@return the storage, for tests and diagnostics; only while no flush runs */
storage_t *engine_get_storage(engine_t &engine) {
  return (engine.storage);
}

#ifdef PT_DEBUG
/** Make the next WAL flush fail.
@param[in]  engine  engine */
void engine_dbg_fail_next_flush(engine_t &engine) {
  wal_dbg_fail_next_flush(*engine.wal);
}

/** Make the next storage flush fail before it commits.
@param[in]  engine  engine */
void engine_dbg_fail_next_store(engine_t &engine) {
  storage_dbg_fail_next_flush(*engine.storage);
}
#endif
