/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db.cc
 *    Database instance lifecycle, writes and recovery.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

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
#include <pthread.h>

#include "db_table.h"
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
#include "ut_noncopyable.h"
#include "ut_rwlock.h"
#include "ut_shared_mutex.h"
#include "ut_time.h"
#include "wal.h"
#include "wal_recv.h"

/** File locked while a directory is open, so that only one database uses
it at a time. */
#define DB_LOCK_FILE "LOCK"

/** State of a WAL replay. */
struct db_replay_t {
  /** Memtable the records are replayed into. */
  memtable *m_mt{nullptr};

  /** Writes up to here are in storage and are skipped. */
  batch_seq_t m_flushed_seq{0};

  /** Records replayed so far. */
  uint64_t m_replayed{0};
};

/** Replay one WAL record into the memtable, unless it is already stored.
Records after F must be consecutive.
@return DB_SUCCESS, DB_CORRUPTION, or the error of the failed step */
db_status_t db::replay(void *ctx, batch_seq_t seq, const unsigned char *payload,
                       size_t len) {
  db_replay_t *replay = static_cast<db_replay_t *>(ctx);

  /* Already in DuckDB; its segment just has not been purged yet. */
  if (seq <= replay->m_flushed_seq) {
    return (DB_SUCCESS);
  }

  /* Every write after F must be in the WAL, without a gap. */
  if (seq != replay->m_flushed_seq + replay->m_replayed + 1) {
    return (DB_CORRUPTION);
  }

  ingest_plan_t plan;

  if (const db_status_t err = ingest_decode(payload, len, plan); !err.ok()) {
    return (err);
  }

  if (const db_status_t err = replay->m_mt->reserve(seq, plan); !err.ok()) {
    return (err);
  }

  replay->m_replayed++;

  /* Replay is in order, so this never waits. */
  replay->m_mt->publish(seq);

  return (DB_SUCCESS);
}

/** Take the directory lock.
@return DB_SUCCESS, DB_LOCKED if another opener holds the lock, or
  DB_IO_ERROR */
db_status_t db::lock_dir(const char *dir) {
  const std::string path = std::string(dir) + "/" + DB_LOCK_FILE;

  if (const db_status_t err =
          os_file_open(path.c_str(), OS_FILE_OPEN_OR_CREATE, m_lock_file);
      !err.ok()) {
    return (err);
  }

  const db_status_t err = os_file_lock(m_lock_file);
  if (!err.ok()) {
    static_cast<void>(os_file_close(m_lock_file));
  }

  return (err);
}

/** Create the memtable at F and restore the current column directories. */
db_status_t db::restore_memtable() {
  const storage_catalog_t &catalog = m_storage->catalog();

  if (const db_status_t err = memtable::create(
          catalog.m_flushed_seq, m_options.m_memtable_limit_bytes, &m_mt);
      !err.ok()) {
    return (err);
  }

  /* Seed write planning and WAL replay with stored column identities.
  Flushed rows remain in DuckDB. */
  for (const auto &entry : catalog.m_tables) {
    if (const db_status_t err = m_mt->restore_schema(entry.second.m_schema);
        !err.ok()) {
      return (err);
    }
  }

  return (DB_SUCCESS);
}

/** Open or create the WAL, replay writes after F and verify its boundaries.
The memtable must already contain the stored column directories. */
db_status_t db::open_wal(const char *dir) {
  ut_a(dir != nullptr);

  const storage_catalog_t &catalog = m_storage->catalog();

  std::vector<batch_seq_t> firsts;
  if (const db_status_t err = wal_scan_segments(dir, firsts); !err.ok()) {
    return (err);
  }

  if (firsts.empty()) {
    /* Stored writes without a WAL: the WAL was lost. */
    if (catalog.m_flushed_seq > 0) {
      return (DB_CORRUPTION);
    }

    if (const db_status_t err =
            wal::create(dir, &m_wal, catalog.m_wal_id, m_options.m_wal_sync,
                        m_options.m_wal_segment_size_bytes);
        !err.ok()) {
      return (err);
    }

    return (DB_SUCCESS);
  }

  db_replay_t replay;
  replay.m_mt = m_mt;
  replay.m_flushed_seq = catalog.m_flushed_seq;

  wal_recv_t wal_recv;
  if (const db_status_t err = wal::open(
          dir, db::replay, &replay, &m_wal, wal_recv, catalog.m_wal_id,
          m_options.m_wal_sync, m_options.m_wal_segment_size_bytes);
      !err.ok()) {
    return (err);
  }

  /* Storage cannot hold writes the WAL never had, and the WAL must still
  have every write after F. */
  if (wal_recv.m_last_seq < catalog.m_flushed_seq ||
      wal_recv.m_first_seq > catalog.m_flushed_seq + 1) {
    return (DB_CORRUPTION);
  }

  /* Replay made every write after F visible, in order. */
  memtable_stats_t stats;
  m_mt->read_stats(stats);

  ut_a(stats.m_flushed_seq == catalog.m_flushed_seq);
  ut_a(stats.m_visible_seq == wal_recv.m_last_seq);
  ut_a(stats.m_live_writes == replay.m_replayed);
  m_next_seq = stats.m_visible_seq + 1;

  return (DB_SUCCESS);
}

/** Register recovered time-series schemas and check their names against DuckDB.
User-table names are inspected temporarily, never cached as database state.
@return DB_SUCCESS, DB_OUT_OF_MEMORY, DB_CORRUPTION or a catalog query error */
db_status_t db::restore_tables() {
  std::vector<schema_ref_t> schemas;
  m_mt->reference_schemas(schemas);

  duckdb_rows_t rows;
  if (const db_status_t read = duckdb_exec(
          m_storage->connection(),
          "SELECT lower(table_name) FROM information_schema.tables "
          "WHERE table_catalog = current_database() AND table_schema = 'main'",
          rows);
      !read.ok()) {
    return (read);
  }

  std::set<std::string> names;
  for (const auto &row : rows) {
    names.insert(row[0].m_s);
  }

  for (const schema_ref_t &schema : schemas) {
    if (names.count(schema->m_name) != 0) {
      return (db_status_t(
          DB_CORRUPTION,
          "user table conflicts with a recovered time-series table: " +
              schema->m_name));
    }

    db_table_t *table = nullptr;
    if (const db_status_t err = m_tables.find_or_create(schema->m_name, table);
        !err.ok()) {
      return (err);
    }

    table->set_schema(schema);
  }

  return (DB_SUCCESS);
}

/** Find the entry before waiting; the cache mutex is released by the lookup. */
db_status_t db::lock_table(const std::string &name, db_table_lock_mode_t mode,
                           db_table_lock &lock, const query_cancel *cancel) {
  db_table_t *table = nullptr;
  if (const db_status_t err = m_tables.find_or_create(name, table); !err.ok()) {
    return (err);
  }

  return (lock.acquire(*this, *table, mode, cancel));
}

/** Start background work after storage, WAL and scans are ready. */
db_status_t db::start_flusher() {
  if (pthread_create(&m_flusher_thread, nullptr, db::flusher_main, this) != 0) {
    return (db_status_t(DB_ERROR, "cannot start the background flush thread"));
  }

  m_flusher_started = true;

  return (DB_SUCCESS);
}

/** Open resources in order; db_open() cleans up every failure. */
db_status_t db::open(const char *dir, const db_options_t &options) {
  m_options = options;

  /* Resolve the DuckDB memory budget before creating directories or files. */
  if (const db_status_t err =
          duckdb_resolve_memory_limit(m_options.m_duckdb_memory_limit_bytes);
      !err.ok()) {
    return (err);
  }

  /* Create the directory if absent and make its entry durable. */
  if (const db_status_t err = os_dir_create(dir); !err.ok()) {
    return (err.with_context("cannot prepare database directory '" +
                             std::string(dir) + "'"));
  }

  /* Keep later file operations independent of the working directory. */
  std::string directory;
  if (const db_status_t err = os_dir_resolve(dir, directory); !err.ok()) {
    return (err);
  }

  dir = directory.c_str();

  /* Exclude another database instance before opening storage or WAL files. */
  if (const db_status_t err = lock_dir(dir); !err.ok()) {
    return (err);
  }

  /* Load committed metadata, or initialize an empty storage file. */
  if (const db_status_t err =
          storage::open(dir, m_options.m_partition_duration_days, &m_storage,
                        m_options.m_duckdb_memory_limit_bytes);
      !err.ok()) {
    return (err);
  }

  /* Restore column identities needed to interpret WAL batches. */
  if (const db_status_t err = restore_memtable(); !err.ok()) {
    return (err);
  }

  /* Open or create the WAL and replay writes not yet stored in DuckDB. */
  if (const db_status_t err = open_wal(dir); !err.ok()) {
    return (err);
  }

  /* Register table names and schemas after replay has recovered all changes. */
  if (const db_status_t err = restore_tables(); !err.ok()) {
    return (err);
  }

  /* Resolve time-series SQL sources to stored partitions and memtable rows. */
  if (const db_status_t err = query_prepare_database(m_storage->connection());
      !err.ok()) {
    return (err);
  }

  /* Start background work only after recovery and query setup are complete. */
  return (start_flusher());
}

/** Check settings before allocating or opening database resources. */
db_status_t db_options_t::validate() const {
  if (m_duckdb_memory_limit_bytes > DUCKDB_MAX_MEMORY_LIMIT_BYTES) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "DuckDB memory limit must be at most 2^53 bytes"));
  }

  if (m_flush_interval_ms == 0) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "background flush interval must be greater than zero"));
  }

  /* Longer than any time a write may have; also keeps the cutoff from
  overflowing. */
  if (m_retention_days > DB_MAX_RETENTION_DAYS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "retention period must be at most " +
                            std::to_string(DB_MAX_RETENTION_DAYS) + " days"));
  }

  if (m_partition_duration_days < 1 ||
      m_partition_duration_days > STORAGE_MAX_PARTITION_DAYS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "partition span must be 1 to " +
                            std::to_string(STORAGE_MAX_PARTITION_DAYS) +
                            " days"));
  }

  return (DB_SUCCESS);
}

/** Open a database, validating before allocation or filesystem changes. */
db_status_t db_open(const char *dir, const db_options_t *options, db **out) {
  *out = nullptr;

  if (dir == nullptr || dir[0] == '\0') {
    return (db_status_t(DB_INVALID_ARGUMENT, "dir must be a path"));
  }

  const db_options_t user_options =
      options != nullptr ? *options : db_options_t();

  if (const db_status_t err = user_options.validate(); !err.ok()) {
    return (err);
  }

  db *database = ut_new<db>();
  if (database == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  if (const db_status_t err = database->open(dir, user_options); !err.ok()) {
    /* Release every resource acquired before the open failed. */
    db_close(database);

    return (err);
  }

  *out = database;

  return (DB_SUCCESS);
}

/** Stop the flusher, release data resources and drop the directory lock last.
 */
void db::close() {
  /* Stop the flusher first; it uses everything below. */
  if (m_flusher_started) {
    {
      std::lock_guard<std::mutex> lock(m_flusher_mutex);

      m_flusher_stop = true;
      m_flusher_wake_cv.notify_one();
    }

    pthread_join(m_flusher_thread, nullptr);
    m_flusher_started = false;
  }

  ut_a(m_writes.empty() && !m_write_active);

  static_cast<void>(wal::destroy(m_wal));
  m_wal = nullptr;

  memtable::destroy(m_mt);
  m_mt = nullptr;

  storage::destroy(m_storage);
  m_storage = nullptr;

  static_cast<void>(os_file_close(m_lock_file));

  return;
}

/** Release a database's resources and destroy its instance. */
void db_close(db *database) {
  if (database == nullptr) {
    return;
  }

  ut_delete(database);

  return;
}

/** Return a complete rejection status from the sole permanent failure.
@return DB_SUCCESS or DB_FENCED with the first recorded cause */
db_status_t db::check_status_low() const {
  ut_ad(m_write_mutex.is_owned());

  return (m_db_status.ok() ? m_db_status : m_db_status.with_code(DB_FENCED));
}

/** Preserve the first fatal cause and wake background and capacity waiters.
@param[in]  status  failure; later calls preserve the first cause */
void db::fail_low(const db_status_t &status) {
  ut_ad(m_write_mutex.is_owned());
  ut_a(!status.ok());

  /* Preserve the first cause; later failures can be its consequences. */
  if (m_db_status.ok()) {
    m_db_status = status;
    /* Publish progress under the wait mutex so no waiter misses this change. */
    std::lock_guard<std::mutex> lock(m_flusher_mutex);
    ++m_progress_epoch;
    m_memtable_space_cv.notify_all();
    m_flusher_wake = true;
    m_flusher_wake_cv.notify_one();
  }

  return;
}

/** Record a fatal failure while serializing against new write reservations.
@param[in]  status  failure; later calls preserve the first cause */
void db::fail(const db_status_t &status) {
  ut_lock_guard_t lock(m_write_mutex);

  fail_low(status);

  return;
}

/** Read failure state and its diagnostic under one write-lock acquisition.
@return DB_SUCCESS or DB_FENCED with the first recorded cause */
db_status_t db::check_status() {
  ut_lock_guard_t lock(m_write_mutex);

  return (check_status_low());
}

/** Copy the newest schema of every table, flushed or not, by name. */
void db::copy_schemas(std::vector<schema_t> &schemas) {
  std::vector<schema_ref_t> references;
  m_mt->reference_schemas(references);

  schemas.clear();
  schemas.reserve(references.size());
  for (const schema_ref_t &schema : references) {
    schemas.push_back(*schema);
  }

  return;
}

/** Destroy an instance through the same shutdown path as explicit close. */
db::~db() {
  close();
}
