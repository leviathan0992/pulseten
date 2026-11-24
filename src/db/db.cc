/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db.cc
 *    Database lifecycle, recovery, writes, maintenance and query snapshots.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <algorithm>
#include <chrono>
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

#include "db_schema.h"
#include "db_table.h"
#include "ingest.h"
#include "memtable.h"
#include "os_file.h"
#include "pt_duckdb.h"
#include "query.h"
#include "store.h"
#include "ut.h"
#include "wal.h"
#include "wal_recovery.h"

/* ====================== Lifecycle and recovery ====================== */

/** State of a WAL replay. */
struct db_replay_context_t {
  /** Memtable the records are replayed into. */
  memtable *m_memtable{nullptr};

  /** Table directory owns schema versions reconstructed during replay. */
  db_tables *m_tables{nullptr};

  /** Writes at or below this sequence are already stored and are skipped. */
  batch_seq_t m_flushed_seq{0};

  /** Durable default for tables created only in the WAL. */
  uint32_t m_partition_duration_hours{0};

  /** Records replayed so far. */
  uint64_t m_replayed_records{0};
};

/** Replay one record after F into the memtable, in sequence order.
@return DB_SUCCESS, DB_CORRUPTION, or the error of the failed step */
db_status_t db::replay(void *ctx, batch_seq_t seq, const unsigned char *payload,
                       size_t len) {
  db_replay_context_t *replay = static_cast<db_replay_context_t *>(ctx);

  /* Every write after F must be in the WAL, without a gap. */
  if (seq != replay->m_flushed_seq + replay->m_replayed_records + 1) {
    return (db_status_t(DB_CORRUPTION,
                        "WAL replay expected sequence " +
                            std::to_string(replay->m_flushed_seq +
                                           replay->m_replayed_records + 1) +
                            ", found " + std::to_string(seq)));
  }

  ingest_plan_t plan;

  if (const db_status_t err = ingest_decode(payload, len, plan); !err.ok()) {
    return (err);
  }

  db_table *table = nullptr;
  if (const db_status_t err =
          replay->m_tables->find_or_create(plan.m_table_name, table);
      !err.ok()) {
    return (err);
  }

  const uint32_t hours = table->m_partition_duration_hours != 0
                             ? table->m_partition_duration_hours
                             : replay->m_partition_duration_hours;
  if (const db_status_t err =
          replay->m_tables->reserve_write(*replay->m_memtable, seq, plan);
      !err.ok()) {
    return (err);
  }

  table->m_partition_duration_hours = hours;
  replay->m_replayed_records++;

  /* Replay is in order, so this never waits. */
  replay->m_memtable->publish(seq);

  return (DB_SUCCESS);
}

/** Create the memtable at F and restore table schemas for replay. */
db_status_t db::init_memtable() {
  const store_catalog_t &catalog = m_store->catalog();

  if (const db_status_t err = memtable::create(
          catalog.m_flushed_seq, m_options.m_memtable_limit_bytes, &m_memtable);
      !err.ok()) {
    return (err);
  }

  m_options.m_partition_duration_hours = catalog.m_partition_duration_hours;

  /* Seed write planning and WAL replay with stored column identities.
  Flushed rows remain in DuckDB. */
  for (const auto &[name, table] : catalog.m_tables) {
    const db_status_t err = table.m_initialized
                                ? m_tables.restore_schema(table.m_schema)
                                : m_tables.restore_declared(name);
    if (!err.ok()) {
      return (err);
    }

    db_table *target = m_tables.find(name);
    target->m_partition_duration_hours = table.m_partition_duration_hours;
    target->m_retention_hours = table.m_retention_hours;
  }

  return (DB_SUCCESS);
}

/** Recover the WAL after the stored boundary and verify sequence continuity. */
db_status_t db::open_wal(const char *dir) {
  ut_a(dir != nullptr);

  const store_catalog_t &catalog = m_store->catalog();

  if (const db_status_t err = wal_cleanup_incomplete_files(dir); !err.ok()) {
    return (err);
  }

  std::vector<batch_seq_t> file_first_seqs;
  if (const db_status_t err = wal_scan_files(dir, file_first_seqs); !err.ok()) {
    return (err);
  }

  m_wal = ut_new<wal>(m_options.m_wal_file_size_bytes);
  if (m_wal == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  if (file_first_seqs.empty()) {
    /* Stored writes without a WAL: the WAL was lost. */
    if (catalog.m_flushed_seq > 0) {
      return (db_status_t(DB_CORRUPTION,
                          "WAL files are missing at stored sequence " +
                              std::to_string(catalog.m_flushed_seq)));
    }

    if (const db_status_t err =
            m_wal->create_low(dir, catalog.m_wal_id, m_options.m_wal_sync);
        !err.ok()) {
      return (err);
    }

    return (DB_SUCCESS);
  }

  /* Check the retained prefix before WAL recovery can truncate a suffix. */
  if (file_first_seqs.front() > catalog.m_flushed_seq + 1) {
    return (db_status_t(DB_CORRUPTION,
                        "WAL starts at sequence " +
                            std::to_string(file_first_seqs.front()) +
                            ", missing writes after stored sequence " +
                            std::to_string(catalog.m_flushed_seq)));
  }

  db_replay_context_t replay;
  replay.m_memtable = m_memtable;
  replay.m_tables = &m_tables;
  replay.m_flushed_seq = catalog.m_flushed_seq;
  replay.m_partition_duration_hours = catalog.m_partition_duration_hours;

  wal_recovery_t wal_recovery;
  if (const db_status_t err = m_wal->open_low(
          dir, db::replay, &replay, wal_recovery, catalog.m_wal_id,
          m_options.m_wal_sync, catalog.m_flushed_seq, catalog.m_flushed_seq);
      !err.ok()) {
    return (err);
  }

  /* Replay made every write after F visible, in order. */
  memtable::stats_t stats;
  m_memtable->stats(stats);

  ut_a(stats.m_flushed_seq == catalog.m_flushed_seq);
  ut_a(stats.m_visible_seq == wal_recovery.m_last_seq);
  ut_a(stats.m_live_writes == replay.m_replayed_records);

  m_next_seq = stats.m_visible_seq + 1;

  return (DB_SUCCESS);
}

/** Check recovered time-series names against SQL tables. */
db_status_t db::validate_table_names() {
  std::set<std::string> sql_tables;
  if (const db_status_t err = m_store->read_sql_tables(sql_tables); !err.ok()) {
    return (err);
  }

  std::set<std::string> names;
  m_tables.collect_names(names);
  for (const std::string &name : names) {
    if (sql_tables.count(name) != 0) {
      return (db_status_t(
          DB_CORRUPTION,
          "SQL table conflicts with a recovered time-series table: " + name));
    }
  }

  return (DB_SUCCESS);
}

/** Find the entry before waiting; the cache mutex is released by the lookup. */
db_status_t db::lock_table(const std::string &name, db_table_lock_mode_t mode,
                           db_table_lock &lock, const query_cancel *cancel,
                           uint32_t timeout_ms) {
  db_table *table = nullptr;
  if (const db_status_t err = m_tables.find_or_create(name, table); !err.ok()) {
    return (err);
  }

  return (lock.acquire(*this, *table, mode, cancel, timeout_ms));
}

/** Recheck existence and generation after waiting for the table X lock. */
db_status_t db::lock_existing_table(const std::string &name,
                                    db_table_lock &lock) {
  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  db_table *table = m_tables.find(name);
  if (table == nullptr) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "time-series table does not exist: " + name));
  }

  const uint64_t generation = table->generation();
  if (!table->exists()) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "time-series table does not exist: " + name));
  }

  UT_DEBUG_SYNC("db.before_existing_table_lock");
  if (const db_status_t err =
          lock.acquire(*this, *table, db_table_lock_mode_t::EXCLUSIVE, nullptr,
                       m_options.m_write_lock_timeout_ms);
      !err.ok()) {
    return (err);
  }

  if (!table->exists() || table->generation() != generation) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "table changed while waiting for access: " + name));
  }

  return (DB_SUCCESS);
}

/** Start the database's background maintenance thread. */
db_status_t db::start_flusher() {
  if (pthread_create(&m_flusher_thread, nullptr, db::flusher_main, this) != 0) {
    return (db_status_t(DB_ERROR, "cannot start the background flush thread"));
  }

  m_flusher_started = true;

  return (DB_SUCCESS);
}

/** Open resources in order; db_open() cleans up every failure. */
db_status_t db::open(const char *dir, const db::options_t &options) {
  if (dir == nullptr || dir[0] == '\0') {
    return (db_status_t(DB_INVALID_ARGUMENT, "dir must be a path"));
  }

  if (const db_status_t err = options.validate(); !err.ok()) {
    return (err);
  }

  m_options = options;

  if (options.m_create_if_missing) {
    /* Make the directory entry durable before initializing a database. */
    if (const db_status_t err = os_dir_create(dir); !err.ok()) {
      return (err.with_context("cannot prepare database directory '" +
                               std::string(dir) + "'"));
    }
  }

  /* Exclude other instances before opening storage or WAL files. */
  std::string directory;
  bool initialize_catalog = false;
  if (const db_status_t err = m_directory.open(dir, options.m_create_if_missing,
                                               directory, initialize_catalog);
      !err.ok()) {
    return (err);
  }

  dir = directory.c_str();

  UT_DEBUG_SYNC("db.after_prepare_open");

  /* The database owns storage throughout initialization and failure cleanup. */
  m_store = ut_new<store>();
  if (m_store == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  if (const db_status_t err = m_store->initialize(
          dir, m_options.m_partition_duration_hours,
          m_options.m_duckdb_memory_limit_bytes, m_options.m_duckdb_threads,
          m_options.m_wal_sync, initialize_catalog);
      !err.ok()) {
    return (err);
  }

  UT_DEBUG_SYNC("db.after_open_store");

  /* Restore column identities needed to interpret WAL batches. */
  if (const db_status_t err = init_memtable(); !err.ok()) {
    return (err);
  }

  /* Open or create the WAL and replay writes not yet stored in DuckDB. */
  if (const db_status_t err = open_wal(dir); !err.ok()) {
    return (err);
  }

  /* Validate table names after replay has recovered all changes. */
  if (const db_status_t err = validate_table_names(); !err.ok()) {
    return (err);
  }

  /* Resolve time-series SQL sources to stored partitions and memtable rows. */
  if (const db_status_t err = query_init(m_store->connection()); !err.ok()) {
    return (err);
  }

  /* Start background work only after recovery and query setup are complete. */
  return (start_flusher());
}

/** Check settings before opening any database resource. */
db_status_t db::options_t::validate() const {
  if (m_duckdb_threads > PT_DUCKDB_MAX_THREADS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "DuckDB threads must be at most " +
                            std::to_string(PT_DUCKDB_MAX_THREADS)));
  }

  if (m_duckdb_memory_limit_bytes > PT_DUCKDB_MAX_MEMORY_LIMIT_BYTES) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "DuckDB memory limit must be at most 2^53 bytes"));
  }

  if (m_flush_interval_ms == 0) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "background flush interval must be greater than zero"));
  }

  /* Longer periods exceed the supported time range and could overflow the
  retention cutoff. */
  if (m_retention_hours > DB_MAX_RETENTION_HOURS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "retention period must be at most " +
                            std::to_string(DB_MAX_RETENTION_HOURS) + " hours"));
  }

  if (m_partition_duration_hours < 1 ||
      m_partition_duration_hours > STORE_MAX_PARTITION_HOURS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "partition duration must be 1 to " +
                            std::to_string(STORE_MAX_PARTITION_HOURS) +
                            " hours"));
  }

  return (DB_SUCCESS);
}

/** Allocate a database and open it; a failed open destroys the instance. */
db_status_t db_open(const char *dir, const db::options_t *options, db **out) {
  *out = nullptr;

  const db::options_t user_options =
      options != nullptr ? *options : db::options_t();

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

/** Drop by path only after every database handle has closed. */
db_status_t db_drop_database(const char *dir) {
  return (db_directory::drop(dir));
}

/** Stop the flusher, release data resources, then drop the directory lock. */
void db::close() {
  /* Stop the flusher first; it uses everything below. */
  if (m_flusher_started) {
    {
      std::lock_guard<std::mutex> lock(m_flusher_mutex);

      m_flusher_stop = true;
      m_flusher_event.set();
    }

    pthread_join(m_flusher_thread, nullptr);
    m_flusher_started = false;
  }

  ut_a(m_writes.empty() && !m_write_active);

  static_cast<void>(wal::destroy(m_wal));
  m_wal = nullptr;

  memtable::destroy(m_memtable);
  m_memtable = nullptr;

  store::destroy(m_store);
  m_store = nullptr;

  m_directory.close();

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

/** Return DB_FENCED carrying the first fatal failure, or success.
@return DB_SUCCESS or DB_FENCED with the first recorded cause */
db_status_t db::status_low() const {
  ut_ad(m_write_mutex.is_owned());

  return (m_status.ok() ? m_status : m_status.with_code(DB_FENCED));
}

/** Preserve the first fatal cause and wake background and capacity waiters.
@param[in]  status  failure; later calls preserve the first cause */
void db::set_status_low(const db_status_t &status) {
  ut_ad(m_write_mutex.is_owned());
  ut_a(!status.ok());

  /* Preserve the first cause; later failures can be its consequences. */
  if (m_status.ok()) {
    m_status = status;

    m_memtable_space_event.set();
    m_flusher_event.set();
  }

  return;
}

/** Record a fatal failure while serializing against new write reservations.
@param[in]  status  failure; later calls preserve the first cause */
void db::set_status(const db_status_t &status) {
  ut_lock_guard_t lock(m_write_mutex);

  set_status_low(status);

  return;
}

/** Read failure state and its diagnostic under one write-lock acquisition.
@return DB_SUCCESS or DB_FENCED with the first recorded cause */
db_status_t db::status() {
  ut_lock_guard_t lock(m_write_mutex);

  return (status_low());
}

/** Retain each table's latest immutable schema. */
void db::collect_schemas(std::vector<schema_ref_t> &schemas) {
  m_tables.collect_schemas(schemas);

  return;
}

/** Destroy an instance through the same shutdown path as explicit close. */
db::~db() {
  close();
}

/** Copy coherent settings and the newest schema for every visible slot. */
void db::collect_tables(std::vector<db_table_info_t> &tables) {
  /* Freeze declarations, settings and write admission while copying slots. */
  std::shared_lock<ut_shared_mutex_t> snapshots(m_snapshot_mutex);
  ut_lock_guard_t writes(m_write_mutex);

  std::vector<std::pair<std::string, db_table *>> entries;
  m_tables.collect_entries(entries);

  tables.clear();
  tables.reserve(entries.size());
  for (const auto &[name, table] : entries) {
    if (!table->exists()) {
      continue;
    }

    db_table_info_t info;
    info.m_name = name;
    info.m_schema = table->schema();
    info.m_partition_duration_hours = table->m_partition_duration_hours;
    info.m_retention_hours = table->m_retention_hours;
    tables.push_back(std::move(info));
  }
}

/** Declare a name and policy durably; the first write supplies the schema. */
db_status_t db::create_table(const std::string &input,
                             const create_table_options_t &options) {
  std::string name;
  if (const db_status_t err =
          schema_normalize_table_name(input.data(), input.size(), name);
      !err.ok()) {
    return (err);
  }

  const uint32_t hours = options.m_partition_duration_hours != 0
                             ? options.m_partition_duration_hours
                             : m_options.m_partition_duration_hours;
  if (hours > STORE_MAX_PARTITION_HOURS ||
      options.m_retention_hours.value_or(0) > DB_MAX_RETENTION_HOURS) {
    return (
        db_status_t(DB_INVALID_ARGUMENT,
                    "table partition or retention hours exceed the maximum"));
  }

  /* Exclude SQL DDL, then serialize this name with first writes. */
  if (const db_status_t err = lock_namespace(false, nullptr); !err.ok()) {
    return (err);
  }

  std::shared_lock<ut_rwlock> names(m_namespace_lock, std::adopt_lock);

  /* Reject a known name before waiting; X rechecks it below. */
  db_table *known = m_tables.find(name);
  if (known != nullptr && known->exists()) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "time-series table already exists: " + name));
  }

  db_table_lock lock;
  if (const db_status_t err =
          lock_table(name, db_table_lock_mode_t::EXCLUSIVE, lock, nullptr,
                     m_options.m_write_lock_timeout_ms);
      !err.ok()) {
    return (err);
  }

  db_table *table = lock.table();
  if (table->exists()) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "time-series table already exists: " + name));
  }

  bool is_sql_table = false;
  if (const db_status_t err = m_store->sql_table_exists(name, is_sql_table);
      !err.ok()) {
    return (err);
  }

  if (is_sql_table) {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "SQL table already exists: " + name));
  }

  /* Store changes run under flush; publication waits for snapshot X. */
  ut_lock_guard_t storage(m_flush_mutex);
  std::unique_lock<ut_shared_mutex_t> snapshots(m_snapshot_mutex);
  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  const db_status_t err =
      m_store->create_table(name, hours, options.m_retention_hours);
  if (err.code() == DB_OUTCOME_UNKNOWN) {
    set_status(err);
  }

  if (!err.ok()) {
    return (err);
  }

  /* Publish the declaration only after its catalog transaction commits. */
  table->m_partition_duration_hours = hours;
  table->m_retention_hours = options.m_retention_hours;
  table->m_declared.store(true);
  {
    std::lock_guard<std::mutex> cached(m_catalog_snapshot_mutex);
    m_catalog_snapshot.reset();
  }

  return (DB_SUCCESS);
}

/** Flush the table first, so WAL replay cannot revive it after the drop. */
db_status_t db::drop_table(const std::string &input) {
  std::string name;
  if (const db_status_t err =
          schema_normalize_table_name(input.data(), input.size(), name);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err = lock_namespace(false, nullptr); !err.ok()) {
    return (err);
  }

  std::shared_lock<ut_rwlock> names(m_namespace_lock, std::adopt_lock);

  db_table_lock lock;
  if (const db_status_t err = lock_existing_table(name, lock); !err.ok()) {
    return (err);
  }

  db_table *table = lock.table();
  const uint64_t generation = table->generation();
  if (generation == UINT64_MAX) {
    return (db_status_t(DB_LIMIT_EXCEEDED, "table generation exhausted"));
  }

  /* Store every row of this incarnation, so WAL replay skips them. */
  ut_lock_guard_t storage(m_flush_mutex);
  flush_info_t flushed;
  const db_status_t stored = flush_low(flushed);
  if (stored.code() != DB_OUT_OF_MEMORY) {
    m_memtable_space_event.set();
  }

  if (!stored.ok()) {
    return (stored);
  }

  UT_DEBUG_SYNC("db.before_drop_table");

  std::unique_lock<ut_shared_mutex_t> snapshots(m_snapshot_mutex);
  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  const db_status_t err = m_store->drop_table(name);
  if (err.code() == DB_OUTCOME_UNKNOWN) {
    set_status(err);
  }

  if (!err.ok()) {
    return (err);
  }

  /* Keep the stable name slot, but clear every part of the old incarnation. */
  {
    ut_lock_guard_t writes(m_write_mutex);
    std::lock_guard<std::mutex> schemas(table->m_schema_mutex);

    std::atomic_store(&table->m_schema, schema_ref_t{});
    table->m_versions.clear();
    table->m_declared.store(false);
    table->m_partition_duration_hours = 0;
    table->m_retention_hours.reset();
    table->m_generation.store(generation + 1);
  }

  {
    std::lock_guard<std::mutex> cached(m_catalog_snapshot_mutex);
    m_catalog_snapshot.reset();
  }

  return (DB_SUCCESS);
}

/** Replace retention atomically without flushing accepted writes. */
db_status_t db::set_table_options(const std::string &input,
                                  const table_options_t &options) {
  std::string name;
  if (const db_status_t err =
          schema_normalize_table_name(input.data(), input.size(), name);
      !err.ok()) {
    return (err);
  }

  if (options.m_retention_hours.value_or(0) > DB_MAX_RETENTION_HOURS) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "table retention exceeds the maximum hours"));
  }

  db_table_lock lock;
  if (const db_status_t err = lock_existing_table(name, lock); !err.ok()) {
    return (err);
  }

  db_table *table = lock.table();
  const uint32_t hours = table->m_partition_duration_hours;

  /* Store changes run under flush; publication waits for snapshot X. */
  ut_lock_guard_t storage(m_flush_mutex);
  std::unique_lock<ut_shared_mutex_t> snapshots(m_snapshot_mutex);
  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  const db_status_t err =
      m_store->set_table_options(name, hours, options.m_retention_hours);
  if (err.code() == DB_OUTCOME_UNKNOWN) {
    set_status(err);
  }

  if (!err.ok()) {
    return (err);
  }

  /* Publish only after the catalog transaction commits. */
  table->m_retention_hours = options.m_retention_hours;
  {
    std::lock_guard<std::mutex> cached(m_catalog_snapshot_mutex);
    m_catalog_snapshot.reset();
  }

  m_flusher_event.set();

  return (DB_SUCCESS);
}

/* ================== Write planning and publication ================== */

/** Context of one write, owned until commit and publication finish. */
class db::write_context final : private ut_noncopyable {
 private:
  /** The database coordinates this write with WAL and memtable state. */
  friend class db;

  /** Retain the database that owns queueing and table-lock policy. */
  explicit write_context(db &database) : m_db(database) {}

  /** Validate input timestamps and cache their minimum and maximum. */
  db_status_t validate_times();

  /** Plan under S, taking X and replanning if the schema must change. */
  db_status_t plan();

  /* ==================== Caller-owned write plan ===================== */

  /** Owner, alive until this write returns. */
  db &m_db;

  /** Database-owned table, valid until database close. */
  db_table *m_target{nullptr};

  /** Shared for existing columns, exclusive for schema changes, through
  WAL commit and publication. */
  db_table_lock m_table_lock;

  /** Normalized table name. */
  std::string m_table_name;

  /** Expected table generation, captured before waiting for table access. */
  uint64_t m_expected_table_generation{0};

  /** The input points. */
  const ingest_point_t *m_points{nullptr};

  /** Number of input points. */
  uint32_t m_point_count{0};

  /** Earliest input timestamp, kept across capacity waits and replanning. */
  int64_t m_min_time_us{INT64_MAX};

  /** Latest input timestamp, kept across capacity waits and replanning. */
  int64_t m_max_time_us{INT64_MIN};

  /** Validated schema change and owned column batch. */
  ingest_plan_t m_plan;

  /** Partition duration in hours, set under the final table lock mode. */
  uint32_t m_partition_hours{0};

  /** Encoded WAL record; stays valid until the WAL wait returns. */
  std::string m_payload;

  /* ================= Protected by db::m_write_mutex ================= */

  /** Ordered sequence, reset to zero if this write is certainly not logged. */
  batch_seq_t m_seq{0};

  /** The leader has completed or rejected this write. */
  bool m_done{false};

  /** Completion result, consumed before the write returns. */
  db_status_t m_status;

  /* ============== Internally synchronized notification ============== */

  /** Wakes this writer on completion or when it becomes group leader. */
  ut_event_t m_ready;
};

/** Validate input timestamps and cache bounds for later policy checks. */
db_status_t db::write_context::validate_times() {
  if (const db_status_t err = ingest_validate_batch(m_points, m_point_count);
      !err.ok()) {
    return (err);
  }

  for (uint32_t p = 0; p < m_point_count; ++p) {
    const int64_t time = m_points[p].m_time_us;
    if (const db_status_t err = ingest_validate_time(p, time); !err.ok()) {
      return (err);
    }

    m_min_time_us = std::min(m_min_time_us, time);
    m_max_time_us = std::max(m_max_time_us, time);
  }

  return (DB_SUCCESS);
}

/** Keep the schema stable while planning and encoding the WAL payload.
@return DB_SUCCESS, or the write validation error; no WAL record is queued
  here */
db_status_t db::write_context::plan() {
  ut_ad(m_target != nullptr && m_table_lock.holds(m_target));

  schema_ref_t schema = m_target->schema();

  if (const db_status_t err = ingest_plan(schema.get(), m_table_name, m_points,
                                          m_point_count, m_plan);
      !err.ok()) {
    return (err);
  }

  if (!m_plan.m_new_columns.empty()) {
    /* Drop S before waiting for X so concurrent creators can progress. */
    m_table_lock.release();

    UT_DEBUG_SYNC("db.before_write_schema_lock");

    if (const db_status_t err = m_db.lock_table(
            m_table_name, db_table_lock_mode_t::EXCLUSIVE, m_table_lock,
            nullptr, m_db.m_options.m_write_lock_timeout_ms);
        !err.ok()) {
      return (err);
    }

    ut_ad(m_table_lock.holds(m_target));

    if (m_target->generation() != m_expected_table_generation) {
      return (
          db_status_t(DB_INVALID_ARGUMENT,
                      "table changed while planning write: " + m_table_name));
    }

    /* Replan against changes committed while switching from S to X. */
    schema = m_target->schema();
    if (const db_status_t err = ingest_plan(schema.get(), m_table_name,
                                            m_points, m_point_count, m_plan);
        !err.ok()) {
      return (err);
    }
  }

  ut_a(m_plan.m_table_name == m_table_name);

  ingest_encode(m_plan, m_payload);
  ut_a(!m_payload.empty());

  return (DB_SUCCESS);
}

/** Reject writes targeting an existing SQL table. */
db_status_t db::validate_write_table(const std::string &name) {
  bool exists = false;

  if (const db_status_t err = m_store->sql_table_exists(name, exists);
      !err.ok()) {
    return (err);
  }

  if (exists) {
    return (
        db_status_t(DB_INVALID_ARGUMENT,
                    "SQL table cannot receive time-series writes: " + name));
  }

  return (DB_SUCCESS);
}

/** Wake the flusher and wait until the memtable can admit a write. */
db_status_t db::wait_for_memtable_space() {
  bool waited = false;

  for (;;) {
    const uint64_t signal_count = m_memtable_space_event.reset();

    if (const db_status_t err = status(); !err.ok()) {
      return (err);
    }

    if (m_memtable->has_space()) {
      return (DB_SUCCESS);
    }

    UT_DEBUG_SYNC("db.before_space_wait");

    m_flusher_event.set();

    if (!waited) {
      std::lock_guard<std::mutex> lock(m_flusher_mutex);
      m_stats.m_throttled_writes++;
      waited = true;
    }

    m_memtable_space_event.wait(signal_count);
  }
}

/** Recheck dynamic time policy using the input's already validated bounds. */
db_status_t db::validate_write_times(const write_context &w) const {
  const uint32_t hours =
      w.m_target->m_retention_hours.value_or(m_options.m_retention_hours);

  const uint32_t future = m_options.m_max_future_skew_seconds;
  if (hours == 0 && future == 0) {
    return (DB_SUCCESS);
  }

  const int64_t now = ut_now_us();
  const int64_t cutoff = now - static_cast<int64_t>(hours) * UT_HOUR_US;
  const int64_t latest = now + static_cast<int64_t>(future) * 1000000;
  if ((hours == 0 || w.m_min_time_us >= cutoff) &&
      (future == 0 || w.m_max_time_us <= latest)) {
    return (DB_SUCCESS);
  }

  /* Only a rejected batch walks again, to name the offending point. */
  for (uint32_t p = 0; p < w.m_point_count; ++p) {
    const int64_t time = w.m_points[p].m_time_us;

    if (hours != 0 && time < cutoff) {
      /* Reject points older than the table's current retention window. */
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "point " + std::to_string(p) +
                              " is older than the retention period of " +
                              std::to_string(hours) + " hours"));
    }

    if (future != 0 && time > latest) {
      /* Reject points beyond the configured offset from the database clock. */
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "point " + std::to_string(p) +
                              " exceeds the maximum future offset of " +
                              std::to_string(future) + " seconds"));
    }
  }

  ut_error;
}

/** Allocate the batch before enqueueing it in the database write stream.
Every accepted write must reach commit_write() to complete its queue lifetime.
@return DB_SUCCESS, a validation/allocation error before enqueue, or DB_FENCED
*/
db_status_t db::reserve_write(write_context &w) {
  ut_lock_guard_t lock(m_write_mutex);
  ut_ad(w.m_target != nullptr && w.m_table_lock.holds(w.m_target));

  if (const db_status_t err = status_low(); !err.ok()) {
    return (err);
  }

  ut_ad(w.m_plan.m_new_columns.empty() || w.m_table_lock.exclusive());

  if (m_next_seq == UINT64_MAX) {
    return (DB_LIMIT_EXCEEDED);
  }

  db_table &table = *w.m_target;
  const uint32_t hours = w.m_partition_hours;
  ut_ad(hours > 0 && (table.m_partition_duration_hours == 0 ||
                      table.m_partition_duration_hours == hours));

  /* Allocate and validate before accepting a record into the WAL queue. */
  if (const db_status_t err =
          m_tables.reserve_write(*m_memtable, m_next_seq, w.m_plan);
      !err.ok()) {
    return (err);
  }

  if (table.m_partition_duration_hours == 0) {
    ut_ad(w.m_table_lock.exclusive());
    table.m_partition_duration_hours = hours;
  }

  w.m_seq = m_next_seq++;
  m_writes.push_back(&w);

  return (DB_SUCCESS);
}

/** Complete the selected writes after WAL I/O finishes.*/
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

    writer->m_ready.set();
  }

  return;
}

/** Publish or fail the active group after WAL I/O finishes. */
void db::finish_write_group(size_t count, batch_seq_t last_seq,
                            const db_status_t &status) {
  ut_ad(m_write_mutex.is_owned());
  ut_a(m_write_active);
  ut_ad(count > 0 && count <= m_writes.size());
  ut_ad(m_writes[count - 1]->m_seq == last_seq);

  if (!status.ok()) {
    /* Even a pre-I/O error leaves reserved sequences that cannot be skipped. */
    set_status_low(status);
  }

  const bool may_be_logged = status.ok() || status.code() == DB_OUTCOME_UNKNOWN;

  db_status_t result = status;

  if (m_status.ok()) {
    /* No fatal error occurred during WAL I/O; make this group visible. */
    ut_ad(m_snapshot_mutex.is_owned());

    m_memtable->publish(last_seq);
  } else if (status.ok()) {
    /* Another operation stopped publication while this WAL group was in I/O. */
    result = m_status.with_code(DB_OUTCOME_UNKNOWN);
  }

  complete_writes(count, result, may_be_logged);

  m_write_active = false;

  return;
}

/** Select a bounded queue prefix; a single large write still makes progress.
Queued writes keep the record payloads alive until the group completes. */
void db::build_write_group(std::vector<wal_record_t> &records) {
  ut_ad(m_write_mutex.is_owned());
  ut_a(!m_write_active);
  ut_a(!m_writes.empty());

  records.clear();

  size_t bytes = 0;

  for (write_context *next : m_writes) {
    ut_ad(next != nullptr && !next->m_done);

    const size_t record_bytes =
        WAL_RECORD_HEADER_BYTES + next->m_payload.size();
    if (!records.empty() && (bytes >= DB_MAX_WRITE_GROUP_BYTES ||
                             record_bytes > DB_MAX_WRITE_GROUP_BYTES - bytes)) {
      break;
    }

    ut_ad(records.empty() || next->m_seq == records.back().m_seq + 1);

    wal_record_t record;
    record.m_seq = next->m_seq;
    record.m_payload =
        reinterpret_cast<const unsigned char *>(next->m_payload.data());
    record.m_payload_bytes = next->m_payload.size();
    records.push_back(record);

    bytes += record_bytes;
  }

  ut_ad(!records.empty());

  return;
}

/** Wait for group completion or lead the next WAL group. */
db_status_t db::commit_write(write_context &w, batch_seq_t &seq) {
  ut_unique_lock_t lock(m_write_mutex);

  /* Wait until another leader completes this write or it becomes queue head. */
  while (!w.m_done && m_writes.front() != &w) {
    const uint64_t signal_count = w.m_ready.reset();

    lock.unlock();
    w.m_ready.wait(signal_count);
    lock.lock();
  }

  if (!w.m_done) {
    if (const db_status_t err = status_low(); !err.ok()) {
      /* Fail this queued write without submitting it to the WAL. */
      complete_writes(1, err, false);
    } else {
      /* This writer is now the leader and selects the next group to commit. */
      std::vector<wal_record_t> records;
      build_write_group(records);

      m_write_active = true;

      /* The leader owns this queue prefix while WAL I/O runs unlocked. */
      lock.unlock();

      UT_DEBUG_SYNC("db.before_wal");

      /* Queue nodes and payloads stay owned until the leader ends I/O. */
      const db_status_t status = m_wal->append(records.data(), records.size());

      std::unique_lock<ut_shared_mutex_t> snapshots(m_snapshot_mutex,
                                                    std::defer_lock);
      if (status.ok()) {
        /* Publish the group without racing query snapshot creation.
        Take the snapshot lock before the write lock to preserve lock order. */
        snapshots.lock();
      }

      lock.lock();

      finish_write_group(records.size(), records.back().m_seq, status);
    }

    if (!m_writes.empty()) {
      /* Wake the next queue head to submit its group or observe the failure. */
      m_writes.front()->m_ready.set();
    }
  }

  seq = w.m_seq;

  return (w.m_status);
}

/** Validate, reserve, log and publish one batch of points. */
db_status_t db::write(const char *table_name, size_t table_name_length,
                      const ingest_point_t *points, uint32_t n_points,
                      batch_seq_t &seq) {
  seq = 0;

  write_context w(*this);

  /* Validate and normalize the name before looking up its table entry. */
  if (const db_status_t err = schema_normalize_table_name(
          table_name, table_name_length, w.m_table_name);
      !err.ok()) {
    return (err.with_context("invalid table name"));
  }

  /* Reject a stopped database before creating table state or locking. */
  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  std::shared_lock<ut_rwlock> namespace_lock(m_namespace_lock, std::defer_lock);

  const db_table *table = m_tables.find(w.m_table_name);
  w.m_expected_table_generation = table == nullptr ? 0 : table->generation();

  if (table == nullptr || table->schema() == nullptr) {
    if (const db_status_t err = lock_namespace(false, nullptr); !err.ok()) {
      return (err);
    }

    namespace_lock =
        std::shared_lock<ut_rwlock>(m_namespace_lock, std::adopt_lock);
  }

  UT_DEBUG_SYNC("db.before_write_table_lock");

  /* Allow concurrent writes to existing columns while blocking schema changes.
  If new columns are needed, plan() releases this shared lock and takes an
  exclusive lock. */
  if (const db_status_t err = lock_table(
          w.m_table_name, db_table_lock_mode_t::SHARED, w.m_table_lock, nullptr,
          m_options.m_write_lock_timeout_ms);
      !err.ok()) {
    return (err);
  }

  w.m_target = w.m_table_lock.table();
  if (w.m_target->generation() != w.m_expected_table_generation) {
    /* Reject the write if its table was dropped while waiting for the lock. */
    return (db_status_t(
        DB_INVALID_ARGUMENT,
        "table changed while waiting for write: " + w.m_table_name));
  }

  /* Reject SQL tables before waiting for memtable capacity. */
  if (w.m_target->schema() == nullptr) {
    if (const db_status_t err = validate_write_table(w.m_table_name);
        !err.ok()) {
      return (err);
    }
  }

  /* Validate once; later policy checks reuse the cached time bounds. */
  w.m_points = points;
  w.m_point_count = n_points;
  if (const db_status_t err = w.validate_times(); !err.ok()) {
    return (err);
  }

  /* Reject stale input before capacity waits or a possible S-to-X upgrade. */
  if (const db_status_t err = validate_write_times(w); !err.ok()) {
    return (err);
  }

  /* Wait for flush progress to free capacity before preparing this write. */
  if (const db_status_t err = wait_for_memtable_space(); !err.ok()) {
    return (err);
  }

  /* Schema changes take X here, before acquiring the global write lock. */
  if (const db_status_t err = w.plan(); !err.ok()) {
    return (err);
  }

  /* plan() may upgrade the table lock; read the final policy after it does. */
  if (const db_status_t err = validate_write_times(w); !err.ok()) {
    return (err);
  }

  /* The partition duration stays fixed while this table mode is held. */
  w.m_partition_hours = w.m_target->m_partition_duration_hours != 0
                            ? w.m_target->m_partition_duration_hours
                            : m_options.m_partition_duration_hours;

  UT_DEBUG_SYNC("db.before_reserve_write");

  if (const db_status_t err = reserve_write(w); !err.ok()) {
    return (err);
  }

  /* The group leader releases m_write_mutex during WAL I/O. */
  return (commit_write(w, seq));
}

/* ================= Flush and background maintenance ================= */

/** Remove WAL files covered by the committed storage boundary. */
db_status_t db::purge_wal() {
  ut_lock_guard_t lock(m_flush_mutex);

  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  uint64_t removed = 0;

  return (m_wal->purge(m_store->catalog().m_flushed_seq, removed));
}

/** Store visible writes and retire their memtable entries. */
db_status_t db::flush_low(db::flush_info_t &info) {
  ut_ad(m_flush_mutex.is_owned());

  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  /* Everything visible now; writes that become visible meanwhile wait for the
  next flush. The batches stay valid until they are retired below, which only
  this flush does. */
  std::vector<const memtable::entry_t *> entries;
  m_memtable->collect_unflushed(entries);

  if (entries.empty()) {
    /* An empty flush still reports the current stored sequence. */
    info.m_flushed_seq = m_store->catalog().m_flushed_seq;

    return (DB_SUCCESS);
  }

  /* Make the WAL prefix durable before DuckDB can durably advance F. With WAL
  sync on this is still needed: writes recovered from the WAL may never have
  been synced. */
  if (const db_status_t err = m_wal->sync(); !err.ok()) {
    /* Uncertain WAL durability requires stopping writes and publication. */
    const db_status_t status = err.with_context("cannot sync WAL before flush");
    set_status(status);

    return (status);
  }

  /* The memtable and the storage must agree on F. */
  ut_a(entries.front()->m_seq == m_store->catalog().m_flushed_seq + 1);

  std::vector<store_write_t> writes;
  writes.resize(entries.size());

  uint64_t rows = 0;

  for (size_t i = 0; i < entries.size(); ++i) {
    writes[i].m_seq = entries[i]->m_seq;
    writes[i].m_batch = &entries[i]->m_batch;

    const db_table *table = m_tables.find(entries[i]->m_batch.m_table_name);
    ut_a(table != nullptr);

    const schema_ref_t schema =
        table->schema(entries[i]->m_batch.m_schema_version);
    ut_a(schema != nullptr);

    /* The table directory retains this version until database close. */
    writes[i].m_schema = schema.get();
    rows += entries[i]->m_batch.m_row_count;
  }

  /* Readers pin before fixing their stored snapshot, so storage can commit
  without a PulseTen reader lock. Publish no memtable retirement until the
  complete storage transaction succeeds. */
  if (const db_status_t err = m_store->append(writes.data(), writes.size());
      !err.ok()) {
    if (err.code() != DB_OUT_OF_MEMORY) {
      /* Commit or rollback failures are OUTCOME_UNKNOWN, never retryable OOM.
       */
      set_status(err);
    }

    return (err);
  }

  ut_a(m_store->catalog().m_flushed_seq == entries.back()->m_seq);

  /* Save the flush result before retire() can free the entries. */
  info.m_rows = rows;
  info.m_writes = entries.size();
  info.m_flushed_seq = entries.back()->m_seq;

  /* Mark these writes as stored; reclaim batches not needed by snapshots. */
  m_memtable->retire(info.m_flushed_seq);

  return (DB_SUCCESS);
}

/** Store every visible, unflushed write in DuckDB in one transaction and retire
it from the memtable. */
db_status_t db::flush(db::flush_info_t &info) {
  info = db::flush_info_t();

  ut_lock_guard_t flush_lock(m_flush_mutex);

  const db_status_t err = flush_low(info);
  if (err.code() != DB_OUT_OF_MEMORY) {
    /* An OOM rollback frees no memtable capacity. */
    m_memtable_space_event.set();
  }

  return (err);
}

/** Drop the stored partitions whose hours have all passed the retention period,
measured back from a given time. */
db_status_t db::expire(int64_t now_us, uint64_t &removed) {
  ut_lock_guard_t flush_lock(m_flush_mutex);

  removed = 0;

  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  /* Partitions ending on or before this hour hold only expired times. */
  std::vector<store_partition_key_t> expired_partitions;
  m_store->collect_expired_partitions(now_us, m_options.m_retention_hours,
                                      expired_partitions);
  if (expired_partitions.empty()) {
    return (DB_SUCCESS);
  }

  /* F does not change on expiry. No snapshot is fixed while the deletion
  runs, so none combines the old stored data with writes after it; cursors
  fixed earlier keep their DuckDB MVCC view. */
  std::unique_lock<ut_shared_mutex_t> snapshots(m_snapshot_mutex);

  /* Recheck after waiting for the snapshot lock. */
  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  const db_status_t err =
      m_store->expire_partitions(expired_partitions, removed);

  /* A rolled-back expiry can run again; an uncertain commit cannot. */
  if (err.code() == DB_OUTCOME_UNKNOWN) {
    set_status(err);
  }

  if (err.ok() && removed > 0) {
    /* Invalidate the cached catalog and count the committed deletions. */

    {
      /* Let subsequent catalog lookups observe the committed deletions. */
      std::lock_guard<std::mutex> lock(m_catalog_snapshot_mutex);

      m_catalog_snapshot.reset();
    }

    {
      /* Count the partitions removed by this successful commit. */
      std::lock_guard<std::mutex> lock(m_flusher_mutex);

      m_stats.m_expired_partitions += removed;
    }
  }

  return (err);
}

/** Wait for a wakeup; during OOM backoff, only close may end the wait early. */
bool db::flusher_wait(bool backoff) {
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(m_options.m_flush_interval_ms);

  for (;;) {
    m_flusher_event.wait_for(deadline - std::chrono::steady_clock::now());
    m_flusher_event.reset();

    std::lock_guard<std::mutex> lock(m_flusher_mutex);
    if (m_flusher_stop) {
      return (false);
    }

    if (!backoff || std::chrono::steady_clock::now() >= deadline) {
      return (true);
    }
  }
}

/** Accumulate a task failure in the background diagnostic.
@param[in,out] result     accumulated errors from this round
@param[in]     task_name  name of the failed task
@param[in]     status     outcome of the task */
static void db_append_background_error(db_status_t &result,
                                       const char *task_name,
                                       const db_status_t &status) {
  if (status.ok() || status.code() == DB_FENCED) {
    return;
  }

  const db_status_t failure = status.with_context(task_name);
  if (result.ok()) {
    result = failure;

    return;
  }

  result =
      db_status_t(result.code(), result.message() + "; " + failure.message());

  return;
}

/** Run background maintenance and retain the pass's failures. */
db_status_t db::flusher_run() {
  db_status_t background_error;

  db::flush_info_t flush_info;
  const db_status_t flush_status = flush(flush_info);
  db_append_background_error(background_error, "flush", flush_status);

  if (flush_status.ok()) {
    /* Stored writes no longer need their WAL files for recovery. */
    const db_status_t purge_status = purge_wal();
    db_append_background_error(background_error, "WAL cleanup", purge_status);
  }

  /* Recoverable failures leave no storage transaction active. */
  if (status().ok()) {
    uint64_t removed = 0;

    const db_status_t expiry_status = expire(ut_now_us(), removed);
    db_append_background_error(background_error, "expire", expiry_status);
  }

  /* I/O is finished; lock only to update shared counters and the last error. */
  std::lock_guard<std::mutex> lock(m_flusher_mutex);

  /* An expiry failure does not undo the completed flush. */
  if (flush_info.m_writes > 0) {
    m_stats.m_background_flushes++;
  }

  if (!background_error.ok()) {
    m_stats.m_background_errors++;

    m_last_background_error = std::move(background_error);
  }

  return (flush_status);
}

/** Run maintenance when its interval elapses or a writer wakes the thread. */
void *db::flusher_main(void *arg) {
  db &database = *static_cast<db *>(arg);

  bool backoff = false;
  while (database.flusher_wait(backoff)) {
    /* Stop on fatal failures; a rolled-back flush OOM does not fence the db. */
    if (!database.status().ok()) {
      break;
    }

    backoff = database.flusher_run().code() == DB_OUT_OF_MEMORY;
  }

  return (nullptr);
}

/** Read the message of the latest background failure. */
db_status_t db::background_status() {
  std::lock_guard<std::mutex> lock(m_flusher_mutex);

  return (m_last_background_error);
}

/** Collect database counters with their owning locks held. */
void db::stats(db::stats_t &stats) {
  {
    std::lock_guard<std::mutex> lock(m_flusher_mutex);
    stats = m_stats;
  }

  stats.m_duckdb_memory_limit_bytes = m_store->memory_limit_bytes();
  stats.m_duckdb_threads = m_store->threads();

  memtable::stats_t mt_stats;
  m_memtable->stats(mt_stats);
  stats.m_flushed_seq = mt_stats.m_flushed_seq;
  stats.m_visible_seq = mt_stats.m_visible_seq;
  stats.m_memtable_bytes = mt_stats.m_live_bytes;

  wal::stats_t log;
  m_wal->stats(log);
  stats.m_wal_file_count = log.m_file_count;

  return;
}

/* ====================== Queries and snapshots ======================= */

/** Pass SQL to DuckDB and close the connection after execution. */
db_status_t db::exec(const std::string &sql, query_cancel *cancel) {
  if (const db_status_t err = status(); !err.ok()) {
    return (err);
  }

  /* Prevent SQL and time-series tables from claiming the same name. */
  std::unique_lock<ut_rwlock> namespace_lock(m_namespace_lock, std::defer_lock);

  query_command command;
  if (const db_status_t err = command.init(m_store->database(), sql, cancel);
      !err.ok()) {
    return (err);
  }

  if (command.changes_namespace()) {
    if (const db_status_t err = lock_namespace(true, cancel); !err.ok()) {
      return (err);
    }

    namespace_lock =
        std::unique_lock<ut_rwlock>(m_namespace_lock, std::adopt_lock);
  }

  /* Collect time-series table names for SQL validation. */
  std::set<std::string> names;
  m_tables.collect_names(names);

  return (command.execute(names));
}

/** Serialize namespace changes without making cancellation wait for SQL. */
db_status_t db::lock_namespace(bool exclusive, const query_cancel *cancel) {
  db_status_t err;

  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(m_options.m_write_lock_timeout_ms);

  const auto allowed = [&] {
    if (cancel != nullptr && cancel->requested()) {
      err = DB_INTERRUPTED;
    } else {
      err = status();
    }

    if (err.ok() && !exclusive && m_options.m_write_lock_timeout_ms != 0 &&
        std::chrono::steady_clock::now() >= deadline) {
      err = db_status_t(DB_LOCK_TIMEOUT,
                        "namespace lock wait exceeded the write deadline");
    }

    return (err.ok());
  };

  const bool acquired = exclusive ? m_namespace_lock.lock_while(allowed)
                                  : m_namespace_lock.lock_shared_while(allowed);
  ut_a(acquired == err.ok());

  return (err);
}

/** Build what a query sees from the stored catalog at F and the pinned
memtable snapshot of writes after F. */
void db_cursor::build_tables(const store_catalog_t &catalog,
                             const std::vector<std::string> &names) {
  ut_ad(m_db != nullptr && m_db->m_snapshot_mutex.is_owned_shared());
  ut_ad(m_memtable_snapshot.m_owner == m_db->m_memtable);
  ut_ad(m_memtable_snapshot.m_base_seq == catalog.m_flushed_seq);
  ut_ad(m_memtable_snapshot.m_base_seq <= m_memtable_snapshot.m_visible_seq);

  m_snapshot.m_flushed_seq = catalog.m_flushed_seq;

  const std::set<std::string> selected(names.begin(), names.end());
  for (const std::string &name : selected) {
    const auto found = catalog.m_tables.find(name);
    if (found == catalog.m_tables.end()) {
      continue;
    }

    const store_table_t &stored = found->second;

    query_table_t &table = m_snapshot.m_tables[name];
    table.m_schema = stored.m_schema;
    table.m_partition_duration_hours = stored.m_partition_duration_hours;

    table.m_partitions.reserve(stored.m_partitions.size());
    for (const auto &[hour, partition] : stored.m_partitions) {
      table.m_partitions.push_back(partition);
    }
  }

  std::map<std::string, schema_version_t> versions;
  m_db->m_memtable->schema_versions(m_memtable_snapshot, names, versions);

  for (const auto &[name, version] : versions) {
    const db_table *source = m_db->m_tables.find(name);
    ut_a(source != nullptr);

    const schema_ref_t schema = source->schema(version);
    ut_a(schema != nullptr);

    query_table_t &table = m_snapshot.m_tables[name];
    table.m_memtable_snapshot = &m_memtable_snapshot;
    if (catalog.m_tables.count(name) == 0) {
      table.m_partition_duration_hours = catalog.m_partition_duration_hours;
    }

    if (schema->m_columns.size() > table.m_schema.m_columns.size()) {
      table.m_schema = *schema;
    }
  }

  /* One fixed now gives every table in the query a consistent cutoff. */
  for (auto &[name, table] : m_snapshot.m_tables) {
    const db_table *source = m_db->m_tables.find(name);
    const uint32_t retention =
        source->m_retention_hours.value_or(m_db->m_options.m_retention_hours);
    if (retention != 0) {
      table.m_cutoff_us =
          m_snapshot.m_now_us - static_cast<int64_t>(retention) * UT_HOUR_US;
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
  if (m_connection != nullptr) {
    duckdb_disconnect(&m_connection);
  }

  /* An empty snapshot is safe to release, including after an open failure. */
  if (m_db != nullptr) {
    m_db->m_memtable->release_snapshot(m_memtable_snapshot);
  }

  m_query = nullptr;
  m_table_locks.release();

  return;
}

/** Release the cursor's query, snapshot and connection. */
db_cursor::~db_cursor() {
  close();
}

/** Delete an allocated cursor; NULL is accepted. */
void db_cursor::destroy(db_cursor *cursor) {
  ut_delete(cursor);

  return;
}

/** Reuse metadata at the stored boundary or read it in this transaction. */
db_status_t db::read_query_catalog(
    duckdb_connection conn, batch_seq_t flushed_seq,
    std::shared_ptr<const store_catalog_t> &out) {
  ut_ad(m_snapshot_mutex.is_owned_shared());

  {
    std::lock_guard<std::mutex> lock(m_catalog_snapshot_mutex);
    if (m_catalog_snapshot != nullptr &&
        m_catalog_snapshot->m_flushed_seq == flushed_seq) {
      out = m_catalog_snapshot;

      return (DB_SUCCESS);
    }
  }

  std::shared_ptr<store_catalog_t> loaded = std::make_shared<store_catalog_t>();
  if (const db_status_t err = store_catalog_reader(conn, *loaded).read();
      !err.ok()) {
    return (err);
  }

  if (loaded->m_flushed_seq != flushed_seq) {
    return (db_status_t(DB_CORRUPTION,
                        "catalog flushed sequence " +
                            std::to_string(loaded->m_flushed_seq) +
                            " differs from query snapshot sequence " +
                            std::to_string(flushed_seq)));
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

/** Begin a read transaction and fix its stored sequence boundary. */
db_status_t db_cursor::begin_snapshot(batch_seq_t &flushed_seq) {
  ut_ad(m_db != nullptr && m_connection != nullptr);
  ut_ad(m_db->m_snapshot_mutex.is_owned_shared());
  ut_ad(m_memtable_snapshot.m_owner == m_db->m_memtable);

  /* Read metadata through the cursor's connection. */
  pt_duckdb_session session(m_connection);

  if (const db_status_t err = session.begin(); !err.ok()) {
    return (err);
  }

  /* Reading F also fixes the transaction's snapshot here and now. */
  pt_duckdb_rows_t rows;
  if (const db_status_t err = pt_duckdb_exec(
          m_connection, "SELECT flushed_seq FROM _pt_state", rows);
      !err.ok()) {
    return (err);
  }

  if (rows.size() != 1 || rows[0][0].m_is_null) {
    return (db_status_t(DB_CORRUPTION, "bad _pt_state"));
  }

  flushed_seq = static_cast<batch_seq_t>(rows[0][0].m_integer);

  return (DB_SUCCESS);
}

/** Combine a DuckDB transaction with a pinned memtable interval. */
db_status_t db_cursor::take_snapshot(const std::vector<std::string> &names) {
  db &database = *m_db;

  std::shared_lock<ut_shared_mutex_t> snapshots(database.m_snapshot_mutex);

  /* Admit the query after waiting for snapshot access. Later database failure
  does not revoke admission; snapshot operations report their own errors. */
  if (const db_status_t err = database.status(); !err.ok()) {
    return (err);
  }

  /* Shared snapshot access keeps V stable until the stored snapshot is fixed.
  Pin first so a concurrent flush cannot reclaim batches this reader needs. */
  database.m_memtable->pin_snapshot(m_memtable_snapshot);
  UT_DEBUG_SYNC("db.after_snapshot_pin");

  batch_seq_t flushed_seq = 0;
  if (const db_status_t err = begin_snapshot(flushed_seq); !err.ok()) {
    return (err);
  }

  std::shared_ptr<const store_catalog_t> stored;
  if (const db_status_t err =
          database.read_query_catalog(m_connection, flushed_seq, stored);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err =
          database.m_memtable->align_snapshot(flushed_seq, m_memtable_snapshot);
      !err.ok()) {
    return (err);
  }

  m_snapshot.m_now_us = ut_now_us();
  build_tables(*stored, names);

  return (DB_SUCCESS);
}

/** Allocate a cursor, releasing partial initialization on failure. */
db_status_t db::create_cursor(db_cursor **out, query_cancel *cancel) {
  *out = nullptr;

  if (cancel != nullptr && cancel->requested()) {
    return (DB_INTERRUPTED);
  }

  db_cursor *cursor = ut_new<db_cursor>();
  if (cursor == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  cursor->m_db = this;

  if (duckdb_connect(m_store->database(), &cursor->m_connection) !=
      DuckDBSuccess) {
    cursor->m_connection = nullptr;
    ut_delete(cursor);

    return (db_status_t(DB_ERROR, "cannot connect to DuckDB"));
  }

  cursor->m_cancel = cancel;
  if (cancel != nullptr) {
    cancel->attach(cursor->m_connection);
  }

  *out = cursor;

  return (DB_SUCCESS);
}

/** Select known time-series sources, then take their S locks in order. */
db_status_t db::lock_query_tables(const std::vector<std::string> &names,
                                  db_table_locks &locks,
                                  const query_cancel *cancel) {
  ut_a(locks.m_locks.empty());

  /* Collect the full set before locking. CTE names and SQL tables do not
  need time-series locks; the set sorts and deduplicates the remaining names. */
  std::set<std::string> requests;

  for (const std::string &name : names) {
    const db_table *table = m_tables.find(name);
    if (table != nullptr && table->exists()) {
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
  for (const std::string &name : names) {
    if (m_snapshot.m_tables.count(name) == 0) {
      continue;
    }

    const db_table *table = m_db->m_tables.find(name);
    ut_a(table != nullptr);

    if (!m_table_locks.holds(table)) {
      return (false);
    }
  }

  return (true);
}

/** Start a SELECT, or EXPLAIN of it, over one consistent snapshot, for reading
its rows chunk by chunk. */
db_status_t db::open_cursor(const std::string &sql, bool explain,
                            db_cursor **out, query_cancel *cancel) {
  *out = nullptr;

  db_cursor *cursor = nullptr;
  if (const db_status_t err = open_cursor_low(sql, explain, &cursor, cancel);
      !err.ok()) {
    db_cursor::destroy(cursor);

    return (err);
  }

  *out = cursor;

  return (DB_SUCCESS);
}

/** Analyze, lock and snapshot, then open the query on one cursor. */
db_status_t db::open_cursor_low(const std::string &sql, bool explain,
                                db_cursor **cursor, query_cancel *cancel) {
  if (const db_status_t err = create_cursor(cursor, cancel); !err.ok()) {
    return (err);
  }

  /* Check the statement before taking a snapshot for it. */
  query_ranges_t ranges;
  std::vector<std::string> tables;
  if (const db_status_t err =
          query_analyze((*cursor)->m_connection, sql, ranges, tables);
      !err.ok()) {
    return (err);
  }

  for (;;) {
    if (const db_status_t err =
            lock_query_tables(tables, (*cursor)->m_table_locks, cancel);
        !err.ok()) {
      return (err);
    }

    UT_DEBUG_SYNC("db.before_query_snapshot");
    if (const db_status_t err = (*cursor)->take_snapshot(tables); !err.ok()) {
      return (err);
    }

    if ((*cursor)->tables_locked(tables)) {
      break;
    }

    /* Drop the transaction, pin and old modes; lock the new sorted set. */
    db_cursor::destroy(*cursor);

    if (const db_status_t err = create_cursor(cursor, cancel); !err.ok()) {
      return (err);
    }
  }

  if (cancel != nullptr && cancel->requested()) {
    return (DB_INTERRUPTED);
  }

  if (const db_status_t err =
          query_cursor::open((*cursor)->m_connection, (*cursor)->m_snapshot,
                             sql, explain, ranges, &(*cursor)->m_query);
      !err.ok()) {
    return (err);
  }

  if (cancel != nullptr) {
    cancel->detach();

    (*cursor)->m_cancel = nullptr;

    if (cancel->requested()) {
      return (DB_INTERRUPTED);
    }
  }

  return (DB_SUCCESS);
}

/** Return the column metadata fixed when the query was opened. */
const std::vector<query_column_t> &db_cursor::columns() const {
  return (m_query->columns());
}

/** Read the next chunk through the cursor's query. */
db_status_t db_cursor::fetch(query_rows_t &rows) {
  return (m_query->fetch(rows));
}

/** Open a temporary cursor and collect DuckDB's query plan. */
db_status_t db::explain(const std::string &sql, std::string &plan) {
  plan.clear();

  db_cursor *cursor = nullptr;

  db_status_t err = open_cursor(sql, true, &cursor);
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
        plan += rows[r][c].m_string + "\n";
      }
    }
  }

  db_cursor::destroy(cursor);

  if (!err.ok()) {
    plan.clear();
  }

  return (err);
}
