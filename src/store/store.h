/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * store.h
 *    Durable storage of flushed writes in a DuckDB file.
 *
 * The file holds four internal tables and one physical table per table
 * and time partition, alongside ordinary tables:
 *
 *   _pt_state       format, flushed sequence F, next table id, persistent
 *                   default partition hours and WAL identity
 *   _pt_tables      table id, name, initialization state and partition hours
 *   _pt_columns     latest logical column directory: id, name, kind and type
 *   _pt_partitions  table id, start UTC hour and physical column count
 *
 *   _pt_t<id>_p<start hour> / _pt_t<id>_n<-start hour>
 *                   _pt_seq, _pt_row, then c0 (time), c1, ... by column id
 *
 * Each table has a fixed partition duration. Boundaries are multiples of
 * that duration in UTC hours since 1970-01-01. Auto-created tables use the
 * database's persistent default. Declared tables may have no columns or
 * partitions yet.
 *
 * store::append() stores consecutive writes after F in one DuckDB
 * transaction: new tables and columns, new partitions, the rows, and the
 * new F. Only after COMMIT succeeds does it publish the new catalog in
 * memory. F records which writes are stored; recovery replays only WAL
 * records after F.
 *
 * Columns are only added. A partition created before a column existed gets
 * the column when a later write needs it; its older rows read as NULL.
 *
 * store::expire_partitions() drops whole partitions whose hours have all
 * passed a cutoff, in one transaction.
 *
 * Ordinary tables and views use DuckDB's catalog and transactions. Their
 * names must not conflict with internal or time-series tables. Retention
 * applies only to time-series data.
 *
 *-------------------------------------------------------------------------
 */

#ifndef store_h
#define store_h

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "db_batch.h"
#include "db_schema.h"
#include "pt_duckdb.h"
#include "ut.h"
#include "ut_debug.h"

/** Name of the storage file in the database directory. */
#define STORE_FILE_NAME "_pt_.duckdb"

/** Current storage format; only this value is accepted. */
#define STORE_FORMAT_VERSION 1

/** Default partition duration of a new file, in hours. */
#define STORE_DEFAULT_PARTITION_HOURS 24

/** Largest partition duration allowed, in hours. */
#define STORE_MAX_PARTITION_HOURS 8784

/** Largest supported retention period, in hours. */
#define STORE_MAX_RETENTION_HOURS 2640000

/** A partition, by (table id, start hour). */
typedef std::pair<uint64_t, int64_t> store_partition_key_t;

/** Metadata for one physical DuckDB partition table. */
struct store_partition_t {
  /** Table the partition belongs to. */
  uint64_t m_table_id{0};

  /** First UTC hour of the partition, hour 0 being 1970-01-01; a multiple
  of the partition duration. */
  int64_t m_start_hour{0};

  /** Name of the physical DuckDB table; see store_partition_name(). */
  std::string m_table_name;

  /** Columns c0 .. c<m_column_count - 1> exist in the physical table. */
  uint32_t m_column_count{0};
};

/** A table as stored. */
struct store_table_t {
  /** Table id in _pt_tables. */
  uint64_t m_table_id{0};

  /** Fixed hours covered by this table's partitions. */
  uint32_t m_partition_duration_hours{STORE_DEFAULT_PARTITION_HOURS};

  /** nullopt inherits the current database default; zero keeps all data. */
  std::optional<uint32_t> m_retention_hours;

  /** The stored column directory has been initialized. */
  bool m_initialized{false};

  /** Current logical column directory; empty before initialization. */
  schema_t m_schema;

  /** Physical partitions by start hour. */
  std::map<int64_t, store_partition_t> m_partitions;
};

/** Committed storage metadata. */
struct store_catalog_t {
  /** Same identity as every file of this database's WAL. */
  uint64_t m_wal_id{0};

  /** All writes through this sequence are stored. */
  batch_seq_t m_flushed_seq{0};

  /** Table id the next new table gets. */
  uint64_t m_next_table_id{1};

  /** Persistent default hours covered by auto-created table partitions. */
  uint32_t m_partition_duration_hours{STORE_DEFAULT_PARTITION_HOURS};

  /** Tables by normalized name. */
  std::map<std::string, store_table_t> m_tables;
};

/** One write to store; batch and schema stay valid throughout append(). */
struct store_write_t {
  /** Sequence number of the write. */
  batch_seq_t m_seq{0};

  /** Batch to append. */
  const batch_t *m_batch{nullptr};

  /** Column definitions for the batch. */
  const schema_t *m_schema{nullptr};
};

/** Build a physical table name from its table id and start hour.
@param[in] start_hour start UTC hour of the partition */
std::string store_partition_name(uint64_t table_id, int64_t start_hour);

/** Return the partition's start hour for the given UTC hour. */
int64_t store_partition_start_hour(int64_t hour,
                                   uint32_t partition_duration_hours);

/** Reads metadata in the connection's current transaction. */
class store_catalog_reader final : private ut_noncopyable {
 public:
  /** Use the active transaction to populate catalog. */
  store_catalog_reader(duckdb_connection conn, store_catalog_t &catalog)
      : m_connection(conn), m_catalog(catalog) {}

  /** Reset and read logical metadata and partitions in the same snapshot. */
  db_status_t read();

 private:
  friend class store;

  /** Read and validate the single row in _pt_state. */
  db_status_t read_state();

  /** Read and validate logical table names. */
  db_status_t read_table_names(std::map<uint64_t, std::string> &names);

  /** Read logical columns grouped by table id. */
  db_status_t read_columns(
      std::map<uint64_t, std::vector<schema_column_t>> &cols);

  /** Load state and logical table schemas. */
  db_status_t read_metadata();

  /** Read and validate partition metadata. */
  db_status_t read_partitions();

  /** Connection defining the metadata snapshot; closed by the caller. */
  duckdb_connection m_connection{nullptr};

  /** Catalog populated and validated by this reader. */
  store_catalog_t &m_catalog;
};

/** Owns the DuckDB storage file and committed metadata. */
class store final : private ut_noncopyable {
 public:
  store() = default;

  ~store();

  /** Store writes F + 1 .. F + n in one transaction and publish the catalog.
  Callers serialize storage mutations and must not hold a PulseTen reader lock
  while this operation performs I/O and commits.
  Write sequences and batch/schema consistency are internal invariants.
  @param[in] writes consecutive writes immediately after the stored sequence
  @param[in] n number of writes; at least one
  @retval DB_SUCCESS committed; the catalog now includes the writes
  @retval DB_CORRUPTION stored metadata disagrees with the open state
  @retval DB_OUTCOME_UNKNOWN commit or rollback failed; reopen to learn F
  @return otherwise the storage error, with the transaction rolled back */
  db_status_t append(const store_write_t *writes, size_t n);

  /** Persist a new declaration with no columns or physical partitions.
  Caller serializes namespace, table, store and snapshot publication. */
  db_status_t create_table(const std::string &name, uint32_t partition_hours,
                           std::optional<uint32_t> retention_hours);

  /** Delete a table's physical partitions and metadata in one transaction.
  Caller has flushed its accepted writes and excludes catalog mutations. */
  db_status_t drop_table(const std::string &name);

  /** Persist mutable settings, declaring an unflushed table if necessary.
  The caller owns table X, flush and snapshot X locks. No data is flushed. */
  db_status_t set_table_options(const std::string &name,
                                uint32_t partition_hours,
                                std::optional<uint32_t> retention_hours);

  /** Open the storage file of a directory, creating it if there is none, and
  validate its catalog.
  @param[in]  partition_duration_hours  partition duration of a new file, 1 to
                              STORE_MAX_PARTITION_HOURS; an existing file
                              keeps its own
  @param[out] out owned store, or NULL on failure
  @param[in] memory_limit_bytes shared budget in bytes; 0 uses DuckDB's default
  @param[in] threads execution threads; 0 keeps DuckDB's platform default,
    otherwise at most PT_DUCKDB_MAX_THREADS
  @return DB_SUCCESS, DB_INVALID_ARGUMENT for an invalid budget or thread count,
  DB_CORRUPTION, DB_IO_ERROR, DB_ERROR or DB_OUT_OF_MEMORY */
  static db_status_t open(const char *dir, uint32_t partition_duration_hours,
                          store **out, uint64_t memory_limit_bytes = 0,
                          uint32_t threads = 0);

  /** Collect partitions ending at or before each table's retention cutoff.
  The caller excludes catalog mutations through expire_partitions(). */
  void collect_expired_partitions(
      int64_t now_us, uint32_t default_retention_hours,
      std::vector<store_partition_key_t> &expired_partitions) const;

  /** Drop the collected partitions in one transaction.
  The caller excludes concurrent append and catalog access. Earlier query
  transactions retain their MVCC view.
  @return DB_SUCCESS, a rolled-back error, or DB_OUTCOME_UNKNOWN */
  db_status_t expire_partitions(
      const std::vector<store_partition_key_t> &expired_partitions,
      uint64_t &removed);

  /** Check the current main schema for a table or view with this name.
  Opens a separate connection; safe alongside storage work. */
  db_status_t ordinary_table_exists(const std::string &name, bool &exists);

  /** Read ordinary table and view names from main in one query during open.
  Uses the storage connection before background work and callers can run. */
  db_status_t read_ordinary_tables(std::set<std::string> &names);

  /** Return the native database handle for opening query connections. */
  duckdb_database database();

  /** Return the resolved DuckDB memory budget in bytes. */
  uint64_t memory_limit_bytes() const;

  /** Return the effective execution thread setting selected at open. */
  uint32_t threads() const;

  /** Return the storage connection for database initialization.
  Use only before user queries or background work can run. The connection is
  also used by appends; callers must not disconnect it or use it concurrently.
  @return the storage connection, valid until store::destroy() */
  duckdb_connection connection();

  /** Return the current committed catalog without copying it.
  Storage owns the catalog; mutations can change its contents.
  Callers must synchronize access with storage mutations and close. */
  const store_catalog_t &catalog();

  /** Close the storage file.
  @param[in]  st  storage; may be NULL */
  static void destroy(store *st);

  /** Open storage in an allocated store; on failure the caller deletes it.
  @param[in] initialize_catalog allow creating the catalog in an empty file */
  db_status_t initialize(const char *dir, uint32_t partition_duration_hours,
                         uint64_t memory_limit_bytes, uint32_t threads,
                         bool initialize_catalog);

 private:
  /** Commit declaration/settings metadata and publish only after success. */
  db_status_t write_table_options(const std::string &name,
                                  uint32_t partition_hours,
                                  std::optional<uint32_t> retention_hours);

  /** Release owned resources; safe after partial initialization. */
  void close();

#ifdef PT_TESTING
  /** Test-only access for repeated close and partial initialization cleanup. */
  friend class store_test_access;
#endif /* PT_TESTING */

  /** Transaction and partition plan for one append. */
  class append_context;

  /** Assert write sequences and batch/schema consistency before appending. */
  void validate_append(const store_write_t *writes, size_t n) const;

  /** Append writes inside the transaction, with pending catalog changes.
  @param[in]  writes  consecutive writes starting immediately after the
                      stored sequence number
  @return DB_SUCCESS, DB_CORRUPTION for a changed stored boundary, or a
    catalog/appender error; the caller handles rollback */
  db_status_t append_low(const store_write_t *writes, size_t n,
                         store_catalog_t &next);

  /** Initialize native handles and catalog; the owner cleans up on failure.
  @param[in]  partition_duration_hours  UTC hours covered by one partition
  @return DB_SUCCESS or the database-open, initialization or catalog-validation
    error */
  db_status_t open_low(const char *dir, uint32_t partition_duration_hours,
                       duckdb_config &config, bool initialize_catalog);

  /** Configure and open native handles; the owner cleans up every failure. */
  db_status_t open_database(const char *path, duckdb_config &config);

  /** Load the catalog from the internal tables and validate physical objects.
  @return DB_SUCCESS, DB_CORRUPTION or DB_ERROR */
  db_status_t load_catalog();

  /** Create internal tables and initial state in one transaction. */
  db_status_t create_catalog(uint32_t partition_duration_hours);

  /** Read partition metadata and validate the physical tables.
  @return DB_SUCCESS, a query error, or DB_CORRUPTION */
  db_status_t load_partitions();

  /** Validate ordinary table names and reject missing or unknown internal
  objects.
  @return DB_SUCCESS, a query error, or DB_CORRUPTION */
  db_status_t validate_catalog_tables();

  /* ===================== Owned native resources ===================== */

  /* Opened before publication; closed after all users finish. */

  /** Native database; closed after disconnecting the storage connection. */
  duckdb_database m_db{nullptr};

  /** Requested memory budget during open, then the effective native setting. */
  uint64_t m_memory_limit_bytes{0};

  /** Requested thread count during open, then the resolved native setting. */
  uint32_t m_threads{0};

  /* =============== Caller-synchronized storage state ================ */

  /* The caller excludes concurrent mutation; db uses m_flush_mutex.
  Catalog reads must not overlap storage mutations. */

  /** Storage transaction state. close() disconnects the session before m_db. */
  pt_duckdb_session m_session;

  /** Catalog as of the last committed storage change. */
  store_catalog_t m_catalog;
};

#endif /* store_h */
