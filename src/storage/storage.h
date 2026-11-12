/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage.h
 *    Durable storage of flushed writes in a DuckDB file.
 *
 * The file holds four internal tables and one physical table per table
 * and time partition, alongside user tables:
 *
 *   _pt_state       one row: format version, flushed_seq (F), next table
 *                   id, partition span in days and WAL identity
 *   _pt_tables      table id and name of every table
 *   _pt_columns     latest logical column directory: id, name, kind and type
 *   _pt_partitions  table id, first UTC day, physical table name, number of
 *                   columns the physical table has, row count
 *
 *   _pt_t<id>_p<first day> / _pt_t<id>_n<-first day>
 *                   _pt_seq, _pt_row, then c0 (time), c1, ... by column id,
 *                   with time inside the partition's days
 *
 * A partition covers partition_days UTC days, counted from 1970-01-01: its
 * first day is a multiple of partition_days. The span is chosen when the
 * file is created and never changes.
 *
 * storage::flush() stores consecutive writes after F in one DuckDB
 * transaction: new tables and columns, new partitions, the rows, and the
 * new F. The same operation commits the transaction and publishes the
 * updated catalog. F records which writes are stored; recovery replays
 * only WAL records after F.
 *
 * Columns are only added. A partition created before a column existed gets
 * the column when a later write needs it; its older rows read as NULL.
 *
 * storage::expire() drops whole partitions whose days have all passed a
 * cutoff, in one transaction.
 * User tables have valid non-reserved names and cannot share names with
 * time-series tables. They use DuckDB transactions and are not expired.
 *
 *-------------------------------------------------------------------------
 */

#ifndef storage_h
#define storage_h

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "batch.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_noncopyable.h"

/** Name of the storage file in the database directory. */
#define STORAGE_FILE_NAME "data.duckdb"

/** Version recorded in a storage file's _pt_state. */
#define STORAGE_FORMAT_VERSION 2

/** Partition span of a new file, in days. */
#define STORAGE_DEFAULT_PARTITION_DAYS 30

/** Largest partition span allowed, in days. */
#define STORAGE_MAX_PARTITION_DAYS 366

/** A partition, by (table id, first day). */
typedef std::pair<uint64_t, int64_t> storage_key_t;

/** A table as stored. */
struct storage_table_t {
  /** Id in _pt_tables. */
  uint64_t m_table_id{0};

  /** Current logical column directory. */
  schema_t m_schema;
};

/** A physical table holding one time partition of one table. */
struct storage_partition_t {
  /** Table the partition belongs to. */
  uint64_t m_table_id{0};

  /** First UTC day of the partition, day 0 being 1970-01-01; a multiple
  of the partition span. */
  int64_t m_first_day{0};

  /** Name of the physical DuckDB table; see storage_partition_name(). */
  std::string m_phys_name;

  /** Columns c0 .. c<m_n_cols - 1> exist in the physical table. */
  uint32_t m_n_cols{0};

  /** Rows stored in the partition. */
  uint64_t m_rows{0};
};

/** What the file holds as of the last commit. */
struct storage_catalog_t {
  /** Same identity as every segment of this database's WAL. */
  uint64_t m_wal_id{0};

  /** Every write up to here is stored F. */
  batch_seq_t m_flushed_seq{0};

  /** Id the next new table gets. */
  uint64_t m_next_table_id{1};

  /** Days covered by one partition. */
  uint32_t m_partition_duration_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Tables by normalized name. */
  std::map<std::string, storage_table_t> m_tables;

  /** Partitions by (table id, first day). */
  std::map<std::pair<uint64_t, int64_t>, storage_partition_t> m_partitions;
};

/** One write to store. */
struct storage_write_t {
  /** Sequence number of the write. */
  batch_seq_t m_seq{0};

  /** Its rows. */
  const batch_t *m_batch{nullptr};

  /** Immutable directory containing at least the batch's column prefix. */
  const schema_t *m_schema{nullptr};
};

/** DuckDB wrapper owned by the storage file. */
class duckdb_backend;

/** Build a physical table name from its table id and first day.
@param[in]  first_day  first UTC day of the partition
@return the physical table name of a partition */
std::string storage_partition_name(uint64_t table_id, int64_t first_day);

/** Find the aligned partition containing a UTC day.
@return the first day of the partition holding a UTC day */
int64_t storage_partition_first_day(int64_t day, uint32_t partition_days);

/** Reads metadata in a borrowed connection's active MVCC transaction.
Does not own the connection or scan business rows. */
class storage_catalog_reader final : private ut_noncopyable {
 public:
  /** Borrow an active connection and its output catalog for this reader. */
  storage_catalog_reader(duckdb_connection conn, storage_catalog_t &catalog)
      : m_conn(conn), m_catalog(catalog) {}

  /** Reset and read logical metadata and partitions in the same snapshot. */
  db_status_t read();

 private:
  friend class storage;

  /** Read the singleton state row. */
  db_status_t read_state_row();

  /** Read and validate logical table names. */
  db_status_t read_table_names(std::map<uint64_t, std::string> &names);

  /** Read logical columns grouped by table id. */
  db_status_t read_columns(std::map<uint64_t, std::vector<col_def_t>> &cols);

  /** Load state and logical table schemas. */
  db_status_t read_metadata();

  /** Read and validate partition metadata. */
  db_status_t read_partitions();

  /** Borrowed connection whose transaction defines the metadata snapshot. */
  duckdb_connection m_conn{nullptr};

  /** Catalog populated and validated by this reader. */
  storage_catalog_t &m_catalog;
};

/** Owns the DuckDB storage file and committed metadata. */
class storage final : private ut_noncopyable {
 public:
  /** Construct an empty, unopened object. */
  storage() = default;

  /** Release every resource still owned by the object. */
  ~storage();

  /** Store writes F + 1 .. F + n in one transaction and publish the catalog.
  Callers serialize storage mutations and must not hold a PulseTen reader lock
  while this operation performs I/O and commits.
  @param[in] writes consecutive writes immediately after the stored sequence
  @param[in] n number of writes; at least one
  @retval DB_SUCCESS committed; the catalog now includes the writes
  @retval DB_INVALID_ARGUMENT the writes do not continue from F
  @retval DB_CORRUPTION stored metadata disagrees with the open state
  @retval DB_OUTCOME_UNKNOWN commit or rollback failed; reopen to learn F
  @return otherwise the storage error, with the transaction rolled back */
  db_status_t flush(const storage_write_t *writes, size_t n);

  /** Open the storage file of a directory, creating it if there is none, and
  load its catalog. The caller must validate partition_days before calling.
  @param[in]  partition_days  partition span of a new file, 1 to
                              STORAGE_MAX_PARTITION_DAYS; an existing file
                              keeps its own
  @return DB_SUCCESS, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR or DB_OUT_OF_MEMORY
  */
  static db_status_t open(const char *dir, uint32_t partition_days,
                          storage **out, uint64_t memory_limit_bytes = 0);

  /** Drop every partition that ends on or before a UTC day, in one transaction.
  Must not run while a flush is prepared. Queries whose DuckDB transaction
  started earlier still read the dropped partitions.
  @param[in]  end_day  partitions whose last day is before this day go
  @retval DB_SUCCESS committed, or nothing to drop
  @retval DB_ERROR DuckDB refused a statement; rolled back
  @retval DB_OUTCOME_UNKNOWN commit or rollback failed; reopen to learn the
  state
  */
  db_status_t expire(int64_t end_day, uint64_t &removed);

  /** Borrow the DuckDB database behind an open storage handle.
  @return the DuckDB database, for opening query connections */
  duckdb_database database();

  /** Borrow the storage connection for db-opening initialization only.
  Use only before user queries or background work can run. The connection is
  also used by flushes; callers must not disconnect it or use it concurrently.
  @return the borrowed connection, valid until storage::destroy() */
  duckdb_connection connection();

  /** Return the current committed catalog without copying it.
  Storage owns the catalog; mutations can change its contents.
  Callers must synchronize access with storage mutations and close. */
  const storage_catalog_t &catalog();

  /** Close the storage file.
  @param[in]  st  storage; may be NULL */
  static void destroy(storage *st);

 private:
  /** Release owned resources; safe after partial initialization. */
  void close();

#ifdef PT_TESTING
  /** Test-only access is implemented outside the production library. */
  friend class storage_test_access;
#endif /* PT_TESTING */

  /** One flush's borrowed transaction and private append plan. */
  class flush_context;

  /** Validate consecutive writes against the committed stored boundary. */
  bool check_flush(const storage_write_t *writes, size_t n) const;

  /** Store writes inside the transaction, on a copy of the catalog.
  @param[in]  writes  consecutive writes starting immediately after the
                      stored sequence number
  @return DB_SUCCESS, DB_CORRUPTION for a changed stored boundary, or a
    catalog/appender error; the caller handles rollback */
  db_status_t flush_low(const storage_write_t *writes, size_t n,
                        storage_catalog_t &next);

  /** Fill a new storage; storage::open() frees it on failure.
  @param[in]  partition_days  number of UTC days covered by one partition
  @return DB_SUCCESS or the database-open, initialization or catalog-validation
    error */
  db_status_t open_low(const char *dir, uint32_t partition_days,
                       uint64_t memory_limit_bytes);

  /** Load the catalog from the internal tables and validate physical objects.
  @return DB_SUCCESS, DB_CORRUPTION or DB_ERROR */
  db_status_t load_catalog();

  /** Create internal tables and initial state in one transaction. */
  db_status_t create_tables(uint32_t partition_days);

  /** Read partition metadata and validate the physical tables.
  @return DB_SUCCESS, a query error, or DB_CORRUPTION */
  db_status_t check_partitions();

  /** Validate user table names and reject missing or unknown internal objects.
  @return DB_SUCCESS, a query error, or DB_CORRUPTION */
  db_status_t check_user_tables();

  /** The DuckDB database and its connection. */
  duckdb_backend *m_duckdb{nullptr};

  /** Catalog as of the last committed storage change. */
  storage_catalog_t m_catalog;

#ifdef PT_TESTING
  /** Make the next flush fail before it commits. */
  bool m_test_fail_next_flush{false};

  /** Keep failing flushes before commit until disabled. */
  bool m_test_fail_flush{false};
#endif /* PT_TESTING */
};

#endif /* storage_h */
