/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage.h
 *    Durable storage of flushed writes in a DuckDB file.
 *
 * The file holds four internal tables and one physical table per table
 * and time partition, alongside ordinary user-created tables:
 *
 *   _pt_state       one row: format version, flushed_seq (F), next table
 *                   id, partition span in days and WAL identity
 *   _pt_tables      table id and name of every table
 *   _pt_columns     every column, with the schema version that added it;
 *                   schema version v is the columns with added_version <= v
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
 * storage_flush_prepare() stores consecutive writes after F in one DuckDB
 * transaction: new tables and columns, new partitions, the rows, and the
 * new F. storage_flush_commit() commits the transaction and publishes the
 * updated catalog. F records which writes are stored; recovery replays
 * only WAL records after F.
 *
 * Columns are only added. A partition created before a column existed gets
 * the column when a later write needs it; its older rows read as NULL.
 *
 * storage_expire() drops whole partitions whose days have all passed a
 * cutoff, in one transaction.
 * Ordinary tables have valid non-reserved names and cannot share names with
 * time-series tables. They use DuckDB transactions and are not expired.
 *
 *-------------------------------------------------------------------------
 */

#ifndef storage_h
#define storage_h

#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "batch.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"

/** Name of the storage file in the database directory. */
#define STORAGE_FILE_NAME "data.duckdb"

/** Version recorded in a storage file's _pt_state. */
#define STORAGE_FORMAT_VERSION 1

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

  /** Every schema version; versions[i] has version i + 1. */
  std::vector<schema_t> m_versions;
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

  /** Every write up to here is stored (F). */
  batch_seq_t m_flushed_seq{0};

  /** Id the next new table gets. */
  uint64_t m_next_table_id{1};

  /** Days covered by one partition. */
  uint32_t m_partition_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Tables by canonical name. */
  std::map<std::string, storage_table_t> m_tables;

  /** Ordinary DuckDB tables in main, by canonical name. */
  std::set<std::string> m_user_tables;

  /** Partitions by (table id, first day). */
  std::map<std::pair<uint64_t, int64_t>, storage_partition_t> m_partitions;
};

/** One write to store. */
struct storage_write_t {
  /** Sequence number of the write. */
  batch_seq_t m_seq{0};

  /** Its rows. */
  const batch_t *m_batch{nullptr};

  /** Schema version the batch follows. */
  const schema_t *m_schema{nullptr};
};

/** DuckDB wrapper owned by the storage file. */
struct duckdb_database_t;

/** An open storage file shared by the storage implementation files. */
struct storage_t {
  /** The DuckDB database and its connection. */
  duckdb_database_t *m_duckdb{nullptr};

  /** Catalog as of the last committed storage or ordinary-table change. */
  storage_catalog_t m_catalog;

  /** State a prepared flush will have once it commits. */
  storage_catalog_t m_pending;

  /** A flush is prepared: its transaction is open and not committed. */
  bool m_prepared{false};

  /** Make the next flush fail before it commits; debug builds only. */
  bool m_dbg_fail_next_flush{false};

  /** Keep failing flushes before commit until disabled; debug builds only. */
  bool m_dbg_fail_flush{false};
};

/** Build a physical table name from its table id and first day.
@param[in]  first_day  first UTC day of the partition
@return the physical table name of a partition */
std::string storage_partition_name(uint64_t table_id, int64_t first_day);

/** Find the aligned partition containing a UTC day.
@return the first day of the partition holding a UTC day */
int64_t storage_partition_first_day(int64_t day, uint32_t partition_days);

/** Open the storage file of a directory, creating it if there is none, and
load its catalog.
@param[in]  partition_days  partition span of a new file, 1 to
                            STORAGE_MAX_PARTITION_DAYS; an existing file
                            keeps its own
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR
  or DB_OUT_OF_MEMORY */
db_status_t storage_open(const char *dir, uint32_t partition_days,
                         storage_t **out);

/** Close the storage file.
@param[in]  st  storage; may be NULL */
void storage_close(storage_t *st);

/** Borrow the storage metadata as of the last committed change.
@return the metadata as of the last commit, valid until the next mutating storage
  operation or storage_close() */
const storage_catalog_t &storage_catalog(storage_t &st);

/** Read catalog metadata using an already active query transaction.
Its F, columns and partitions share that transaction's MVCC snapshot. Does not
scan business rows, information_schema or use the writer cache. Ordinary
table names and physical objects are validated during storage_open().
@return DB_SUCCESS or the catalog query/validation error */
db_status_t storage_read_catalog(duckdb_connection conn,
                                 storage_catalog_t &catalog);

/** Store writes F + 1 .. F + n, leaving the transaction open. On success,
the caller must call storage_flush_commit(). The catalog does not change
until the commit.
@param[in]  writes  consecutive writes starting immediately after the
                    stored sequence number
@param[in]  n       number of writes; at least one
@retval DB_SUCCESS prepared; the transaction remains open
@retval DB_INVALID_ARGUMENT the writes do not start at F + 1 or are not
  consecutive; nothing changed
@retval DB_CORRUPTION stored metadata disagrees with the open state
@retval DB_OUTCOME_UNKNOWN rollback failed; the handle must be reopened
@return otherwise the storage error; nothing is prepared */
db_status_t storage_flush_prepare(storage_t &st, const storage_write_t *writes,
                                  size_t n);

/** Commit the prepared flush and publish its catalog. This may
perform WAL synchronization or automatic checkpoint work; callers must not
hold a PulseTen reader lock.
@retval DB_SUCCESS committed; the catalog now includes the writes
@retval DB_OUTCOME_UNKNOWN the commit failed; reopen to learn F */
db_status_t storage_flush_commit(storage_t &st);

/** Drop every partition that ends on or before a UTC day, in one transaction.
Must not run while a flush is prepared. Queries whose DuckDB transaction
started earlier still read the dropped partitions.
@param[in]  end_day  partitions whose last day is before this day go
@retval DB_SUCCESS committed, or nothing to drop
@retval DB_ERROR DuckDB refused a statement; rolled back
@retval DB_OUTCOME_UNKNOWN commit or rollback failed; reopen to learn the state
*/
db_status_t storage_expire(storage_t &st, int64_t end_day, uint64_t &removed);

/** Update the ordinary-table catalog after a successful DDL commit. The
caller serializes this with flushes and other storage changes.
@param[in]  table  canonical ordinary-table name */
void storage_user_table_committed(storage_t &st, const std::string &table,
                                  bool exists);

/** Borrow the DuckDB database behind an open storage handle.
@return the DuckDB database, for opening query connections */
duckdb_database storage_database(storage_t &st);

/** Borrow the storage connection for engine-opening initialization only.
Use only before user queries or background work can run. The connection is
also used by flushes; callers must not disconnect it or use it concurrently.
@return the borrowed connection, valid until storage_close() */
duckdb_connection storage_connection(storage_t &st);

/** Run a read-only SQL query returning one integer. For tests.
@param[out]  value  single non-NULL integer result
@return DB_SUCCESS or DB_ERROR */
db_status_t storage_query_int(storage_t &st, const std::string &sql,
                              int64_t &value);

#ifdef PT_DEBUG
/** Make the next flush preparation fail after writing rows. Debug builds
only; used by tests. */
void storage_dbg_fail_next_flush(storage_t &st);

/** Enable or disable persistent pre-commit flush failures. Debug builds only;
the caller must serialize with storage mutations. */
void storage_dbg_set_flush_failure(storage_t &st, bool fail);
#endif

#endif
