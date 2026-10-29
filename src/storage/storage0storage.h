/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage0storage.h
 *    Durable storage of flushed writes in a DuckDB file.
 *
 * The file holds four internal tables and one physical table per table
 * and time partition:
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
 * storage_flush() writes a run of consecutive writes after F in one DuckDB
 * transaction: new tables and columns, new partitions, the rows, and the
 * new F. Either all of it is committed or none of it. F is the only record
 * of which writes are in DuckDB; recovery replays only WAL records after F.
 *
 * Columns are only added. A partition created before a column existed gets
 * the column when a later write needs it; its older rows read as NULL.
 *
 * storage_expire() drops whole partitions whose days have all passed a
 * cutoff, in one transaction. storage_drop_table() drops a table with its
 * partitions and catalog rows, in one transaction; table ids are never
 * reused, so a later table of the same name gets new physical tables.
 *
 *-------------------------------------------------------------------------
 */

#ifndef storage0storage_h
#define storage0storage_h

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "batch0batch.h"
#include "schema0schema.h"
#include "ut0err.h"
#include "ut0ut.h"

/** Name of the storage file in the database directory. */
#define STORAGE_FILE_NAME "data.duckdb"

/** Version recorded in a storage file's _pt_state. */
#define STORAGE_FORMAT_VERSION 1

/** Partition span of a new file, in days. */
#define STORAGE_DEFAULT_PARTITION_DAYS 30

/** Largest partition span allowed, in days. */
#define STORAGE_MAX_PARTITION_DAYS 366

/** A table as stored. */
struct storage_table_t {
  /** Id in _pt_tables; never reused, not even after a drop. */
  uint64_t table_id{0};

  /** Every schema version; versions[i] has version i + 1. */
  std::vector<schema_t> versions;
};

/** A physical table holding one time partition of one table. */
struct storage_partition_t {
  /** Table the partition belongs to. */
  uint64_t table_id{0};

  /** First UTC day of the partition, day 0 being 1970-01-01; a multiple
  of the partition span. */
  int64_t first_day{0};

  /** Name of the physical DuckDB table; see storage_partition_name(). */
  std::string phys_name;

  /** Columns c0 .. c<n_cols - 1> exist in the physical table. */
  uint32_t n_cols{0};

  /** Rows stored in the partition. */
  uint64_t rows{0};
};

/** What the file holds as of the last commit. */
struct storage_state_t {
  /** Same identity as every segment of this database's WAL. */
  uint64_t wal_id{0};

  /** Every write up to here is stored (F). */
  batch_seq_t flushed_seq{0};

  /** Id the next new table gets. */
  uint64_t next_table_id{1};

  /** Days covered by one partition. */
  uint32_t partition_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Tables by canonical name. */
  std::map<std::string, storage_table_t> tables;

  /** Partitions by (table id, first day). */
  std::map<std::pair<uint64_t, int64_t>, storage_partition_t> partitions;
};

/** One write to store. */
struct storage_write_t {
  /** Sequence number of the write. */
  batch_seq_t seq{0};

  /** Its rows. */
  const batch_t *batch{nullptr};

  /** Schema version the batch follows. */
  const schema_t *schema{nullptr};
};

/** An open storage file. */
struct storage_t;

/** Build a physical table name from its table id and first day.
@param[in]  table_id   table id
@param[in]  first_day  first UTC day of the partition
@return the physical table name of a partition */
std::string storage_partition_name(uint64_t table_id, int64_t first_day);

/** Find the aligned partition containing a UTC day.
@param[in]  day             UTC day
@param[in]  partition_days  partition span
@return the first day of the partition holding a UTC day */
int64_t storage_partition_first_day(int64_t day, uint32_t partition_days);

/** Open the storage file of a directory, creating it if there is none, and
load its state.
@param[in]   dir             existing directory
@param[in]   partition_days  partition span of a new file, 1 to
                             STORAGE_MAX_PARTITION_DAYS; an existing file
                             keeps its own
@param[out]  storage         open storage
@param[out]  error           message on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR
  or DB_OUT_OF_MEMORY */
dberr_t storage_open(const char *dir, uint32_t partition_days,
                     storage_t **storage, std::string &error);

/** Close the storage file.
@param[in]  storage  storage; may be NULL */
void storage_close(storage_t *storage);

/** Borrow the stored catalog as of the last committed change.
@param[in,out]  storage  analytical storage
@return the state as of the last commit, valid until the next storage_flush()
  or storage_close() */
const storage_state_t *storage_get_state(storage_t &storage);

/** Read only the internal metadata using an already active query transaction.
Its F, columns and partitions share that transaction's MVCC snapshot. Does not
scan or verify business rows or use the writer cache.
@param[in]   conn   connection with an active read transaction
@param[out]  state  metadata read from that transaction's snapshot
@param[out]  error  reason for failure
@return DB_SUCCESS or the catalog query/validation error */
dberr_t storage_snapshot(duckdb_connection conn, storage_state_t &state,
                         std::string &error);

/** Store writes F + 1 .. F + n in one transaction and advance F.
@param[in]   storage  storage
@param[in]   writes   the writes, in sequence order starting at F + 1
@param[in]   n        number of writes, at least 1
@param[out]  error    message on failure
@retval DB_SUCCESS committed
@retval DB_INVALID_ARGUMENT the writes do not start at F + 1 or are not
  consecutive; nothing changed
@retval DB_ERROR DuckDB refused a statement; rolled back
@retval DB_OUTCOME_UNKNOWN the commit itself failed; reopen to learn F */
dberr_t storage_flush(storage_t &storage, const storage_write_t *writes,
                      size_t n, std::string &error);

/** First half of storage_flush(): do all the work of storing writes F + 1 ..
F + n, leaving the transaction open. Must be followed by
storage_flush_commit(). The state does not change
until the commit.
@param[in,out]  storage  analytical storage
@param[in]      writes   consecutive writes starting immediately after the
                         stored sequence number
@param[in]      n        number of writes; at least one
@param[out]     error    reason for failure
@return as storage_flush(), except never DB_OUTCOME_UNKNOWN; on failure
  nothing is prepared */
dberr_t storage_flush_prepare(storage_t &storage, const storage_write_t *writes,
                              size_t n, std::string &error);

/** Second half of storage_flush(): commit the prepared flush. This may
perform WAL synchronization or automatic checkpoint work; callers must not
hold a PulseTen reader lock.
@param[in,out]  storage  analytical storage
@param[out]     error    reason for failure
@retval DB_SUCCESS committed; the state now includes the writes
@retval DB_OUTCOME_UNKNOWN the commit failed; reopen to learn F */
dberr_t storage_flush_commit(storage_t &storage, std::string &error);

/** Drop every partition that ends on or before a UTC day, in one transaction.
Must not run while a flush is prepared. Queries whose DuckDB transaction
started earlier still read the dropped partitions.
@param[in]   storage  storage
@param[in]   end_day  partitions whose last day is before this day go
@param[out]  removed  partitions dropped
@param[out]  error    message on failure
@retval DB_SUCCESS committed, or nothing to drop
@retval DB_ERROR DuckDB refused a statement; rolled back
@retval DB_OUTCOME_UNKNOWN the commit failed; reopen to learn the state */
dberr_t storage_expire(storage_t &storage, int64_t end_day, uint64_t &removed,
                       std::string &error);

/** Drop a table: its partitions and its catalog rows, in one transaction.
Must not run while a flush is prepared. Queries whose DuckDB transaction
started earlier still read the table.
@param[in]   storage  storage
@param[in]   table    canonical table name
@param[out]  error    message on failure
@retval DB_SUCCESS committed, or the table is not stored
@retval DB_ERROR DuckDB refused a statement; rolled back
@retval DB_OUTCOME_UNKNOWN the commit failed; reopen to learn the state */
dberr_t storage_drop_table(storage_t &storage, const std::string &table,
                           std::string &error);

/** Borrow the DuckDB database behind an open storage handle.
@param[in,out]  storage  analytical storage
@return the DuckDB database, for opening query connections */
duckdb_database storage_get_database(storage_t &storage);

/** Run a read-only SQL query returning one integer. For tests.
@param[in,out]  storage  analytical storage
@param[in]      sql      SQL statement to check or execute
@param[out]     value    single non-NULL integer result
@return DB_SUCCESS or DB_ERROR */
dberr_t storage_query_int(storage_t &storage, const std::string &sql,
                          int64_t &value);

#ifdef PT_DEBUG
/** Make the next storage_flush() fail after it has written rows, before it
commits. Debug builds only; used by tests.
@param[in,out]  storage  analytical storage */
void storage_dbg_fail_next_flush(storage_t &storage);
#endif

#endif
