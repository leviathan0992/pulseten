/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query.h
 *    SQL over stored and unflushed writes, through DuckDB.
 *
 * A query sees one snapshot: the stored partitions as of F, read in a
 * DuckDB transaction that started at F, and the memtable batches after F.
 * When DuckDB binds a table name it does not know, a replacement scan turns
 * it into a subquery over the native partition tables and a scan of the
 * memtable batches:
 *
 *   SELECT c0 AS "time", c1 AS "host", ... FROM _pt_t1_p20454
 *   WHERE _pt_seq <= F
 *   UNION ALL ...
 *   UNION ALL SELECT "time", "host", ... FROM _pt_memtable_scan(<token>, 'cpu')
 *
 * DuckDB's own optimizer and executor run the result: filters and
 * projections reach the partition scans, which skip data with their zone
 * maps. A partition lacking columns added later supplies NULLs.
 *
 * Before the query runs, simple conditions on "time" bound the times it
 * may read from each table, and only the partitions overlapping those
 * times are put into the subquery. The bounds are conservative: they may
 * keep a partition the query does not need, never drop one it does. The
 * WHERE clause itself is still applied by DuckDB.
 *
 * A query accepts one SELECT. It may name managed tables of the snapshot,
 * ordinary tables visible in its DuckDB transaction,
 * common table expressions and subqueries, but no table functions, no
 * qualified names and no internal or system objects. A separate DDL entry
 * accepts CREATE TABLE and DROP TABLE for ordinary tables only.
 *
 *-------------------------------------------------------------------------
 */

#ifndef query_h
#define query_h

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include <duckdb.h>

#include "batch.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"

/** Most rows one fetch returns: one DuckDB vector. */
#define QUERY_CHUNK_ROWS 2048

/** Times, in microseconds, a query may read from a table: [lo, hi].
Empty when lo > hi. */
struct query_range_t {
  /** Earliest time, inclusive. */
  int64_t m_lo{INT64_MIN};

  /** Latest time, inclusive. */
  int64_t m_hi{INT64_MAX};
};

/** Time ranges by canonical table name; an absent table is unbounded. */
typedef std::map<std::string, query_range_t> query_ranges_t;

/** A table as one query sees it. */
struct query_table_t {
  /** Columns of the newest schema version in the snapshot. */
  schema_t m_schema;

  /** Stored partitions. */
  std::vector<storage_partition_t> m_partitions;

  /** Unflushed batches, in sequence order. */
  std::vector<const batch_t *> m_batches;
};

/** What one query sees. */
struct query_snapshot_t {
  /** Writes up to here are read from the partitions, later ones from the
  batches. */
  batch_seq_t m_flushed_seq{0};

  /** Days covered by one stored partition. */
  uint32_t m_partition_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Tables by canonical name. */
  std::map<std::string, query_table_t> m_tables;
};

/** One value of a result. */
struct query_value_t {
  /** The value is NULL; the members below are then unused. */
  bool m_is_null{true};

  /** BOOLEAN. */
  bool m_b{false};

  /** BIGINT, and TIMESTAMP in microseconds. */
  int64_t m_i{0};

  /** DOUBLE. */
  double m_d{0};

  /** VARCHAR. */
  std::string m_s;
};

/** One column of a result. */
struct query_column_t {
  /** Name as DuckDB gives it. */
  std::string m_name;

  /** Type the values are returned as. */
  col_type_t m_type{col_type_t::BIGINT};
};

/** Rows of a result: rows[r][c] is the value of column c in row r. */
typedef std::vector<std::vector<query_value_t>> query_rows_t;

/** A query being read chunk by chunk. */
struct query_cursor_t;

/** Validated ordinary-table DDL. */
struct query_ddl_t {
  /** Canonical target name in main. */
  std::string m_table;

  /** CREATE TABLE rather than DROP TABLE. */
  bool m_is_create{false};

  /** DROP TABLE IF EXISTS permits an absent table. */
  bool m_if_exists{false};
};

/** Validate one CREATE TABLE or DROP TABLE statement. CTAS reads follow
the same restrictions as SELECT. Temporary tables, foreign keys and cascading
drops are rejected.
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
db_status_t query_check_ddl(duckdb_connection conn, const std::string &sql,
                            query_ddl_t &ddl);

/** Execute checked DDL inside an active transaction, with replacement scans
bound to a consistent snapshot. Does not commit the transaction.
@param[in]  snap  snapshot retained until execution finishes
@return DB_SUCCESS or DB_ERROR */
db_status_t query_execute_ddl(duckdb_connection conn,
                              const query_snapshot_t &snap,
                              const std::string &sql);

/** Install the memtable scan, then the replacement scan whose generated
SQL calls it. Call once during engine opening, before user queries or
background work can use the database. The connection is borrowed and is not
disconnected.
@param[in,out]  conn  connection to the database; NULL is rejected
@return DB_SUCCESS or DB_ERROR */
db_status_t query_install_scans(duckdb_connection conn);

/** Validate one SELECT and extract its conservative time ranges.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
db_status_t query_validate_select(duckdb_connection conn,
                                  const std::string &sql,
                                  query_ranges_t &ranges);

/** Commit the transaction of a query connection.
@return DB_SUCCESS, or DB_OUTCOME_UNKNOWN if the outcome cannot be known */
db_status_t query_commit(duckdb_connection conn);

/** Start the snapshot transaction of a query connection and read F.
@return DB_SUCCESS or DB_ERROR */
db_status_t query_begin_snapshot(duckdb_connection conn,
                                 batch_seq_t &flushed_seq);

/** Start a checked SELECT, or EXPLAIN of it, in the snapshot transaction of
the connection, for reading its rows chunk by chunk. The statement is bound
and planned here; it runs during the fetches, so the snapshot and the
transaction must stay valid until query_cursor_close().
@param[in]   conn  connection after query_begin_snapshot(); runs nothing else
until query_cursor_close()
@param[in]   ranges  bounds extracted while validating this SQL
@param[out]  out   open cursor, or NULL on failure
@return DB_SUCCESS, DB_ERROR for a SQL error, DB_INVALID_ARGUMENT for a result
  type that cannot be returned, or DB_OUT_OF_MEMORY */
db_status_t query_cursor_open(duckdb_connection conn,
                              const query_snapshot_t &snap,
                              const std::string &sql, bool explain,
                              const query_ranges_t &ranges,
                              query_cursor_t **out);

/** Read the column metadata fixed when the cursor was opened.
@return the result columns of a cursor */
const std::vector<query_column_t> &query_cursor_columns(
    const query_cursor_t &cursor);

/** Read the next rows, at most QUERY_CHUNK_ROWS. The first fetch runs the
statement until it produces rows.
@param[out]  rows  the rows; empty once every row was read
@return DB_SUCCESS, DB_ERROR for an error while executing, DB_INVALID_ARGUMENT
  for a value that does not fit its returned type, or DB_INTERRUPTED after
  query_cursor_interrupt(); after a failure the cursor returns no more rows.
Once a fetch returns DB_INTERRUPTED, every later fetch returns DB_INTERRUPTED */
db_status_t query_cursor_fetch(query_cursor_t &cursor, query_rows_t &rows);

/** Request interruption of an active cursor. Thread-safe with respect to
query_cursor_fetch(), not to query_cursor_close(). The running or next fetch
observes the request; after it returns DB_INTERRUPTED, every later fetch does
too. Normal end is reached when query_cursor_fetch() returns DB_SUCCESS with
empty rows; interrupting after that leaves later fetches successful and empty.
*/
void query_cursor_interrupt(query_cursor_t &cursor);

/** Stop a query and free its cursor.
@param[in]  cursor  cursor; may be NULL */
void query_cursor_close(query_cursor_t *cursor);

#endif
