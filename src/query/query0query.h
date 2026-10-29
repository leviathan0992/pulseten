/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0query.h
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
 *   UNION ALL SELECT "time", "host", ... FROM _pt_hot_scan(<token>, 'cpu')
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
 * Only a single SELECT is accepted. It may name tables of the snapshot,
 * common table expressions and subqueries, but no table functions, no
 * qualified names and no internal or system objects.
 *
 *-------------------------------------------------------------------------
 */

#ifndef query0query_h
#define query0query_h

#include <duckdb.h>

#include <map>
#include <string>
#include <vector>

#include "batch0batch.h"
#include "schema0schema.h"
#include "storage0storage.h"
#include "ut0err.h"
#include "ut0ut.h"

/** Most rows one fetch returns: one DuckDB vector. */
#define QUERY_CHUNK_ROWS 2048

/** A table as one query sees it. */
struct query_table_t {
  /** Columns of the newest schema version in the snapshot. */
  schema_t schema;

  /** Stored partitions. */
  std::vector<storage_partition_t> partitions;

  /** Unflushed batches, in sequence order. */
  std::vector<const batch_t *> batches;
};

/** What one query sees. */
struct query_snapshot_t {
  /** Writes up to here are read from the partitions, later ones from the
  batches. */
  batch_seq_t flushed_seq{0};

  /** Days covered by one stored partition. */
  uint32_t partition_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Tables by canonical name. */
  std::map<std::string, query_table_t> tables;
};

/** One value of a result. */
struct query_value_t {
  /** The value is NULL; the members below are then unused. */
  bool is_null{true};

  /** BOOLEAN. */
  bool b{false};

  /** BIGINT, and TIMESTAMP in microseconds. */
  int64_t i{0};

  /** DOUBLE. */
  double d{0};

  /** VARCHAR. */
  std::string s;
};

/** One column of a result. */
struct query_column_t {
  /** Name as DuckDB gives it. */
  std::string name;

  /** Type the values are returned as. */
  col_type_t type{COL_BIGINT};
};

/** Rows of a result: rows[r][c] is the value of column c in row r. */
typedef std::vector<std::vector<query_value_t>> query_rows_t;

/** A query being read chunk by chunk. */
struct query_cursor_t;

/** Register the replacement scan and the memtable scan with a database. Call
once, before the first query.
@param[in]  db  database
@return DB_SUCCESS or DB_ERROR */
dberr_t query_install(duckdb_database db);

/** Check that a statement is a single SELECT reading only allowed names.
@param[in]   conn   a connection, used to parse the statement
@param[in]   sql    statement
@param[out]  error  reason for rejection
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
dberr_t query_check(duckdb_connection conn, const std::string &sql,
                    std::string &error);

/** Start the snapshot transaction of a query connection and read F.
@param[in]   conn         a new connection
@param[out]  flushed_seq  F as seen by the transaction
@param[out]  error        DuckDB's message on failure
@return DB_SUCCESS or DB_ERROR */
dberr_t query_begin(duckdb_connection conn, batch_seq_t &flushed_seq,
                    std::string &error);

/** Start a checked SELECT, or EXPLAIN of it, in the snapshot transaction of
the connection, for reading its rows chunk by chunk. The statement is bound
and planned here; it runs during the fetches, so the snapshot and the
transaction must stay valid until query_close().
@param[in]   conn     connection after query_begin(); runs nothing else until
                      query_close()
@param[in]   snap     what the query sees
@param[in]   sql      statement accepted by query_check()
@param[in]   explain  run EXPLAIN on it instead
@param[out]  cursor   open cursor, or NULL on failure
@param[out]  error    reason on failure
@return DB_SUCCESS, DB_ERROR for a SQL error, DB_INVALID_ARGUMENT for a result
  type that cannot be returned, or DB_OUT_OF_MEMORY */
dberr_t query_open(duckdb_connection conn, const query_snapshot_t &snap,
                   const std::string &sql, bool explain,
                   query_cursor_t **cursor, std::string &error);

/** Read the column metadata fixed when the cursor was opened.
@param[in]  cursor  query cursor
@return the result columns of a cursor */
const std::vector<query_column_t> &query_cursor_columns(
    const query_cursor_t &cursor);

/** Read the next rows, at most QUERY_CHUNK_ROWS. The first fetch runs the
statement until it produces rows.
@param[in]   cursor  cursor
@param[out]  rows    the rows; empty once every row was read
@param[out]  error   reason on failure
@return DB_SUCCESS, DB_ERROR for an error while executing, DB_INVALID_ARGUMENT
  for a value that does not fit its returned type, or DB_INTERRUPTED after
  query_interrupt(); after a failure the cursor returns no more rows. Once a
  fetch returns DB_INTERRUPTED, every later fetch returns DB_INTERRUPTED */
dberr_t query_fetch(query_cursor_t &cursor, query_rows_t &rows,
                    std::string &error);

/** Request interruption of an active cursor. Thread-safe with respect to
query_fetch(), not to query_close(). The running or next fetch observes the
request; after it returns DB_INTERRUPTED, every later fetch does too. Normal
end is reached when query_fetch() returns DB_SUCCESS with empty rows;
interrupting after that leaves later fetches successful and empty.
@param[in]  cursor  cursor */
void query_interrupt(query_cursor_t &cursor);

/** Stop a query and free its cursor.
@param[in]  cursor  cursor; may be NULL */
void query_close(query_cursor_t *cursor);

#endif
