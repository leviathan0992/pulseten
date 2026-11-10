/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_priv.h
 *    Definitions shared by the files of the query module only.
 *
 * A running query is registered under a token and under the id of its
 * DuckDB connection and database instance. Replacement scans use both; the
 * memtable scan by the token written into the generated SQL. A query is
 * registered from query_cursor_open() to query_cursor_close(), the whole time
 * DuckDB may call back, and its snapshot stays valid for that time.
 *
 *-------------------------------------------------------------------------
 */

#ifndef query_priv_h
#define query_priv_h

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include <duckdb.h>

#include "query.h"
#include "ut.h"
#include "ut_err.h"

/** Name of the table function that scans memtable batches. */
#define QUERY_MEMTABLE_SCAN "_pt_memtable_scan"

/** Times, in microseconds, a query may read from a table: [lo, hi].
Empty when lo > hi. */
struct query_range_t {
  /** Earliest time, inclusive. */
  int64_t m_lo{INT64_MIN};

  /** Latest time, inclusive. */
  int64_t m_hi{INT64_MAX};
};

/** Time ranges of a query by canonical table name. A table that is not
listed may be read at any time. */
typedef std::map<std::string, query_range_t> query_ranges_t;

/** A connection id is local to its DatabaseInstance. The database address
is stable while the connection owns the instance. */
using query_connection_key_t = std::pair<uintptr_t, uint64_t>;

/** Get the composite connection identity. C++ adapter; no allocation.
@return the database-instance address and database-local connection id */
query_connection_key_t query_connection_key(duckdb_connection conn);

/** Find a running query by its connection.
@return its snapshot, or NULL */
const query_snapshot_t *query_find_by_connection(
    const query_connection_key_t &connection, uint64_t &token,
    const query_ranges_t *&ranges);

/** Find the snapshot of a registered query.
@return the snapshot of a running query, or NULL */
const query_snapshot_t *query_find_by_token(uint64_t token);

/** Build the SELECT that stands for a table: its partitions that overlap a
time range, and its memtable batches.
@param[in]  name   canonical table name
@param[in]  range  times the query may read from the table
@return the SQL text */
std::string query_build_table_sql(uint64_t token, const std::string &name,
                                  const query_table_t &table,
                                  uint32_t partition_days,
                                  const query_range_t &range,
                                  batch_seq_t flushed_seq = UINT64_MAX);

/** Install the memtable scan table function on a connection.
@return DB_SUCCESS or DB_ERROR if DuckDB refuses registration */
db_status_t query_install_memtable_scan(duckdb_connection conn);

/** Register the table replacement callback in the database of a connection.
C++ adapter.
Call once during engine opening, before user queries or background work.
@param[in,out]  conn  borrowed connection to the target database */
void query_register_table_replacement(duckdb_connection conn);

/** Parse and validate a SELECT after C API syntax checking. C++ adapter.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
db_status_t query_validate_select_statement(const std::string &sql);

/** Parse and validate ordinary-table DDL after C API syntax checking.
@param[in]  sql  syntactically valid single statement
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
db_status_t query_check_ddl_statement(const std::string &sql, query_ddl_t &ddl);

/** Work out the times a statement may read from each table, from simple
conditions on "time". C++ adapter.
@param[out]  ranges  ranges found; empty if the statement cannot be read */
void query_find_ranges(const std::string &sql, query_ranges_t &ranges);

#endif
