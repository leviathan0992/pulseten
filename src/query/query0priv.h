/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0priv.h
 *    Definitions shared by the files of the query module only.
 *
 * A running query is registered under a token and under the id of its
 * DuckDB connection and database instance. Replacement scans use both; the
 * memtable scan by the token written into the generated SQL. A query is
 * registered from query_open() to query_close(), the whole time DuckDB may
 * call back, and its snapshot stays valid for that time.
 *
 *-------------------------------------------------------------------------
 */

#ifndef query0priv_h
#define query0priv_h

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "query0query.h"

/** Name of the table function that scans memtable batches. */
#define QUERY_HOT_SCAN "_pt_hot_scan"

/** Times, in microseconds, a query may read from a table: [lo, hi].
Empty when lo > hi. */
struct query_range_t {
  /** Earliest time, inclusive. */
  int64_t lo{INT64_MIN};

  /** Latest time, inclusive. */
  int64_t hi{INT64_MAX};
};

/** Time ranges of a query by canonical table name. A table that is not
listed may be read at any time. */
typedef std::map<std::string, query_range_t> query_ranges_t;

/** A connection id is local to its DatabaseInstance. The database address
is stable while the connection owns the instance. */
using query_connection_key_t = std::pair<uintptr_t, uint64_t>;

/** Get the composite connection identity. C++ adapter; no allocation.
@param[in]  conn  DuckDB connection
@return the database-instance address and database-local connection id */
query_connection_key_t query_connection_key(duckdb_connection conn);

/** Find a running query by its connection.
@param[in]   connection  database instance and DuckDB connection id
@param[out]  token       its token
@param[out]  ranges      its time ranges
@return its snapshot, or NULL */
const query_snapshot_t *query_find_by_connection(
    const query_connection_key_t &connection, uint64_t &token,
    const query_ranges_t **ranges);

/** Find the snapshot of a registered query.
@param[in]  token  registration token of the query
@return the snapshot of a running query, or NULL */
const query_snapshot_t *query_find_by_token(uint64_t token);

/** Build the SELECT that stands for a table: its partitions that overlap a
time range, and its memtable batches.
@param[in]  token           token of the query
@param[in]  name            canonical table name
@param[in]  table           the table in the query's snapshot
@param[in]  partition_days  days covered by one partition
@param[in]  range           times the query may read from the table
@param[in]  flushed_seq     logical cold upper bound F; replacement scans
                            always pass their snapshot F explicitly
@return the SQL text */
std::string query_table_sql(uint64_t token, const std::string &name,
                            const query_table_t &table, uint32_t partition_days,
                            const query_range_t &range,
                            batch_seq_t flushed_seq = UINT64_MAX);

/** Register the memtable scan table function on a connection.
@param[in,out]  conn  connection on which the internal hot-scan function is
                      registered
@return DB_SUCCESS or DB_ERROR if DuckDB refuses registration */
dberr_t query_install_hot_scan(duckdb_connection conn);

/** Register the replacement scan with the database. C++ adapter.
@param[in,out]  db  DuckDB database receiving the replacement-scan callback */
void query_install_replacement(duckdb_database db);

/** Parse a statement and check what it reads. C++ adapter.
@param[in]   sql    a single statement that DuckDB's C API parsed
@param[out]  error  reason for rejection
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
dberr_t query_check_statement(const std::string &sql, std::string &error);

/** Work out the times a statement may read from each table, from simple
conditions on "time". C++ adapter.
@param[in]   sql     statement accepted by query_check()
@param[out]  ranges  ranges found; empty if the statement cannot be read */
void query_find_ranges(const std::string &sql, query_ranges_t &ranges);

#endif
