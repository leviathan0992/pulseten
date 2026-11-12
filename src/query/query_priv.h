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
 * registered from query_cursor::open() to query_cursor::destroy(), the whole
 * time DuckDB may call back, and its snapshot stays valid for that time.
 *
 *-------------------------------------------------------------------------
 */

#ifndef query_priv_h
#define query_priv_h

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "query.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_noncopyable.h"

/** Name of the table function that scans memtable batches. */
#define QUERY_MEMTABLE_SCAN "_pt_memtable_scan"

/** A connection id is local to its DatabaseInstance. The database address
is stable while the connection owns the instance. */
using query_connection_key_t = std::pair<uintptr_t, uint64_t>;

/** Get the composite connection identity. C++ adapter; no allocation.
@return the database-instance address and database-local connection id */
query_connection_key_t query_connection_key(duckdb_connection conn);

/** Configured decoding of one DuckDB result column. */
class query_reader final {
 public:
  /** Initialize the output type and conversion rules. */
  bool init(duckdb_prepared_statement prepared, idx_t col, col_type_t &out);

  /** Read one value using the configured conversion rules. */
  bool read(duckdb_vector vector, idx_t row, query_value_t &out) const;

 private:
  /** DuckDB type of the column. */
  duckdb_type m_type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: the integer type it is stored as. */
  duckdb_type m_decimal_type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: 10 to the power of its scale. */
  double m_decimal_divisor{1};
};

/** A registered query. */
struct query_entry_t {
  /** Connection the query runs on. */
  query_connection_key_t m_connection_key{0, 0};

  /** What it sees; owned by its db cursor. */
  const query_snapshot_t *m_snapshot{nullptr};

  /** Times each table may be read at; owned by its query cursor. */
  const query_ranges_t *m_ranges{nullptr};

  /** First native callback failure; DuckDB C callbacks carry only text. */
  dberr_t m_callback_error{DB_SUCCESS};
};

/** Running-query state shared by native callbacks. */
class query_registry final : private ut_noncopyable {
 public:
  /** Access the registry retained through process teardown. */
  static query_registry &instance();

  /** Register a query and return its nonzero token. */
  uint64_t register_query(const query_connection_key_t &connection,
                          const query_snapshot_t &snapshot,
                          const query_ranges_t &ranges);

  /** Remove both indexes of a query, ignoring an unknown token. */
  void unregister(uint64_t token);

  /** Find a query in the connection index. */
  const query_snapshot_t *find_by_connection(
      const query_connection_key_t &connection, uint64_t &token,
      const query_ranges_t *&ranges);

  /** Preserve a callback failure category under the registry mutex. */
  void record_error(uint64_t token, dberr_t error);

  /** Restore a callback category without losing DuckDB diagnostic text. */
  db_status_t restore_error(uint64_t token, const db_status_t &status);

  /** Find a query in the token index. */
  const query_snapshot_t *find_by_token(uint64_t token);

 private:
  /** Protects every field below; a leaf lock, held without other locks. */
  std::mutex m_mutex;

  /** Queries indexed by their process-wide token. */
  std::map<uint64_t, query_entry_t> m_by_token;

  /** Token of the query running on each connection. */
  std::map<query_connection_key_t, uint64_t> m_by_connection;

  /** Next token to assign. */
  uint64_t m_next_token{1};
};

/** Install the memtable scan table function on the connection's database.
Call once during db opening, before registering the table replacement callback.
@param[in,out]  conn  borrowed connection; NULL is rejected
@return DB_SUCCESS or DB_ERROR if DuckDB refuses registration */
db_status_t query_install_memtable_scan(duckdb_connection conn);

/** Allow immutable memtable bindings to be retained by this query's plan.
Call before registering the function; the original C trampoline stays fixed. */
void query_enable_memtable_reuse(duckdb_table_function function);

/** Prepare a private, single-execution handle in an already fixed transaction.
The connection and snapshot belong exclusively to this registered cursor.
@return DuckDBSuccess, or DuckDBError with the prepared handle's diagnostic */
duckdb_state query_prepare(duckdb_connection connection, const char *sql,
                           uint64_t token, const query_snapshot_t &snapshot,
                           duckdb_prepared_statement *out);

/** Register table replacement and plan retention after the memtable scan.
Call once during db opening, before user queries or background work.
@param[in,out]  conn  non-NULL borrowed connection to the target database */
void query_register_callbacks(duckdb_connection conn);

/** Build the SELECT that stands for a table: its partitions that overlap a
time range, and its memtable batches.
@param[in]  name   normalized table name
@param[in]  range  times the query may read from the table
@return the SQL text */
std::string query_build_table_sql(uint64_t token, const std::string &name,
                                  const query_table_t &table,
                                  uint32_t partition_days,
                                  const query_range_t &range,
                                  batch_seq_t flushed_seq = UINT64_MAX);

#endif /* query_priv_h */
