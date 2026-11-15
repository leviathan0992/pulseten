/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * pt_duckdb.h
 *    DuckDB transactions, error conversion and SQL execution helpers.
 *
 * Fallible operations return db_status_t, owning both the code and diagnostic.
 * Result handles are destroyed only after the diagnostic has been copied.
 * Other helpers return values or void.
 *
 * DuckDB types are shared by the store, db and query modules;
 * the public C API in pulseten.h does not expose them.
 *
 *-------------------------------------------------------------------------
 */

#ifndef pt_duckdb_h
#define pt_duckdb_h

#include <cstdint>
#include <string>
#include <vector>

#include <duckdb.h>

#include "db_schema.h"
#include "ut.h"
#include "ut_debug.h"

/** Largest byte budget parsed exactly by DuckDB's double-based C setting. */
constexpr uint64_t PT_DUCKDB_MAX_MEMORY_LIMIT_BYTES = uint64_t{1} << 53;

/** Largest explicit execution thread count accepted by PulseTen. */
constexpr uint32_t PT_DUCKDB_MAX_THREADS = 1024;

/** Read the effective memory_limit in bytes before publishing the database. */
uint64_t pt_duckdb_memory_limit_bytes(duckdb_connection connection);

/** Convert a typed DuckDB failure while retaining its diagnostic.
@return the internal failure category and owned message */
db_status_t pt_duckdb_error(duckdb_error_type type, const char *message);

/** Convert a failure from an API that exposes only a prefixed diagnostic.
@return the recognized category, or DB_ERROR for an unknown prefix */
db_status_t pt_duckdb_error(const char *message);

/** Runs transactions on an existing connection. The caller keeps it open. */
class pt_duckdb_session final : private ut_noncopyable {
 public:
  /** Use the given connection for transaction operations. */
  explicit pt_duckdb_session(duckdb_connection conn = nullptr)
      : m_connection(conn) {}

  /** Return the connection used by this session. */
  duckdb_connection connection() const { return (m_connection); }

  /** Begin a transaction on this connection. */
  db_status_t begin();

  /** Roll back while retaining the original operation's diagnostic. */
  db_status_t rollback(db_status_t cause = DB_SUCCESS);

  /** Commit; a failed attempt reports an unknown outcome. */
  db_status_t commit();

 private:
  friend class store;

  /** Connection used by this session. */
  duckdb_connection m_connection{nullptr};
};

/** One value of a query result. */
struct pt_duckdb_cell_t {
  /** The value is NULL; the members below are then unused. */
  bool m_is_null{true};

  /** Integer and boolean results. */
  int64_t m_integer{0};

  /** VARCHAR results. */
  std::string m_string;
};

/** Rows of a query result. */
typedef std::vector<std::vector<pt_duckdb_cell_t>> pt_duckdb_rows_t;

/** Run a statement and discard its result.
@return DB_SUCCESS or a classified DuckDB failure with an owned diagnostic */
db_status_t pt_duckdb_exec(duckdb_connection conn, const std::string &sql);

/** Run a statement and collect its rows; statements without a row result
leave rows empty. Rows are cleared before execution and on failure.
Supported non-NULL value types are BOOLEAN, UTINYINT, INTEGER, UINTEGER, BIGINT,
UBIGINT and VARCHAR.
@return DB_SUCCESS, a classified DuckDB failure, or DB_CORRUPTION for a
  non-NULL value with an unsupported column type */
db_status_t pt_duckdb_exec(duckdb_connection conn, const std::string &sql,
                           pt_duckdb_rows_t &rows);

/** Quote a string as an SQL literal, escaping embedded quotes. */
std::string pt_duckdb_literal(const std::string &name);

/** Map a data type to a DuckDB type.
@return the DuckDB type */
duckdb_type pt_duckdb_convert_type(data_type_t type);

#endif /* pt_duckdb_h */
