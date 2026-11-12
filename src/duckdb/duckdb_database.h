/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * duckdb_database.h
 *    Thin wrapper around the DuckDB C API: opening a database and
 *    connection, executing SQL and reading internal query results.
 *
 * Fallible operations return db_status_t, owning both the code and diagnostic.
 * Result handles are destroyed only after the diagnostic has been copied.
 * Other helpers return values or void.
 *
 * DuckDB types are shared by internal modules such as storage and query;
 * the public C API in pulseten.h does not expose them.
 *
 *-------------------------------------------------------------------------
 */

#ifndef duckdb_database_h
#define duckdb_database_h

#include <cstdint>
#include <string>
#include <vector>

#include <duckdb.h>

#include "schema.h"
#include "ut_err.h"
#include "ut_noncopyable.h"

/** Largest byte budget parsed exactly by DuckDB's double-based C setting. */
constexpr uint64_t DUCKDB_MAX_MEMORY_LIMIT_BYTES = uint64_t{1} << 53;

/** Resolve 0 to min(effective capacity / 8, 2 GiB); validate explicit budgets.
Run before allocating database resources or creating files.
On failure, bytes is unchanged. */
db_status_t duckdb_resolve_memory_limit(uint64_t &bytes);

/** Convert a typed DuckDB failure while retaining its diagnostic.
@return the internal failure category and owned message */
db_status_t duckdb_error(duckdb_error_type type, const char *message);

/** Convert a failure from an API that exposes only a prefixed diagnostic.
@return the recognized category, or DB_ERROR for an unknown prefix */
db_status_t duckdb_error(const char *message);

/** A borrowed connection; destruction never disconnects it. */
class duckdb_session final : private ut_noncopyable {
 public:
  /** Borrow a native connection for the caller's operation. */
  explicit duckdb_session(duckdb_connection conn = nullptr) : m_conn(conn) {}

  /** Borrow the native connection. */
  duckdb_connection connection() const { return (m_conn); }

  /** Begin a transaction on this connection. */
  db_status_t begin();

  /** Roll back while retaining the original operation's diagnostic. */
  db_status_t rollback(db_status_t cause = DB_SUCCESS);

  /** Commit; a failed attempt reports an unknown outcome. */
  db_status_t commit();

 private:
  friend class duckdb_backend;
#ifdef PT_TESTING
  /** Rollback injection is implemented only in test support. */
  friend class storage_test_access;
#endif /* PT_TESTING */

  /** Borrowed handle; only duckdb_backend owns and disconnects it. */
  duckdb_connection m_conn{nullptr};

#ifdef PT_TESTING
  /** Fail the next rollback; protected by the owning subsystem's lock. */
  bool m_test_fail_next_rollback{false};
#endif /* PT_TESTING */
};

/** Owns a DuckDB database and its storage connection. */
class duckdb_backend final : private ut_noncopyable {
 public:
  /** Construct an unopened backend. */
  duckdb_backend() = default;

  /** Disconnect the storage connection before closing the database. */
  ~duckdb_backend();

  /** Allocate and open a backend, returning NULL on failure. */
  static db_status_t open(const char *path, duckdb_backend **out,
                          uint64_t memory_limit_bytes = 0);

  /** Destroy a backend; NULL is accepted. */
  static void destroy(duckdb_backend *backend);

  /** Release owned handles; safe after partial initialization. */
  void close();

  /** Borrow the native database for opening query connections. */
  duckdb_database database() const { return (m_db); }

  /** Borrow the storage connection. */
  duckdb_connection connection() const { return (m_session.connection()); }

  /** Access transaction operations on the storage connection. */
  duckdb_session &session() { return (m_session); }

 private:
  /** Open handles; the factory releases config and cleans up failures. */
  db_status_t open_low(const char *path, duckdb_config &config,
                       uint64_t memory_limit_bytes);

  /** Owned native database. */
  duckdb_database m_db{nullptr};

  /** Storage connection whose native handle this backend owns. */
  duckdb_session m_session;
};

/** One value of a query result. */
struct duckdb_cell_t {
  /** The value is NULL; the members below are then unused. */
  bool m_is_null{true};

  /** Integer and boolean results. */
  int64_t m_i{0};

  /** VARCHAR results. */
  std::string m_s;
};

/** Rows of a query result. */
typedef std::vector<std::vector<duckdb_cell_t>> duckdb_rows_t;

/** Run a statement and discard its result.
@return DB_SUCCESS or a classified DuckDB failure with an owned diagnostic */
db_status_t duckdb_exec(duckdb_connection conn, const std::string &sql);

/** Run a statement and collect its rows; statements without a row result
leave rows empty. Rows are cleared before execution and on failure.
Supported non-NULL value types are BOOLEAN, UTINYINT, INTEGER, UINTEGER, BIGINT,
UBIGINT and VARCHAR.
@return DB_SUCCESS, a classified DuckDB failure, or DB_CORRUPTION for a
  non-NULL value with an unsupported column type */
db_status_t duckdb_exec(duckdb_connection conn, const std::string &sql,
                        duckdb_rows_t &rows);

/** Quote a string as an SQL literal.
@return name as a single-quoted SQL string literal */
std::string duckdb_literal(const std::string &name);

/** Map a column type to a DuckDB type.
@return the DuckDB type */
duckdb_type duckdb_type_from_col(col_type_t type);

#endif /* duckdb_database_h */
