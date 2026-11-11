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

/** An open DuckDB database with one connection. */
struct duckdb_database_t {
  /** The database file. */
  duckdb_database m_db{nullptr};

  /** The connection that every storage statement runs on. */
  duckdb_connection m_conn{nullptr};

#ifdef PT_DEBUG
  /** Fail the next ROLLBACK without executing it; protected by the owning
  subsystem's storage lock. Debug builds only. */
  bool m_dbg_fail_next_rollback{false};
#endif /* PT_DEBUG */
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

/** Open or create a database file.
@return DB_SUCCESS, DB_IO_ERROR, DB_ERROR or DB_OUT_OF_MEMORY */
db_status_t duckdb_database_open(const char *path, duckdb_database_t **out);

/** Close the connection and the database.
@param[in]  database  database; may be NULL */
void duckdb_database_close(duckdb_database_t *database);

/** Run a statement and discard its result.
@return DB_SUCCESS or DB_ERROR with an owned DuckDB diagnostic */
db_status_t duckdb_database_exec(duckdb_database_t &database,
                                 const std::string &sql);

/** Roll back a failed operation, preserving its diagnostic and appending any
rollback failure. A failed rollback leaves the connection unsafe to reuse.
@param[in]  cause  original operation result; success for a plain rollback
@return cause after a successful rollback, or DB_OUTCOME_UNKNOWN retaining
  both the original cause and rollback failure */
db_status_t duckdb_database_rollback(duckdb_database_t &database,
                                     db_status_t cause = DB_SUCCESS);

/** Commit a transaction without allowing diagnostic failures to change its
outcome classification. A failed attempt tries rollback best effort.
@return DB_SUCCESS or DB_OUTCOME_UNKNOWN */
db_status_t duckdb_database_commit(duckdb_database_t &database);

/** Run an internal query. Supported non-NULL value types are BOOLEAN,
UTINYINT, INTEGER, UINTEGER, BIGINT, UBIGINT and VARCHAR.
@return DB_SUCCESS, DB_ERROR, or DB_CORRUPTION for a non-NULL value with an
  unsupported column type */
db_status_t duckdb_database_query(duckdb_database_t &database,
                                  const std::string &sql, duckdb_rows_t &rows);

/** Quote a string as an SQL literal.
@return name as a single-quoted SQL string literal */
std::string duckdb_literal(const std::string &name);

/** Map a column type to a DuckDB type.
@return the DuckDB type */
duckdb_type duckdb_type_from_col(col_type_t type);

#endif /* duckdb_database_h */
