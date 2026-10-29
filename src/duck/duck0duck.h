/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * duck0duck.h
 *    Thin wrapper around the DuckDB C API: opening a database and
 *    connection, executing SQL and reading internal query results.
 *
 * Fallible operations return dberr_t. duck_exec() and duck_query() save
 * diagnostics in duck.error; duck_open() and duck_commit() take a separate
 * error output parameter. Other helpers return values or void.
 *
 * DuckDB types are shared by internal modules such as storage and query;
 * the public C API in pulseten.h does not expose them.
 *
 *-------------------------------------------------------------------------
 */

#ifndef duck0duck_h
#define duck0duck_h

#include <duckdb.h>

#include <cstdint>
#include <string>
#include <vector>

#include "schema0schema.h"
#include "ut0err.h"

/** An open DuckDB database with one connection. */
struct duck_t {
  /** The database file. */
  duckdb_database db{nullptr};

  /** The connection that every storage statement runs on. */
  duckdb_connection conn{nullptr};

  /** Message of the last failed call. */
  std::string error;
};

/** One value of a query result. */
struct duck_cell_t {
  /** The value is NULL; the members below are then unused. */
  bool is_null{true};

  /** Integer and boolean results. */
  int64_t i{0};

  /** VARCHAR results. */
  std::string s;
};

/** Rows of a query result. */
typedef std::vector<std::vector<duck_cell_t>> duck_rows_t;

/** Open or create a database file.
@param[in]   path   file path
@param[out]  duck   open database
@param[out]  error  DuckDB's message on failure
@return DB_SUCCESS, DB_IO_ERROR, DB_ERROR or DB_OUT_OF_MEMORY */
dberr_t duck_open(const char *path, duck_t **duck, std::string &error);

/** Close the connection and the database.
@param[in]  duck  database; may be NULL */
void duck_close(duck_t *duck);

/** Run a statement and discard its result.
@param[in,out]  duck  DuckDB database and connection
@param[in]      sql   SQL statement to check or execute
@return DB_SUCCESS or DB_ERROR with duck.error set */
dberr_t duck_exec(duck_t &duck, const std::string &sql);

/** Commit a transaction without allowing diagnostic failures to change its
outcome classification. A failed attempt tries rollback best effort.
@param[in,out]  duck   DuckDB database and connection
@param[out]     error  best-effort message
@return DB_SUCCESS or DB_OUTCOME_UNKNOWN */
dberr_t duck_commit(duck_t &duck, std::string &error);

/** Run an internal query. Supported non-NULL value types are BOOLEAN,
UTINYINT, INTEGER, UINTEGER, BIGINT, UBIGINT and VARCHAR.
@param[in,out]  duck  DuckDB database and connection
@param[in]      sql   SQL statement to check or execute
@param[out]     rows  result rows
@return DB_SUCCESS, DB_ERROR, or DB_CORRUPTION for a non-NULL value with an
  unsupported column type */
dberr_t duck_query(duck_t &duck, const std::string &sql, duck_rows_t &rows);

/** Quote a string as an SQL literal.
@param[in]  name  string bytes to quote, escaping single quotes
@return name as a single-quoted SQL string literal */
std::string duck_literal(const std::string &name);

/** Map a column type to a DuckDB type.
@param[in]  type  column value type
@return the DuckDB type */
duckdb_type duck_type(col_type_t type);

#endif
