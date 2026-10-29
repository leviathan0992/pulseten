/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * duck0duck.cc
 *    DuckDB C API wrapper.
 *
 *-------------------------------------------------------------------------
 */

#include "duck0duck.h"

#include <duckdb_static_extension.h>

#include <new>

#include "ut0new.h"

/** Destroy a DuckDB result on every return. */
struct duck_result_guard_t {
  /** Result to destroy. */
  duckdb_result value{};

  /** Destroy the owned DuckDB result. */
  ~duck_result_guard_t() { duckdb_destroy_result(&value); }
};

/** Destroy a fetched DuckDB chunk on every return. */
struct duck_chunk_guard_t {
  /** Chunk to destroy. */
  duckdb_data_chunk value{nullptr};

  /** Destroy the owned DuckDB data chunk. */
  ~duck_chunk_guard_t() { duckdb_destroy_data_chunk(&value); }
};

/** Copy DuckDB's message.
@param[out]  out      destination for the message
@param[in]   message  DuckDB error text; NULL selects the default diagnostic */
static void duck_copy_error(std::string &out, const char *message) {
  out = message != nullptr ? message : "unknown DuckDB error";
}

/** Describe function of DuckDB's built-in function extension, linked
statically from its archive.
@param[in,out]  descriptor  requested descriptor layout and extension details to
fill
@return 0 if the extension descriptor was filled successfully; nonzero on
  failure */
extern "C" int32_t duckdb_extension_core_functions_describe(
    duckdb_extension_descriptor *descriptor);

/** Register the statically linked extensions before the first database is
opened. The static local runs once, even with concurrent callers; calling the
registration again would be a no-op anyway.
@return true if registration succeeded */
static bool duck_register_extensions() {
  static const int32_t result = duckdb_register_static_extension(
      duckdb_extension_core_functions_describe);

  return (result == 0);
}

/** Settings for every database: no network, no extension loading. */
static const char *duck_settings[][2] = {
    {"autoinstall_known_extensions", "false"},
    {"autoload_known_extensions", "false"},
};

/** Own every resource until a database is successfully handed out. */
struct duck_open_guard_t {
  /** Settings for opening the file. */
  duckdb_config config{nullptr};

  /** The database being opened; NULL once handed out. */
  duck_t *duck{nullptr};

  /** DuckDB's message if opening failed. */
  char *error{nullptr};

  /** Release the open configuration, diagnostic and any untransferred
  database. */
  ~duck_open_guard_t() {
    duckdb_free(error);
    duckdb_destroy_config(&config);
    duck_close(duck);
  }
};

/** Open or create a database file.
@param[in]   path       file path
@param[out]  out        open database
@param[out]  error  DuckDB's message on failure
@return DB_SUCCESS, DB_IO_ERROR, DB_ERROR or DB_OUT_OF_MEMORY */
dberr_t duck_open(const char *path, duck_t **out, std::string &error) {
  *out = nullptr;
  error.clear();

  if (!duck_register_extensions()) {
    duck_copy_error(error, "cannot register DuckDB's built-in functions");

    return (DB_ERROR);
  }

  duck_open_guard_t owner;
  if (duckdb_create_config(&owner.config) != DuckDBSuccess) {
    return (DB_OUT_OF_MEMORY);
  }
  for (size_t i = 0; i < sizeof(duck_settings) / sizeof(duck_settings[0]);
       ++i) {
    if (duckdb_set_config(owner.config, duck_settings[i][0],
                          duck_settings[i][1]) != DuckDBSuccess) {
      return (DB_ERROR);
    }
  }

  owner.duck = ut_new<duck_t>();
  if (owner.duck == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }
  if (duckdb_open_ext(path, &owner.duck->db, owner.config, &owner.error) !=
      DuckDBSuccess) {
    duck_copy_error(error, owner.error);

    return (DB_IO_ERROR);
  }
  if (duckdb_connect(owner.duck->db, &owner.duck->conn) != DuckDBSuccess) {
    duck_copy_error(error, "cannot connect to DuckDB");

    return (DB_IO_ERROR);
  }

  *out = owner.duck;
  owner.duck = nullptr;

  return (DB_SUCCESS);
}

/** Close the connection and the database.
@param[in]  duck  database; may be NULL */
void duck_close(duck_t *duck) {
  if (duck == nullptr) {
    return;
  }

  duckdb_disconnect(&duck->conn);
  duckdb_close(&duck->db);
  ut_delete(duck);
}

/** Run a statement and discard its result.
@param[in,out]  duck  DuckDB database and connection
@param[in]      sql   SQL statement to check or execute
@return DB_SUCCESS or DB_ERROR with duck.error set */
dberr_t duck_exec(duck_t &duck, const std::string &sql) {
  duck_result_guard_t result;
  const duckdb_state state =
      duckdb_query(duck.conn, sql.c_str(), &result.value);

  if (state != DuckDBSuccess) {
    duck_copy_error(duck.error, duckdb_result_error(&result.value));
  }

  return (state == DuckDBSuccess ? DB_SUCCESS : DB_ERROR);
}

/** Commit a transaction without allowing diagnostic failures to change its
outcome classification.
@param[in,out]  duck   DuckDB database and connection
@param[out]     error  best-effort message
@return DB_SUCCESS or DB_OUTCOME_UNKNOWN */
dberr_t duck_commit(duck_t &duck, std::string &error) {
  if (duck_exec(duck, "COMMIT") == DB_SUCCESS) {
    return (DB_SUCCESS);
  }

  /* Once COMMIT was attempted, a failure is an unknown outcome. */
  duck_copy_error(error, duck.error.c_str());
  (void)duck_exec(duck, "ROLLBACK");

  return (DB_OUTCOME_UNKNOWN);
}

/** Read one cell of a chunk.
@param[in]   vector  column vector containing the cell
@param[in]   type    column value type
@param[in]   row     row index within the vector
@param[out]  cell    decoded cell, including its NULL flag
@return false for an unsupported column type */
static bool duck_read_cell(duckdb_vector vector, duckdb_type type, idx_t row,
                           duck_cell_t &cell) {
  if (!duckdb_validity_row_is_valid(duckdb_vector_get_validity(vector), row)) {
    cell.is_null = true;

    return (true);
  }

  void *data = duckdb_vector_get_data(vector);
  cell.is_null = false;

  switch (type) {
    case DUCKDB_TYPE_BOOLEAN:
      cell.i = static_cast<bool *>(data)[row] ? 1 : 0;

      return (true);
    case DUCKDB_TYPE_UTINYINT:
      cell.i = static_cast<uint8_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_INTEGER:
      cell.i = static_cast<int32_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UINTEGER:
      cell.i = static_cast<uint32_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_BIGINT:
      cell.i = static_cast<int64_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UBIGINT:
      cell.i = (int64_t)static_cast<uint64_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_VARCHAR: {
      duckdb_string_t *s = &static_cast<duckdb_string_t *>(data)[row];
      cell.s.assign(duckdb_string_t_data(s), duckdb_string_t_length(*s));

      return (true);
    }
    default:
      return (false);
  }
}

/** Run an internal query. Supported non-NULL value types are BOOLEAN,
UTINYINT, INTEGER, UINTEGER, BIGINT, UBIGINT and VARCHAR.
@param[in,out]  duck  DuckDB database and connection
@param[in]      sql   SQL statement to check or execute
@param[out]     rows  result rows
@return DB_SUCCESS, DB_ERROR, or DB_CORRUPTION for a non-NULL value with an
  unsupported column type */
dberr_t duck_query(duck_t &duck, const std::string &sql, duck_rows_t &rows) {
  rows.clear();
  duck_result_guard_t result;

  if (duckdb_query(duck.conn, sql.c_str(), &result.value) != DuckDBSuccess) {
    duck_copy_error(duck.error, duckdb_result_error(&result.value));

    return (DB_ERROR);
  }

  const idx_t n_cols = duckdb_column_count(&result.value);
  for (;;) {
    duck_chunk_guard_t chunk;
    chunk.value = duckdb_fetch_chunk(result.value);
    if (chunk.value == nullptr) {
      const char *message = duckdb_result_error(&result.value);
      if (message != nullptr) {
        duck_copy_error(duck.error, message);
        rows.clear();

        return (DB_ERROR);
      }

      return (DB_SUCCESS);
    }

    const idx_t n = duckdb_data_chunk_get_size(chunk.value);
    for (idx_t r = 0; r < n; ++r) {
      std::vector<duck_cell_t> row(n_cols);
      for (idx_t c = 0; c < n_cols; ++c) {
        duckdb_vector vector = duckdb_data_chunk_get_vector(chunk.value, c);
        if (!duck_read_cell(vector, duckdb_column_type(&result.value, c), r,
                            row[c])) {
          duck_copy_error(duck.error,
                          "unexpected column type in internal query");
          rows.clear();

          return (DB_CORRUPTION);
        }
      }
      rows.push_back(std::move(row));
    }
  }
}

/** Quote a string as an SQL literal.
@param[in]  name  string bytes to quote, escaping single quotes
@return name as a single-quoted SQL string literal */
std::string duck_literal(const std::string &name) {
  std::string out = "'";

  for (size_t i = 0; i < name.size(); ++i) {
    out += name[i];
    if (name[i] == '\'') {
      out += '\'';
    }
  }

  return (out + "'");
}

/** Map a column type to a DuckDB type.
@param[in]  type  column value type
@return the DuckDB type */
duckdb_type duck_type(col_type_t type) {
  switch (type) {
    case COL_BOOLEAN:
      return (DUCKDB_TYPE_BOOLEAN);
    case COL_BIGINT:
      return (DUCKDB_TYPE_BIGINT);
    case COL_DOUBLE:
      return (DUCKDB_TYPE_DOUBLE);
    case COL_TIMESTAMP:
      return (DUCKDB_TYPE_TIMESTAMP);
    case COL_VARCHAR:
      return (DUCKDB_TYPE_VARCHAR);
  }

  return (DUCKDB_TYPE_VARCHAR);
}
