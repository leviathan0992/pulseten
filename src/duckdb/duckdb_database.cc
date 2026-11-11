/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * duckdb_database.cc
 *    DuckDB C API wrapper.
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb_database.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>
#include <duckdb_static_extension.h>

#include "schema.h"
#include "ut_err.h"
#include "ut_new.h"

/** Destroy a DuckDB result on every return. */
struct duckdb_result_guard_t {
  /** Result to destroy. */
  duckdb_result m_value{};

  /** Destroy the owned DuckDB result. */
  ~duckdb_result_guard_t() { duckdb_destroy_result(&m_value); }
};

/** Destroy a fetched DuckDB chunk on every return. */
struct duckdb_chunk_guard_t {
  /** Chunk to destroy. */
  duckdb_data_chunk m_value{nullptr};

  /** Destroy the owned DuckDB data chunk. */
  ~duckdb_chunk_guard_t() { duckdb_destroy_data_chunk(&m_value); }
};

/** Own DuckDB's diagnostic before its result is destroyed.
@param[in]  message  DuckDB error text; NULL selects the default diagnostic */
static db_status_t duckdb_error(dberr_t code, const char *message) {
  return (
      db_status_t(code, message != nullptr ? message : "unknown DuckDB error"));
}

/** Describe function of DuckDB's built-in function extension, linked
statically from its archive.
fill
@return 0 if the extension descriptor was filled successfully; nonzero on
  failure */
extern "C" int32_t duckdb_extension_core_functions_describe(
    duckdb_extension_descriptor *descriptor);

/** Register the statically linked extensions before the first database is
opened. The static local runs once, even with concurrent callers; calling the
registration again would be a no-op anyway.
@return true if registration succeeded */
static bool duckdb_register_extensions() {
  static const int32_t result = duckdb_register_static_extension(
      duckdb_extension_core_functions_describe);

  return (result == 0);
}

/** Settings for every database: no network, no extension loading. */
static const char *duckdb_settings[][2] = {
    {"autoinstall_known_extensions", "false"},
    {"autoload_known_extensions", "false"},
};

/** Own every resource until a database is successfully handed out. */
struct duckdb_open_guard_t {
  /** Settings for opening the file. */
  duckdb_config m_config{nullptr};

  /** The database being opened; NULL once handed out. */
  duckdb_database_t *m_duckdb{nullptr};

  /** DuckDB's message if opening failed. */
  char *m_error{nullptr};

  /** Release the open configuration, diagnostic and any untransferred
  database. */
  ~duckdb_open_guard_t() {
    duckdb_free(m_error);
    duckdb_destroy_config(&m_config);
    duckdb_database_close(m_duckdb);
  }
};

/** Open or create a database file. */
db_status_t duckdb_database_open(const char *path, duckdb_database_t **out) {
  *out = nullptr;
  if (!duckdb_register_extensions()) {
    return (
        db_status_t(DB_ERROR, "cannot register DuckDB's built-in functions"));
  }

  duckdb_open_guard_t owner;
  if (duckdb_create_config(&owner.m_config) != DuckDBSuccess) {
    return (DB_OUT_OF_MEMORY);
  }
  for (size_t i = 0; i < sizeof(duckdb_settings) / sizeof(duckdb_settings[0]);
       ++i) {
    if (duckdb_set_config(owner.m_config, duckdb_settings[i][0],
                          duckdb_settings[i][1]) != DuckDBSuccess) {
      return (db_status_t(DB_ERROR, "cannot configure DuckDB"));
    }
  }

  owner.m_duckdb = ut_new<duckdb_database_t>();
  if (owner.m_duckdb == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }
  if (duckdb_open_ext(path, &owner.m_duckdb->m_db, owner.m_config,
                      &owner.m_error) != DuckDBSuccess) {
    return (duckdb_error(DB_IO_ERROR, owner.m_error));
  }
  if (duckdb_connect(owner.m_duckdb->m_db, &owner.m_duckdb->m_conn) !=
      DuckDBSuccess) {
    return (db_status_t(DB_IO_ERROR, "cannot connect to DuckDB"));
  }

  *out = owner.m_duckdb;
  owner.m_duckdb = nullptr;

  return (DB_SUCCESS);
}

/** Close the connection and the database. */
void duckdb_database_close(duckdb_database_t *database) {
  if (database == nullptr) {
    return;
  }

  duckdb_disconnect(&database->m_conn);
  duckdb_close(&database->m_db);
  ut_delete(database);
}

/** Run a statement and discard its result. */
db_status_t duckdb_database_exec(duckdb_database_t &database,
                                 const std::string &sql) {
#ifdef PT_DEBUG
  if (sql == "ROLLBACK" && database.m_dbg_fail_next_rollback) {
    database.m_dbg_fail_next_rollback = false;

    return (db_status_t(DB_ERROR, "injected rollback failure"));
  }
#endif /* PT_DEBUG */

  duckdb_result_guard_t result;
  if (duckdb_query(database.m_conn, sql.c_str(), &result.m_value) !=
      DuckDBSuccess) {
    return (duckdb_error(DB_ERROR, duckdb_result_error(&result.m_value)));
  }

  return (DB_SUCCESS);
}

/** Roll back a failed operation without losing its original diagnostic. */
db_status_t duckdb_database_rollback(duckdb_database_t &database,
                                     db_status_t cause) {
  const db_status_t rollback = duckdb_database_exec(database, "ROLLBACK");
  if (rollback.ok()) {
    return (cause);
  }

  std::string message = cause.ok() ? "" : cause.message() + "; ";
  message += "rollback: " + rollback.message();

  return (db_status_t(DB_OUTCOME_UNKNOWN, std::move(message)));
}

/** Commit a transaction without allowing diagnostic failures to change its
outcome classification. */
db_status_t duckdb_database_commit(duckdb_database_t &database) {
  const db_status_t status = duckdb_database_exec(database, "COMMIT");
  if (status.ok()) {
    return (status);
  }

  /* COMMIT was attempted: even a successful rollback cannot establish its
   * outcome. */
  return (
      duckdb_database_rollback(database, status.with_code(DB_OUTCOME_UNKNOWN)));
}

/** Read one cell of a chunk.
@param[out]  cell  decoded cell, including its NULL flag
@return false for an unsupported column type */
static bool duckdb_read_cell(duckdb_vector vector, duckdb_type type, idx_t row,
                             duckdb_cell_t &cell) {
  if (!duckdb_validity_row_is_valid(duckdb_vector_get_validity(vector), row)) {
    cell.m_is_null = true;

    return (true);
  }

  void *data = duckdb_vector_get_data(vector);
  cell.m_is_null = false;

  switch (type) {
    case DUCKDB_TYPE_BOOLEAN:
      cell.m_i = static_cast<bool *>(data)[row] ? 1 : 0;

      return (true);
    case DUCKDB_TYPE_UTINYINT:
      cell.m_i = static_cast<uint8_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_INTEGER:
      cell.m_i = static_cast<int32_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UINTEGER:
      cell.m_i = static_cast<uint32_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_BIGINT:
      cell.m_i = static_cast<int64_t *>(data)[row];

      return (true);
    case DUCKDB_TYPE_UBIGINT:
      cell.m_i = static_cast<int64_t>(static_cast<uint64_t *>(data)[row]);

      return (true);
    case DUCKDB_TYPE_VARCHAR: {
      duckdb_string_t *s = &static_cast<duckdb_string_t *>(data)[row];
      cell.m_s.assign(duckdb_string_t_data(s), duckdb_string_t_length(*s));

      return (true);
    }
    default:
      return (false);
  }
}

/** Run an internal query. */
db_status_t duckdb_database_query(duckdb_database_t &database,
                                  const std::string &sql, duckdb_rows_t &rows) {
  rows.clear();
  duckdb_result_guard_t result;

  if (duckdb_query(database.m_conn, sql.c_str(), &result.m_value) !=
      DuckDBSuccess) {
    return (duckdb_error(DB_ERROR, duckdb_result_error(&result.m_value)));
  }

  const idx_t n_cols = duckdb_column_count(&result.m_value);
  for (;;) {
    duckdb_chunk_guard_t chunk;
    chunk.m_value = duckdb_fetch_chunk(result.m_value);
    if (chunk.m_value == nullptr) {
      const char *message = duckdb_result_error(&result.m_value);
      if (message != nullptr) {
        rows.clear();

        return (duckdb_error(DB_ERROR, message));
      }

      return (DB_SUCCESS);
    }

    const idx_t n = duckdb_data_chunk_get_size(chunk.m_value);
    for (idx_t r = 0; r < n; ++r) {
      std::vector<duckdb_cell_t> row(n_cols);
      for (idx_t c = 0; c < n_cols; ++c) {
        duckdb_vector vector = duckdb_data_chunk_get_vector(chunk.m_value, c);
        if (!duckdb_read_cell(vector, duckdb_column_type(&result.m_value, c), r,
                              row[c])) {
          rows.clear();

          return (db_status_t(DB_CORRUPTION,
                              "unexpected column type in internal query"));
        }
      }
      rows.push_back(std::move(row));
    }
  }
}

/** Quote a string as an SQL literal. */
std::string duckdb_literal(const std::string &name) {
  std::string out = "'";

  for (size_t i = 0; i < name.size(); ++i) {
    out += name[i];
    if (name[i] == '\'') {
      out += '\'';
    }
  }

  return (out + "'");
}

/** Map a column type to a DuckDB type. */
duckdb_type duckdb_type_from_col(col_type_t type) {
  switch (type) {
    case col_type_t::BOOLEAN:
      return (DUCKDB_TYPE_BOOLEAN);
    case col_type_t::BIGINT:
      return (DUCKDB_TYPE_BIGINT);
    case col_type_t::DOUBLE:
      return (DUCKDB_TYPE_DOUBLE);
    case col_type_t::TIMESTAMP:
      return (DUCKDB_TYPE_TIMESTAMP);
    case col_type_t::VARCHAR:
      return (DUCKDB_TYPE_VARCHAR);
  }

  return (DUCKDB_TYPE_VARCHAR);
}
