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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <duckdb.h>
#include <duckdb_static_extension.h>

#include "os_memory.h"
#include "schema.h"
#include "ut_err.h"
#include "ut_new.h"
#include "ut_noncopyable.h"

/** Resolve only automatic values; an explicit budget never probes the host. */
db_status_t duckdb_resolve_memory_limit(uint64_t &bytes) {
  if (bytes > DUCKDB_MAX_MEMORY_LIMIT_BYTES) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "DuckDB memory limit must be at most 2^53 bytes"));
  }

  if (bytes != 0) {
    return (DB_SUCCESS);
  }

  uint64_t capacity = 0;
  if (const db_status_t err = os_effective_memory(capacity); !err.ok()) {
    return (
        err.with_context("cannot choose automatic DuckDB memory limit; "
                         "set an explicit DuckDB memory limit"));
  }

  bytes = std::min(capacity / 8, uint64_t{2} << 30);

  return (DB_SUCCESS);
}

/** Destroy a DuckDB result on every return. */
class duckdb_result_guard final : private ut_noncopyable {
 public:
  /** Result to destroy. */
  duckdb_result m_value{};

  /** Destroy the owned DuckDB result. */
  ~duckdb_result_guard() { duckdb_destroy_result(&m_value); }
};

/** Destroy a fetched DuckDB chunk on every return. */
class duckdb_chunk_guard final : private ut_noncopyable {
 public:
  /** Chunk to destroy. */
  duckdb_data_chunk m_value{nullptr};

  /** Destroy the owned DuckDB data chunk. */
  ~duckdb_chunk_guard() { duckdb_destroy_data_chunk(&m_value); }
};

/** Preserve the error category and copy the message before its handle closes.
 */
db_status_t duckdb_error(duckdb_error_type type, const char *message) {
  dberr_t code = DB_ERROR;
  if (type == DUCKDB_ERROR_OUT_OF_MEMORY) {
    code = DB_OUT_OF_MEMORY;
  } else if (type == DUCKDB_ERROR_IO) {
    code = DB_IO_ERROR;
  } else if (type == DUCKDB_ERROR_INTERRUPT) {
    code = DB_INTERRUPTED;
  } else if (type == DUCKDB_ERROR_DATA_CORRUPTION) {
    code = DB_CORRUPTION;
  }

  return (
      db_status_t(code, message != nullptr ? message : "unknown DuckDB error"));
}

/** Older C entrypoints expose only DuckDB's standard error prefix. */
db_status_t duckdb_error(const char *message) {
  const std::string_view text = message != nullptr ? message : "";
  duckdb_error_type type = DUCKDB_ERROR_INVALID;
  if (text.starts_with("Out of Memory Error:")) {
    type = DUCKDB_ERROR_OUT_OF_MEMORY;
  } else if (text.starts_with("IO Error:")) {
    type = DUCKDB_ERROR_IO;
  } else if (text.starts_with("INTERRUPT Error:")) {
    type = DUCKDB_ERROR_INTERRUPT;
  } else if (text.starts_with("Data Corruption Error:")) {
    type = DUCKDB_ERROR_DATA_CORRUPTION;
  }

  return (duckdb_error(type, message));
}

/** Describe function of DuckDB's built-in function extension, linked
statically from its archive.
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

/** Open the database and its storage connection. The caller releases config
on every return and closes any partially opened database on failure. */
db_status_t duckdb_backend::open_low(const char *path, duckdb_config &config,
                                     uint64_t memory_limit_bytes) {
  /* Build startup settings with automatic extension installation and loading
  disabled. */
  if (duckdb_create_config(&config) != DuckDBSuccess) {
    return (DB_OUT_OF_MEMORY);
  }

  for (size_t i = 0; i < sizeof(duckdb_settings) / sizeof(duckdb_settings[0]);
       ++i) {
    if (duckdb_set_config(config, duckdb_settings[i][0],
                          duckdb_settings[i][1]) != DuckDBSuccess) {
      return (db_status_t(DB_ERROR, "cannot configure DuckDB"));
    }
  }

  const std::string memory_limit = std::to_string(memory_limit_bytes) + " B";
  if (duckdb_set_config(config, "memory_limit", memory_limit.c_str()) !=
      DuckDBSuccess) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "cannot configure DuckDB memory limit"));
  }

  /* Open the existing database file or create a new one with these settings. */
  char *error = nullptr;
  const duckdb_state result = duckdb_open_ext(path, &m_db, config, &error);
  /* Preserve the diagnostic before releasing DuckDB's error text. */
  const db_status_t err =
      result == DuckDBSuccess ? db_status_t(DB_SUCCESS) : duckdb_error(error);
  duckdb_free(error);
  if (!err.ok()) {
    return (err);
  }

  /* Create the connection used by storage statements. */
  if (duckdb_connect(m_db, &m_session.m_conn) != DuckDBSuccess) {
    return (db_status_t(DB_ERROR, "cannot connect to DuckDB"));
  }

  return (DB_SUCCESS);
}

/** Open or create a database file. */
db_status_t duckdb_backend::open(const char *path, duckdb_backend **out,
                                 uint64_t memory_limit_bytes) {
  *out = nullptr;
  if (const db_status_t err = duckdb_resolve_memory_limit(memory_limit_bytes);
      !err.ok()) {
    return (err);
  }
  if (!duckdb_register_extensions()) {
    /* This build provides built-in SQL functions through a static extension. */
    return (
        db_status_t(DB_ERROR, "cannot register DuckDB's built-in functions"));
  }

  duckdb_backend *backend = ut_new<duckdb_backend>();
  if (backend == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  duckdb_config config = nullptr;
  const db_status_t err = backend->open_low(path, config, memory_limit_bytes);
  duckdb_destroy_config(&config);
  if (!err.ok()) {
    duckdb_backend::destroy(backend);

    return (err);
  }

  *out = backend;

  return (DB_SUCCESS);
}

/** Close the connection and the database. */
duckdb_backend::~duckdb_backend() {
  close();
}

void duckdb_backend::destroy(duckdb_backend *backend) {
  ut_delete(backend);

  return;
}

void duckdb_backend::close() {
  duckdb_disconnect(&m_session.m_conn);
  duckdb_close(&m_db);

  return;
}

/** Begin a transaction on the database connection. */
db_status_t duckdb_session::begin() {
  return (duckdb_exec(m_conn, "BEGIN TRANSACTION"));
}

/** Roll back a failed operation without losing its original diagnostic. */
db_status_t duckdb_session::rollback(db_status_t cause) {
  db_status_t rollback = DB_SUCCESS;
#ifdef PT_TESTING
  if (m_test_fail_next_rollback) {
    m_test_fail_next_rollback = false;
    rollback = db_status_t(DB_ERROR, "injected rollback failure");
  }
#endif /* PT_TESTING */

  if (rollback.ok()) {
    rollback = duckdb_exec(m_conn, "ROLLBACK");
  }

  if (rollback.ok()) {
    return (cause);
  }

  std::string message = cause.ok() ? "" : cause.message() + "; ";
  message += "rollback: " + rollback.message();

  return (db_status_t(DB_OUTCOME_UNKNOWN, std::move(message)));
}

/** Commit a transaction without allowing diagnostic failures to change its
outcome classification. */
db_status_t duckdb_session::commit() {
  const db_status_t status = duckdb_exec(m_conn, "COMMIT");
  if (status.ok()) {
    return (status);
  }

  /* COMMIT was attempted: even a successful rollback cannot establish its
   * outcome. */
  return (rollback(status.with_code(DB_OUTCOME_UNKNOWN)));
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

/** Execute SQL and optionally collect its row result. */
static db_status_t duckdb_exec_low(duckdb_connection conn,
                                   const std::string &sql,
                                   duckdb_rows_t *rows) {
  if (rows != nullptr) {
    rows->clear();
  }

  duckdb_result_guard result;
  if (duckdb_query(conn, sql.c_str(), &result.m_value) != DuckDBSuccess) {
    return (duckdb_error(duckdb_result_error_type(&result.m_value),
                         duckdb_result_error(&result.m_value)));
  }

  if (rows == nullptr) {
    return (DB_SUCCESS);
  }

  switch (duckdb_result_return_type(result.m_value)) {
    case DUCKDB_RESULT_TYPE_NOTHING:
    case DUCKDB_RESULT_TYPE_CHANGED_ROWS:
      return (DB_SUCCESS);
    case DUCKDB_RESULT_TYPE_QUERY_RESULT:
      break;
    default:
      return (db_status_t(DB_ERROR, "unexpected DuckDB result type"));
  }

  const idx_t n_cols = duckdb_column_count(&result.m_value);
  for (;;) {
    duckdb_chunk_guard chunk;
    chunk.m_value = duckdb_fetch_chunk(result.m_value);
    if (chunk.m_value == nullptr) {
      const char *message = duckdb_result_error(&result.m_value);
      if (message != nullptr) {
        rows->clear();

        return (
            duckdb_error(duckdb_result_error_type(&result.m_value), message));
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
          rows->clear();

          return (db_status_t(DB_CORRUPTION,
                              "unexpected column type in internal query"));
        }
      }

      rows->push_back(std::move(row));
    }
  }
}

/** Run a statement and discard its result. */
db_status_t duckdb_exec(duckdb_connection conn, const std::string &sql) {
  return (duckdb_exec_low(conn, sql, nullptr));
}

/** Run a statement and collect its rows. */
db_status_t duckdb_exec(duckdb_connection conn, const std::string &sql,
                        duckdb_rows_t &rows) {
  return (duckdb_exec_low(conn, sql, &rows));
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
