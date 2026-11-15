/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * pt_duckdb.cc
 *    DuckDB C API wrapper.
 *
 *-------------------------------------------------------------------------
 */

#include "pt_duckdb.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "db_schema.h"
#include "ut.h"

/** Keep the error category; copy the message before its handle closes. */
db_status_t pt_duckdb_error(duckdb_error_type type, const char *message) {
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

/** Classify errors from C APIs that return only a diagnostic string. */
db_status_t pt_duckdb_error(const char *message) {
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

  return (pt_duckdb_error(type, message));
}

/** Begin a transaction on the database connection. */
db_status_t pt_duckdb_session::begin() {
  return (pt_duckdb_exec(m_connection, "BEGIN TRANSACTION"));
}

/** Roll back a failed operation without losing its original diagnostic. */
db_status_t pt_duckdb_session::rollback(db_status_t cause) {
  db_status_t rollback = UT_DEBUG_FAULT("store.rollback");
  if (rollback.ok()) {
    rollback = pt_duckdb_exec(m_connection, "ROLLBACK");
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
db_status_t pt_duckdb_session::commit() {
  db_status_t status = UT_DEBUG_FAULT("store.commit");
  if (status.ok()) {
    status = pt_duckdb_exec(m_connection, "COMMIT");
  }

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
static bool pt_duckdb_read_cell(duckdb_vector vector, duckdb_type type,
                                idx_t row, pt_duckdb_cell_t &cell) {
  if (!duckdb_validity_row_is_valid(duckdb_vector_get_validity(vector), row)) {
    cell.m_is_null = true;

    return (true);
  }

  void *data = duckdb_vector_get_data(vector);
  cell.m_is_null = false;

  switch (type) {
    case DUCKDB_TYPE_BOOLEAN:
      cell.m_integer = static_cast<bool *>(data)[row] ? 1 : 0;

      return (true);

    case DUCKDB_TYPE_UTINYINT:
      cell.m_integer = static_cast<uint8_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_INTEGER:
      cell.m_integer = static_cast<int32_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_UINTEGER:
      cell.m_integer = static_cast<uint32_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_BIGINT:
      cell.m_integer = static_cast<int64_t *>(data)[row];

      return (true);

    case DUCKDB_TYPE_UBIGINT:
      cell.m_integer = static_cast<int64_t>(static_cast<uint64_t *>(data)[row]);

      return (true);

    case DUCKDB_TYPE_VARCHAR: {
      duckdb_string_t *s = &static_cast<duckdb_string_t *>(data)[row];
      cell.m_string.assign(duckdb_string_t_data(s), duckdb_string_t_length(*s));

      return (true);
    }

    default:
      return (false);
  }
}

/** Decode one chunk while its caller retains the result and native buffers. */
static db_status_t pt_duckdb_read_chunk(duckdb_result &result,
                                        duckdb_data_chunk chunk,
                                        pt_duckdb_rows_t &rows) {
  const idx_t n_cols = duckdb_column_count(&result);
  const idx_t n = duckdb_data_chunk_get_size(chunk);

  for (idx_t r = 0; r < n; ++r) {
    std::vector<pt_duckdb_cell_t> row(n_cols);

    for (idx_t c = 0; c < n_cols; ++c) {
      duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, c);
      if (!pt_duckdb_read_cell(vector, duckdb_column_type(&result, c), r,
                               row[c])) {
        return (db_status_t(DB_CORRUPTION,
                            "unexpected column type in internal query"));
      }
    }

    rows.push_back(std::move(row));
  }

  return (DB_SUCCESS);
}

/** Collect rows, releasing each chunk before fetching the next one. */
static db_status_t pt_duckdb_read_result(duckdb_result &result,
                                         pt_duckdb_rows_t &rows) {
  switch (duckdb_result_return_type(result)) {
    case DUCKDB_RESULT_TYPE_NOTHING:
    case DUCKDB_RESULT_TYPE_CHANGED_ROWS:
      return (DB_SUCCESS);

    case DUCKDB_RESULT_TYPE_QUERY_RESULT:
      break;

    default:
      return (db_status_t(DB_ERROR, "unexpected DuckDB result type"));
  }

  for (;;) {
    duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
    if (chunk == nullptr) {
      const char *message = duckdb_result_error(&result);

      return (
          message == nullptr
              ? db_status_t()
              : pt_duckdb_error(duckdb_result_error_type(&result), message));
    }

    const db_status_t err = pt_duckdb_read_chunk(result, chunk, rows);
    duckdb_destroy_data_chunk(&chunk);
    if (!err.ok()) {
      return (err);
    }
  }
}

/** Execute SQL, copy any error, then release the result on either outcome. */
static db_status_t pt_duckdb_exec_low(duckdb_connection conn,
                                      const std::string &sql,
                                      pt_duckdb_rows_t *rows) {
  if (rows != nullptr) {
    rows->clear();
  }

  duckdb_result result{};
  db_status_t status;
  if (duckdb_query(conn, sql.c_str(), &result) != DuckDBSuccess) {
    status = pt_duckdb_error(duckdb_result_error_type(&result),
                             duckdb_result_error(&result));
  } else if (rows != nullptr) {
    status = pt_duckdb_read_result(result, *rows);
    if (!status.ok()) {
      rows->clear();
    }
  }

  duckdb_destroy_result(&result);

  return (status);
}

/** Execute SQL and discard its row result. */
db_status_t pt_duckdb_exec(duckdb_connection conn, const std::string &sql) {
  return (pt_duckdb_exec_low(conn, sql, nullptr));
}

/** Run a statement and collect its rows. */
db_status_t pt_duckdb_exec(duckdb_connection conn, const std::string &sql,
                           pt_duckdb_rows_t &rows) {
  return (pt_duckdb_exec_low(conn, sql, &rows));
}

/** Quote an SQL string literal, escaping embedded quotes. */
std::string pt_duckdb_literal(const std::string &name) {
  std::string out = "'";

  for (size_t i = 0; i < name.size(); ++i) {
    out += name[i];
    if (name[i] == '\'') {
      out += '\'';
    }
  }

  return (out + "'");
}

/** Map an internal data type to DuckDB's C type code. */
duckdb_type pt_duckdb_convert_type(data_type_t type) {
  switch (type) {
    case data_type_t::BOOLEAN:
      return (DUCKDB_TYPE_BOOLEAN);

    case data_type_t::BIGINT:
      return (DUCKDB_TYPE_BIGINT);

    case data_type_t::DOUBLE:
      return (DUCKDB_TYPE_DOUBLE);

    case data_type_t::TIMESTAMP:
      return (DUCKDB_TYPE_TIMESTAMP);

    case data_type_t::VARCHAR:
      return (DUCKDB_TYPE_VARCHAR);
  }

  return (DUCKDB_TYPE_VARCHAR);
}
