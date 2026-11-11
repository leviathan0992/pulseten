/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_engine.h
 *    Helpers for engine tests: unique-id writes and whole query results.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_engine_h
#define test_engine_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "engine.h"
#include "ingest.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"

/** A query result read whole. */
struct test_query_t {
  /** Result columns. */
  std::vector<query_column_t> m_columns;

  /** rows[r][c] is the value of column c in row r. */
  std::vector<std::vector<query_value_t>> m_rows;
};

/** Run a SELECT through a cursor and read every row.
@param[out]  result  the rows; empty on failure
@return as engine_cursor_open(), then as engine_cursor_fetch() */
static inline db_status_t query_all(engine_t &engine, const std::string &sql,
                                    test_query_t &result) {
  result = test_query_t();

  engine_cursor_t *cursor = nullptr;

  db_status_t err = engine_cursor_open(engine, sql, false, &cursor);
  if (!err.ok()) {
    return (err);
  }

  result.m_columns = engine_cursor_columns(*cursor);

  query_rows_t rows;

  for (;;) {
    err = engine_cursor_fetch(*cursor, rows);
    if (!err.ok() || rows.empty()) {
      break;
    }

    for (size_t r = 0; r < rows.size(); ++r) {
      result.m_rows.push_back(std::move(rows[r]));
    }
  }

  engine_cursor_close(cursor);

  if (!err.ok()) {
    result = test_query_t();
  }

  return (err);
}

/** Run SQL on the storage's database, returning one integer column.
@return true on success */
static inline bool storage_ints(storage_t &st, const std::string &sql,
                                std::vector<int64_t> &values) {
  duckdb_connection conn = nullptr;

  if (duckdb_connect(storage_database(st), &conn) != DuckDBSuccess) {
    return (false);
  }

  duckdb_result result;
  if (duckdb_query(conn, sql.c_str(), &result) != DuckDBSuccess) {
    duckdb_destroy_result(&result);
    duckdb_disconnect(&conn);

    return (false);
  }
  if (duckdb_column_count(&result) != 1) {
    duckdb_destroy_result(&result);
    duckdb_disconnect(&conn);

    return (false);
  }

  for (idx_t r = 0; r < duckdb_row_count(&result); ++r) {
    values.push_back(duckdb_value_int64(&result, 0, r));
  }

  duckdb_destroy_result(&result);
  duckdb_disconnect(&conn);

  return (true);
}

/** Write one point to table "k" with field id = v.
@return the status from engine_write() */
static inline db_status_t write_id(engine_t &engine, int64_t v) {
  ingest_field_t f[] = {field_int("id", v), field_str("pad", "pt")};
  ingest_point_t p = point(T0 + v % 1000, nullptr, 0, f, 2);
  batch_seq_t seq = 0;

  return (engine_write(engine, "k", 1, &p, 1, seq));
}

/** Query every id, including stored and unflushed rows.
@return every id of table "k", or an empty vector if the table is absent */
static inline std::vector<int64_t> all_ids(engine_t &engine) {
  std::vector<int64_t> ids;
  std::vector<schema_t> schemas;
  engine_copy_schemas(engine, schemas);

  for (const schema_t &schema : schemas) {
    if (schema.m_name != "k") {
      continue;
    }

    test_query_t result;
    CHECK(query_all(engine, "SELECT id FROM k", result).ok());
    for (const std::vector<query_value_t> &row : result.m_rows) {
      CHECK(row.size() == 1 && !row[0].m_is_null);
      if (row.size() == 1 && !row[0].m_is_null) {
        ids.push_back(row[0].m_i);
      }
    }
    break;
  }

  return (ids);
}

#endif /* test_engine_h */
