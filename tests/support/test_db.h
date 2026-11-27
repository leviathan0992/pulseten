/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_db.h
 *    Helpers for db tests: standard options, unique-id writes and whole
 *    query results.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_db_h
#define test_db_h

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "db.h"
#include "ingest.h"
#include "query.h"
#include "store.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"

/** Options under test control: the background flusher never wakes within a
test, and no memtable limit holds writers back, so flushes happen only where
the test asks for them.
@return immutable options shared by every caller */
static inline const db::options_t *deferred_flush_options() {
  static const db::options_t options = [] {
    db::options_t o;
    o.m_flush_interval_ms = 24 * 60 * 60 * 1000;
    o.m_memtable_limit_bytes = 0;

    return (o);
  }();

  return (&options);
}

/** deferred_flush_options() with one-day partitions and a retention period.
@param[in]  retention_days  retention period in days; 0 keeps all data
@return options for the caller to adjust further */
static inline db::options_t daily_partition_options(
    uint32_t retention_days = 0) {
  db::options_t options = *deferred_flush_options();
  options.m_partition_duration_hours = 24;
  options.m_retention_hours = retention_days * 24;

  return (options);
}

/** A query result read whole. */
struct test_query_t {
  /** Result columns. */
  std::vector<query_column_t> m_columns;

  /** rows[r][c] is the value of column c in row r. */
  std::vector<std::vector<query_value_t>> m_rows;
};

/** Run a SELECT through a cursor and read every row.
@param[out]  result  the rows; empty on failure
@return as db::open_cursor(), then as db_cursor::fetch() */
static inline db_status_t query_all(db &database, const std::string &sql,
                                    test_query_t &result) {
  result = test_query_t();

  db_cursor *cursor = nullptr;

  db_status_t err = database.open_cursor(sql, false, &cursor);
  if (!err.ok()) {
    return (err);
  }

  result.m_columns = cursor->columns();

  query_rows_t rows;

  for (;;) {
    err = cursor->fetch(rows);
    if (!err.ok() || rows.empty()) {
      break;
    }

    for (size_t r = 0; r < rows.size(); ++r) {
      result.m_rows.push_back(std::move(rows[r]));
    }
  }

  db_cursor::destroy(cursor);

  if (!err.ok()) {
    result = test_query_t();
  }

  return (err);
}

/** Run a query that returns one non-NULL integer; a failure is recorded as a
test failure.
@return the integer, or -1 on failure */
static inline int64_t query_int(db &database, const std::string &sql) {
  test_query_t r;
  const db_status_t err = query_all(database, sql, r);

  if (!err.ok() || r.m_rows.size() != 1 || r.m_rows[0].size() != 1 ||
      r.m_rows[0][0].m_is_null) {
    fprintf(stderr, "query failed: %s\n  %s\n", sql.c_str(),
            err.message().c_str());
    CHECK(false);

    return (-1);
  }

  return (r.m_rows[0][0].m_integer);
}

/** Run SQL on the storage's database, returning one integer column.
@return true on success */
static inline bool store_ints(store &st, const std::string &sql,
                              std::vector<int64_t> &values) {
  duckdb_connection conn = nullptr;

  if (duckdb_connect(st.database(), &conn) != DuckDBSuccess) {
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

/** Read the current flushed boundary after a test-controlled flush. */
static inline batch_seq_t test_flushed_seq(db &database) {
  db::stats_t stats;
  database.stats(stats);

  return (stats.m_flushed_seq);
}

/** Write one point to table "k" with field id = v.
@return the status from db::write() */
static inline db_status_t write_id(db &database, int64_t v) {
  ingest_field_t f[] = {field_int("id", v), field_str("pad", "pt")};

  ingest_point_t p = point(TEST_BASE_TIME_US + v % 1000, nullptr, 0, f, 2);

  return (database.write("k", 1, &p, 1));
}

/** Query every id, including stored and unflushed rows.
@return every id of table "k", or an empty vector if the table is absent */
static inline std::vector<int64_t> all_ids(db &database) {
  std::vector<int64_t> ids;
  std::vector<db_table_info_t> tables;
  database.collect_tables(tables);

  for (const db_table_info_t &table : tables) {
    if (table.m_name != "k" || table.m_schema == nullptr) {
      continue;
    }

    test_query_t result;
    CHECK(query_all(database, "SELECT id FROM k", result).ok());

    for (const std::vector<query_value_t> &row : result.m_rows) {
      CHECK(row.size() == 1 && !row[0].m_is_null);

      if (row.size() == 1 && !row[0].m_is_null) {
        ids.push_back(row[0].m_integer);
      }
    }

    break;
  }

  return (ids);
}

/** Reopen with a long background interval and return every recovered id.
@return ids read from storage and replayed memtable batches */
static inline std::vector<int64_t> reopen_ids(const test_dir_t &dir) {
  db *database = nullptr;

  std::vector<int64_t> ids;

  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  if (database != nullptr) {
    ids = all_ids(*database);

    db_close(database);
  }

  return (ids);
}

#endif /* test_db_h */
