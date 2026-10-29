/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test0engine.h
 *    Helpers for engine tests: unique-id writes, reading them back from
 *    storage and memtable together, and whole query results.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test0engine_h
#define test0engine_h

#include <map>
#include <string>
#include <vector>

#include "engine0engine.h"
#include "test0memtable.h"
#include "test0point.h"
#include "test0util.h"

/** A query result read whole. */
struct test_query_t {
  /** Result columns. */
  std::vector<query_column_t> columns;

  /** rows[r][c] is the value of column c in row r. */
  std::vector<std::vector<query_value_t>> rows;
};

/** Run a SELECT through a cursor and read every row.
@param[in,out]  engine  database engine
@param[in]      sql     a single SELECT
@param[out]     result  the rows; empty on failure
@param[out]     error   reason for failure
@return as engine_cursor_open(), then as engine_cursor_fetch() */
static inline dberr_t query_all(engine_t &engine, const std::string &sql,
                                test_query_t &result, std::string &error) {
  result = test_query_t();

  engine_cursor_t *cursor = nullptr;

  dberr_t err = engine_cursor_open(engine, sql, false, &cursor, error);
  if (err != DB_SUCCESS) {
    return (err);
  }

  result.columns = engine_cursor_columns(*cursor);

  query_rows_t rows;

  for (;;) {
    err = engine_cursor_fetch(*cursor, rows, error);
    if (err != DB_SUCCESS || rows.empty()) {
      break;
    }

    for (size_t r = 0; r < rows.size(); ++r) {
      result.rows.push_back(std::move(rows[r]));
    }
  }

  engine_cursor_close(cursor);

  if (err != DB_SUCCESS) {
    result = test_query_t();
  }

  return (err);
}

/** Run SQL on the storage's database, returning one integer column.
@param[in,out]  st      analytical storage
@param[in]      sql     the query
@param[out]     values  integers from the result column
@return true on success */
static inline bool storage_ints(storage_t &st, const std::string &sql,
                                std::vector<int64_t> &values) {
  duckdb_connection conn = nullptr;

  if (duckdb_connect(storage_get_database(st), &conn) != DuckDBSuccess) {
    return (false);
  }

  duckdb_result result;
  const bool ok = duckdb_query(conn, sql.c_str(), &result) == DuckDBSuccess &&
                  duckdb_column_count(&result) == 1;

  for (idx_t r = 0; ok && r < duckdb_row_count(&result); ++r) {
    values.push_back(duckdb_value_int64(&result, 0, r));
  }

  duckdb_destroy_result(&result);
  duckdb_disconnect(&conn);

  return (ok);
}

/** Write one point to table "k" with field id = v.
@param[in,out]  engine  database engine
@param[in]      v       field value
@return the status from engine_write() */
static inline dberr_t write_id(engine_t *engine, int64_t v) {
  ingest_field_t f[] = {field_int("id", v), field_str("pad", "pt")};
  ingest_point_t p = point(T0 + v % 1000, nullptr, 0, f, 2);
  batch_seq_t seq = 0;

  return (engine_write(*engine, "k", 1, &p, 1, seq, test_error));
}

/** Read ids from both analytical storage and the unflushed memtable.
@param[in,out]  engine  database engine
@return every id of table "k" in storage, then in the memtable */
static inline std::vector<int64_t> all_ids(engine_t *engine) {
  std::vector<int64_t> ids;

  /* Stored: union of the partitions of table k. */
  storage_t *st = engine_get_storage(*engine);
  const storage_state_t *state = storage_get_state(*st);
  std::map<std::string, storage_table_t>::const_iterator t =
      state->tables.find("k");

  if (t != state->tables.end()) {
    const col_def_t *id_col =
        schema_find_col(t->second.versions.back().cols, "id");
    std::string sql;

    std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
        p;
    for (p = state->partitions.begin(); p != state->partitions.end(); ++p) {
      if (p->first.first != t->second.table_id) {
        continue;
      }
      if (!sql.empty()) {
        sql += " UNION ALL ";
      }
      sql += "SELECT c" + std::to_string(id_col->id) + " FROM " +
             p->second.phys_name;
    }

    if (!sql.empty()) {
      CHECK(storage_ints(*st, sql, ids));
    }
  }

  /* Not yet stored. */
  memtable_t &mt = *engine_get_memtable(*engine);
  memtable_snapshot_t snap;
  snapshot_all(mt, snap);

  if (snap.tables.count("k") == 1) {
    const memtable_table_view_t &view = snap.tables["k"];
    const col_def_t *id_col = schema_find_col(view.schema->cols, "id");

    for (size_t i = 0; i < view.batches.size(); ++i) {
      const batch_col_t &col = view.batches[i]->cols[id_col->id];
      for (uint32_t r = 0; r < view.batches[i]->n_rows; ++r) {
        ids.push_back(col.ints[r]);
      }
    }
  }

  memtable_snapshot_release(mt, snap);

  return (ids);
}

#endif
