/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_engine.h
 *    Helpers for engine tests: unique-id writes, reading them back from
 *    storage and memtable together, and whole query results.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_engine_h
#define test_engine_h

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "batch.h"
#include "engine.h"
#include "ingest.h"
#include "memtable.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "test_memtable.h"
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

/** Read ids from both analytical storage and the unflushed memtable.
@return every id of table "k" in storage, then in the memtable */
static inline std::vector<int64_t> all_ids(engine_t &engine) {
  std::vector<int64_t> ids;

  /* Stored: union of the partitions of table k. */
  storage_t &st = engine_storage(engine);
  const storage_catalog_t &state = storage_catalog(st);
  std::map<std::string, storage_table_t>::const_iterator t =
      state.m_tables.find("k");

  if (t != state.m_tables.end()) {
    const col_def_t *id_col =
        schema_find_col(t->second.m_versions.back().m_cols, "id");
    std::string sql;

    std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
        p;
    for (p = state.m_partitions.begin(); p != state.m_partitions.end(); ++p) {
      if (p->first.first != t->second.m_table_id) {
        continue;
      }
      if (!sql.empty()) {
        sql += " UNION ALL ";
      }
      sql += "SELECT c" + std::to_string(id_col->m_id) + " FROM " +
             p->second.m_phys_name;
    }

    if (!sql.empty()) {
      CHECK(storage_ints(st, sql, ids));
    }
  }

  /* Not yet stored. */
  memtable_t &mt = engine_memtable(engine);
  memtable_snapshot_t snap;
  snapshot_all(mt, snap);

  if (snap.m_tables.count("k") == 1) {
    const memtable_table_view_t &view = snap.m_tables["k"];
    const col_def_t *id_col = schema_find_col(view.m_schema->m_cols, "id");

    for (size_t i = 0; i < view.m_batches.size(); ++i) {
      const batch_col_t &col = view.m_batches[i]->m_cols[id_col->m_id];
      for (uint32_t r = 0; r < view.m_batches[i]->m_n_rows; ++r) {
        ids.push_back(col.m_ints[r]);
      }
    }
  }

  memtable_snapshot_release(mt, snap);

  return (ids);
}

#endif
