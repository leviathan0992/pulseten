/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine0drop-t.cc
 *    Tests for dropping tables: stored and unflushed rows go for good, a
 *    name may come back with other columns, queries that started before a
 *    drop, the table list, and writes running during drops.
 *
 *-------------------------------------------------------------------------
 */

#include <atomic>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "test0engine.h"

/** Options: no background thread, one-day partitions.
@return options with background flushing disabled */
static engine_options_t manual() {
  engine_options_t options;
  options.background_flush = false;
  options.partition_days = 1;

  return (options);
}

/** Write n points of table name with v = from .. from + n - 1, one a minute
from T0 + from minutes.
@param[in,out]  engine  database engine
@param[in]      name    logical table name
@param[in]      from    first row or id to generate, inclusive
@param[in]      n       number of points to write
@return the status from engine_write() */
static dberr_t write_rows(engine_t *engine, const char *name, int64_t from,
                          int64_t n) {
  std::vector<ingest_field_t> f(n);
  std::vector<ingest_point_t> p(n);

  for (int64_t i = 0; i < n; ++i) {
    f[i] = field_int("v", from + i);
    p[i] = point(T0 + (from + i) * 60 * 1000000LL, nullptr, 0, &f[i], 1);
  }

  batch_seq_t seq = 0;

  return (engine_write(*engine, name, strlen(name), p.data(), (uint32_t)n, seq,
                       test_error));
}

/** Execute a query and read its single integer result.
@param[in,out]  engine  database engine
@param[in]      sql     SQL statement to check or execute
@return the single BIGINT a query returns, or -1 */
static int64_t query_int(engine_t *engine, const std::string &sql) {
  test_query_t r;
  std::string error;

  if (query_all(*engine, sql, r, error) != DB_SUCCESS || r.rows.size() != 1 ||
      r.rows[0][0].is_null) {
    fprintf(stderr, "query failed: %s: %s\n", sql.c_str(), error.c_str());

    return (-1);
  }

  return (r.rows[0][0].i);
}

/** Flush, checking that it succeeds.
@param[in,out]  engine  database engine */
static void flush(engine_t *engine) {
  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
}

/** Drop a table by its NUL-terminated name.
@param[in,out]  engine  database engine
@param[in]      name    table, column or file name
@return as engine_drop_table() */
static dberr_t drop(engine_t *engine, const char *name) {
  return (engine_drop_table(*engine, name, strlen(name), test_error));
}

/** Count the physical time-partition tables in storage.
@param[in,out]  engine  database engine
@return physical partition tables in the DuckDB file */
static int64_t physical_tables(engine_t *engine) {
  int64_t n = -1;
  CHECK(storage_query_int(*(engine_get_storage(*engine)),
                          "SELECT count(*) FROM information_schema.tables "
                          "WHERE regexp_matches(table_name, '^_pt_t[0-9]+_')",
                          n) == DB_SUCCESS);

  return (n);
}

/** List the logical table names in sorted order.
@param[in,out]  engine  database engine
@return the names of the tables */
static std::vector<std::string> table_names(engine_t *engine) {
  std::vector<schema_t> tables;
  std::vector<std::string> names;

  engine_list_tables(*engine, tables);
  for (size_t i = 0; i < tables.size(); ++i) {
    names.push_back(tables[i].name);
  }

  return (names);
}

/** Dropping removes stored and unflushed rows for good, and the name may come
back with other columns. */
static void test_drop() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = manual();

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* m: three days stored, more rows only in the memtable; k stays. */
  CHECK(write_rows(engine, "m", 0, 3 * 1440) == DB_SUCCESS);
  CHECK(write_rows(engine, "k", 0, 10) == DB_SUCCESS);
  flush(engine);
  CHECK(write_rows(engine, "m", 3 * 1440, 100) == DB_SUCCESS);
  CHECK(physical_tables(engine) == 4);

  CHECK(drop(engine, "M") == DB_SUCCESS);

  std::string error;
  test_query_t r;
  CHECK(query_all(*engine, "SELECT count(*) FROM m", r, error) == DB_ERROR);
  CHECK(query_int(engine, "SELECT count(*) FROM k") == 10);
  CHECK(physical_tables(engine) == 1);
  CHECK(table_names(engine) == std::vector<std::string>{"k"});

  /* Not there any more, or never a table name. */
  CHECK(drop(engine, "m") == DB_NOT_FOUND);
  CHECK(drop(engine, "nothing") == DB_NOT_FOUND);
  CHECK(engine_drop_table(*engine, "_pt_x", 5, error) == DB_INVALID_ARGUMENT);

  /* A table only in the memtable. */
  CHECK(write_rows(engine, "hot", 0, 5) == DB_SUCCESS);
  CHECK(drop(engine, "hot") == DB_SUCCESS);
  CHECK(table_names(engine) == std::vector<std::string>{"k"});

  /* The name comes back with another type. */
  ingest_field_t f[] = {field_str("v", "text")};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "m", 1, &p, 1, seq, test_error) == DB_SUCCESS);
  CHECK(query_int(engine, "SELECT count(*) FROM m WHERE v = 'text'") == 1);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Neither the dropped rows nor the WAL brings anything back. */
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(query_int(engine, "SELECT count(*) FROM m") == 1);
  CHECK(query_int(engine, "SELECT count(*) FROM k") == 10);
  CHECK(query_all(*engine, "SELECT count(*) FROM hot", r, error) == DB_ERROR);
  CHECK(table_names(engine) == (std::vector<std::string>{"k", "m"}));

  /* The new m is stored under a new table id. */
  flush(engine);
  CHECK(physical_tables(engine) == 2);
  CHECK(engine_close(engine) == DB_SUCCESS);

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(query_int(engine, "SELECT count(*) FROM m WHERE v = 'text'") == 1);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** A query started before a drop reads the whole table it started with. */
static void test_open_cursor() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = manual();

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  CHECK(write_rows(engine, "m", 0, 4000) == DB_SUCCESS);
  flush(engine);
  CHECK(write_rows(engine, "m", 4000, 1000) == DB_SUCCESS);

  /* A query started before the drop reads the whole table it started
  with, while the name is used again. */
  engine_cursor_t *cursor = nullptr;
  CHECK(engine_cursor_open(*engine, "SELECT v FROM m", false, &cursor,
                           test_error) == DB_SUCCESS);

  CHECK(drop(engine, "m") == DB_SUCCESS);
  CHECK(write_rows(engine, "m", 0, 7) == DB_SUCCESS);
  flush(engine);

  size_t rows = 0;
  query_rows_t chunk;
  std::string error;

  for (;;) {
    const dberr_t err = engine_cursor_fetch(*cursor, chunk, error);
    CHECK(err == DB_SUCCESS);
    if (err != DB_SUCCESS || chunk.empty()) {
      if (err != DB_SUCCESS) {
        fprintf(stderr, "fetch: %s\n", error.c_str());
      }
      break;
    }
    rows += chunk.size();
  }

  CHECK(rows == 5000);
  engine_cursor_close(cursor);

  CHECK(query_int(engine, "SELECT count(*) FROM m") == 7);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Writers keep writing to k and m while m is dropped again and again. */
static void test_concurrent_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = manual();
  options.background_flush = true;
  options.flush_interval_ms = 5;

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  std::atomic<bool> stop{false};
  std::atomic<int64_t> k_written{0};
  std::atomic<int64_t> failures{0};

  std::thread k_writer([&] {
    for (int64_t i = 0; !stop.load(); ++i) {
      if (write_rows(engine, "k", i % 1000, 3) == DB_SUCCESS) {
        k_written += 3;
      } else {
        failures++;
      }
    }
  });

  std::thread m_writer([&] {
    for (int64_t i = 0; !stop.load(); ++i) {
      if (write_rows(engine, "m", i % 1000, 2) != DB_SUCCESS) {
        failures++;
      }
    }
  });

  int dropped = 0;
  for (int i = 0; i < 20; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const dberr_t err = drop(engine, "m");
    CHECK(err == DB_SUCCESS || err == DB_NOT_FOUND);
    dropped += err == DB_SUCCESS;
  }

  stop = true;
  k_writer.join();
  m_writer.join();

  CHECK(failures == 0);
  CHECK(dropped > 0);

  engine_stats_t stats;
  engine_get_stats(*engine, stats);
  CHECK(stats.background_errors == 0);

  /* Every write to k is there; m holds what came after the last drop. */
  CHECK(query_int(engine, "SELECT count(*) FROM k") == k_written.load());
  const int64_t m_rows = query_int(engine, "SELECT count(*) FROM m");
  CHECK(m_rows >= 0);
  memtable_stats_t memory;
  engine_get_memtable_stats(*engine, memory);
  CHECK(memory.dropped_tables == 0);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Recovery agrees. */
  options.background_flush = false;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(query_int(engine, "SELECT count(*) FROM k") == k_written.load());
  CHECK(query_int(engine, "SELECT count(*) FROM m") == m_rows);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Rejected plans must release their schema reference before a drop. */
static void test_rejected_plan_releases_schema() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = manual();
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(write_rows(engine, "m", 0, 1) == DB_SUCCESS);
  ingest_field_t f = field_str("v", "wrong type");
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "m", 1, &p, 1, seq, test_error) ==
        DB_SCHEMA_CONFLICT);
  CHECK(drop(engine, "m") == DB_SUCCESS);
  memtable_stats_t stats;
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.dropped_tables == 0 && stats.garbage_writes == 0);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_drop();
  test_rejected_plan_releases_schema();
  test_open_cursor();
  test_concurrent_writes();

  return (test_result("engine0drop-t"));
}
