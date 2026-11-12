/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_layout_bench-t.cc
 *    Layout benchmark setup and timing failures must stop the run.
 *
 *-------------------------------------------------------------------------
 */

#include <cstddef>
#include <vector>

#include <duckdb.h>

#include "test_point.h"
#include "test_util.h"

/* Exercise the benchmark's real private functions and command entry point. */
#define main query_layout_benchmark_main
#include "query_layout-bench.cc"
#undef main

/** Table creation, data insertion and view creation each propagate failure. */
static void test_partition_failures() {
  duckdb_database database = nullptr;
  duckdb_connection conn = nullptr;
  CHECK(duckdb_open(nullptr, &database) == DuckDBSuccess);
  CHECK(duckdb_connect(database, &conn) == DuckDBSuccess);
  bench_data_t data;
  data.m_days = 2;

  CHECK(bench_exec(conn, "CREATE TABLE conflict_0 (v INTEGER)"));
  CHECK(bench_partition(conn, data, 1, "conflict") == -1);
  CHECK(bench_partition(conn, data, 1, "missing") == -1);
  CHECK(bench_exec(conn,
                   "CREATE TABLE metrics (time TIMESTAMP, host VARCHAR, "
                   "cpu DOUBLE, mem DOUBLE)"));
  CHECK(bench_exec(conn, "CREATE VIEW existing AS SELECT * FROM metrics"));
  CHECK(bench_partition(conn, data, 1, "existing") == -1);
  CHECK(bench_partition(conn, data, 1, "valid") == 2);

  duckdb_disconnect(&conn);
  duckdb_close(&database);

  return;
}

/** Both backends reject invalid timed queries and allow a clean retry. */
static void test_query_failures() {
  test_dir_t dir;
  bench_data_t data;
  data.m_days = 2;
  data.m_n = 20;
  data.m_step_us = 2.0 * DAY / 20;
  std::vector<bench_query_t> queries(1);
  queries[0].m_label = "invalid query";
  queries[0].m_sql = "SELECT missing_column FROM metrics";
  std::vector<double> times;
  int partitions = 0;

  CHECK(!bench_pulseten(dir.m_path, data, 1, queries, times, partitions));
  CHECK(times.empty());
  queries[0].m_sql = "SELECT count(*) FROM metrics";
  CHECK(bench_pulseten(dir.m_path, data, 1, queries, times, partitions));
  CHECK(times.size() == 1 && times[0] >= 0);
  CHECK(partitions > 0);

  queries[0].m_sql = "SELECT missing_column FROM metrics";
  std::vector<std::vector<double>> native_times;
  std::vector<int> native_parts;
  CHECK(
      !bench_layouts(dir.m_path, data, 1, queries, native_times, native_parts));
  CHECK(native_times.empty());
  queries[0].m_sql = "SELECT count(*) FROM metrics";
  native_parts.clear();
  CHECK(
      bench_layouts(dir.m_path, data, 1, queries, native_times, native_parts));
  CHECK(native_times.size() == 4 && native_parts.size() == 4);
  for (size_t i = 0; i < native_times.size(); ++i) {
    CHECK(native_times[i].size() == 1 && native_times[i][0] >= 0);
  }

  return;
}

/** Invalid sizes, repeats, flags and products fail before dataset generation.
 */
static void test_bad_arguments() {
  test_dir_t dir;
  char name[] = "query_layout-bench";
  char days[] = "2";
  char rows[] = "10";
  char runs[] = "1";
  char zero[] = "0";
  char bad_flag[] = "2";
  char huge[] = "9223372036854775807";
  char *args[] = {name, dir.m_path, days, zero, runs, runs};
  CHECK(query_layout_benchmark_main(6, args) == 2);
  args[3] = rows;
  args[4] = zero;
  CHECK(query_layout_benchmark_main(6, args) == 2);
  args[4] = runs;
  args[5] = bad_flag;
  CHECK(query_layout_benchmark_main(6, args) == 2);
  args[5] = runs;
  args[3] = huge;
  CHECK(query_layout_benchmark_main(6, args) == 2);

  return;
}

/** Run the layout benchmark error-path regressions.
@return 0 if every check passed */
int main() {
  test_partition_failures();
  test_query_failures();
  test_bad_arguments();

  return (test_result("query_layout_bench-t"));
}
