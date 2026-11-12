/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_bench-t.cc
 *    Query benchmark failures must not publish successful measurements.
 *
 *-------------------------------------------------------------------------
 */

#include <cstdio>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "test_util.h"

/* Exercise the benchmark's real setup and timing paths in this translation
unit, while keeping its command entry point available for argument checks. */
#define main query_benchmark_main
#include "query-bench.cc"
#undef main

/** Invalid queries fail and release owned databases; a valid retry succeeds. */
static void test_query_failures() {
  test_dir_t dir;
  std::vector<bench_query_t> queries(1);
  queries[0].m_label = "invalid query";
  queries[0].m_sql = "SELECT missing_column FROM metrics";
  std::vector<double> times;

  CHECK(!bench_db(dir.m_path, 100, 1, 100, queries, times));
  CHECK(times.empty());
  queries[0].m_sql = "SELECT count(*) FROM metrics";
  CHECK(bench_db(dir.m_path, 100, 1, 100, queries, times));
  CHECK(times.size() == 1 && times[0] >= 0);

  queries[0].m_sql = "SELECT missing_column FROM metrics";
  times.clear();
  std::vector<double> day_times;
  CHECK(!bench_native(dir.m_path, 100, 1, queries, times, day_times));
  CHECK(times.empty() && day_times.empty());
  queries[0].m_sql = "SELECT count(*) FROM metrics";
  CHECK(bench_native(dir.m_path, 100, 1, queries, times, day_times));
  CHECK(times.size() == 1 && times[0] >= 0);
  CHECK(day_times.size() == 1 && day_times[0] >= 0);

  return;
}

/** Rejected arguments do not reach timing loops or allocate huge data sets. */
static void test_bad_arguments() {
  test_dir_t dir;
  char name[] = "query-bench";
  char rows[] = "100";
  char runs[] = "1";
  char zero[] = "0";
  char negative[] = "-1";
  char overflow[] = "18446744073709551616";
  char trailing[] = "100x";
  char *args[] = {name, dir.m_path, rows, zero};
  CHECK(query_benchmark_main(4, args) == 2);
  args[3] = runs;
  args[2] = zero;
  CHECK(query_benchmark_main(4, args) == 2);
  args[2] = negative;
  CHECK(query_benchmark_main(4, args) == 2);
  args[2] = overflow;
  CHECK(query_benchmark_main(4, args) == 2);
  args[2] = trailing;
  CHECK(query_benchmark_main(4, args) == 2);

  return;
}

/** An existing directory is not owned by a failed benchmark invocation. */
static void test_existing_directory() {
  test_dir_t dir;
  const std::string native = std::string(dir.m_path) + "/native";
  CHECK(mkdir(native.c_str(), 0755) == 0);
  const std::string sentinel = native + "/keep";
  FILE *file = fopen(sentinel.c_str(), "wb");
  CHECK(file != nullptr);
  if (file != nullptr) {
    CHECK(fputs("preserve", file) >= 0);
    CHECK(fclose(file) == 0);
  }

  std::vector<bench_query_t> queries(1);
  queries[0].m_sql = "SELECT 1";
  std::vector<double> times;
  std::vector<double> day_times;
  CHECK(!bench_native(dir.m_path, 100, 1, queries, times, day_times));
  CHECK(access(sentinel.c_str(), F_OK) == 0);
  test_remove_dir(native.c_str());

  return;
}

/** Run the benchmark error-path regressions.
@return 0 if every check passed */
int main() {
  test_query_failures();
  test_bad_arguments();
  test_existing_directory();

  return (test_result("query_bench-t"));
}
