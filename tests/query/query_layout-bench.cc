/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_layout-bench.cc
 *    Query latency by partition granularity.
 *
 * Generates rows of metrics (time, host, cpu, mem) spread evenly over a
 * number of UTC days, then stores them in plain DuckDB four ways: one
 * table, and partitions of 30, 7 and 1 days, each partition a table with
 * a time CHECK constraint, behind a UNION ALL view. Optionally the same rows
 * go through PulseTen with its default partition span, fully flushed. Every
 * query runs once to warm up, then the median of the timed runs is reported in
 * ms.
 *
 * Usage: query_layout-bench <dir> <days> <rows_per_day> [runs] [pulseten]
 * Pass 1 as pulseten to include PulseTen.
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <time.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <duckdb.h>

#include "engine.h"
#include "ingest.h"
#include "storage.h"
#include "test_engine.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"

/** Read the monotonic clock in nanoseconds.
@return the monotonic clock in nanoseconds */
static uint64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
          static_cast<uint64_t>(ts.tv_nsec));
}

/** Format a timestamp as an SQL TIMESTAMP literal, to the second.
@param[in]  us  UTC timestamp in microseconds since 1970-01-01
@return a SQL TIMESTAMP literal of a time, to the second */
static std::string timestamp_literal(int64_t us) {
  const time_t secs = static_cast<time_t>(us / 1000000);
  struct tm tm;
  gmtime_r(&secs, &tm);

  char buf[64];
  strftime(buf, sizeof(buf), "TIMESTAMP '%Y-%m-%d %H:%M:%S'", &tm);

  return (buf);
}

/** The data set: n rows over days days from T0. */
struct bench_data_t {
  /** Days the rows span. */
  int m_days{0};

  /** Rows. */
  uint64_t m_n{0};

  /** Microseconds between consecutive rows. */
  double m_step_us{0};
};

struct bench_query_t {
  /** Name printed in the report. */
  const char *m_label;

  /** SQL with "FROM metrics", replaced per layout. */
  std::string m_sql;
};

/** Build the SQL queries used by this benchmark.
@return the queries of the benchmark over a data set */
static std::vector<bench_query_t> bench_queries(const bench_data_t &d) {
  const int64_t end = T0 + static_cast<int64_t>(d.m_days) * DAY;
  const std::string where = " WHERE time >= ";
  std::vector<bench_query_t> q(7);

  q[0].m_label = "count/avg/max, all rows";
  q[0].m_sql = "SELECT count(*), avg(cpu), max(mem) FROM metrics";

  q[1].m_label = "GROUP BY host, all rows";
  q[1].m_sql = "SELECT host, avg(cpu) FROM metrics GROUP BY host";

  q[2].m_label = "aggregate, newest 7 days";
  q[2].m_sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
               timestamp_literal(end - 7 * DAY);

  q[3].m_label = "aggregate, newest day";
  q[3].m_sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
               timestamp_literal(end - DAY);

  q[4].m_label = "aggregate, newest hour";
  q[4].m_sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
               timestamp_literal(end - 3600LL * 1000000);

  q[5].m_label = "aggregate, oldest day";
  q[5].m_sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
               timestamp_literal(T0) + " AND time < " +
               timestamp_literal(T0 + DAY);

  q[6].m_label = "one host, newest 100 rows";
  q[6].m_sql =
      "SELECT time, cpu FROM metrics WHERE host = 'host-7' "
      "ORDER BY time DESC LIMIT 100";

  return (q);
}

/** Run a statement, printing DuckDB's error if it fails.
@return true on success */
static bool bench_exec(duckdb_connection conn, const std::string &sql) {
  duckdb_result result;
  const bool ok = duckdb_query(conn, sql.c_str(), &result) == DuckDBSuccess;

  if (!ok) {
    fprintf(stderr, "%s\n  %s\n", sql.substr(0, 200).c_str(),
            duckdb_result_error(&result));
  }

  duckdb_destroy_result(&result);

  return (ok);
}

/** Median of the timed runs, reading every chunk of the result.
@return median execution-and-fetch time in milliseconds; -1 on query failure */
static double bench_time_native(duckdb_connection conn, const std::string &sql,
                                int runs) {
  std::vector<double> ms;

  for (int i = 0; i <= runs; ++i) {
    duckdb_result result;
    const uint64_t start = now_ns();
    const bool ok = duckdb_query(conn, sql.c_str(), &result) == DuckDBSuccess;
    if (!ok) {
      fprintf(stderr, "native query failed: %s\n  %s\n", sql.c_str(),
              duckdb_result_error(&result));
      duckdb_destroy_result(&result);

      return (-1);
    }

    for (;;) {
      duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
      if (chunk == nullptr) {
        break;
      }
      duckdb_destroy_data_chunk(&chunk);
    }

    const double elapsed = static_cast<double>(now_ns() - start) / 1e6;
    duckdb_destroy_result(&result);

    if (i > 0) {
      ms.push_back(elapsed);
    }
  }

  std::sort(ms.begin(), ms.end());

  return (ms[ms.size() / 2]);
}

/** Rewrite a metrics query to read a selected view.
@return sql reading from view instead of metrics */
static std::string bench_on(const std::string &sql, const std::string &view) {
  std::string out = sql;
  const size_t at = out.find("FROM metrics");
  out.replace(at, strlen("FROM metrics"), "FROM " + view);

  return (out);
}

/** Split metrics into partitions of span_days days behind a view.
@return number of partitions, or -1 after the first SQL failure */
static int bench_partition(duckdb_connection conn, const bench_data_t &d,
                           int span_days, const std::string &view) {
  std::string sql = "CREATE VIEW " + view + " AS ";
  int parts = 0;

  for (int start = 0; start < d.m_days; start += span_days) {
    const std::string table = view + "_" + std::to_string(parts);
    const std::string from =
        timestamp_literal(T0 + static_cast<int64_t>(start) * DAY);
    const std::string to =
        timestamp_literal(T0 + static_cast<int64_t>(start + span_days) * DAY);

    if (!bench_exec(conn, "CREATE TABLE " + table +
                              " (time TIMESTAMP, host VARCHAR, cpu DOUBLE, "
                              "mem DOUBLE, CHECK (time >= " +
                              from + " AND time < " + to + "))")) {
      return (-1);
    }
    if (!bench_exec(conn, "INSERT INTO " + table +
                              " SELECT * FROM metrics WHERE time >= " + from +
                              " AND time < " + to + " ORDER BY time")) {
      return (-1);
    }

    sql += (parts > 0 ? " UNION ALL " : "") + std::string("SELECT * FROM ") +
           table;
    ++parts;
  }

  if (!bench_exec(conn, sql)) {
    return (-1);
  }

  return (parts);
}

/** Generate and time all native layouts on a borrowed connection.
@return true if every setup statement and timed query succeed */
static bool bench_layouts_low(duckdb_connection conn, const bench_data_t &d,
                              int runs,
                              const std::vector<bench_query_t> &queries,
                              std::vector<std::vector<double>> &times,
                              std::vector<int> &parts) {
  char gen[512];
  snprintf(gen, sizeof(gen),
           "CREATE TABLE metrics AS SELECT "
           "make_timestamp(%lld + CAST(i * %.6f AS BIGINT)) AS time, "
           "'host-' || CAST(i %% 100 AS VARCHAR) AS host, "
           "CAST(i %% 1000 AS DOUBLE) / 10 AS cpu, "
           "CAST(i * 7 %% 1000 AS DOUBLE) / 10 AS mem "
           "FROM range(%llu) t(i)",
           static_cast<long long>(T0), d.m_step_us,
           static_cast<unsigned long long>(d.m_n));

  const uint64_t start = now_ns();
  if (!bench_exec(conn, gen)) {
    return (false);
  }

  const int spans[] = {30, 7, 1};
  const char *views[] = {"p30", "p7", "p1"};
  parts.push_back(1);
  for (int i = 0; i < 3; ++i) {
    const int count = bench_partition(conn, d, spans[i], views[i]);
    if (count < 0) {
      return (false);
    }
    parts.push_back(count);
  }

  printf("  plain DuckDB: generated and partitioned in %.1f s\n",
         static_cast<double>(now_ns() - start) / 1e9);
  const char *names[] = {"metrics", "p30", "p7", "p1"};
  for (int l = 0; l < 4; ++l) {
    std::vector<double> values;
    for (size_t q = 0; q < queries.size(); ++q) {
      const double ms =
          bench_time_native(conn, bench_on(queries[q].m_sql, names[l]), runs);
      if (ms < 0) {
        return (false);
      }
      values.push_back(ms);
    }
    times.push_back(values);
  }

  return (true);
}

/** Own the database while generating and timing native layouts.
@return true if every setup statement and timed query succeed */
static bool bench_layouts(const char *base, const bench_data_t &d, int runs,
                          const std::vector<bench_query_t> &queries,
                          std::vector<std::vector<double>> &times,
                          std::vector<int> &parts) {
  const std::string dir = std::string(base) + "/layouts";
  if (mkdir(dir.c_str(), 0755) != 0) {
    return (false);
  }
  const std::string path = dir + "/layouts.duckdb";
  duckdb_database db = nullptr;
  if (duckdb_open(path.c_str(), &db) != DuckDBSuccess) {
    duckdb_close(&db);
    test_remove_dir(dir.c_str());

    return (false);
  }
  duckdb_connection conn = nullptr;
  if (duckdb_connect(db, &conn) != DuckDBSuccess) {
    duckdb_disconnect(&conn);
    duckdb_close(&db);
    test_remove_dir(dir.c_str());

    return (false);
  }

  const bool ok = bench_layouts_low(conn, d, runs, queries, times, parts);
  duckdb_disconnect(&conn);
  duckdb_close(&db);
  test_remove_dir(dir.c_str());

  return (ok);
}

/** Load, flush and time PulseTen using an engine owned by the caller.
@param[out]  n_parts  number of physical partitions after flushing
@return true if loading, flushing and every timed query succeed */
static bool bench_pulseten_low(engine_t &engine, const bench_data_t &d,
                               int runs,
                               const std::vector<bench_query_t> &queries,
                               std::vector<double> &times, int &n_parts) {
  db_status_t status;

  std::vector<std::string> hosts;
  for (int h = 0; h < 100; ++h) {
    hosts.push_back("host-" + std::to_string(h));
  }

  const uint32_t batch = 10000;
  std::vector<ingest_tag_t> tags(batch);
  std::vector<ingest_field_t> fields(2 * batch);
  std::vector<ingest_point_t> points(batch);
  const uint64_t start = now_ns();

  for (uint64_t from = 0; from < d.m_n; from += batch) {
    const uint32_t count =
        static_cast<uint32_t>(d.m_n - from < batch ? d.m_n - from : batch);
    for (uint32_t k = 0; k < count; ++k) {
      const uint64_t i = from + k;
      tags[k] = tag("host", hosts[i % 100].c_str());
      fields[2 * k] = field_double("cpu", static_cast<double>(i % 1000) / 10);
      fields[2 * k + 1] =
          field_double("mem", static_cast<double>(i * 7 % 1000) / 10);
      points[k] =
          point(T0 + static_cast<int64_t>(static_cast<double>(i) * d.m_step_us),
                &tags[k], 1, &fields[2 * k], 2);
    }

    batch_seq_t seq = 0;
    status = engine_write(engine, "metrics", 7, points.data(), count, seq);
    if (!status.ok()) {
      fprintf(stderr, "PulseTen load: %s\n", status.message().c_str());

      return (false);
    }

    /* Flush every million rows to bound the memtable. */
    if ((from / batch) % 100 == 99) {
      engine_flush_info_t info;
      status = engine_flush(engine, info);
      if (!status.ok()) {
        fprintf(stderr, "PulseTen flush: %s\n", status.message().c_str());

        return (false);
      }
    }
  }

  engine_flush_info_t info;
  status = engine_flush(engine, info);
  if (!status.ok()) {
    fprintf(stderr, "PulseTen flush: %s\n", status.message().c_str());

    return (false);
  }
  printf("  PulseTen: loaded and flushed in %.1f s\n",
         static_cast<double>(now_ns() - start) / 1e9);
  n_parts = static_cast<int>(
      storage_catalog(engine_storage(engine)).m_partitions.size());

  for (size_t q = 0; q < queries.size(); ++q) {
    std::vector<double> ms;
    for (int i = 0; i <= runs; ++i) {
      test_query_t result;
      const uint64_t t0 = now_ns();
      status = query_all(engine, queries[q].m_sql, result);
      if (!status.ok()) {
        fprintf(stderr, "PulseTen query: %s\n", status.message().c_str());

        return (false);
      }
      if (i > 0) {
        ms.push_back(static_cast<double>(now_ns() - t0) / 1e6);
      }
    }
    std::sort(ms.begin(), ms.end());
    times.push_back(ms[ms.size() / 2]);
  }

  return (true);
}

/** Own the engine while loading and timing the PulseTen layout.
@param[out]  n_parts  number of physical partitions after flushing
@return true if loading, flushing, timing and closing succeed */
static bool bench_pulseten(const char *base, const bench_data_t &d, int runs,
                           const std::vector<bench_query_t> &queries,
                           std::vector<double> &times, int &n_parts) {
  db_status_t status;

  const std::string dir = std::string(base) + "/pulseten";
  if (mkdir(dir.c_str(), 0755) != 0) {
    return (false);
  }
  engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  engine_t *engine = nullptr;

  status = engine_open(dir.c_str(), &options, &engine);
  if (!status.ok()) {
    fprintf(stderr, "engine_open: %s\n", status.message().c_str());
    test_remove_dir(dir.c_str());

    return (false);
  }

  const bool ok = bench_pulseten_low(*engine, d, runs, queries, times, n_parts);
  const db_status_t close_err = engine_close(engine);
  test_remove_dir(dir.c_str());

  return (ok && close_err.ok());
}

/** Run the benchmark.
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc < 4 || argc > 6) {
    fprintf(stderr, "usage: %s <dir> <days> <rows_per_day> [runs] [pulseten]\n",
            argv[0]);

    return (2);
  }

  uint64_t days = 0;
  uint64_t rows_per_day = 0;
  uint64_t run_count = 5;
  if (!test_parse_positive_count(argv[2], days) ||
      !test_parse_positive_count(argv[3], rows_per_day)) {
    fprintf(stderr, "days and rows_per_day must be positive integers\n");

    return (2);
  }
  if (argc > 4 && !test_parse_positive_count(argv[4], run_count)) {
    fprintf(stderr, "runs must be a positive integer\n");

    return (2);
  }
  if (days > INT_MAX || days > static_cast<uint64_t>((INT64_MAX - T0) / DAY) ||
      rows_per_day > static_cast<uint64_t>(INT64_MAX) / days ||
      run_count >= INT_MAX) {
    fprintf(stderr, "benchmark argument out of range\n");

    return (2);
  }
  if (argc > 5 && strcmp(argv[5], "0") != 0 && strcmp(argv[5], "1") != 0) {
    fprintf(stderr, "pulseten must be 0 or 1\n");

    return (2);
  }
  bench_data_t d;
  d.m_days = static_cast<int>(days);
  d.m_n = rows_per_day * days;
  d.m_step_us = static_cast<double>(d.m_days) * static_cast<double>(DAY) /
                static_cast<double>(d.m_n);
  const int runs = static_cast<int>(run_count);
  const bool with_pulseten = argc > 5 && strcmp(argv[5], "1") == 0;

  printf("%llu rows over %d UTC days (%s rows per day), median of %d runs\n",
         static_cast<unsigned long long>(d.m_n), d.m_days, argv[3], runs);

  const std::vector<bench_query_t> queries = bench_queries(d);
  std::vector<double> pulseten;
  int pulseten_parts = 0;

  /* PulseTen first: opening it registers DuckDB's built-in functions. */
  if (with_pulseten) {
    if (!bench_pulseten(argv[1], d, runs, queries, pulseten, pulseten_parts)) {
      return (1);
    }
  } else {
    engine_t *engine = nullptr;
    const std::string dir = std::string(argv[1]) + "/register";
    if (mkdir(dir.c_str(), 0755) != 0) {
      return (1);
    }
    if (!engine_open(dir.c_str(), nullptr, &engine).ok()) {
      test_remove_dir(dir.c_str());

      return (1);
    }
    const db_status_t close_err = engine_close(engine);
    test_remove_dir(dir.c_str());
    if (!close_err.ok()) {
      return (1);
    }
  }

  std::vector<std::vector<double>> times;
  std::vector<int> parts;

  if (!bench_layouts(argv[1], d, runs, queries, times, parts)) {
    return (1);
  }

  printf("\n%-28s %9s %9s %9s %9s", "query (ms)", "1 table", "30-day", "7-day",
         "1-day");
  if (with_pulseten) {
    printf(" %9s", "PulseTen");
  }
  printf("\n%-28s %9d %9d %9d %9d", "partitions", parts[0], parts[1], parts[2],
         parts[3]);
  if (with_pulseten) {
    printf(" %9d", pulseten_parts);
  }
  printf("\n");

  for (size_t q = 0; q < queries.size(); ++q) {
    printf("%-28s", queries[q].m_label);
    for (size_t l = 0; l < times.size(); ++l) {
      printf(" %9.2f", times[l][q]);
    }
    if (with_pulseten) {
      printf(" %9.2f", pulseten[q]);
    }
    printf("\n");
  }

  return (0);
}
