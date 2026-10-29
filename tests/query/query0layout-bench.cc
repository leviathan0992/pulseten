/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0layout-bench.cc
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
 * Usage: query0layout-bench <dir> <days> <rows_per_day> [runs] [pulseten]
 * Pass 1 as pulseten to include PulseTen.
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "engine0engine.h"
#include "test0engine.h"
#include "test0point.h"
#include "test0util.h"

/** Read the monotonic clock in nanoseconds.
@return the monotonic clock in nanoseconds */
static uint64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  return ((uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec);
}

/** Format a timestamp as an SQL TIMESTAMP literal, to the second.
@param[in]  us  UTC timestamp in microseconds since 1970-01-01
@return a SQL TIMESTAMP literal of a time, to the second */
static std::string timestamp_literal(int64_t us) {
  const time_t secs = (time_t)(us / 1000000);
  struct tm tm;
  gmtime_r(&secs, &tm);

  char buf[64];
  strftime(buf, sizeof(buf), "TIMESTAMP '%Y-%m-%d %H:%M:%S'", &tm);

  return (buf);
}

/** The data set: n rows over days days from T0. */
struct bench_data_t {
  /** Days the rows span. */
  int days{0};

  /** Rows. */
  uint64_t n{0};

  /** Microseconds between consecutive rows. */
  double step_us{0};
};

struct bench_query_t {
  /** Name printed in the report. */
  const char *label;

  /** SQL with "FROM metrics", replaced per layout. */
  std::string sql;
};

/** Build the SQL queries used by this benchmark.
@param[in]  d  generated data set and row/time bounds
@return the queries of the benchmark over a data set */
static std::vector<bench_query_t> bench_queries(const bench_data_t &d) {
  const int64_t end = T0 + (int64_t)d.days * DAY;
  const std::string where = " WHERE time >= ";
  std::vector<bench_query_t> q(7);

  q[0].label = "count/avg/max, all rows";
  q[0].sql = "SELECT count(*), avg(cpu), max(mem) FROM metrics";

  q[1].label = "GROUP BY host, all rows";
  q[1].sql = "SELECT host, avg(cpu) FROM metrics GROUP BY host";

  q[2].label = "aggregate, newest 7 days";
  q[2].sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
             timestamp_literal(end - 7 * DAY);

  q[3].label = "aggregate, newest day";
  q[3].sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
             timestamp_literal(end - DAY);

  q[4].label = "aggregate, newest hour";
  q[4].sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
             timestamp_literal(end - 3600LL * 1000000);

  q[5].label = "aggregate, oldest day";
  q[5].sql = "SELECT count(*), avg(cpu) FROM metrics" + where +
             timestamp_literal(T0) + " AND time < " +
             timestamp_literal(T0 + DAY);

  q[6].label = "one host, newest 100 rows";
  q[6].sql =
      "SELECT time, cpu FROM metrics WHERE host = 'host-7' "
      "ORDER BY time DESC LIMIT 100";

  return (q);
}

/** Run a statement, printing DuckDB's error if it fails.
@param[in]  conn  DuckDB connection
@param[in]  sql   SQL statement to check or execute
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
@param[in]  conn  DuckDB connection
@param[in]  sql   SQL statement to check or execute
@param[in]  runs  number of timed runs, excluding warm-up
@return median execution-and-fetch time in milliseconds; -1 on query failure */
static double bench_time_native(duckdb_connection conn, const std::string &sql,
                                int runs) {
  std::vector<double> ms;

  for (int i = 0; i <= runs; ++i) {
    duckdb_result result;
    const uint64_t start = now_ns();
    const bool ok = duckdb_query(conn, sql.c_str(), &result) == DuckDBSuccess;

    for (;;) {
      duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
      if (chunk == nullptr) {
        break;
      }
      duckdb_destroy_data_chunk(&chunk);
    }

    const double elapsed = (double)(now_ns() - start) / 1e6;
    duckdb_destroy_result(&result);

    if (!ok) {
      return (-1);
    }
    if (i > 0) {
      ms.push_back(elapsed);
    }
  }

  std::sort(ms.begin(), ms.end());

  return (ms[ms.size() / 2]);
}

/** Rewrite a metrics query to read a selected view.
@param[in]  sql   SQL statement to check or execute
@param[in]  view  name of the SQL view
@return sql reading from view instead of metrics */
static std::string bench_on(const std::string &sql, const std::string &view) {
  std::string out = sql;
  const size_t at = out.find("FROM metrics");
  out.replace(at, strlen("FROM metrics"), "FROM " + view);

  return (out);
}

/** Split metrics into partitions of span_days days behind a view.
@param[in]  conn       DuckDB connection
@param[in]  d          data set whose time bounds define the partitions
@param[in]  span_days  days covered by each physical partition
@param[in]  view       name of the SQL view
@return number of partitions */
static int bench_partition(duckdb_connection conn, const bench_data_t &d,
                           int span_days, const std::string &view) {
  std::string sql = "CREATE VIEW " + view + " AS ";
  int parts = 0;

  for (int start = 0; start < d.days; start += span_days) {
    const std::string table = view + "_" + std::to_string(parts);
    const std::string from = timestamp_literal(T0 + (int64_t)start * DAY);
    const std::string to =
        timestamp_literal(T0 + (int64_t)(start + span_days) * DAY);

    bench_exec(conn, "CREATE TABLE " + table +
                         " (time TIMESTAMP, host VARCHAR, cpu DOUBLE, "
                         "mem DOUBLE, CHECK (time >= " +
                         from + " AND time < " + to + "))");
    bench_exec(conn, "INSERT INTO " + table +
                         " SELECT * FROM metrics WHERE time >= " + from +
                         " AND time < " + to + " ORDER BY time");

    sql += (parts > 0 ? " UNION ALL " : "") + std::string("SELECT * FROM ") +
           table;
    ++parts;
  }

  bench_exec(conn, sql);

  return (parts);
}

/** Time every query on one table and on 30-, 7- and 1-day partitions.
@param[in]   base     directory for scratch native DuckDB files
@param[in]   d        generated rows and query bounds
@param[in]   runs     number of timed query runs
@param[in]   queries  queries to benchmark
@param[out]  times    times[layout][query]
@param[out]  parts    physical partition count for each layout
@return true if every layout was loaded and timed successfully */
static bool bench_layouts(const char *base, const bench_data_t &d, int runs,
                          const std::vector<bench_query_t> &queries,
                          std::vector<std::vector<double>> *times,
                          std::vector<int> *parts) {
  const std::string dir = std::string(base) + "/layouts";
  mkdir(dir.c_str(), 0755);
  const std::string path = dir + "/layouts.duckdb";

  duckdb_database db = nullptr;
  duckdb_connection conn = nullptr;

  if (duckdb_open(path.c_str(), &db) != DuckDBSuccess ||
      duckdb_connect(db, &conn) != DuckDBSuccess) {
    return (false);
  }

  /* Generate the rows inside DuckDB, in time order. */
  char gen[512];
  snprintf(gen, sizeof(gen),
           "CREATE TABLE metrics AS SELECT "
           "make_timestamp(%lld + CAST(i * %.6f AS BIGINT)) AS time, "
           "'host-' || CAST(i %% 100 AS VARCHAR) AS host, "
           "CAST(i %% 1000 AS DOUBLE) / 10 AS cpu, "
           "CAST(i * 7 %% 1000 AS DOUBLE) / 10 AS mem "
           "FROM range(%llu) t(i)",
           (long long)T0, d.step_us, (unsigned long long)d.n);

  const uint64_t start = now_ns();
  bool ok = bench_exec(conn, gen);

  const int spans[] = {30, 7, 1};
  const char *views[] = {"p30", "p7", "p1"};

  parts->push_back(1);
  for (int i = 0; ok && i < 3; ++i) {
    parts->push_back(bench_partition(conn, d, spans[i], views[i]));
  }

  printf("  plain DuckDB: generated and partitioned in %.1f s\n",
         (double)(now_ns() - start) / 1e9);

  const char *names[] = {"metrics", "p30", "p7", "p1"};

  for (int l = 0; ok && l < 4; ++l) {
    std::vector<double> t;
    for (size_t q = 0; q < queries.size(); ++q) {
      t.push_back(
          bench_time_native(conn, bench_on(queries[q].sql, names[l]), runs));
    }
    times->push_back(t);
  }

  duckdb_disconnect(&conn);
  duckdb_close(&db);
  test_remove_dir(dir.c_str());

  return (ok);
}

/** Load the same rows through PulseTen, flushing as it goes, and time the
queries.
@param[in]   base     directory for the scratch PulseTen database
@param[in]   d        generated rows and query bounds
@param[in]   runs     number of timed query runs
@param[in]   queries  queries to benchmark
@param[out]  times    median query times in milliseconds
@param[out]  n_parts  number of physical partitions after flushing
@return true if loading, flushing and every timed query succeed */
static bool bench_pulseten(const char *base, const bench_data_t &d, int runs,
                           const std::vector<bench_query_t> &queries,
                           std::vector<double> *times, int *n_parts) {
  const std::string dir = std::string(base) + "/pulseten";
  mkdir(dir.c_str(), 0755);

  engine_options_t options;
  options.background_flush = false;

  engine_t *engine = nullptr;
  std::string error;
  if (engine_open(dir.c_str(), &options, &engine, nullptr, error) !=
      DB_SUCCESS) {
    fprintf(stderr, "engine_open: %s\n", error.c_str());

    return (false);
  }

  std::vector<std::string> hosts;
  for (int h = 0; h < 100; ++h) {
    hosts.push_back("host-" + std::to_string(h));
  }

  const uint32_t batch = 10000;
  std::vector<ingest_tag_t> tags(batch);
  std::vector<ingest_field_t> fields(2 * batch);
  std::vector<ingest_point_t> points(batch);

  const uint64_t start = now_ns();
  bool ok = true;

  for (uint64_t from = 0; ok && from < d.n; from += batch) {
    const uint32_t count = (uint32_t)(d.n - from < batch ? d.n - from : batch);

    for (uint32_t k = 0; k < count; ++k) {
      const uint64_t i = from + k;

      tags[k] = tag("host", hosts[i % 100].c_str());
      fields[2 * k] = field_double("cpu", (double)(i % 1000) / 10);
      fields[2 * k + 1] = field_double("mem", (double)(i * 7 % 1000) / 10);
      points[k] = point(T0 + (int64_t)((double)i * d.step_us), &tags[k], 1,
                        &fields[2 * k], 2);
    }

    batch_seq_t seq = 0;
    ok = engine_write(*engine, "metrics", 7, &points[0], count, seq, error) ==
         DB_SUCCESS;

    /* Flush every million rows to bound the memtable. */
    if (ok && (from / batch) % 100 == 99) {
      engine_flush_info_t info;
      ok = engine_flush(*engine, info, error) == DB_SUCCESS;
    }
  }

  if (ok) {
    engine_flush_info_t info;
    ok = engine_flush(*engine, info, error) == DB_SUCCESS;
  }

  if (!ok) {
    fprintf(stderr, "PulseTen load: %s\n", error.c_str());
  } else {
    printf("  PulseTen: loaded and flushed in %.1f s\n",
           (double)(now_ns() - start) / 1e9);

    *n_parts = (int)storage_get_state(*(engine_get_storage(*engine)))
                   ->partitions.size();
  }

  for (size_t q = 0; ok && q < queries.size(); ++q) {
    std::vector<double> ms;

    for (int i = 0; ok && i <= runs; ++i) {
      test_query_t result;
      const uint64_t t0 = now_ns();

      ok = query_all(*engine, queries[q].sql, result, error) == DB_SUCCESS;

      if (i > 0) {
        ms.push_back((double)(now_ns() - t0) / 1e6);
      }
    }

    if (ok) {
      std::sort(ms.begin(), ms.end());
      times->push_back(ms[ms.size() / 2]);
    }
  }

  (void)engine_close(engine);
  test_remove_dir(dir.c_str());

  return (ok);
}

/** Run the benchmark.
@param[in]  argc  number of command-line arguments, including the program name
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "usage: %s <dir> <days> <rows_per_day> [runs] [pulseten]\n",
            argv[0]);

    return (2);
  }

  bench_data_t d;
  d.days = atoi(argv[2]);
  d.n = (uint64_t)atoll(argv[3]) * (uint64_t)d.days;
  d.step_us = (double)d.days * (double)DAY / (double)d.n;

  const int runs = argc > 4 ? atoi(argv[4]) : 5;
  const bool with_pulseten = argc > 5 && atoi(argv[5]) == 1;

  printf("%llu rows over %d UTC days (%s rows per day), median of %d runs\n",
         (unsigned long long)d.n, d.days, argv[3], runs);

  const std::vector<bench_query_t> queries = bench_queries(d);
  std::vector<double> pulseten;
  int pulseten_parts = 0;

  /* PulseTen first: opening it registers DuckDB's built-in functions. */
  if (with_pulseten) {
    if (!bench_pulseten(argv[1], d, runs, queries, &pulseten,
                        &pulseten_parts)) {
      return (1);
    }
  } else {
    engine_t *engine = nullptr;
    const std::string dir = std::string(argv[1]) + "/register";
    mkdir(dir.c_str(), 0755);
    if (engine_open(dir.c_str(), nullptr, &engine, nullptr, test_error) ==
        DB_SUCCESS) {
      (void)engine_close(engine);
    }
    test_remove_dir(dir.c_str());
  }

  std::vector<std::vector<double>> times;
  std::vector<int> parts;

  if (!bench_layouts(argv[1], d, runs, queries, &times, &parts)) {
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
    printf("%-28s", queries[q].label);
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
