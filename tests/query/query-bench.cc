/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query-bench.cc
 *    Query latency over stored and unflushed data.
 *
 * Loads N rows of table metrics (time, host tag, cpu and mem DOUBLE) spread
 * evenly over 10 UTC days in time order, once per memtable share: the newest
 * share of the rows stays in the memtable, the rest is flushed to DuckDB.
 * Two baselines hold the same rows in plain DuckDB: one table, and ten
 * day tables with the same time CHECK constraints behind a UNION ALL view.
 * Every query runs once to warm up, then the median of the timed runs is
 * reported in ms.
 *
 * Usage: query-bench <dir> [rows] [runs]
 * Defaults: 1000000 rows, 7 runs.
 *
 *-------------------------------------------------------------------------
 */

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <time.h>
#include <vector>

#include <duckdb.h>
#include <sys/stat.h>

#include "engine.h"
#include "ingest.h"
#include "memtable.h"
#include "test_engine.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"

/** Spread of the data: 10 UTC days from T0. */
static const int64_t SPAN_US = 10 * DAY;

/** Points per write while loading. */
static const uint32_t LOAD_BATCH = 10000;

/** Read the monotonic clock in nanoseconds.
@return the monotonic clock in nanoseconds */
static uint64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
          static_cast<uint64_t>(ts.tv_nsec));
}

/** Row i of the data set. */
struct bench_row_t {
  /** Time of the row. */
  int64_t m_time_us{0};

  /** Host number, 0 to 99. */
  int m_host{0};

  /** CPU usage. */
  double m_cpu{0};

  /** Memory usage. */
  double m_mem{0};
};

/** Generate one deterministic benchmark row.
@param[in]  i  zero-based row or record index
@return row i of a data set of n rows */
static bench_row_t bench_row(uint64_t i, uint64_t n) {
  bench_row_t r;
  r.m_time_us = T0 + static_cast<int64_t>(static_cast<double>(i) *
                                          static_cast<double>(SPAN_US) /
                                          static_cast<double>(n));
  r.m_host = static_cast<int>(i % 100);
  r.m_cpu = static_cast<double>(i % 1000) / 10.0;
  r.m_mem = static_cast<double>(i * 7 % 1000) / 10.0;

  return (r);
}

/** A query of the benchmark. */
struct bench_query_t {
  /** Name printed in the report. */
  const char *m_label;

  /** The query. */
  std::string m_sql;
};

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

/** Build the SQL queries used by this benchmark.
@return the queries of the benchmark */
static std::vector<bench_query_t> bench_queries() {
  const int64_t end = T0 + SPAN_US;
  std::vector<bench_query_t> q(7);

  q[0].m_label = "fixed cost: SELECT 1";
  q[0].m_sql = "SELECT 1";

  q[1].m_label = "count/avg/max, all rows";
  q[1].m_sql = "SELECT count(*), avg(cpu), max(mem) FROM metrics";

  q[2].m_label = "GROUP BY host, all rows";
  q[2].m_sql = "SELECT host, avg(cpu) FROM metrics GROUP BY host";

  q[3].m_label = "aggregate, oldest day";
  q[3].m_sql = "SELECT count(*), avg(cpu) FROM metrics WHERE time >= " +
               timestamp_literal(T0) + " AND time < " +
               timestamp_literal(T0 + DAY);

  q[4].m_label = "aggregate, newest day";
  q[4].m_sql = "SELECT count(*), avg(cpu) FROM metrics WHERE time >= " +
               timestamp_literal(end - DAY);

  q[5].m_label = "aggregate, newest hour";
  q[5].m_sql = "SELECT count(*), avg(cpu) FROM metrics WHERE time >= " +
               timestamp_literal(end - 3600LL * 1000000);

  q[6].m_label = "one host, newest 100 rows";
  q[6].m_sql =
      "SELECT time, cpu FROM metrics WHERE host = 'host-7' "
      "ORDER BY time DESC LIMIT 100";

  return (q);
}

/** Write rows [from, to) to the engine in loads of LOAD_BATCH points.
@param[in]  from  first row or id to generate, inclusive
@return true if every batch in the requested row range is accepted */
static bool bench_load(engine_t &engine, uint64_t from, uint64_t to, uint64_t n,
                       const std::vector<std::string> &hosts) {
  db_status_t status;

  std::vector<ingest_tag_t> tags(LOAD_BATCH);
  std::vector<ingest_field_t> fields(2 * LOAD_BATCH);
  std::vector<ingest_point_t> points(LOAD_BATCH);

  for (uint64_t start = from; start < to; start += LOAD_BATCH) {
    const uint32_t count = static_cast<uint32_t>(
        to - start < LOAD_BATCH ? to - start : LOAD_BATCH);

    for (uint32_t k = 0; k < count; ++k) {
      const bench_row_t r = bench_row(start + k, n);

      tags[k] = tag("host", hosts[r.m_host].c_str());
      fields[2 * k] = field_double("cpu", r.m_cpu);
      fields[2 * k + 1] = field_double("mem", r.m_mem);
      points[k] = point(r.m_time_us, &tags[k], 1, &fields[2 * k], 2);
    }

    batch_seq_t seq = 0;

    status = engine_write(engine, "metrics", 7, &points[0], count, seq);
    if (!status.ok()) {
      fprintf(stderr, "load failed: %s\n", status.message().c_str());

      return (false);
    }
  }

  return (true);
}

/** Median of the timed runs of one query, in ms; -1 on error.
@return median execution-and-fetch time in milliseconds; -1 on query failure */
static double bench_time_engine(engine_t &engine, const std::string &sql,
                                int runs) {
  db_status_t status;

  std::vector<double> ms;

  for (int i = 0; i <= runs; ++i) {
    test_query_t result;

    const uint64_t start = now_ns();

    status = query_all(engine, sql, result);
    if (!status.ok()) {
      fprintf(stderr, "query failed: %s\n  %s\n", sql.c_str(),
              status.message().c_str());

      return (-1);
    }

    if (i > 0) {
      ms.push_back(static_cast<double>(now_ns() - start) / 1e6);
    }
  }

  std::sort(ms.begin(), ms.end());

  return (ms[ms.size() / 2]);
}

/** Run a query on plain DuckDB tables, reading every chunk.
@return the median time in milliseconds, without the first run */
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

    /* Read every chunk, as query_all() does. */
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

/** Run a statement, preserving its failure for the caller.
@return true on success */
static bool bench_exec(duckdb_connection conn, const std::string &sql) {
  duckdb_result result;
  const bool ok = duckdb_query(conn, sql.c_str(), &result) == DuckDBSuccess;
  if (!ok) {
    fprintf(stderr, "native: %s\n  %s\n", sql.c_str(),
            duckdb_result_error(&result));
  }
  duckdb_destroy_result(&result);

  return (ok);
}

/** Rewrite a metrics query to read the per-day view.
@return sql with "FROM metrics" replaced by "FROM days" */
static std::string bench_on_days(const std::string &sql) {
  std::string out = sql;
  const size_t at = out.find("FROM metrics");

  if (at != std::string::npos) {
    out.replace(at, strlen("FROM metrics"), "FROM days");
  }

  return (out);
}

/** Own the native baseline's appender until every path has released it. */
struct bench_appender_guard_t {
  /** Appender being filled or closed. */
  duckdb_appender m_app{nullptr};

  /** Destroy the owned appender. */
  ~bench_appender_guard_t() {
    static_cast<void>(duckdb_appender_destroy(&m_app));
  }
};

/** Print the first appender failure before its handle is destroyed.
@return false */
static bool bench_appender_error(duckdb_appender app) {
  const char *message = app != nullptr ? duckdb_appender_error(app) : nullptr;
  fprintf(stderr, "native appender: %s\n",
          message != nullptr ? message : "cannot create appender");

  return (false);
}

/** Load and time the native baseline on a connection owned by the caller.
@return true if setup, appending and every timed query succeed */
static bool bench_native_low(duckdb_connection conn, uint64_t n, int runs,
                             const std::vector<bench_query_t> &queries,
                             std::vector<double> &times,
                             std::vector<double> &day_times) {
  if (!bench_exec(conn,
                  "CREATE TABLE metrics (time TIMESTAMP, host VARCHAR, "
                  "cpu DOUBLE, mem DOUBLE)")) {
    return (false);
  }

  bench_appender_guard_t owner;
  duckdb_appender &app = owner.m_app;
  if (duckdb_appender_create(conn, nullptr, "metrics", &app) != DuckDBSuccess) {
    return (bench_appender_error(app));
  }

  char host[32];
  for (uint64_t i = 0; i < n; ++i) {
    const bench_row_t r = bench_row(i, n);
    duckdb_timestamp ts;
    ts.micros = r.m_time_us;
    snprintf(host, sizeof(host), "host-%d", r.m_host);

    if (duckdb_append_timestamp(app, ts) != DuckDBSuccess) {
      return (bench_appender_error(app));
    }
    if (duckdb_append_varchar(app, host) != DuckDBSuccess) {
      return (bench_appender_error(app));
    }
    if (duckdb_append_double(app, r.m_cpu) != DuckDBSuccess) {
      return (bench_appender_error(app));
    }
    if (duckdb_append_double(app, r.m_mem) != DuckDBSuccess) {
      return (bench_appender_error(app));
    }
    if (duckdb_appender_end_row(app) != DuckDBSuccess) {
      return (bench_appender_error(app));
    }
  }

  if (duckdb_appender_flush(app) != DuckDBSuccess) {
    return (bench_appender_error(app));
  }
  if (duckdb_appender_close(app) != DuckDBSuccess) {
    return (bench_appender_error(app));
  }

  /* The same rows split by UTC day. */
  std::string view = "CREATE VIEW days AS ";
  for (int d = 0; d < 10; ++d) {
    const std::string table = "d" + std::to_string(d);
    const std::string from = timestamp_literal(T0 + d * DAY);
    const std::string to = timestamp_literal(T0 + (d + 1) * DAY);

    if (!bench_exec(conn, "CREATE TABLE " + table +
                              " (time TIMESTAMP, host VARCHAR, cpu DOUBLE, "
                              "mem DOUBLE, CHECK (time >= " +
                              from + " AND time < " + to + "))")) {
      return (false);
    }
    if (!bench_exec(conn, "INSERT INTO " + table +
                              " SELECT * FROM metrics WHERE time >= " + from +
                              " AND time < " + to)) {
      return (false);
    }
    view +=
        (d > 0 ? " UNION ALL " : "") + std::string("SELECT * FROM ") + table;
  }

  if (!bench_exec(conn, view)) {
    return (false);
  }
  for (size_t q = 0; q < queries.size(); ++q) {
    const double ms = bench_time_native(conn, queries[q].m_sql, runs);
    if (ms < 0) {
      return (false);
    }
    times.push_back(ms);

    const double day_ms =
        bench_time_native(conn, bench_on_days(queries[q].m_sql), runs);
    if (day_ms < 0) {
      return (false);
    }
    day_times.push_back(day_ms);
  }

  return (true);
}

/** Own the native baseline's database while setup and timing run.
@return true if loading both layouts and every timed query succeed */
static bool bench_native(const char *base, uint64_t n, int runs,
                         const std::vector<bench_query_t> &queries,
                         std::vector<double> &times,
                         std::vector<double> &day_times) {
  const std::string dir = std::string(base) + "/native";
  if (mkdir(dir.c_str(), 0755) != 0) {
    fprintf(stderr, "cannot create native benchmark directory\n");

    return (false);
  }

  const std::string path = dir + "/native.duckdb";
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

  const bool ok = bench_native_low(conn, n, runs, queries, times, day_times);
  duckdb_disconnect(&conn);
  duckdb_close(&db);
  test_remove_dir(dir.c_str());

  return (ok);
}

/** Load and time one memtable share on an engine owned by the caller.
@return true if loading, flushing and every timed query succeed */
static bool bench_engine_low(engine_t &engine, uint64_t n, int runs,
                             int memtable_percent,
                             const std::vector<bench_query_t> &queries,
                             std::vector<double> &times) {
  db_status_t status;

  std::vector<std::string> hosts;
  for (int h = 0; h < 100; ++h) {
    hosts.push_back("host-" + std::to_string(h));
  }

  const uint64_t percent = static_cast<uint64_t>(memtable_percent);
  const uint64_t memtable_rows = n / 100 * percent + n % 100 * percent / 100;
  const uint64_t stored = n - memtable_rows;
  const uint64_t load_start = now_ns();
  if (!bench_load(engine, 0, stored, n, hosts)) {
    return (false);
  }
  if (stored > 0) {
    engine_flush_info_t info;

    status = engine_flush(engine, info);
    if (!status.ok()) {
      fprintf(stderr, "flush failed: %s\n", status.message().c_str());

      return (false);
    }
  }
  if (!bench_load(engine, stored, n, n, hosts)) {
    return (false);
  }

  const double load_s = static_cast<double>(now_ns() - load_start) / 1e9;
  test_query_t check;

  status = query_all(engine, "SELECT count(*) FROM metrics", check);
  if (!status.ok()) {
    fprintf(stderr, "count failed: %s\n", status.message().c_str());

    return (false);
  }
  if (check.m_rows.size() != 1 || check.m_rows[0].size() != 1 ||
      check.m_rows[0][0].m_is_null ||
      check.m_rows[0][0].m_i != static_cast<int64_t>(n)) {
    fprintf(stderr, "benchmark row count mismatch\n");

    return (false);
  }

  memtable_stats_t stats;
  engine_get_memtable_stats(engine, stats);
  printf("  memtable %3d%%: loaded in %.1f s, %llu rows in memtable\n",
         memtable_percent, load_s,
         static_cast<unsigned long long>(stats.m_live_rows));
  for (size_t q = 0; q < queries.size(); ++q) {
    const double ms = bench_time_engine(engine, queries[q].m_sql, runs);
    if (ms < 0) {
      return (false);
    }
    times.push_back(ms);
  }

  return (true);
}

/** Own the engine while loading and timing one memtable share.
@return true if loading, flushing, timing and closing succeed */
static bool bench_engine(const char *base, uint64_t n, int runs,
                         int memtable_percent,
                         const std::vector<bench_query_t> &queries,
                         std::vector<double> &times) {
  db_status_t status;

  char dir[1024];
  snprintf(dir, sizeof(dir), "%s/memtable-%d", base, memtable_percent);
  if (mkdir(dir, 0755) != 0) {
    fprintf(stderr, "cannot create memtable benchmark directory\n");

    return (false);
  }

  engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  engine_t *engine = nullptr;

  status = engine_open(dir, &options, &engine);
  if (!status.ok()) {
    fprintf(stderr, "engine_open failed: %s\n", status.message().c_str());
    test_remove_dir(dir);

    return (false);
  }

  const bool ok =
      bench_engine_low(*engine, n, runs, memtable_percent, queries, times);
  const db_status_t close_err = engine_close(engine);
  test_remove_dir(dir);

  return (ok && close_err.ok());
}

/** Run the benchmark.
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc < 2 || argc > 4) {
    fprintf(stderr, "usage: %s <dir> [rows] [runs]\n", argv[0]);

    return (2);
  }

  uint64_t n = 1000000;
  uint64_t run_count = 7;
  if (argc > 2 && !test_parse_positive_count(argv[2], n)) {
    fprintf(stderr, "rows must be a positive integer\n");

    return (2);
  }
  if (argc > 3 && !test_parse_positive_count(argv[3], run_count)) {
    fprintf(stderr, "runs must be a positive integer\n");

    return (2);
  }
  if (n > INT64_MAX || run_count >= INT_MAX) {
    fprintf(stderr, "benchmark argument out of range\n");

    return (2);
  }
  const int runs = static_cast<int>(run_count);
  const int memtable_shares[] = {0, 10, 50, 100};
  const int n_shares = sizeof(memtable_shares) / sizeof(memtable_shares[0]);

  const std::vector<bench_query_t> queries = bench_queries();
  std::vector<std::vector<double>> times(n_shares + 1);
  std::vector<double> day_times;

  printf("%llu rows over 10 UTC days, median of %d runs\n",
         static_cast<unsigned long long>(n), runs);

  /* Engine runs first: opening it registers DuckDB's built-in functions
  for the native baseline as well. */
  for (int h = 0; h < n_shares; ++h) {
    if (!bench_engine(argv[1], n, runs, memtable_shares[h], queries,
                      times[h + 1])) {
      return (1);
    }
  }

  if (!bench_native(argv[1], n, runs, queries, times[0], day_times)) {
    return (1);
  }

  printf("\n%-28s %10s %10s", "query (ms)", "native", "native-10");
  for (int h = 0; h < n_shares; ++h) {
    printf("   memtable %3d%%", memtable_shares[h]);
  }
  printf("\n");

  for (size_t q = 0; q < queries.size(); ++q) {
    printf("%-28s %10.2f %10.2f", queries[q].m_label, times[0][q],
           day_times[q]);
    for (int h = 0; h < n_shares; ++h) {
      printf(" %10.2f", times[h + 1][q]);
    }
    printf("\n");
  }

  return (0);
}
