/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0query-bench.cc
 *    Query latency over stored and unflushed data.
 *
 * Loads N rows of table metrics (time, host tag, cpu and mem DOUBLE) spread
 * evenly over 10 UTC days in time order, once per hot share: the newest
 * share of the rows stays in the memtable, the rest is flushed to DuckDB.
 * Two baselines hold the same rows in plain DuckDB: one table, and ten
 * day tables with the same time CHECK constraints behind a UNION ALL view.
 * Every query runs once to warm up, then the median of the timed runs is
 * reported in ms.
 *
 * Usage: query0query-bench <dir> [rows] [runs]
 * Defaults: 1000000 rows, 7 runs.
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

/** Spread of the data: 10 UTC days from T0. */
static const int64_t SPAN_US = 10 * DAY;

/** Points per write while loading. */
static const uint32_t LOAD_BATCH = 10000;

/** Read the monotonic clock in nanoseconds.
@return the monotonic clock in nanoseconds */
static uint64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  return ((uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec);
}

/** Row i of the data set. */
struct bench_row_t {
  /** Time of the row. */
  int64_t time_us{0};

  /** Host number, 0 to 99. */
  int host{0};

  /** CPU usage. */
  double cpu{0};

  /** Memory usage. */
  double mem{0};
};

/** Generate one deterministic benchmark row.
@param[in]  i  zero-based row or record index
@param[in]  n  total row count used to generate the data distribution
@return row i of a data set of n rows */
static bench_row_t bench_row(uint64_t i, uint64_t n) {
  bench_row_t r;
  r.time_us = T0 + (int64_t)((double)i * (double)SPAN_US / (double)n);
  r.host = (int)(i % 100);
  r.cpu = (double)(i % 1000) / 10.0;
  r.mem = (double)(i * 7 % 1000) / 10.0;

  return (r);
}

/** A query of the benchmark. */
struct bench_query_t {
  /** Name printed in the report. */
  const char *label;

  /** The query. */
  std::string sql;
};

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

/** Build the SQL queries used by this benchmark.
@return the queries of the benchmark */
static std::vector<bench_query_t> bench_queries() {
  const int64_t end = T0 + SPAN_US;
  std::vector<bench_query_t> q(7);

  q[0].label = "fixed cost: SELECT 1";
  q[0].sql = "SELECT 1";

  q[1].label = "count/avg/max, all rows";
  q[1].sql = "SELECT count(*), avg(cpu), max(mem) FROM metrics";

  q[2].label = "GROUP BY host, all rows";
  q[2].sql = "SELECT host, avg(cpu) FROM metrics GROUP BY host";

  q[3].label = "aggregate, oldest day";
  q[3].sql = "SELECT count(*), avg(cpu) FROM metrics WHERE time >= " +
             timestamp_literal(T0) + " AND time < " +
             timestamp_literal(T0 + DAY);

  q[4].label = "aggregate, newest day";
  q[4].sql = "SELECT count(*), avg(cpu) FROM metrics WHERE time >= " +
             timestamp_literal(end - DAY);

  q[5].label = "aggregate, newest hour";
  q[5].sql = "SELECT count(*), avg(cpu) FROM metrics WHERE time >= " +
             timestamp_literal(end - 3600LL * 1000000);

  q[6].label = "one host, newest 100 rows";
  q[6].sql =
      "SELECT time, cpu FROM metrics WHERE host = 'host-7' "
      "ORDER BY time DESC LIMIT 100";

  return (q);
}

/** Write rows [from, to) to the engine in loads of LOAD_BATCH points.
@param[in,out]  engine  database engine
@param[in]      from    first row or id to generate, inclusive
@param[in]      to      first row or id to exclude
@param[in]      n       total data-set row count used by the deterministic
                        generator
@param[in]      hosts   host names used by the generated data set
@return true if every batch in the requested row range is accepted */
static bool bench_load(engine_t *engine, uint64_t from, uint64_t to, uint64_t n,
                       const std::vector<std::string> &hosts) {
  std::vector<ingest_tag_t> tags(LOAD_BATCH);
  std::vector<ingest_field_t> fields(2 * LOAD_BATCH);
  std::vector<ingest_point_t> points(LOAD_BATCH);

  for (uint64_t start = from; start < to; start += LOAD_BATCH) {
    const uint32_t count =
        (uint32_t)(to - start < LOAD_BATCH ? to - start : LOAD_BATCH);

    for (uint32_t k = 0; k < count; ++k) {
      const bench_row_t r = bench_row(start + k, n);

      tags[k] = tag("host", hosts[r.host].c_str());
      fields[2 * k] = field_double("cpu", r.cpu);
      fields[2 * k + 1] = field_double("mem", r.mem);
      points[k] = point(r.time_us, &tags[k], 1, &fields[2 * k], 2);
    }

    batch_seq_t seq = 0;
    std::string error;
    if (engine_write(*engine, "metrics", 7, &points[0], count, seq, error) !=
        DB_SUCCESS) {
      fprintf(stderr, "load failed: %s\n", error.c_str());

      return (false);
    }
  }

  return (true);
}

/** Median of the timed runs of one query, in ms; -1 on error.
@param[in,out]  engine  database engine
@param[in]      sql     SQL statement to check or execute
@param[in]      runs    number of timed runs, excluding warm-up
@return median execution-and-fetch time in milliseconds; -1 on query failure */
static double bench_time_engine(engine_t *engine, const std::string &sql,
                                int runs) {
  std::vector<double> ms;

  for (int i = 0; i <= runs; ++i) {
    test_query_t result;
    std::string error;
    const uint64_t start = now_ns();

    if (query_all(*engine, sql, result, error) != DB_SUCCESS) {
      fprintf(stderr, "query failed: %s\n  %s\n", sql.c_str(), error.c_str());

      return (-1);
    }

    if (i > 0) {
      ms.push_back((double)(now_ns() - start) / 1e6);
    }
  }

  std::sort(ms.begin(), ms.end());

  return (ms[ms.size() / 2]);
}

/** Run a query on plain DuckDB tables, reading every chunk.
@param[in]  conn  DuckDB connection
@param[in]  sql   SQL statement to check or execute
@param[in]  runs  number of timed runs, excluding warm-up
@return the median time in milliseconds, without the first run */
static double bench_time_native(duckdb_connection conn, const std::string &sql,
                                int runs) {
  std::vector<double> ms;

  for (int i = 0; i <= runs; ++i) {
    duckdb_result result;
    const uint64_t start = now_ns();

    const bool ok = duckdb_query(conn, sql.c_str(), &result) == DuckDBSuccess;

    /* Read every chunk, as query_all() does. */
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
      fprintf(stderr, "native query failed: %s\n", sql.c_str());

      return (-1);
    }

    if (i > 0) {
      ms.push_back(elapsed);
    }
  }

  std::sort(ms.begin(), ms.end());

  return (ms[ms.size() / 2]);
}

/** Run a statement whose result is ignored.
@param[in]  conn  DuckDB connection
@param[in]  sql   SQL statement to check or execute */
static void bench_exec(duckdb_connection conn, const std::string &sql) {
  duckdb_result result;
  if (duckdb_query(conn, sql.c_str(), &result) != DuckDBSuccess) {
    fprintf(stderr, "native: %s\n  %s\n", sql.c_str(),
            duckdb_result_error(&result));
  }
  duckdb_destroy_result(&result);
}

/** Rewrite a metrics query to read the per-day view.
@param[in]  sql  SQL statement to check or execute
@return sql with "FROM metrics" replaced by "FROM days" */
static std::string bench_on_days(const std::string &sql) {
  std::string out = sql;
  const size_t at = out.find("FROM metrics");

  if (at != std::string::npos) {
    out.replace(at, strlen("FROM metrics"), "FROM days");
  }

  return (out);
}

/** Load the rows into plain DuckDB, one table and ten day tables, and time
the queries on both.
@param[in]   base       directory for scratch native DuckDB files
@param[in]   n          total number of generated rows
@param[in]   runs       number of timed query runs
@param[in]   queries    queries to benchmark
@param[out]  times      median query times on the single table, in
                        milliseconds
@param[out]  day_times  median query times on the daily layout, in
                        milliseconds
@return true if loading both layouts and every timed query succeed */
static bool bench_native(const char *base, uint64_t n, int runs,
                         const std::vector<bench_query_t> &queries,
                         std::vector<double> *times,
                         std::vector<double> *day_times) {
  const std::string dir = std::string(base) + "/native";
  mkdir(dir.c_str(), 0755);
  const std::string path = dir + "/native.duckdb";

  duckdb_database db = nullptr;
  duckdb_connection conn = nullptr;

  if (duckdb_open(path.c_str(), &db) != DuckDBSuccess ||
      duckdb_connect(db, &conn) != DuckDBSuccess) {
    fprintf(stderr, "cannot open native DuckDB\n");

    return (false);
  }

  duckdb_result result;
  duckdb_query(conn,
               "CREATE TABLE metrics (time TIMESTAMP, host VARCHAR, "
               "cpu DOUBLE, mem DOUBLE)",
               &result);
  duckdb_destroy_result(&result);

  duckdb_appender app = nullptr;
  duckdb_appender_create(conn, nullptr, "metrics", &app);

  char host[32];
  for (uint64_t i = 0; i < n; ++i) {
    const bench_row_t r = bench_row(i, n);
    duckdb_timestamp ts;
    ts.micros = r.time_us;
    snprintf(host, sizeof(host), "host-%d", r.host);

    duckdb_append_timestamp(app, ts);
    duckdb_append_varchar(app, host);
    duckdb_append_double(app, r.cpu);
    duckdb_append_double(app, r.mem);
    duckdb_appender_end_row(app);
  }

  duckdb_appender_destroy(&app);

  /* The same rows split by UTC day, as PulseTen stores them. */
  std::string view = "CREATE VIEW days AS ";

  for (int d = 0; d < 10; ++d) {
    const std::string table = "d" + std::to_string(d);
    const std::string from = timestamp_literal(T0 + d * DAY);
    const std::string to = timestamp_literal(T0 + (d + 1) * DAY);

    bench_exec(conn, "CREATE TABLE " + table +
                         " (time TIMESTAMP, host VARCHAR, cpu DOUBLE, "
                         "mem DOUBLE, CHECK (time >= " +
                         from + " AND time < " + to + "))");
    bench_exec(conn, "INSERT INTO " + table +
                         " SELECT * FROM metrics WHERE time >= " + from +
                         " AND time < " + to);

    view +=
        (d > 0 ? " UNION ALL " : "") + std::string("SELECT * FROM ") + table;
  }

  bench_exec(conn, view);

  for (size_t q = 0; q < queries.size(); ++q) {
    times->push_back(bench_time_native(conn, queries[q].sql, runs));
    day_times->push_back(
        bench_time_native(conn, bench_on_days(queries[q].sql), runs));
  }

  duckdb_disconnect(&conn);
  duckdb_close(&db);
  test_remove_dir(dir.c_str());

  return (true);
}

/** Load with a share of the newest rows left in the memtable, then time the
queries.
@param[in]   base         directory for the scratch PulseTen database
@param[in]   n            total number of generated rows
@param[in]   runs         number of timed query runs
@param[in]   hot_percent  percentage of newest rows to leave unflushed
@param[in]   queries      queries to benchmark
@param[out]  times        median query times in milliseconds
@return true if loading, flushing and every timed query succeed */
static bool bench_engine(const char *base, uint64_t n, int runs,
                         int hot_percent,
                         const std::vector<bench_query_t> &queries,
                         std::vector<double> *times) {
  char dir[1024];
  snprintf(dir, sizeof(dir), "%s/hot-%d", base, hot_percent);
  mkdir(dir, 0755);

  engine_options_t options;
  options.background_flush = false;

  engine_t *engine = nullptr;
  std::string error;
  if (engine_open(dir, &options, &engine, nullptr, error) != DB_SUCCESS) {
    fprintf(stderr, "engine_open failed: %s\n", error.c_str());

    return (false);
  }

  std::vector<std::string> hosts;
  for (int h = 0; h < 100; ++h) {
    hosts.push_back("host-" + std::to_string(h));
  }

  const uint64_t cold = n - n * (uint64_t)hot_percent / 100;
  const uint64_t load_start = now_ns();

  bool ok = bench_load(engine, 0, cold, n, hosts);

  if (ok && cold > 0) {
    engine_flush_info_t info;
    ok = engine_flush(*engine, info, error) == DB_SUCCESS;
  }

  if (ok) {
    ok = bench_load(engine, cold, n, n, hosts);
  }

  const double load_s = (double)(now_ns() - load_start) / 1e9;

  /* Every row must be visible exactly once before timing. */
  if (ok) {
    test_query_t check;
    ok = query_all(*engine, "SELECT count(*) FROM metrics", check, error) ==
             DB_SUCCESS &&
         check.rows[0][0].i == (int64_t)n;
  }

  if (ok) {
    memtable_stats_t stats;
    engine_get_memtable_stats(*engine, stats);
    printf("  hot %3d%%: loaded in %.1f s, %llu rows in memtable\n",
           hot_percent, load_s, (unsigned long long)stats.live_rows);

    for (size_t q = 0; q < queries.size(); ++q) {
      times->push_back(bench_time_engine(engine, queries[q].sql, runs));
    }
  }

  (void)engine_close(engine);
  test_remove_dir(dir);

  return (ok);
}

/** Run the benchmark.
@param[in]  argc  number of command-line arguments, including the program name
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s <dir> [rows] [runs]\n", argv[0]);

    return (2);
  }

  const uint64_t n = argc > 2 ? (uint64_t)atoll(argv[2]) : 1000000;
  const int runs = argc > 3 ? atoi(argv[3]) : 7;
  const int hot[] = {0, 10, 50, 100};
  const int n_hot = sizeof(hot) / sizeof(hot[0]);

  const std::vector<bench_query_t> queries = bench_queries();
  std::vector<std::vector<double>> times(n_hot + 1);
  std::vector<double> day_times;

  printf("%llu rows over 10 UTC days, median of %d runs\n",
         (unsigned long long)n, runs);

  /* Engine runs first: opening it registers DuckDB's built-in functions
  for the native baseline as well. */
  for (int h = 0; h < n_hot; ++h) {
    if (!bench_engine(argv[1], n, runs, hot[h], queries, &times[h + 1])) {
      return (1);
    }
  }

  if (!bench_native(argv[1], n, runs, queries, &times[0], &day_times)) {
    return (1);
  }

  printf("\n%-28s %10s %10s", "query (ms)", "native", "native-10");
  for (int h = 0; h < n_hot; ++h) {
    printf("   hot %3d%%", hot[h]);
  }
  printf("\n");

  for (size_t q = 0; q < queries.size(); ++q) {
    printf("%-28s %10.2f %10.2f", queries[q].label, times[0][q], day_times[q]);
    for (int h = 0; h < n_hot; ++h) {
      printf(" %10.2f", times[h + 1][q]);
    }
    printf("\n");
  }

  return (0);
}
