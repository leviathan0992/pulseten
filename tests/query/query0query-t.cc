/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0query-t.cc
 *    Tests for SQL over stored and unflushed writes: results from the
 *    memtable, from DuckDB and from both, columns added over time, filters
 *    reaching the partition scans, refused statements, result types, and
 *    consistent snapshots while writes and flushes run.
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "duck0duck.h"
#include "query0priv.h"
#include "test0engine.h"
#include "ut0time.h"

/** Select engine options with background flushing disabled.
@return options without the background thread */
static const engine_options_t *manual() {
  static engine_options_t options;
  options.background_flush = false;

  return (&options);
}

/** Run a query that must succeed.
@param[in,out]  engine  database engine
@param[in]      sql     SQL statement to check or execute
@return the query result; a failed call is recorded as a test failure */
static test_query_t run(engine_t *engine, const std::string &sql) {
  test_query_t result;
  std::string error;

  const dberr_t err = query_all(*engine, sql, result, error);
  CHECK(err == DB_SUCCESS);
  if (err != DB_SUCCESS) {
    fprintf(stderr, "query failed: %s\n  %s\n", sql.c_str(), error.c_str());
  }

  return (result);
}

/** Execute a query and read its single BIGINT result.
@param[in,out]  engine  database engine
@param[in]      sql     SQL statement to check or execute
@return the single BIGINT a query returns */
static int64_t run_int(engine_t *engine, const std::string &sql) {
  test_query_t r = run(engine, sql);

  if (r.rows.size() != 1 || r.rows[0].size() != 1 || r.rows[0][0].is_null) {
    CHECK(false);

    return (-1);
  }

  return (r.rows[0][0].i);
}

/** Write cpu points: host tag, usage DOUBLE; time T0 + i seconds.
@param[in,out]  engine  database engine
@param[in]      host    host tag value
@param[in]      from    first generated point index, inclusive
@param[in]      to      first point index to exclude */
static void write_cpu(engine_t *engine, const char *host, int from, int to) {
  for (int i = from; i < to; ++i) {
    ingest_tag_t t[] = {tag("host", host)};
    ingest_field_t f[] = {field_double("usage", i * 0.5)};
    ingest_point_t p = point(T0 + (int64_t)i * 1000000, t, 1, f, 1);
    batch_seq_t seq = 0;

    CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq, test_error) ==
          DB_SUCCESS);
  }
}

/** Flush, checking that it succeeds.
@param[in,out]  engine  database engine */
static void flush(engine_t *engine) {
  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
}

/** Queries give the same answers over the memtable, DuckDB, or both. */
static void test_hot_cold_and_both() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  const std::string count = "SELECT count(*) FROM cpu";
  const std::string by_host =
      "SELECT host, count(*), sum(usage) FROM cpu GROUP BY host ORDER BY host";

  /* Only in the memtable. */
  write_cpu(engine, "a", 0, 10);
  CHECK(run_int(engine, count) == 10);

  /* Only in DuckDB. */
  flush(engine);
  CHECK(run_int(engine, count) == 10);

  /* Both: the same answer as if everything were in one place. */
  write_cpu(engine, "b", 10, 15);
  CHECK(run_int(engine, count) == 15);

  test_query_t r = run(engine, by_host);
  CHECK(r.rows.size() == 2);
  CHECK(r.columns[0].type == COL_VARCHAR && r.columns[1].type == COL_BIGINT);
  CHECK(r.rows[0][0].s == "a" && r.rows[0][1].i == 10);
  CHECK(r.rows[0][2].d == 22.5);
  CHECK(r.rows[1][0].s == "b" && r.rows[1][1].i == 5 && r.rows[1][2].d == 30);

  /* Names match case-insensitively; time comes back in microseconds. */
  CHECK(run_int(engine, "SELECT count(*) FROM CPU WHERE Host = 'b'") == 5);
  r = run(engine, "SELECT max(time) FROM cpu");
  CHECK(r.columns[0].type == COL_TIMESTAMP);
  CHECK(r.rows[0][0].i == T0 + 14 * 1000000LL);

  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Rows written before a column was added read NULL for it, stored or not. */
static void test_columns_added_later() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* Stored with one column, then a column added in the memtable. */
  write_cpu(engine, "a", 0, 3);
  flush(engine);

  ingest_tag_t t[] = {tag("host", "a")};
  ingest_field_t f[] = {field_double("usage", 1), field_int("cores", 8)};
  ingest_point_t p = point(T0 + 100000000, t, 1, f, 2);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq, test_error) == DB_SUCCESS);

  /* Stored rows read NULL for the new column. */
  CHECK(run_int(engine, "SELECT count(*) FROM cpu WHERE cores IS NULL") == 3);
  CHECK(run_int(engine, "SELECT max(cores) FROM cpu") == 8);

  /* After flushing, the old partition lacks the column; still NULL. */
  flush(engine);
  CHECK(run_int(engine, "SELECT count(*) FROM cpu WHERE cores IS NULL") == 3);
  CHECK(run_int(engine, "SELECT count(cores) FROM cpu") == 1);

  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Options for one-day partitions without a background flush.
@return options with background flushing disabled */
static const engine_options_t *daily() {
  static engine_options_t options;
  options.background_flush = false;
  options.partition_days = 1;

  return (&options);
}

/** Read the execution plan of a query.
@param[in,out]  engine  database engine
@param[in]      sql     SQL statement to check or execute
@return the plan of a query */
static std::string explain(engine_t *engine, const std::string &sql) {
  std::string plan;
  std::string error;

  CHECK(engine_explain(*engine, sql, plan, error) == DB_SUCCESS);
  if (getenv("PT_SHOW_PLAN") != nullptr) {
    printf("%s\n%s\n", sql.c_str(), plan.c_str());
  }

  return (plan);
}

/** A time filter leaves the days it excludes out of the generated SQL. */
static void test_filter_pushdown() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, daily(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* Two UTC days in DuckDB, one more in the memtable. */
  write_cpu(engine, "a", 0, 5);
  for (int i = 0; i < 5; ++i) {
    ingest_field_t f[] = {field_double("usage", 1)};
    ingest_point_t p = point(T0 + DAY + i, nullptr, 0, f, 1);
    batch_seq_t seq = 0;
    CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq, test_error) ==
          DB_SUCCESS);
  }
  flush(engine);
  write_cpu(engine, "b", 0, 2);

  const std::string sql =
      "SELECT count(*) FROM cpu WHERE time >= TIMESTAMP '2026-01-02'";
  CHECK(run_int(engine, sql) == 5);

  /* The day the filter excludes is left out of the generated SQL; the
  other is scanned for the time column only, and the memtable scan is
  filtered above it. */
  const std::string plan = explain(engine, sql);

  const std::string day1 = storage_partition_name(1, ut_utc_day(T0));
  const std::string day2 = storage_partition_name(1, ut_utc_day(T0 + DAY));

  CHECK(plan.find(day1) == std::string::npos);
  CHECK(plan.find(day2) != std::string::npos);
  CHECK(plan.find("Projections: c0") != std::string::npos);
  CHECK(plan.find("_PT_HOT_SCAN") != std::string::npos);
  CHECK(plan.find("Filter") != std::string::npos);

  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Build the physical partition name of a day relative to T0.
@param[in]  d  day offset from T0 for stored table id 1
@return the partition name of day d after T0, table 1 */
static std::string day_name(int d) {
  return (storage_partition_name(1, ut_utc_day(T0) + d));
}

/** Pruning by time conditions never changes the rows a query returns. */
static void test_partition_pruning() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, daily(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* Ten days in DuckDB at 12:00 and 23:00, two more days in the
  memtable. */
  for (int d = 0; d < 12; ++d) {
    if (d == 10) {
      flush(engine);
    }

    for (int h = 12; h <= 23; h += 11) {
      ingest_field_t f[] = {field_int("v", d)};
      ingest_point_t p =
          point(T0 + d * DAY + h * 3600000000LL, nullptr, 0, f, 1);
      batch_seq_t seq = 0;
      CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq, test_error) ==
            DB_SUCCESS);
    }
  }

  /* Every condition gives the same rows with and without pruning: NOT NOT
  hides the condition from PulseTen but not from DuckDB. */
  const char *conds[] = {
      "time >= '2026-01-03'",
      "time > TIMESTAMP '2026-01-03 12:00:00'",
      "time < TIMESTAMP '2026-01-03 12:00:00'",
      "time <= DATE '2026-01-03'",
      "time < DATE '2026-01-03'",
      "time = TIMESTAMP '2026-01-05 23:00:00'",
      "time BETWEEN '2026-01-02 23:00:00' AND '2026-01-04 12:00:00'",
      "TIMESTAMP '2026-01-04' <= time AND v >= 0",
      "cpu.time >= '2026-01-08' AND time < '2026-01-12'",
      "time > '2026-01-05' AND time < '2026-01-03'",
      "time >= '2026-01-05' OR time < '2026-01-02'",
      "time > TIMESTAMP '2026-01-04' - INTERVAL 1 DAY",
      "time >= '2027-01-01'",
      "time < '2025-01-01'",
  };

  for (size_t i = 0; i < sizeof(conds) / sizeof(conds[0]); ++i) {
    const std::string c = conds[i];
    const int64_t pruned =
        run_int(engine, "SELECT count(*) FROM cpu WHERE " + c);
    const int64_t full =
        run_int(engine, "SELECT count(*) FROM cpu WHERE NOT NOT (" + c + ")");

    CHECK(pruned == full);
    if (pruned != full) {
      fprintf(stderr, "pruning changed %s: %lld, not %lld\n", c.c_str(),
              (long long)pruned, (long long)full);
    }
  }

  /* A DATE compares as midnight, not as the whole day: days 0 and 1. */
  CHECK(run_int(engine,
                "SELECT count(*) FROM cpu WHERE time <= DATE '2026-01-03'") ==
        4);

  /* Statements that read the table more than once. */
  const char *stmts[] = {
      "SELECT count(*) FROM (SELECT * FROM cpu WHERE time < '2026-01-03' "
      "UNION ALL SELECT * FROM cpu WHERE time >= '2026-01-09')",
      "SELECT count(*) FROM cpu a JOIN cpu b ON a.v = b.v "
      "WHERE a.time >= '2026-01-06'",
      "WITH x AS (SELECT * FROM cpu WHERE time >= '2026-01-06') "
      "SELECT count(*) FROM x WHERE time < '2026-01-08'",
      "SELECT count(*) FROM cpu WHERE v IN "
      "(SELECT v FROM cpu WHERE time >= '2026-01-10')",
      "SELECT count(*) FROM cpu c WHERE EXISTS (SELECT 1 FROM cpu d "
      "WHERE d.v = c.v AND c.time >= '2026-01-07')",
  };
  const int64_t expected[] = {4 + 8, 7 * 2 * 2, 4, 6, 12};

  for (size_t i = 0; i < sizeof(stmts) / sizeof(stmts[0]); ++i) {
    const int64_t n = run_int(engine, stmts[i]);

    CHECK(n == expected[i]);
    if (n != expected[i]) {
      fprintf(stderr, "%s: %lld, not %lld\n", stmts[i], (long long)n,
              (long long)expected[i]);
    }
  }

  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Look up the conservative time bounds inferred for a table.
@param[in]   sql    SQL statement to check or execute
@param[in]   table  logical table name
@param[out]  range  inferred inclusive time bounds, when present
@return the range query_find_ranges() gives a table, and whether it gives one */
static bool range_of(const std::string &sql, const char *table,
                     query_range_t *range) {
  query_ranges_t ranges;
  query_find_ranges(sql, ranges);

  query_ranges_t::const_iterator it = ranges.find(table);
  if (it == ranges.end()) {
    return (false);
  }

  *range = it->second;

  return (true);
}

/** Microseconds of a UTC time "YYYY-MM-DD HH:MM:SS" relative to T0. */
#define AT(d, h) (T0 + (int64_t)(d) * DAY + (int64_t)(h) * 3600000000LL)

/** The time ranges extracted from statements, and the partitions they keep. */
static void test_time_ranges() {
  query_range_t r;

  CHECK(range_of("SELECT * FROM cpu WHERE time >= '2026-01-03'", "cpu", &r));
  CHECK(r.lo == AT(2, 0) && r.hi == INT64_MAX);

  CHECK(
      range_of("SELECT * FROM CPU c WHERE c.time > TIMESTAMP "
               "'2026-01-03 01:00:00' AND v = 1 AND time < DATE '2026-01-05'",
               "cpu", &r));
  CHECK(r.lo == AT(2, 1) + 1 && r.hi == AT(4, 0) - 1);

  CHECK(
      range_of("SELECT * FROM cpu WHERE '2026-01-02' <= time AND "
               "time BETWEEN '2026-01-01' AND '2026-01-04'",
               "cpu", &r));
  CHECK(r.lo == AT(1, 0) && r.hi == AT(3, 0));

  CHECK(range_of("SELECT * FROM cpu WHERE time = '2026-01-02 05:00:00'", "cpu",
                 &r));
  CHECK(r.lo == AT(1, 5) && r.hi == AT(1, 5));

  /* Contradictory: empty. */
  CHECK(
      range_of("SELECT * FROM cpu WHERE time > '2026-01-05' AND "
               "time < '2026-01-03'",
               "cpu", &r));
  CHECK(r.lo > r.hi);

  /* Two references: the hull, here every time; an empty one adds
  nothing. */
  CHECK(range_of("SELECT * FROM cpu WHERE time < '2026-01-02' UNION ALL "
                 "SELECT * FROM cpu WHERE time >= '2026-01-10' UNION ALL "
                 "SELECT * FROM cpu WHERE time > '2026-02-01' AND "
                 "time < '2026-01-01'",
                 "cpu", &r) == false);
  CHECK(
      range_of("SELECT * FROM cpu WHERE time < '2026-01-02' UNION ALL "
               "SELECT * FROM cpu WHERE time >= '2026-01-10' AND "
               "time < '2026-01-03'",
               "cpu", &r));
  CHECK(r.lo == INT64_MIN && r.hi == AT(1, 0) - 1);

  CHECK(
      range_of("WITH x AS (SELECT * FROM cpu WHERE time >= '2026-01-06') "
               "SELECT * FROM x, (SELECT * FROM cpu WHERE "
               "time >= '2026-01-08') y",
               "cpu", &r));
  CHECK(r.lo == AT(5, 0) && r.hi == INT64_MAX);

  CHECK(
      range_of("SELECT * FROM mem WHERE v IN (SELECT v FROM cpu WHERE "
               "time BETWEEN '2026-01-02' AND '2026-01-03')",
               "cpu", &r));
  CHECK(r.lo == AT(1, 0) && r.hi == AT(2, 0));

  /* No range: OR, NOT, expressions, other qualifiers, a reference without
  a condition, a join in FROM. */
  const char *open[] = {
      "SELECT * FROM cpu",
      "SELECT * FROM cpu WHERE time >= '2026-01-09' OR v = 0",
      "SELECT * FROM cpu WHERE NOT (time < '2026-01-09')",
      "SELECT * FROM cpu WHERE time > now() - INTERVAL 1 DAY",
      "SELECT * FROM cpu WHERE time::DATE = '2026-01-02'",
      "SELECT * FROM cpu WHERE time >= 5",
      "SELECT * FROM cpu WHERE time >= 'not a time'",
      "SELECT * FROM cpu WHERE time >= TRY_CAST('2026-01-02' AS TIMESTAMP)",
      "SELECT * FROM cpu a WHERE b.time >= '2026-01-02'",
      ("SELECT * FROM cpu c WHERE EXISTS (SELECT 1 FROM mem d WHERE "
       "c.time >= '2026-01-02')"),
      "SELECT (SELECT count(*) FROM cpu) FROM cpu WHERE time >= '2026-01-09'",
      "SELECT * FROM cpu JOIN mem USING (time) WHERE time >= '2026-01-09'",
  };

  for (size_t i = 0; i < sizeof(open) / sizeof(open[0]); ++i) {
    const bool bounded = range_of(open[i], "cpu", &r);

    CHECK(!bounded);
    if (bounded) {
      fprintf(stderr, "bounded: %s\n", open[i]);
    }
  }

  /* The generated SQL holds only the overlapping partitions. */
  query_table_t table;
  table.schema.name = "cpu";

  col_def_t time_col;
  time_col.name = "time";
  time_col.role = COL_ROLE_TIME;
  time_col.type = COL_TIMESTAMP;
  table.schema.cols.push_back(time_col);

  for (int d = 0; d < 5; ++d) {
    storage_partition_t p;
    p.table_id = 1;
    p.first_day = ut_utc_day(T0) + d;
    p.phys_name = day_name(d);
    p.n_cols = 1;
    table.partitions.push_back(p);
  }

  r.lo = AT(1, 23);
  r.hi = AT(3, 0);
  std::string sql = query_table_sql(1, "cpu", table, 1, r);
  CHECK(sql.find(day_name(0)) == std::string::npos);
  CHECK(sql.find(day_name(1)) != std::string::npos);
  CHECK(sql.find(day_name(2)) != std::string::npos);
  CHECK(sql.find(day_name(3)) != std::string::npos);
  CHECK(sql.find(day_name(4)) == std::string::npos);

  /* Nothing overlaps and nothing is in the memtable: typed and empty. */
  r.lo = AT(10, 0);
  r.hi = INT64_MAX;
  sql = query_table_sql(1, "cpu", table, 1, r);
  CHECK(sql.find("_pt_") == std::string::npos);
  CHECK(sql.find("LIMIT 0") != std::string::npos);
}

/** Partitions follow the span chosen at creation, which an existing directory
keeps. */
static void test_partition_span() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* 90 days, one point a day: 30-day partitions aligned to 1970-01-01. */
  for (int d = 0; d < 90; ++d) {
    ingest_field_t f[] = {field_int("v", d)};
    ingest_point_t p = point(T0 + d * DAY, nullptr, 0, f, 1);
    batch_seq_t seq = 0;
    CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq, test_error) ==
          DB_SUCCESS);
  }
  flush(engine);

  const storage_state_t *state =
      storage_get_state(*(engine_get_storage(*engine)));
  CHECK(state->partition_days == 30);

  const int64_t first = ut_utc_day(T0);
  const size_t expected = (size_t)((first + 89) / 30 - first / 30 + 1);
  CHECK(state->partitions.size() == expected);

  std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
      it;
  for (it = state->partitions.begin(); it != state->partitions.end(); ++it) {
    CHECK(it->second.first_day % 30 == 0);
  }

  CHECK(run_int(engine, "SELECT count(*) FROM cpu") == 90);
  CHECK(run_int(engine,
                "SELECT count(*) FROM cpu WHERE time >= '2026-02-15' "
                "AND time < '2026-03-01'") == 14);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* An existing directory keeps its span. */
  engine_options_t options;
  options.background_flush = false;
  options.partition_days = 7;

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);
  state = storage_get_state(*(engine_get_storage(*engine)));
  CHECK(state->partition_days == 30 && state->partitions.size() == expected);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* A span out of range is refused. */
  test_dir_t other;
  options.partition_days = 0;
  CHECK(engine_open(other.path, &options, &engine, nullptr, test_error) ==
        DB_INVALID_ARGUMENT);
  options.partition_days = STORAGE_MAX_PARTITION_DAYS + 1;
  CHECK(engine_open(other.path, &options, &engine, nullptr, test_error) ==
        DB_INVALID_ARGUMENT);
}

/** Statements that write or read outside the tables are refused; ordinary
reads are allowed. */
static void test_refused() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);
  write_cpu(engine, "a", 0, 2);

  const char *refused[] = {
      "DELETE FROM cpu",
      "SELECT 1; SELECT 2",
      "SELECT * FROM _pt_state",
      "SELECT * FROM main.cpu",
      "SELECT * FROM information_schema.tables",
      "SELECT * FROM duckdb_tables()",
      "SELECT * FROM duckdb_tables",
      "SELECT * FROM sqlite_master",
      "SELECT * FROM read_csv('/etc/passwd')",
      "SELECT * FROM '/etc/passwd'",
      "SELECT * FROM cpu WHERE host IN (SELECT name FROM _pt_tables)",
      "SELECT * FROM _pt_hot_scan(1, 'cpu')",
      "ATTACH 'x.db'",
      /* Syntax errors and statement counts, found by the C API. */
      "SELEC 1",
      "SELECT (",
      "SELECT 'unterminated",
      "SELECT 1 FROM cpu WHERE",
      "",
      "   ",
      "SELECT 1; SELECT 2",
  };

  for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); ++i) {
    test_query_t r;
    std::string error;

    const dberr_t err = query_all(*engine, refused[i], r, error);
    CHECK(err == DB_INVALID_ARGUMENT);
    CHECK(!error.empty());
    if (err != DB_INVALID_ARGUMENT) {
      fprintf(stderr, "not refused: %s\n", refused[i]);
    }
  }

  /* An unknown table is a SQL error, not a refusal. */
  test_query_t r;
  std::string error;
  CHECK(query_all(*engine, "SELECT * FROM nothing", r, error) == DB_ERROR);

  /* Allowed: subqueries, CTEs, joins of tables, VALUES, no FROM. */
  CHECK(run_int(engine,
                "WITH x AS (SELECT * FROM cpu) SELECT count(*) FROM x") == 2);
  CHECK(run_int(engine,
                "SELECT count(*) FROM cpu a JOIN cpu b ON a.time = b.time") ==
        2);
  CHECK(run_int(engine, "SELECT (SELECT count(*) FROM cpu) + 1") == 3);
  CHECK(run_int(engine, "SELECT count(*) FROM (VALUES (1), (2)) t(x)") == 2);

  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** PIVOT expressions obey the same read boundary as ordinary SELECTs. */
static void test_pivot_references() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);
  write_cpu(engine, "a", 0, 2);

  const std::string path = dir.file("pivot.csv");
  FILE *file = fopen(path.c_str(), "w");
  CHECK(file != nullptr);
  if (file != nullptr) {
    fputs("suffix\nx\n", file);
    CHECK(fclose(file) == 0);
  }

  const std::string refused[] = {
      "SELECT * FROM cpu PIVOT(sum(usage) FOR "
      "(host || (SELECT '' FROM _pt_state)) IN ('a'))",
      "SELECT * FROM cpu PIVOT(sum(usage) FOR "
      "(host || (SELECT suffix FROM read_csv(" +
          duck_literal(path) + "))) IN ('a'))",
      "SELECT * FROM cpu UNPIVOT (val FOR col IN "
      "((usage + (SELECT flushed_seq FROM _pt_state)) AS x))",
  };

  for (const std::string &sql : refused) {
    test_query_t result;
    std::string error;
    CHECK(query_all(*engine, sql, result, error) == DB_INVALID_ARGUMENT);
    CHECK(!error.empty());
  }

  /* An unbounded reference in FOR must also prevent another occurrence
  of cpu from pruning away the rows needed by that subquery. */
  const std::string pruning =
      "SELECT count(*) FROM cpu WHERE time >= '2027-01-01' UNION ALL "
      "SELECT coalesce(aa, 0)::BIGINT FROM (SELECT * FROM "
      "(VALUES ('a', 1)) t(host, usage) PIVOT(sum(usage) FOR "
      "(host || (SELECT min(host) FROM cpu)) IN ('aa')))";
  const std::string allowed =
      "SELECT count(*) FROM (SELECT * FROM cpu PIVOT(sum(usage) FOR "
      "(host || (SELECT '')) IN ('a')))";

  for (int pass = 0; pass < 2; ++pass) {
    if (pass == 1) {
      flush(engine);
    }

    CHECK(run_int(engine, allowed) == 2);
    CHECK(run_int(engine,
                  "SELECT count(*) FROM cpu UNPIVOT "
                  "(val FOR col IN (usage))") == 2);
    test_query_t result = run(engine, pruning);
    CHECK(result.rows.size() == 2);
    if (result.rows.size() == 2) {
      CHECK(result.rows[0][0].i == 0);
      CHECK(result.rows[1][0].i == 1);
    }
  }

  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Result columns map to the returned types; others are refused. */
static void test_result_types() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);
  write_cpu(engine, "a", 0, 2);

  test_query_t r = run(engine,
                       "SELECT sum(1), avg(usage), true, 'x', NULL::BIGINT, "
                       "1.25::DECIMAL(10, 2), DATE '1970-01-02', "
                       "42::UTINYINT FROM cpu");
  CHECK(r.rows.size() == 1 && r.columns.size() == 8);

  if (r.rows.size() == 1 && r.columns.size() == 8) {
    const std::vector<query_value_t> &v = r.rows[0];
    CHECK(r.columns[0].type == COL_BIGINT && v[0].i == 2);
    CHECK(r.columns[1].type == COL_DOUBLE && v[1].d == 0.25);
    CHECK(r.columns[2].type == COL_BOOLEAN && v[2].b);
    CHECK(r.columns[3].type == COL_VARCHAR && v[3].s == "x");
    CHECK(v[4].is_null);
    CHECK(r.columns[5].type == COL_DOUBLE && v[5].d == 1.25);
    CHECK(r.columns[6].type == COL_TIMESTAMP && v[6].i == 86400000000LL);
    CHECK(v[7].i == 42);
  }

  /* A type that cannot be returned, and a value that does not fit. */
  std::string error;
  CHECK(query_all(*engine, "SELECT [1, 2]", r, error) == DB_INVALID_ARGUMENT);
  CHECK(query_all(*engine, "SELECT 18446744073709551615::UBIGINT", r, error) ==
        DB_INVALID_ARGUMENT);

  CHECK(engine_close(engine) == DB_SUCCESS);
}

struct writer_arg_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int thread_no{0};

  /** Writes to make. */
  int count{0};

  /** Calls that failed. */
  int errors{0};
};

/** Write count ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  for (int i = 0; i < arg->count; ++i) {
    if (write_id(arg->engine, arg->thread_no * 100000 + i) != DB_SUCCESS) {
      arg->errors++;
    }
  }
}

struct reader_arg_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *stop{nullptr};

  /** Protects *stop. */
  std::mutex *mutex{nullptr};

  /** Queries run. */
  int queries{0};

  /** Calls that failed. */
  int errors{0};
};

/** Query over and over while writes and flushes run: each answer must count
every write once, and counts never go back.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void reader_main(reader_arg_t *arg) {
  int64_t last = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->mutex);
      if (*arg->stop) {
        break;
      }
    }

    test_query_t r;
    std::string error;

    if (query_all(*arg->engine, "SELECT count(*), count(DISTINCT id) FROM k", r,
                  error) != DB_SUCCESS) {
      /* Before the first write the table does not exist yet. */
      if (error.find("does not exist") == std::string::npos) {
        fprintf(stderr, "reader: %s\n", error.c_str());
        arg->errors++;
      }
      continue;
    }

    const int64_t rows = r.rows[0][0].i;
    const int64_t distinct = r.rows[0][1].i;

    if (rows != distinct || rows < last) {
      arg->errors++;
    }

    last = rows;
    arg->queries++;
  }
}

/** Queries during writes and frequent flushes always see a consistent
snapshot. */
static void test_snapshot_under_load() {
  test_dir_t dir;

  /* Background flushes every few milliseconds, so queries keep starting
  around flush commits. */
  engine_options_t options;
  options.flush_interval_ms = 3;

  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  bool stop = false;
  std::mutex mutex;
  std::vector<reader_arg_t> readers(2);
  std::vector<std::thread> reader_threads;

  for (size_t i = 0; i < readers.size(); ++i) {
    readers[i].engine = engine;
    readers[i].stop = &stop;
    readers[i].mutex = &mutex;
    reader_threads.push_back(std::thread(reader_main, &readers[i]));
  }

  const int threads = 4;
  const int count = 150;
  std::vector<writer_arg_t> writers(threads);
  std::vector<std::thread> writer_threads;

  for (int t = 0; t < threads; ++t) {
    writers[t].engine = engine;
    writers[t].thread_no = t;
    writers[t].count = count;
    writer_threads.push_back(std::thread(writer_main, &writers[t]));
  }

  for (int t = 0; t < threads; ++t) {
    writer_threads[t].join();
    CHECK(writers[t].errors == 0);
  }

  {
    std::lock_guard<std::mutex> lock(mutex);
    stop = true;
  }

  int queries = 0;
  for (size_t i = 0; i < readers.size(); ++i) {
    reader_threads[i].join();
    CHECK(readers[i].errors == 0);
    queries += readers[i].queries;
  }

  engine_stats_t stats;
  engine_get_stats(*engine, stats);
  printf("snapshot under load: %d queries during %llu background flushes\n",
         queries, (unsigned long long)stats.background_flushes);
  CHECK(queries > 0 && stats.background_flushes > 0);

  CHECK(run_int(engine, "SELECT count(DISTINCT id) FROM k") == threads * count);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_hot_cold_and_both();
  test_columns_added_later();
  test_filter_pushdown();
  test_partition_pruning();
  test_time_ranges();
  test_partition_span();
  test_refused();
  test_pivot_references();
  test_result_types();
  test_snapshot_under_load();

  return (test_result("query0query-t"));
}
