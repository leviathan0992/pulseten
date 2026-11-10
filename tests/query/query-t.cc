/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query-t.cc
 *    Tests for SQL over stored and unflushed writes: results from the
 *    memtable, from DuckDB and from both, columns added over time, filters
 *    reaching the partition scans, refused statements, result types, and
 *    consistent snapshots while writes and flushes run.
 *
 *-------------------------------------------------------------------------
 */

#include "query.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "duckdb_database.h"
#include "engine.h"
#include "ingest.h"
#include "query_priv.h"
#include "schema.h"
#include "storage.h"
#include "test_engine.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_time.h"

/** Select engine options with a long background interval and no capacity limit.
@return options with a long background interval and no capacity limit */
static const engine_options_t *deferred_flush() {
  static engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;

  return (&options);
}

/** Run a query that must succeed.
@return the query result; a failed call is recorded as a test failure */
static test_query_t run(engine_t &engine, const std::string &sql) {
  db_status_t status;

  test_query_t result;

  status = query_all(engine, sql, result);
  const db_status_t err = status;
  CHECK(err.ok());
  if (!err.ok()) {
    fprintf(stderr, "query failed: %s\n  %s\n", sql.c_str(),
            status.message().c_str());
  }

  return (result);
}

/** Execute a query and read its single BIGINT result.
@return the single BIGINT a query returns */
static int64_t run_int(engine_t &engine, const std::string &sql) {
  test_query_t r = run(engine, sql);

  if (r.m_rows.size() != 1 || r.m_rows[0].size() != 1 ||
      r.m_rows[0][0].m_is_null) {
    CHECK(false);

    return (-1);
  }

  return (r.m_rows[0][0].m_i);
}

/** Write cpu points: host tag, usage DOUBLE; time T0 + i seconds.
@param[in]  from  first generated point index, inclusive */
static void write_cpu(engine_t &engine, const char *host, int from, int to) {
  for (int i = from; i < to; ++i) {
    ingest_tag_t t[] = {tag("host", host)};
    ingest_field_t f[] = {field_double("usage", i * 0.5)};
    ingest_point_t p =
        point(T0 + static_cast<int64_t>(i) * 1000000, t, 1, f, 1);
    batch_seq_t seq = 0;

    CHECK(engine_write(engine, "cpu", 3, &p, 1, seq).ok());
  }
}

/** Flush, checking that it succeeds. */
static void flush(engine_t &engine) {
  engine_flush_info_t info;
  CHECK(engine_flush(engine, info).ok());
}

/** Installation returns an owned refusal or an empty successful status,
and leaves the borrowed storage connection usable. */
static void test_scan_installation() {
  db_status_t status;

  status = db_status_t(DB_ERROR, "stale error");
  status = query_install_scans(nullptr);
  CHECK(status.code() == DB_ERROR);
  CHECK(status.message() == "cannot register the memtable scan");

  test_dir_t dir;
  storage_t *storage = nullptr;
  status = storage_open(dir.m_path, STORAGE_DEFAULT_PARTITION_DAYS, &storage);
  const db_status_t err = status;
  CHECK(err.ok());
  if (!err.ok()) {
    return;
  }

  status = db_status_t(DB_ERROR, "stale error");
  status = query_install_scans(storage_connection(*storage));
  CHECK(status.ok());
  CHECK(status.message().empty());

  int64_t value = 0;
  CHECK(storage_query_int(*storage, "SELECT 42", value).ok());
  CHECK(value == 42);
  storage_close(storage);
}

/** Queries give the same answers over the memtable, DuckDB, or both. */
static void test_hot_cold_and_both() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  const std::string count = "SELECT count(*) FROM cpu";
  const std::string by_host =
      "SELECT host, count(*), sum(usage) FROM cpu GROUP BY host ORDER BY host";

  /* Only in the memtable. */
  write_cpu(*engine, "a", 0, 10);
  CHECK(run_int(*engine, count) == 10);

  /* Only in DuckDB. */
  flush(*engine);
  CHECK(run_int(*engine, count) == 10);

  /* Both: the same answer as if everything were in one place. */
  write_cpu(*engine, "b", 10, 15);
  CHECK(run_int(*engine, count) == 15);

  test_query_t r = run(*engine, by_host);
  CHECK(r.m_rows.size() == 2);
  CHECK(r.m_columns[0].m_type == col_type_t::VARCHAR &&
        r.m_columns[1].m_type == col_type_t::BIGINT);
  CHECK(r.m_rows[0][0].m_s == "a" && r.m_rows[0][1].m_i == 10);
  CHECK(r.m_rows[0][2].m_d == 22.5);
  CHECK(r.m_rows[1][0].m_s == "b" && r.m_rows[1][1].m_i == 5 &&
        r.m_rows[1][2].m_d == 30);

  /* Names match case-insensitively; time comes back in microseconds. */
  CHECK(run_int(*engine, "SELECT count(*) FROM CPU WHERE Host = 'b'") == 5);
  r = run(*engine, "SELECT max(time) FROM cpu");
  CHECK(r.m_columns[0].m_type == col_type_t::TIMESTAMP);
  CHECK(r.m_rows[0][0].m_i == T0 + 14 * 1000000LL);

  CHECK(engine_close(engine).ok());
}

/** Rows written before a column was added read NULL for it, stored or not. */
static void test_columns_added_later() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  /* Stored with one column, then a column added in the memtable. */
  write_cpu(*engine, "a", 0, 3);
  flush(*engine);

  ingest_tag_t t[] = {tag("host", "a")};
  ingest_field_t f[] = {field_double("usage", 1), field_int("cores", 8)};
  ingest_point_t p = point(T0 + 100000000, t, 1, f, 2);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq).ok());

  /* Stored rows read NULL for the new column. */
  CHECK(run_int(*engine, "SELECT count(*) FROM cpu WHERE cores IS NULL") == 3);
  CHECK(run_int(*engine, "SELECT max(cores) FROM cpu") == 8);

  /* After flushing, the old partition lacks the column; still NULL. */
  flush(*engine);
  CHECK(run_int(*engine, "SELECT count(*) FROM cpu WHERE cores IS NULL") == 3);
  CHECK(run_int(*engine, "SELECT count(cores) FROM cpu") == 1);

  CHECK(engine_close(engine).ok());
}

/** Options for one-day partitions with a long background interval.
@return options with a long background interval and no capacity limit */
static const engine_options_t *daily() {
  static engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_partition_days = 1;

  return (&options);
}

/** Read the execution plan of a query.
@return the plan of a query */
static std::string explain(engine_t &engine, const std::string &sql) {
  std::string plan;

  CHECK(engine_explain(engine, sql, plan).ok());
  if (getenv("PT_SHOW_PLAN") != nullptr) {
    printf("%s\n%s\n", sql.c_str(), plan.c_str());
  }

  return (plan);
}

/** A time filter leaves the days it excludes out of the generated SQL. */
static void test_filter_pushdown() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, daily(), &engine).ok());

  /* Two UTC days in DuckDB, one more in the memtable. */
  write_cpu(*engine, "a", 0, 5);
  for (int i = 0; i < 5; ++i) {
    ingest_field_t f[] = {field_double("usage", 1)};
    ingest_point_t p = point(T0 + DAY + i, nullptr, 0, f, 1);
    batch_seq_t seq = 0;
    CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq).ok());
  }
  flush(*engine);
  write_cpu(*engine, "b", 0, 2);

  const std::string sql =
      "SELECT count(*) FROM cpu WHERE time >= TIMESTAMP '2026-01-02'";
  CHECK(run_int(*engine, sql) == 5);

  /* The day the filter excludes is left out of the generated SQL; the
  other is scanned for the time column only, and the memtable scan is
  filtered above it. */
  const std::string plan = explain(*engine, sql);

  const std::string day1 = storage_partition_name(1, ut_utc_day(T0));
  const std::string day2 = storage_partition_name(1, ut_utc_day(T0 + DAY));

  CHECK(plan.find(day1) == std::string::npos);
  CHECK(plan.find(day2) != std::string::npos);
  CHECK(plan.find("Projections: c0") != std::string::npos);
  CHECK(plan.find("_PT_MEMTABLE_SCAN") != std::string::npos);
  CHECK(plan.find("Filter") != std::string::npos);

  CHECK(engine_close(engine).ok());
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
  CHECK(engine_open(dir.m_path, daily(), &engine).ok());

  /* Ten days in DuckDB at 12:00 and 23:00, two more days in the
  memtable. */
  for (int d = 0; d < 12; ++d) {
    if (d == 10) {
      flush(*engine);
    }

    for (int h = 12; h <= 23; h += 11) {
      ingest_field_t f[] = {field_int("v", d)};
      ingest_point_t p =
          point(T0 + d * DAY + h * 3600000000LL, nullptr, 0, f, 1);
      batch_seq_t seq = 0;
      CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq).ok());
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
        run_int(*engine, "SELECT count(*) FROM cpu WHERE " + c);
    const int64_t full =
        run_int(*engine, "SELECT count(*) FROM cpu WHERE NOT NOT (" + c + ")");

    CHECK(pruned == full);
    if (pruned != full) {
      fprintf(stderr, "pruning changed %s: %lld, not %lld\n", c.c_str(),
              static_cast<long long>(pruned), static_cast<long long>(full));
    }
  }

  /* A DATE compares as midnight, not as the whole day: days 0 and 1. */
  CHECK(run_int(*engine,
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
    const int64_t n = run_int(*engine, stmts[i]);

    CHECK(n == expected[i]);
    if (n != expected[i]) {
      fprintf(stderr, "%s: %lld, not %lld\n", stmts[i],
              static_cast<long long>(n), static_cast<long long>(expected[i]));
    }
  }

  CHECK(engine_close(engine).ok());
}

/** Look up the conservative time bounds inferred for a table.
@param[out]  range  inferred inclusive time bounds, when present
@return the range query_find_ranges() gives a table, and whether it gives one */
static bool range_of(const std::string &sql, const char *table,
                     query_range_t &range) {
  query_ranges_t ranges;
  query_find_ranges(sql, ranges);

  query_ranges_t::const_iterator it = ranges.find(table);
  if (it == ranges.end()) {
    return (false);
  }

  range = it->second;

  return (true);
}

/** Microseconds of a UTC time "YYYY-MM-DD HH:MM:SS" relative to T0. */
#define AT(d, h) \
  (T0 + static_cast<int64_t>(d) * DAY + static_cast<int64_t>(h) * 3600000000LL)

/** The time ranges extracted from statements, and the partitions they keep. */
static void test_time_ranges() {
  query_range_t r;

  CHECK(range_of("SELECT * FROM cpu WHERE time >= '2026-01-03'", "cpu", r));
  CHECK(r.m_lo == AT(2, 0) && r.m_hi == INT64_MAX);

  CHECK(
      range_of("SELECT * FROM CPU c WHERE c.time > TIMESTAMP "
               "'2026-01-03 01:00:00' AND v = 1 AND time < DATE '2026-01-05'",
               "cpu", r));
  CHECK(r.m_lo == AT(2, 1) + 1 && r.m_hi == AT(4, 0) - 1);

  CHECK(
      range_of("SELECT * FROM cpu WHERE '2026-01-02' <= time AND "
               "time BETWEEN '2026-01-01' AND '2026-01-04'",
               "cpu", r));
  CHECK(r.m_lo == AT(1, 0) && r.m_hi == AT(3, 0));

  CHECK(range_of("SELECT * FROM cpu WHERE time = '2026-01-02 05:00:00'", "cpu",
                 r));
  CHECK(r.m_lo == AT(1, 5) && r.m_hi == AT(1, 5));

  /* Contradictory: empty. */
  CHECK(
      range_of("SELECT * FROM cpu WHERE time > '2026-01-05' AND "
               "time < '2026-01-03'",
               "cpu", r));
  CHECK(r.m_lo > r.m_hi);

  /* Two references: the hull, here every time; an empty one adds
  nothing. */
  CHECK(range_of("SELECT * FROM cpu WHERE time < '2026-01-02' UNION ALL "
                 "SELECT * FROM cpu WHERE time >= '2026-01-10' UNION ALL "
                 "SELECT * FROM cpu WHERE time > '2026-02-01' AND "
                 "time < '2026-01-01'",
                 "cpu", r) == false);
  CHECK(
      range_of("SELECT * FROM cpu WHERE time < '2026-01-02' UNION ALL "
               "SELECT * FROM cpu WHERE time >= '2026-01-10' AND "
               "time < '2026-01-03'",
               "cpu", r));
  CHECK(r.m_lo == INT64_MIN && r.m_hi == AT(1, 0) - 1);

  CHECK(
      range_of("WITH x AS (SELECT * FROM cpu WHERE time >= '2026-01-06') "
               "SELECT * FROM x, (SELECT * FROM cpu WHERE "
               "time >= '2026-01-08') y",
               "cpu", r));
  CHECK(r.m_lo == AT(5, 0) && r.m_hi == INT64_MAX);

  CHECK(
      range_of("SELECT * FROM mem WHERE v IN (SELECT v FROM cpu WHERE "
               "time BETWEEN '2026-01-02' AND '2026-01-03')",
               "cpu", r));
  CHECK(r.m_lo == AT(1, 0) && r.m_hi == AT(2, 0));

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
    const bool bounded = range_of(open[i], "cpu", r);

    CHECK(!bounded);
    if (bounded) {
      fprintf(stderr, "bounded: %s\n", open[i]);
    }
  }

  /* The generated SQL holds only the overlapping partitions. */
  query_table_t table;
  table.m_schema.m_name = "cpu";

  col_def_t time_col;
  time_col.m_name = "time";
  time_col.m_kind = col_kind_t::TIME;
  time_col.m_type = col_type_t::TIMESTAMP;
  table.m_schema.m_cols.push_back(time_col);

  for (int d = 0; d < 5; ++d) {
    storage_partition_t p;
    p.m_table_id = 1;
    p.m_first_day = ut_utc_day(T0) + d;
    p.m_phys_name = day_name(d);
    p.m_n_cols = 1;
    table.m_partitions.push_back(p);
  }

  r.m_lo = AT(1, 23);
  r.m_hi = AT(3, 0);
  std::string sql = query_build_table_sql(1, "cpu", table, 1, r);
  CHECK(sql.find(day_name(0)) == std::string::npos);
  CHECK(sql.find(day_name(1)) != std::string::npos);
  CHECK(sql.find(day_name(2)) != std::string::npos);
  CHECK(sql.find(day_name(3)) != std::string::npos);
  CHECK(sql.find(day_name(4)) == std::string::npos);

  /* Nothing overlaps and nothing is in the memtable: typed and empty. */
  r.m_lo = AT(10, 0);
  r.m_hi = INT64_MAX;
  sql = query_build_table_sql(1, "cpu", table, 1, r);
  CHECK(sql.find("_pt_") == std::string::npos);
  CHECK(sql.find("LIMIT 0") != std::string::npos);
}

/** Partitions follow the span chosen at creation, which an existing directory
keeps. */
static void test_partition_span() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  /* 90 days, one point a day: 30-day partitions aligned to 1970-01-01. */
  for (int d = 0; d < 90; ++d) {
    ingest_field_t f[] = {field_int("v", d)};
    ingest_point_t p = point(T0 + d * DAY, nullptr, 0, f, 1);
    batch_seq_t seq = 0;
    CHECK(engine_write(*engine, "cpu", 3, &p, 1, seq).ok());
  }
  flush(*engine);

  const storage_catalog_t &state = storage_catalog(engine_storage(*engine));
  CHECK(state.m_partition_days == 30);

  const int64_t first = ut_utc_day(T0);
  const size_t expected =
      static_cast<size_t>((first + 89) / 30 - first / 30 + 1);
  CHECK(state.m_partitions.size() == expected);

  std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
      it;
  for (it = state.m_partitions.begin(); it != state.m_partitions.end(); ++it) {
    CHECK(it->second.m_first_day % 30 == 0);
  }

  CHECK(run_int(*engine, "SELECT count(*) FROM cpu") == 90);
  CHECK(run_int(*engine,
                "SELECT count(*) FROM cpu WHERE time >= '2026-02-15' "
                "AND time < '2026-03-01'") == 14);
  CHECK(engine_close(engine).ok());

  /* An existing directory keeps its span. */
  engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_partition_days = 7;

  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  const storage_catalog_t &reopened = storage_catalog(engine_storage(*engine));
  CHECK(reopened.m_partition_days == 30 &&
        reopened.m_partitions.size() == expected);
  CHECK(engine_close(engine).ok());

  /* A span out of range is refused. */
  test_dir_t other;
  options.m_partition_days = 0;
  CHECK(engine_open(other.m_path, &options, &engine).code() ==
        DB_INVALID_ARGUMENT);
  options.m_partition_days = STORAGE_MAX_PARTITION_DAYS + 1;
  CHECK(engine_open(other.m_path, &options, &engine).code() ==
        DB_INVALID_ARGUMENT);
}

/** Statements that write or read outside the tables are refused; ordinary
reads are allowed. */
static void test_refused() {
  db_status_t status;

  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  write_cpu(*engine, "a", 0, 2);

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
      "SELECT * FROM _pt_memtable_scan(1, 'cpu')",
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

    status = query_all(*engine, refused[i], r);
    const db_status_t err = status;
    CHECK(err.code() == DB_INVALID_ARGUMENT);
    CHECK(!status.message().empty());
    if (err.code() != DB_INVALID_ARGUMENT) {
      fprintf(stderr, "not refused: %s\n", refused[i]);
    }
  }

  /* An unknown table is a SQL error, not a refusal. */
  test_query_t r;

  status = query_all(*engine, "SELECT * FROM nothing", r);
  CHECK(status.code() == DB_ERROR);

  /* Allowed: subqueries, CTEs, joins of tables, VALUES, no FROM. */
  CHECK(run_int(*engine,
                "WITH x AS (SELECT * FROM cpu) SELECT count(*) FROM x") == 2);
  CHECK(run_int(*engine,
                "SELECT count(*) FROM cpu a JOIN cpu b ON a.time = b.time") ==
        2);
  CHECK(run_int(*engine, "SELECT (SELECT count(*) FROM cpu) + 1") == 3);
  CHECK(run_int(*engine, "SELECT count(*) FROM (VALUES (1), (2)) t(x)") == 2);

  CHECK(engine_close(engine).ok());
}

/** PIVOT expressions obey the same read boundary as ordinary SELECTs. */
static void test_pivot_references() {
  db_status_t status;

  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  write_cpu(*engine, "a", 0, 2);

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
          duckdb_literal(path) + "))) IN ('a'))",
      "SELECT * FROM cpu UNPIVOT (val FOR col IN "
      "((usage + (SELECT flushed_seq FROM _pt_state)) AS x))",
  };

  for (const std::string &sql : refused) {
    test_query_t result;

    status = query_all(*engine, sql, result);
    CHECK(status.code() == DB_INVALID_ARGUMENT);
    CHECK(!status.message().empty());
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
      flush(*engine);
    }

    CHECK(run_int(*engine, allowed) == 2);
    CHECK(run_int(*engine,
                  "SELECT count(*) FROM cpu UNPIVOT "
                  "(val FOR col IN (usage))") == 2);
    test_query_t result = run(*engine, pruning);
    CHECK(result.m_rows.size() == 2);
    if (result.m_rows.size() == 2) {
      CHECK(result.m_rows[0][0].m_i == 0);
      CHECK(result.m_rows[1][0].m_i == 1);
    }
  }

  CHECK(engine_close(engine).ok());
}

/** Result columns map to the returned types; others are refused. */
static void test_result_types() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  write_cpu(*engine, "a", 0, 2);

  test_query_t r = run(*engine,
                       "SELECT sum(1), avg(usage), true, 'x', NULL::BIGINT, "
                       "1.25::DECIMAL(10, 2), DATE '1970-01-02', "
                       "42::UTINYINT FROM cpu");
  CHECK(r.m_rows.size() == 1 && r.m_columns.size() == 8);

  if (r.m_rows.size() == 1 && r.m_columns.size() == 8) {
    const std::vector<query_value_t> &v = r.m_rows[0];
    CHECK(r.m_columns[0].m_type == col_type_t::BIGINT && v[0].m_i == 2);
    CHECK(r.m_columns[1].m_type == col_type_t::DOUBLE && v[1].m_d == 0.25);
    CHECK(r.m_columns[2].m_type == col_type_t::BOOLEAN && v[2].m_b);
    CHECK(r.m_columns[3].m_type == col_type_t::VARCHAR && v[3].m_s == "x");
    CHECK(v[4].m_is_null);
    CHECK(r.m_columns[5].m_type == col_type_t::DOUBLE && v[5].m_d == 1.25);
    CHECK(r.m_columns[6].m_type == col_type_t::TIMESTAMP &&
          v[6].m_i == 86400000000LL);
    CHECK(v[7].m_i == 42);
  }

  /* A type that cannot be returned, and a value that does not fit. */
  CHECK(query_all(*engine, "SELECT [1, 2]", r).code() == DB_INVALID_ARGUMENT);
  CHECK(query_all(*engine, "SELECT 18446744073709551615::UBIGINT", r).code() ==
        DB_INVALID_ARGUMENT);

  CHECK(engine_close(engine).ok());
}

struct writer_arg_t {
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Writes to make. */
  int m_count{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Write count ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  for (int i = 0; i < arg->m_count; ++i) {
    if (!write_id(*arg->m_engine, arg->m_thread_no * 100000 + i).ok()) {
      arg->m_errors++;
    }
  }
}

struct reader_arg_t {
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** Set when the thread should stop; protected by the m_mutex. */
  bool *m_stop{nullptr};

  /** Protects *stop. */
  std::mutex *m_mutex{nullptr};

  /** Queries run. */
  int m_queries{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Query over and over while writes and flushes run: each answer must count
every write once, and counts never go back.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void reader_main(reader_arg_t *arg) {
  db_status_t status;

  int64_t last = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->m_mutex);
      if (*arg->m_stop) {
        break;
      }
    }

    test_query_t r;

    status = query_all(*arg->m_engine,
                       "SELECT count(*), count(DISTINCT id) FROM k", r);
    if (!status.ok()) {
      /* Before the first write the table does not exist yet. */
      if (status.message().find("does not exist") == std::string::npos) {
        fprintf(stderr, "reader: %s\n", status.message().c_str());
        arg->m_errors++;
      }
      continue;
    }

    const int64_t rows = r.m_rows[0][0].m_i;
    const int64_t distinct = r.m_rows[0][1].m_i;

    if (rows != distinct || rows < last) {
      arg->m_errors++;
    }

    last = rows;
    arg->m_queries++;
  }
}

/** Queries during writes and frequent flushes always see a consistent
snapshot. */
static void test_snapshot_under_load() {
  test_dir_t dir;

  /* Background flushes every few milliseconds, so queries keep starting
  around flush commits. */
  engine_options_t options;
  options.m_flush_interval_ms = 3;
  /* Keep writes overlapping flushes even when page-cache writes finish fast. */
  options.m_memtable_limit_bytes = 1024;

  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, &options, &engine).ok());

  bool stop = false;
  std::mutex mutex;
  std::vector<reader_arg_t> readers(2);
  std::vector<std::thread> reader_threads;

  for (size_t i = 0; i < readers.size(); ++i) {
    readers[i].m_engine = engine;
    readers[i].m_stop = &stop;
    readers[i].m_mutex = &mutex;
    reader_threads.push_back(std::thread(reader_main, &readers[i]));
  }

  const int threads = 4;
  const int count = 150;
  std::vector<writer_arg_t> writers(threads);
  std::vector<std::thread> writer_threads;

  for (int t = 0; t < threads; ++t) {
    writers[t].m_engine = engine;
    writers[t].m_thread_no = t;
    writers[t].m_count = count;
    writer_threads.push_back(std::thread(writer_main, &writers[t]));
  }

  for (int t = 0; t < threads; ++t) {
    writer_threads[t].join();
    CHECK(writers[t].m_errors == 0);
  }

  {
    std::lock_guard<std::mutex> lock(mutex);
    stop = true;
  }

  int queries = 0;
  for (size_t i = 0; i < readers.size(); ++i) {
    reader_threads[i].join();
    CHECK(readers[i].m_errors == 0);
    queries += readers[i].m_queries;
  }

  engine_stats_t stats;
  engine_get_stats(*engine, stats);
  printf("snapshot under load: %d queries during %llu background flushes\n",
         queries, static_cast<unsigned long long>(stats.m_background_flushes));
  CHECK(queries > 0 && stats.m_background_flushes > 0);

  CHECK(run_int(*engine, "SELECT count(DISTINCT id) FROM k") ==
        threads * count);
  CHECK(engine_close(engine).ok());
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_scan_installation();
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

  return (test_result("query-t"));
}
