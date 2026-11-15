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
 * Covers: src/query/query.cc, src/query/query_duckdb.cc,
 *         src/query/query_memtable.cc
 *
 *-------------------------------------------------------------------------
 */

#include "query.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "db.h"
#include "db_schema.h"
#include "ingest.h"
#include "pt_duckdb.h"
#include "store.h"
#include "test_db.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_store.h"
#include "test_util.h"
#include "ut.h"

/** Run a query that must succeed.
@return the query result; a failed call is recorded as a test failure */
static test_query_t run_query(db &database, const std::string &sql) {
  db_status_t status;

  test_query_t result;

  status = query_all(database, sql, result);

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
static int64_t query_int(db &database, const std::string &sql) {
  test_query_t r = run_query(database, sql);

  if (r.m_rows.size() != 1 || r.m_rows[0].size() != 1 ||
      r.m_rows[0][0].m_is_null) {
    CHECK(false);

    return (-1);
  }

  return (r.m_rows[0][0].m_integer);
}

/** Write cpu points: host tag, usage DOUBLE; time TEST_BASE_TIME_US + i
seconds.
@param[in]  from  first generated point index, inclusive */
static void write_cpu(db &database, const char *host, int from, int to) {
  for (int i = from; i < to; ++i) {
    ingest_tag_t t[] = {tag("host", host)};
    ingest_field_t f[] = {field_double("usage", i * 0.5)};

    ingest_point_t p = point(
        TEST_BASE_TIME_US + static_cast<int64_t>(i) * 1000000, t, 1, f, 1);

    batch_seq_t seq = 0;

    CHECK(database.write("cpu", 3, &p, 1, seq).ok());
  }

  return;
}

/** Flush, checking that it succeeds. */
static void flush(db &database) {
  db::flush_info_t info;
  CHECK(database.flush(info).ok());

  return;
}

/** Query components register in order, report status and leave the storage
connection usable. Database opens exercise their orchestration. */
static void test_scan_registration() {
  db_status_t status;

  status = db_status_t(DB_ERROR, "stale error");
  status = query_init(nullptr);
  CHECK(status.code() == DB_ERROR);
  CHECK(!status.message().empty());

  test_dir_t dir;
  test_dir_t store_dir;
  store *st = nullptr;
  status = store::open(dir.m_path, STORE_DEFAULT_PARTITION_HOURS, &st);

  const db_status_t err = status;
  CHECK(err.ok());

  if (!err.ok()) {
    return;
  }

  status = db_status_t(DB_ERROR, "stale error");
  status = query_init(st->connection());
  CHECK(status.ok());
  CHECK(status.message().empty());

  int64_t value = 0;
  CHECK(test_store_query_int(*st, "SELECT 42", value).ok());
  CHECK(value == 42);

  store::destroy(st);

  return;
}

/** Test context kept alive until native callbacks finish. */
struct callback_failure_t {
  /** Connection whose query owns the registry entry. */
  duckdb_connection m_connection{nullptr};

  /** Fail binding rather than execution initialization. */
  bool m_fail_bind{false};

  /** Token resolved during binding, before execution workers start. */
  std::atomic<uint64_t> m_token{0};
};

/** Report a typed allocation failure through a text-only bind callback. */
static void failing_bind(duckdb_bind_info info) {
  callback_failure_t &failure =
      *static_cast<callback_failure_t *>(duckdb_bind_get_extra_info(info));
  const query_ranges_t *ranges = nullptr;
  uint64_t token = 0;
  CHECK(query_registry::instance().find_by_connection(
            query_connection_key(failure.m_connection), token, ranges) !=
        nullptr);

  failure.m_token.store(token, std::memory_order_release);

  if (failure.m_fail_bind) {
    const db_status_t error(DB_OUT_OF_MEMORY,
                            "injected callback allocation failure");
    query_registry::instance().record_error(
        failure.m_token.load(std::memory_order_acquire), error);
    duckdb_bind_set_error(info, error.message().c_str());

    return;
  }

  duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "value", type);
  duckdb_destroy_logical_type(&type);
}

/** Report a typed allocation failure during execution initialization. */
static void failing_init(duckdb_init_info info) {
  callback_failure_t &failure =
      *static_cast<callback_failure_t *>(duckdb_init_get_extra_info(info));
  const db_status_t error(DB_OUT_OF_MEMORY,
                          "injected callback allocation failure");
  query_registry::instance().record_error(
      failure.m_token.load(std::memory_order_acquire), error);
  duckdb_init_set_error(info, error.message().c_str());
}

/** Initialization fails before the scan can produce rows. */
static void unexpected_scan(duckdb_function_info, duckdb_data_chunk) {
  CHECK(false);
}

/** Preserve callback error categories through prepare and repeated fetches. */
static void test_callback_errors(bool fail_bind) {
  test_dir_t dir;
  test_dir_t store_dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, STORE_DEFAULT_PARTITION_HOURS, &st).ok());

  callback_failure_t failure;
  failure.m_connection = st->connection();
  failure.m_fail_bind = fail_bind;

  duckdb_table_function function = duckdb_create_table_function();
  duckdb_table_function_set_name(function, "callback_failure");
  duckdb_table_function_set_extra_info(function, &failure, nullptr);
  duckdb_table_function_set_bind(function, failing_bind);
  duckdb_table_function_set_init(function, failing_init);
  duckdb_table_function_set_function(function, unexpected_scan);
  CHECK(duckdb_register_table_function(failure.m_connection, function) ==
        DuckDBSuccess);

  duckdb_destroy_table_function(&function);

  pt_duckdb_session session(failure.m_connection);
  CHECK(session.begin().ok());

  query_snapshot_t snapshot;
  query_ranges_t ranges;
  query_cursor *cursor = nullptr;

  db_status_t status = query_cursor::open(failure.m_connection, snapshot,
                                          "SELECT * FROM callback_failure()",
                                          false, ranges, &cursor);

  if (!fail_bind) {
    CHECK(status.ok());

    query_rows_t rows;
    status = cursor->fetch(rows);
    CHECK(rows.empty());

    const db_status_t repeated = cursor->fetch(rows);
    CHECK(repeated.code() == status.code());
    CHECK(repeated.message() == status.message());
  }

  CHECK(status.code() == DB_OUT_OF_MEMORY);
  CHECK(status.message().find("injected callback allocation failure") !=
        std::string::npos);

  query_cursor::destroy(cursor);
  CHECK(query_registry::instance().find_by_token(
            failure.m_token.load(std::memory_order_acquire)) == nullptr);
  CHECK(session.rollback().ok());

  /* Failed callbacks must not poison the next query on this connection. */
  CHECK(session.begin().ok());
  CHECK(query_cursor::open(failure.m_connection, snapshot, "SELECT 42", false,
                           ranges, &cursor)
            .ok());

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 42);
  CHECK(cursor->fetch(rows).ok() && rows.empty());
  CHECK(cursor->fetch(rows).ok() && rows.empty());

  query_cursor::destroy(cursor);
  CHECK(session.rollback().ok());

  store::destroy(st);
}

/** Queries give the same answers over the memtable, DuckDB, or both. */
static void test_memtable_and_storage_sources() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  const std::string count = "SELECT count(*) FROM cpu";
  const std::string by_host =
      "SELECT host, count(*), sum(usage) FROM cpu GROUP BY host ORDER BY host";

  /* Only in the memtable. */
  write_cpu(*database, "a", 0, 10);
  CHECK(query_int(*database, count) == 10);

  /* Only in DuckDB. */
  flush(*database);
  CHECK(query_int(*database, count) == 10);

  /* Both: the same answer as if everything were in one place. */
  write_cpu(*database, "b", 10, 15);
  CHECK(query_int(*database, count) == 15);

  test_query_t r = run_query(*database, by_host);
  CHECK(r.m_rows.size() == 2);
  CHECK(r.m_columns[0].m_data_type == data_type_t::VARCHAR &&
        r.m_columns[1].m_data_type == data_type_t::BIGINT);
  CHECK(r.m_rows[0][0].m_string == "a" && r.m_rows[0][1].m_integer == 10);
  CHECK(r.m_rows[0][2].m_double == 22.5);
  CHECK(r.m_rows[1][0].m_string == "b" && r.m_rows[1][1].m_integer == 5 &&
        r.m_rows[1][2].m_double == 30);

  /* Names match case-insensitively; time comes back in microseconds. */
  CHECK(query_int(*database, "SELECT count(*) FROM CPU WHERE Host = 'b'") == 5);

  r = run_query(*database, "SELECT max(time) FROM cpu");
  CHECK(r.m_columns[0].m_data_type == data_type_t::TIMESTAMP);
  CHECK(r.m_rows[0][0].m_integer == TEST_BASE_TIME_US + 14 * 1000000LL);

  db_close(database);

  return;
}

/** Rows written before a column was added read NULL for it, stored or not. */
static void test_columns_added_later() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  /* Stored with one column, then a column added in the memtable. */
  write_cpu(*database, "a", 0, 3);
  flush(*database);
  CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 3);
  CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 3);

  ingest_tag_t t[] = {tag("host", "a")};
  ingest_field_t f[] = {field_double("usage", 1), field_int("cores", 8)};

  ingest_point_t p = point(TEST_BASE_TIME_US + 100000000, t, 1, f, 2);
  batch_seq_t seq = 0;
  CHECK(database->write("cpu", 3, &p, 1, seq).ok());

  /* Stored rows read NULL for the new column. */
  CHECK(query_int(*database, "SELECT count(*) FROM cpu WHERE cores IS NULL") ==
        3);
  CHECK(query_int(*database, "SELECT max(cores) FROM cpu") == 8);

  /* After flushing, the old partition lacks the column; still NULL. */
  flush(*database);
  CHECK(query_int(*database, "SELECT count(*) FROM cpu WHERE cores IS NULL") ==
        3);
  CHECK(query_int(*database, "SELECT count(cores) FROM cpu") == 1);

  db_close(database);

  return;
}

/** Read the execution plan of a query.
@return the plan of a query */
static std::string explain(db &database, const std::string &sql) {
  std::string plan;

  CHECK(database.explain(sql, plan).ok());

  if (getenv("PT_SHOW_PLAN") != nullptr) {
    printf("%s\n%s\n", sql.c_str(), plan.c_str());
  }

  return (plan);
}

/** A time filter leaves the days it excludes out of the generated SQL. */
static void test_filter_pushdown() {
  test_dir_t dir;
  db *database = nullptr;
  const db::options_t options = daily_partition_options();
  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* Two UTC days in DuckDB, one more in the memtable. */
  write_cpu(*database, "a", 0, 5);

  for (int i = 0; i < 5; ++i) {
    ingest_field_t f[] = {field_double("usage", 1)};

    ingest_point_t p =
        point(TEST_BASE_TIME_US + TEST_DAY_US + i, nullptr, 0, f, 1);

    batch_seq_t seq = 0;
    CHECK(database->write("cpu", 3, &p, 1, seq).ok());
  }

  flush(*database);
  write_cpu(*database, "b", 0, 2);

  const std::string sql =
      "SELECT count(*) FROM cpu WHERE time >= TIMESTAMP '2026-01-02'";
  CHECK(query_int(*database, sql) == 5);

  /* The day the filter excludes is left out of the generated SQL; the
  other is scanned for the time column only, and the memtable scan is
  filtered above it. */
  const std::string plan = explain(*database, sql);

  const std::string day1 =
      store_partition_name(1, ut_utc_hour(TEST_BASE_TIME_US));

  const std::string day2 =
      store_partition_name(1, ut_utc_hour(TEST_BASE_TIME_US + TEST_DAY_US));

  CHECK(plan.find(day1) == std::string::npos);
  CHECK(plan.find(day2) != std::string::npos);
  CHECK(plan.find("Projections: c0") != std::string::npos);
  CHECK(plan.find("_PT_MEMTABLE_SCAN") != std::string::npos);
  CHECK(plan.find("Filter") != std::string::npos);

  db_close(database);

  return;
}

/** Build the physical partition name of a day relative to TEST_BASE_TIME_US.
@param[in]  d  day offset from TEST_BASE_TIME_US for stored table id 1
@return the partition name of day d after TEST_BASE_TIME_US, table 1 */
static std::string day_name(int d) {
  return (store_partition_name(1, ut_utc_hour(TEST_BASE_TIME_US) + d * 24));
}

/** Pruning by time conditions never changes the rows a query returns. */
static void test_partition_pruning() {
  test_dir_t dir;
  db *database = nullptr;
  const db::options_t options = daily_partition_options();
  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* Ten days in DuckDB at 12:00 and 23:00, two more days in the memtable. */
  for (int d = 0; d < 12; ++d) {
    if (d == 10) {
      flush(*database);
      CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 20);
      CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 20);
    }

    for (int h = 12; h <= 23; h += 11) {
      ingest_field_t f[] = {field_int("v", d)};

      ingest_point_t p =
          point(TEST_BASE_TIME_US + d * TEST_DAY_US + h * 3600000000LL, nullptr,
                0, f, 1);

      batch_seq_t seq = 0;
      CHECK(database->write("cpu", 3, &p, 1, seq).ok());
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
        query_int(*database, "SELECT count(*) FROM cpu WHERE " + c);

    const int64_t full = query_int(
        *database, "SELECT count(*) FROM cpu WHERE NOT NOT (" + c + ")");

    CHECK(pruned == full);

    if (pruned != full) {
      fprintf(stderr, "pruning changed %s: %lld, not %lld\n", c.c_str(),
              static_cast<long long>(pruned), static_cast<long long>(full));
    }
  }

  /* A DATE compares as midnight, not as the whole day: days 0 and 1. */
  CHECK(query_int(*database,
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
    const int64_t n = query_int(*database, stmts[i]);

    CHECK(n == expected[i]);

    if (n != expected[i]) {
      fprintf(stderr, "%s: %lld, not %lld\n", stmts[i],
              static_cast<long long>(n), static_cast<long long>(expected[i]));
    }
  }

  /* A later flush must replace cached metadata with the new partitions. */
  flush(*database);
  CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 24);
  CHECK(query_int(*database,
                  "SELECT count(*) FROM cpu WHERE time >= '2026-01-11'") == 4);

  db_close(database);

  return;
}

/** Lock candidates include sources hidden in CTEs, joins and subqueries. */
static void test_table_sources() {
  test_dir_t store_dir;
  store *st = nullptr;
  CHECK(store::open(store_dir.m_path, 24, &st).ok());

  const char *queries[] = {
      ("WITH recent AS (SELECT * FROM CPU) "
       "SELECT * FROM recent JOIN memory ON true"),
      "SELECT (SELECT count(*) FROM CPU) FROM memory",
      "SELECT * FROM (SELECT * FROM CPU UNION ALL SELECT * FROM memory) s",
      ("SELECT * FROM (SELECT * FROM CPU JOIN memory ON true) "
       "PIVOT(count(*) FOR host IN ('a'))")};

  for (const char *sql : queries) {
    query_ranges_t ranges;
    std::vector<std::string> names;
    const db_status_t err = query_analyze(st->connection(), sql, ranges, names);
    CHECK(err.ok());

    if (!err.ok()) {
      fprintf(stderr, "%s: %s\n", sql, err.message().c_str());
    }

    CHECK(std::find(names.begin(), names.end(), "cpu") != names.end());
    CHECK(std::find(names.begin(), names.end(), "memory") != names.end());
  }

  store::destroy(st);

  return;
}

/** Look up the conservative time bounds inferred for a table.
@param[out]  range  inferred inclusive time bounds, when present
@return whether SELECT validation inferred a range for the table */
static bool range_of(duckdb_connection connection, const std::string &sql,
                     const char *table_name, query_range_t &range) {
  query_ranges_t ranges;
  std::vector<std::string> tables;
  const db_status_t err = query_analyze(connection, sql, ranges, tables);
  CHECK(err.ok());

  if (!err.ok()) {
    return (false);
  }

  query_ranges_t::const_iterator it = ranges.find(table_name);
  if (it == ranges.end()) {
    return (false);
  }

  range = it->second;

  return (true);
}

/** Microseconds of a UTC time "YYYY-MM-DD HH:MM:SS" relative to
TEST_BASE_TIME_US. */
#define TEST_TIME_US(d, h)                                     \
  (TEST_BASE_TIME_US + static_cast<int64_t>(d) * TEST_DAY_US + \
   static_cast<int64_t>(h) * 3600000000LL)

/** The time ranges extracted from statements, and the partitions they keep. */
static void test_time_ranges() {
  test_dir_t store_dir;
  store *st = nullptr;
  CHECK(store::open(store_dir.m_path, 24, &st).ok());

  duckdb_connection connection = st->connection();
  query_range_t r;

  CHECK(range_of(connection, "SELECT * FROM cpu WHERE time >= '2026-01-03'",
                 "cpu", r));
  CHECK(r.m_start_time_us == TEST_TIME_US(2, 0) &&
        r.m_end_time_us == INT64_MAX);

  CHECK(range_of(connection,
                 "SELECT * FROM CPU c WHERE c.time > TIMESTAMP "
                 "'2026-01-03 01:00:00' AND v = 1 AND time < DATE '2026-01-05'",
                 "cpu", r));
  CHECK(r.m_start_time_us == TEST_TIME_US(2, 1) + 1 &&
        r.m_end_time_us == TEST_TIME_US(4, 0) - 1);

  CHECK(range_of(connection,
                 "SELECT * FROM cpu WHERE '2026-01-02' <= time AND "
                 "time BETWEEN '2026-01-01' AND '2026-01-04'",
                 "cpu", r));
  CHECK(r.m_start_time_us == TEST_TIME_US(1, 0) &&
        r.m_end_time_us == TEST_TIME_US(3, 0));

  CHECK(range_of(connection,
                 "SELECT * FROM cpu WHERE time = '2026-01-02 05:00:00'", "cpu",
                 r));
  CHECK(r.m_start_time_us == TEST_TIME_US(1, 5) &&
        r.m_end_time_us == TEST_TIME_US(1, 5));

  /* Contradictory: empty. */
  CHECK(range_of(connection,
                 "SELECT * FROM cpu WHERE time > '2026-01-05' AND "
                 "time < '2026-01-03'",
                 "cpu", r));
  CHECK(r.m_start_time_us > r.m_end_time_us);

  /* Two references: the hull, here every time; an empty one adds nothing. */
  CHECK(range_of(connection,
                 "SELECT * FROM cpu WHERE time < '2026-01-02' UNION ALL "
                 "SELECT * FROM cpu WHERE time >= '2026-01-10' UNION ALL "
                 "SELECT * FROM cpu WHERE time > '2026-02-01' AND "
                 "time < '2026-01-01'",
                 "cpu", r) == false);
  CHECK(range_of(connection,
                 "SELECT * FROM cpu WHERE time < '2026-01-02' UNION ALL "
                 "SELECT * FROM cpu WHERE time >= '2026-01-10' AND "
                 "time < '2026-01-03'",
                 "cpu", r));
  CHECK(r.m_start_time_us == INT64_MIN &&
        r.m_end_time_us == TEST_TIME_US(1, 0) - 1);

  CHECK(range_of(connection,
                 "WITH x AS (SELECT * FROM cpu WHERE time >= '2026-01-06') "
                 "SELECT * FROM x, (SELECT * FROM cpu WHERE "
                 "time >= '2026-01-08') y",
                 "cpu", r));
  CHECK(r.m_start_time_us == TEST_TIME_US(5, 0) &&
        r.m_end_time_us == INT64_MAX);

  CHECK(range_of(connection,
                 "SELECT * FROM mem WHERE v IN (SELECT v FROM cpu WHERE "
                 "time BETWEEN '2026-01-02' AND '2026-01-03')",
                 "cpu", r));
  CHECK(r.m_start_time_us == TEST_TIME_US(1, 0) &&
        r.m_end_time_us == TEST_TIME_US(2, 0));

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
    const bool bounded = range_of(connection, open[i], "cpu", r);

    CHECK(!bounded);

    if (bounded) {
      fprintf(stderr, "bounded: %s\n", open[i]);
    }
  }

  /* The generated SQL holds only the overlapping partitions. */
  query_table_t table;
  table.m_partition_duration_hours = 24;
  table.m_schema.m_name = "cpu";

  schema_column_t time_col;
  time_col.m_name = "time";
  time_col.m_column_type = column_type_t::TIME;
  time_col.m_data_type = data_type_t::TIMESTAMP;
  table.m_schema.m_columns.push_back(time_col);

  for (int d = 0; d < 5; ++d) {
    store_partition_t p;
    p.m_table_id = 1;
    p.m_start_hour = ut_utc_hour(TEST_BASE_TIME_US) + d * 24;
    p.m_table_name = day_name(d);
    p.m_column_count = 1;
    table.m_partitions.push_back(p);
  }

  r.m_start_time_us = TEST_TIME_US(1, 23);
  r.m_end_time_us = TEST_TIME_US(3, 0);

  std::string sql = query_build_scan_sql(1, "cpu", table, r);
  CHECK(sql.find(day_name(0)) == std::string::npos);
  CHECK(sql.find(day_name(1)) != std::string::npos);
  CHECK(sql.find(day_name(2)) != std::string::npos);
  CHECK(sql.find(day_name(3)) != std::string::npos);
  CHECK(sql.find(day_name(4)) == std::string::npos);

  /* Retention prunes physical branches before SQL planning. */
  table.m_cutoff_us = TEST_TIME_US(2, 12);
  sql = query_build_scan_sql(1, "cpu", table, r);
  CHECK(sql.find(day_name(1)) == std::string::npos);
  CHECK(sql.find(day_name(2)) != std::string::npos);
  CHECK(sql.find(day_name(3)) != std::string::npos);
  CHECK(sql.find("c0 >= TIMESTAMP '2026-01-03 12:00:00.000000'") !=
        std::string::npos);
  CHECK(sql.find("AND c0 >=", sql.find("AND c0 >=") + 1) == std::string::npos);
  table.m_cutoff_us = TEST_TIME_US(2, 0);
  sql = query_build_scan_sql(1, "cpu", table, r);
  CHECK(sql.find("AND c0 >=") == std::string::npos);
  table.m_cutoff_us = TEST_TIME_US(2, 12);

  memtable::snapshot_t pinned;
  table.m_memtable_snapshot = &pinned;
  r.m_end_time_us = table.m_cutoff_us - 1;
  sql = query_build_scan_sql(1, "cpu", table, r);
  CHECK(sql.find("FROM") == std::string::npos);
  CHECK(sql.find("LIMIT 0") != std::string::npos);
  table.m_memtable_snapshot = nullptr;
  table.m_cutoff_us = INT64_MIN;

  /* Nothing overlaps and nothing is in the memtable: typed and empty. */
  r.m_start_time_us = TEST_TIME_US(10, 0);
  r.m_end_time_us = INT64_MAX;
  sql = query_build_scan_sql(1, "cpu", table, r);
  CHECK(sql.find("_pt_") == std::string::npos);
  CHECK(sql.find("LIMIT 0") != std::string::npos);

  store::destroy(st);

  return;
}

/** Partitions follow the duration chosen at creation, which an existing
directory keeps. */
static void test_partition_span() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  /* 90 days, one point a day: 30-day partitions aligned to 1970-01-01. */
  for (int d = 0; d < 90; ++d) {
    ingest_field_t f[] = {field_int("v", d)};

    ingest_point_t p =
        point(TEST_BASE_TIME_US + d * TEST_DAY_US, nullptr, 0, f, 1);

    batch_seq_t seq = 0;
    CHECK(database->write("cpu", 3, &p, 1, seq).ok());
  }

  flush(*database);

  const int64_t first = ut_utc_day(TEST_BASE_TIME_US);
  const size_t expected =
      static_cast<size_t>((first + 89) / 30 - first / 30 + 1);

  CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 90);
  CHECK(query_int(*database,
                  "SELECT count(*) FROM cpu WHERE time >= '2026-02-15' "
                  "AND time < '2026-03-01'") == 14);

  db_close(database);

  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, STORE_DEFAULT_PARTITION_HOURS, &st).ok());

  const store_catalog_t &state = st->catalog();
  CHECK(state.m_partition_duration_hours == 720);
  const auto &partitions = state.m_tables.at("cpu").m_partitions;
  CHECK(partitions.size() == expected);

  for (const auto &partition : partitions) {
    CHECK(partition.second.m_start_hour % 720 == 0);
  }

  store::destroy(st);

  /* An existing directory keeps its duration. */
  db::options_t options = *deferred_flush_options();
  options.m_partition_duration_hours = 168;

  CHECK(db_open(dir.m_path, &options, &database).ok());
  CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 90);

  db_close(database);
  REQUIRE(
      store::open(dir.m_path, options.m_partition_duration_hours, &st).ok());

  const store_catalog_t &reopened = st->catalog();
  CHECK(reopened.m_partition_duration_hours == 720 &&
        reopened.m_tables.at("cpu").m_partitions.size() == expected);

  store::destroy(st);

  /* A duration out of range is refused. */
  test_dir_t other;
  options.m_partition_duration_hours = 0;
  CHECK(db_open(other.m_path, &options, &database).code() ==
        DB_INVALID_ARGUMENT);

  options.m_partition_duration_hours = STORE_MAX_PARTITION_HOURS + 1;
  CHECK(db_open(other.m_path, &options, &database).code() ==
        DB_INVALID_ARGUMENT);

  return;
}

/** Statements that write or read outside the tables are refused; read-only
queries are allowed. */
static void test_rejected_statements() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  write_cpu(*database, "a", 0, 2);

  const char *refused[] = {
      "DELETE FROM cpu",
      "SELECT 1; SELECT 2",
      "SELECT * FROM duckdb_tables()",
      "SELECT * FROM read_csv('/etc/passwd')",
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

    status = query_all(*database, refused[i], r);

    const db_status_t err = status;
    CHECK(err.code() == DB_INVALID_ARGUMENT);
    CHECK(!status.message().empty());

    if (err.code() != DB_INVALID_ARGUMENT) {
      fprintf(stderr, "not refused: %s\n", refused[i]);
    }
  }

  /* An unknown table is a SQL error, not a refusal. */
  test_query_t r;

  status = query_all(*database, "SELECT * FROM nothing", r);
  CHECK(status.code() == DB_ERROR);

  /* Allowed: subqueries, CTEs, joins of tables, VALUES, no FROM. */
  CHECK(query_int(*database, "SELECT count(*) FROM main.cpu") == 2);
  CHECK(query_int(*database,
                  "WITH x AS (SELECT * FROM cpu) SELECT count(*) FROM x") == 2);
  CHECK(query_int(*database,
                  "SELECT count(*) FROM cpu a JOIN cpu b ON a.time = b.time") ==
        2);
  CHECK(query_int(*database, "SELECT (SELECT count(*) FROM cpu) + 1") == 3);
  CHECK(query_int(*database, "SELECT count(*) FROM (VALUES (1), (2)) t(x)") ==
        2);

  db_close(database);

  return;
}

/** PIVOT expressions obey the same read boundary as standard SELECTs. */
static void test_pivot_references() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  write_cpu(*database, "a", 0, 2);

  const std::string path = dir.file("pivot.csv");
  FILE *file = fopen(path.c_str(), "w");
  CHECK(file != nullptr);

  if (file != nullptr) {
    fputs("suffix\nx\n", file);
    CHECK(fclose(file) == 0);
  }

  const std::string refused[] = {
      "SELECT * FROM cpu PIVOT(sum(usage) FOR "
      "(host || (SELECT '' FROM duckdb_tables())) IN ('a'))",
      "SELECT * FROM cpu PIVOT(sum(usage) FOR "
      "(host || (SELECT suffix FROM read_csv(" +
          pt_duckdb_literal(path) + "))) IN ('a'))",
      "SELECT * FROM cpu UNPIVOT (val FOR col IN "
      "((usage + (SELECT count(*) FROM duckdb_tables())) AS x))",
  };

  for (const std::string &sql : refused) {
    test_query_t result;

    status = query_all(*database, sql, result);
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
      flush(*database);
    }

    CHECK(query_int(*database, allowed) == 2);
    CHECK(query_int(*database,
                    "SELECT count(*) FROM cpu UNPIVOT "
                    "(val FOR col IN (usage))") == 2);

    test_query_t result = run_query(*database, pruning);
    CHECK(result.m_rows.size() == 2);

    if (result.m_rows.size() == 2) {
      CHECK(result.m_rows[0][0].m_integer == 0);
      CHECK(result.m_rows[1][0].m_integer == 1);
    }
  }

  db_close(database);

  return;
}

/** Result columns map to the returned types; others are refused. */
static void test_result_types() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  write_cpu(*database, "a", 0, 2);

  test_query_t r =
      run_query(*database,
                "SELECT sum(1), avg(usage), true, 'x', NULL::BIGINT, "
                "1.25::DECIMAL(10, 2), DATE '1970-01-02', "
                "42::UTINYINT FROM cpu");
  CHECK(r.m_rows.size() == 1 && r.m_columns.size() == 8);

  if (r.m_rows.size() == 1 && r.m_columns.size() == 8) {
    const std::vector<query_value_t> &v = r.m_rows[0];
    CHECK(r.m_columns[0].m_data_type == data_type_t::BIGINT &&
          v[0].m_integer == 2);
    CHECK(r.m_columns[1].m_data_type == data_type_t::DOUBLE &&
          v[1].m_double == 0.25);
    CHECK(r.m_columns[2].m_data_type == data_type_t::BOOLEAN && v[2].m_boolean);
    CHECK(r.m_columns[3].m_data_type == data_type_t::VARCHAR &&
          v[3].m_string == "x");
    CHECK(v[4].m_is_null);
    CHECK(r.m_columns[5].m_data_type == data_type_t::DOUBLE &&
          v[5].m_double == 1.25);
    CHECK(r.m_columns[6].m_data_type == data_type_t::TIMESTAMP &&
          v[6].m_integer == 86400000000LL);
    CHECK(v[7].m_integer == 42);
  }

  /* A type that cannot be returned, and a value that does not fit. */
  CHECK(query_all(*database, "SELECT [1, 2]", r).code() == DB_INVALID_ARGUMENT);
  CHECK(
      query_all(*database, "SELECT 18446744073709551615::UBIGINT", r).code() ==
      DB_INVALID_ARGUMENT);

  db_close(database);

  return;
}

struct writer_arg_t {
  /** Database under test. */
  db *m_db{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_index{0};

  /** Writes to make. */
  int m_write_count{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Write count ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  for (int i = 0; i < arg->m_write_count; ++i) {
    if (!write_id(*arg->m_db, arg->m_thread_index * 100000 + i).ok()) {
      arg->m_errors++;
    }
  }

  return;
}

struct reader_arg_t {
  /** Database under test. */
  db *m_db{nullptr};

  /** Set when the thread should stop; protected by m_mutex. */
  bool *m_stop{nullptr};

  /** Protects *m_stop. */
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

    status =
        query_all(*arg->m_db, "SELECT count(*), count(DISTINCT id) FROM k", r);

    if (!status.ok()) {
      /* Before the first write the table does not exist yet. */
      if (status.message().find("does not exist") == std::string::npos) {
        fprintf(stderr, "reader: %s\n", status.message().c_str());
        arg->m_errors++;
      }

      continue;
    }

    const int64_t rows = r.m_rows[0][0].m_integer;
    const int64_t distinct = r.m_rows[0][1].m_integer;

    if (rows != distinct || rows < last) {
      arg->m_errors++;
    }

    last = rows;
    arg->m_queries++;
  }

  return;
}

/** Queries during writes and frequent flushes always see a consistent
snapshot. */
static void test_snapshot_under_load() {
  test_dir_t dir;

  /* Background flushes every few milliseconds, so queries keep starting
  around flush commits. */
  db::options_t options;
  options.m_flush_interval_ms = 3;

  /* Keep writes overlapping flushes even when page-cache writes finish fast. */
  options.m_memtable_limit_bytes = 1024;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  bool stop = false;
  std::mutex mutex;
  std::vector<reader_arg_t> readers(2);
  std::vector<std::thread> reader_threads;

  for (size_t i = 0; i < readers.size(); ++i) {
    readers[i].m_db = database;
    readers[i].m_stop = &stop;
    readers[i].m_mutex = &mutex;
    reader_threads.push_back(std::thread(reader_main, &readers[i]));
  }

  const int threads = 4;
  const int count = 150;
  std::vector<writer_arg_t> writers(threads);
  std::vector<std::thread> writer_threads;

  for (int t = 0; t < threads; ++t) {
    writers[t].m_db = database;
    writers[t].m_thread_index = t;
    writers[t].m_write_count = count;
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

  db::stats_t stats;
  database->stats(stats);
  printf("snapshot under load: %d queries during %llu background flushes\n",
         queries, static_cast<unsigned long long>(stats.m_background_flushes));
  CHECK(queries > 0 && stats.m_background_flushes > 0);

  CHECK(query_int(*database, "SELECT count(DISTINCT id) FROM k") ==
        threads * count);

  db_close(database);

  return;
}

/** Successive threads can fetch and close a cursor while it retains S. */
static void test_cursor_thread_transfer() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, nullptr, &database).ok());

  write_cpu(*database, "a", 0, 3);

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT * FROM cpu", false, &cursor).ok());

  db_status_t fetched = DB_ERROR;
  size_t count = 0;

  std::thread reader([&] {
    query_rows_t rows;

    do {
      fetched = cursor->fetch(rows);
      count += rows.size();
    } while (fetched.ok() && !rows.empty());
  });

  reader.join();
  CHECK(fetched.ok() && count == 3);

  std::thread closer([&] { db_cursor::destroy(cursor); });
  closer.join();
  CHECK(query_int(*database, "SELECT count(*) FROM cpu") == 3);

  db_close(database);

  return;
}

/** Built-in SET skips namespace locking; extensions and later DDL do not. */
static void test_command_settings() {
  duckdb_database database = nullptr;
  REQUIRE(duckdb_open(nullptr, &database) == DuckDBSuccess);

  for (const char *sql : {"SET threads = 4", "RESET threads", "USE main",
                          "SET search_path = 'main'", "SET VARIABLE n = 4",
                          "RESET VARIABLE n", "SET worker_threads = 4"}) {
    query_command command;
    CHECK(command.init(database, sql, nullptr).ok());
    CHECK(!command.changes_namespace());
  }

  for (const char *sql :
       {"SET unknown_extension_setting = 4", "RESET unknown_extension_setting",
        "SET threads = 4; CREATE TABLE ordinary (v BIGINT)",
        "USE main; DROP TABLE ordinary"}) {
    query_command command;
    CHECK(command.init(database, sql, nullptr).ok());
    CHECK(command.changes_namespace());
  }

  duckdb_close(&database);

  return;
}

/** Each source keeps its own duration through pruning, joins and reopening. */
static void test_mixed_hour_spans() {
  test_dir_t dir;
  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  store::destroy(st);

  db *database = nullptr;
  db::options_t options = *deferred_flush_options();
  REQUIRE(db_open(dir.m_path, &options, &database).ok());
  for (const auto &[name, hours] :
       std::vector<std::pair<std::string, uint32_t>>{
           {"hourly", 1}, {"daily", 24}, {"five", 5}, {"empty", 3}}) {
    REQUIRE(database->create_table(name, {hours, std::nullopt}).ok());
  }

  db_close(database);
  options.m_partition_duration_hours = 168;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());
  CHECK(!database->exec("CREATE TABLE empty(v BIGINT)").ok());
  CHECK(query_int(*database,
                  "WITH empty AS (SELECT 9 AS v) SELECT v FROM empty") == 9);

  db_cursor *cursor = nullptr;
  const db_status_t uninitialized =
      database->open_cursor("SELECT * FROM empty", false, &cursor);
  CHECK(uninitialized.code() == DB_TABLE_NOT_INITIALIZED);
  CHECK(uninitialized.message().find("not initialized") != std::string::npos);
  CHECK(cursor == nullptr);

  ingest_field_t f = field_int("v", 1);
  const int64_t offsets[] = {
      -1, 0, UT_HOUR_US - 1, UT_HOUR_US, 5 * UT_HOUR_US, TEST_DAY_US};

  std::vector<ingest_point_t> points;

  for (int64_t offset : offsets) {
    points.push_back(point(TEST_BASE_TIME_US + offset, nullptr, 0, &f, 1));
  }

  for (const char *name : {"hourly", "daily", "five"}) {
    batch_seq_t seq = 0;
    REQUIRE(
        database->write(name, strlen(name), points.data(), points.size(), seq)
            .ok());
  }

  for (int pass = 0; pass < 2; ++pass) {
    if (pass == 1) {
      flush(*database);
    }

    for (const char *name : {"hourly", "daily", "five"}) {
      CHECK(query_int(*database, "SELECT count(*) FROM " + std::string(name)) ==
            6);
      CHECK(query_int(*database,
                      "SELECT count(*) FROM " + std::string(name) +
                          " WHERE time >= make_timestamp(" +
                          std::to_string(TEST_BASE_TIME_US + UT_HOUR_US) +
                          ") AND time < make_timestamp(" +
                          std::to_string(TEST_BASE_TIME_US + 5 * UT_HOUR_US) +
                          ")") == 1);
    }

    CHECK(query_int(*database,
                    "SELECT count(*) FROM hourly h JOIN daily d "
                    "ON h.time = d.time JOIN five f ON h.time = f.time") == 6);
  }

  db_close(database);
  REQUIRE(db_open(dir.m_path, &options, &database).ok());
  CHECK(query_int(*database, "SELECT count(*) FROM hourly") == 6);

  batch_seq_t seq = 0;
  REQUIRE(database->write("empty", 5, points.data(), points.size(), seq).ok());
  CHECK(query_int(*database, "SELECT count(*) FROM empty") == 6);
  flush(*database);

  db_close(database);
  REQUIRE(store::open(dir.m_path, 168, &st).ok());
  CHECK_EQ(st->catalog().m_partition_duration_hours, uint32_t{24});
  CHECK_EQ(st->catalog().m_tables.at("hourly").m_partition_duration_hours,
           uint32_t{1});
  CHECK_EQ(st->catalog().m_tables.at("daily").m_partition_duration_hours,
           uint32_t{24});
  CHECK_EQ(st->catalog().m_tables.at("five").m_partition_duration_hours,
           uint32_t{5});
  CHECK_EQ(st->catalog().m_tables.at("empty").m_partition_duration_hours,
           uint32_t{3});
  store::destroy(st);
}

/** Auto-creation stays WAL-only and uses the persisted default after replay. */
static void test_unflushed_default_span() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_partition_duration_hours = 5;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &f, 1);
  batch_seq_t seq = 0;
  REQUIRE(database->write("m", 1, &p, 1, seq).ok());

  db_close(database);

  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  CHECK(st->catalog().m_tables.empty());
  CHECK_EQ(st->catalog().m_flushed_seq, batch_seq_t{0});
  CHECK_EQ(st->catalog().m_partition_duration_hours, uint32_t{5});
  store::destroy(st);
  options.m_partition_duration_hours = 24;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 1);
  flush(*database);

  db_close(database);
  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  CHECK_EQ(st->catalog().m_tables.at("m").m_partition_duration_hours,
           uint32_t{5});
  store::destroy(st);
}

/** Run query validation, snapshot, conversion and concurrency tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"command_settings", test_command_settings},
      {"mixed_hour_spans", test_mixed_hour_spans},
      {"unflushed_default_span", test_unflushed_default_span},
      {"scan_registration", test_scan_registration},
      {"callback_errors/bind", [] { test_callback_errors(true); }},
      {"callback_errors/init", [] { test_callback_errors(false); }},
      {"cursor_thread_transfer", test_cursor_thread_transfer},

      {"memtable_and_storage_sources", test_memtable_and_storage_sources},
      {"columns_added_later", test_columns_added_later},

      {"filter_pushdown", test_filter_pushdown},
      {"partition_pruning", test_partition_pruning},

      {"table_sources", test_table_sources},
      {"time_ranges", test_time_ranges},
      {"partition_span", test_partition_span},

      {"rejected_statements", test_rejected_statements},
      {"pivot_references", test_pivot_references},
      {"result_types", test_result_types},

      {"snapshot_under_load", test_snapshot_under_load},
  };

  return (test_main("query-t", argc, argv, cases));
}
