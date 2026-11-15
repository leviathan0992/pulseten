/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * store-t.cc
 *    Tests for the DuckDB store: creating the file, flushing writes with
 *    schema changes and day partitions, reloading, and rollback.
 *
 * Covers: src/store/store.cc, src/store/pt_duckdb.cc
 *
 *-------------------------------------------------------------------------
 */

#include "store.h"

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "db_schema.h"
#include "ingest.h"
#include "pt_duckdb.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_store.h"
#include "test_store_access.h"
#include "test_util.h"
#include "ut.h"

/** Execute a query and read its single integer result.
@return the single integer a query returns, or -1 */
static int64_t query_int(store &st, const std::string &sql) {
  int64_t v = -1;
  CHECK(test_store_query_int(st, sql, v).ok());

  return (v);
}

/** Compare the metadata describing two stored database states.
@return true if two states describe the same file contents */
static bool states_equal(const store_catalog_t &a, const store_catalog_t &b) {
  if (a.m_flushed_seq != b.m_flushed_seq ||
      a.m_next_table_id != b.m_next_table_id ||
      a.m_tables.size() != b.m_tables.size()) {
    return (false);
  }

  std::map<std::string, store_table_t>::const_iterator t;

  for (t = a.m_tables.begin(); t != a.m_tables.end(); ++t) {
    if (b.m_tables.count(t->first) == 0) {
      return (false);
    }

    const store_table_t &x = t->second;
    const store_table_t &y = b.m_tables.find(t->first)->second;

    if (x.m_table_id != y.m_table_id || x.m_initialized != y.m_initialized ||
        x.m_retention_hours != y.m_retention_hours ||
        x.m_partition_duration_hours != y.m_partition_duration_hours ||
        x.m_partitions.size() != y.m_partitions.size() ||
        x.m_schema.m_columns.size() != y.m_schema.m_columns.size()) {
      return (false);
    }

    for (size_t c = 0; c < x.m_schema.m_columns.size(); ++c) {
      const schema_column_t &p = x.m_schema.m_columns[c];
      const schema_column_t &q = y.m_schema.m_columns[c];
      if (p.m_name != q.m_name || p.m_column_type != q.m_column_type ||
          p.m_data_type != q.m_data_type) {
        return (false);
      }
    }
    for (const auto &[hour, p] : x.m_partitions) {
      const auto q = y.m_partitions.find(hour);
      if (q == y.m_partitions.end() || p.m_table_id != q->second.m_table_id ||
          p.m_start_hour != q->second.m_start_hour ||
          p.m_table_name != q->second.m_table_name ||
          p.m_column_count != q->second.m_column_count) {
        return (false);
      }
    }
  }

  return (true);
}

/** A new file has the internal tables and an empty state. */
static void test_open_new() {
  db_status_t status;

  test_dir_t dir;
  store *st = nullptr;

  status = store::open(dir.m_path, 24, &st);
  CHECK(status.ok());
  CHECK(status.message().empty());

  const store_catalog_t &state = st->catalog();
  CHECK(state.m_flushed_seq == 0 && state.m_next_table_id == 1);
  CHECK(state.m_tables.empty());

  store::destroy(st);

  status = store::open(dir.m_path, 24, &st);
  CHECK(status.ok());
  CHECK(st->catalog().m_flushed_seq == 0);

  store::destroy(st);

  return;
}

/** A nonempty foreign database fails catalog loading without initialization
or changes to its existing table. */
static void test_open_foreign_database() {
  test_dir_t dir;
  const std::string path = dir.file(STORE_FILE_NAME);
  duckdb_database native = nullptr;
  duckdb_connection connection = nullptr;
  CHECK(duckdb_open(path.c_str(), &native) == DuckDBSuccess);
  CHECK(duckdb_connect(native, &connection) == DuckDBSuccess);
  CHECK(pt_duckdb_exec(connection,
                       "CREATE TABLE user_table AS SELECT 7::BIGINT AS v")
            .ok());

  duckdb_disconnect(&connection);
  duckdb_close(&native);

  store *st = nullptr;
  const db_status_t err = store::open(dir.m_path, 24, &st);
  CHECK(err.code() == DB_ERROR && st == nullptr);
  CHECK(err.message().find("_pt_state") != std::string::npos);

  /* A failed catalog load must release native handles and keep the file. */
  CHECK(duckdb_open(path.c_str(), &native) == DuckDBSuccess);
  CHECK(duckdb_connect(native, &connection) == DuckDBSuccess);

  pt_duckdb_rows_t rows;
  CHECK(pt_duckdb_exec(connection, "SELECT v FROM user_table", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 7);
  CHECK(pt_duckdb_exec(connection,
                       "SELECT count(*) FROM information_schema.tables "
                       "WHERE table_schema = 'main'",
                       rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);

  duckdb_disconnect(&connection);
  duckdb_close(&native);

  return;
}

/** Sessions leave connections open; stores support repeated close and partial
initialization cleanup. */
static void test_duckdb_ownership() {
  test_dir_t dir;
  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());

  CHECK(pt_duckdb_exec(st->connection(), "CREATE TABLE t (v BIGINT)").ok());

  {
    pt_duckdb_session session(st->connection());
    CHECK(session.begin().ok());
    CHECK(
        pt_duckdb_exec(session.connection(), "INSERT INTO t VALUES (1)").ok());
    CHECK(session.commit().ok());
  }

  pt_duckdb_rows_t rows;
  CHECK(pt_duckdb_exec(st->connection(), "SELECT v FROM t", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);

  store_test_access::close(*st);
  store_test_access::close(*st);
  store::destroy(st);

  st = nullptr;
  CHECK(!store::open(dir.file("missing").c_str(), 24, &st).ok());
  CHECK(st == nullptr);

  return;
}

/** Both execution overloads handle statements by their actual result type,
including writes that return rows. Failed execution clears old output. */
static void test_pt_duckdb_exec() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());

  const duckdb_connection conn = st->connection();
  pt_duckdb_rows_t rows;

  CHECK(pt_duckdb_exec(conn, "CREATE TABLE user_table (v BIGINT)").ok());
  CHECK(pt_duckdb_exec(conn, "INSERT INTO user_table VALUES (1)").ok());
  CHECK(pt_duckdb_exec(conn, "SELECT v FROM user_table", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);

  /* A changed-row count is not a returned row set. */
  CHECK(pt_duckdb_exec(conn, "INSERT INTO user_table VALUES (2)", rows).ok());
  CHECK(rows.empty());
  CHECK(pt_duckdb_exec(conn, "INSERT INTO user_table VALUES (3) RETURNING v",
                       rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 3);
  CHECK(pt_duckdb_exec(
            conn, "UPDATE user_table SET v = 4 WHERE v = 3 RETURNING v", rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 4);
  CHECK(pt_duckdb_exec(conn, "DELETE FROM user_table WHERE v = 4 RETURNING v",
                       rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 4);

  CHECK(pt_duckdb_exec(conn, "CREATE TABLE other (v BIGINT)", rows).ok());
  CHECK(rows.empty());
  CHECK(
      pt_duckdb_exec(conn, "SELECT v FROM user_table WHERE false", rows).ok());
  CHECK(rows.empty());
  CHECK(pt_duckdb_exec(conn, "SELECT i FROM range(5000) AS t(i)", rows).ok());
  CHECK(rows.size() == 5000);

  if (rows.size() == 5000) {
    CHECK(rows[0][0].m_integer == 0 && rows[4999][0].m_integer == 4999);
  }

  const db_status_t failed =
      pt_duckdb_exec(conn, "SELECT * FROM missing_exec_table", rows);
  CHECK(failed.code() == DB_ERROR && !failed.message().empty());
  CHECK(rows.empty());

  const std::string diagnostic = failed.message();
  CHECK(pt_duckdb_exec(conn, "SELECT * FROM missing_exec_table").code() ==
        DB_ERROR);
  CHECK(pt_duckdb_exec(conn, "SELECT v FROM user_table ORDER BY v", rows).ok());
  CHECK(rows.size() == 2);

  if (rows.size() == 2) {
    CHECK(rows[0][0].m_integer == 1 && rows[1][0].m_integer == 2);
  }

  CHECK(failed.message() == diagnostic);

  /* Discarding RETURNING output still completes the write. */
  CHECK(pt_duckdb_exec(conn, "INSERT INTO user_table VALUES (5) RETURNING v")
            .ok());
  CHECK(pt_duckdb_exec(conn, "SELECT count(*) FROM user_table", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 3);

  store::destroy(st);

  return;
}

/** An unknown format is rejected without initializing a replacement store. */
static void test_unknown_format() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());
  CHECK(pt_duckdb_exec(st->connection(),
                       "UPDATE _pt_state SET format = 4294967295")
            .ok());

  store::destroy(st);
  CHECK(store::open(dir.m_path, 24, &st).code() == DB_CORRUPTION);
  CHECK(st == nullptr);

  duckdb_database native = nullptr;
  duckdb_connection connection = nullptr;
  CHECK(duckdb_open(dir.file(STORE_FILE_NAME).c_str(), &native) ==
        DuckDBSuccess);
  CHECK(duckdb_connect(native, &connection) == DuckDBSuccess);

  pt_duckdb_rows_t rows;
  CHECK(pt_duckdb_exec(connection, "SELECT format FROM _pt_state", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 4294967295LL);

  duckdb_disconnect(&connection);
  duckdb_close(&native);
}

/** Invalid metadata is rejected before the next write can use table id zero. */
static void test_zero_table_id() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());
  CHECK(
      pt_duckdb_exec(st->connection(), "UPDATE _pt_state SET next_table_id = 0")
          .ok());

  store::destroy(st);
  CHECK(store::open(dir.m_path, 24, &st).code() == DB_CORRUPTION);
  CHECK(st == nullptr);

  store::destroy(st);

  return;
}

/** Invalid stored columns are rejected by both catalog reads and open,
before a caller can restore their schema into a memtable. */
static void test_invalid_stored_schemas() {
  const std::string mutations[] = {
      "DELETE FROM _pt_columns",
      "INSERT INTO _pt_columns "
      "SELECT 1, i, 'v' || CAST(i AS VARCHAR), 2, 2 FROM range(2, " +
          std::to_string(SCHEMA_MAX_COLUMNS + 1) + ") AS t(i)",
      "UPDATE _pt_columns SET col_id = 2 WHERE col_id = 1",
      "UPDATE _pt_columns SET name = 'event_time' WHERE col_id = 0",
      "UPDATE _pt_columns SET kind = 2 WHERE col_id = 0",
      "UPDATE _pt_columns SET type = 2 WHERE col_id = 0",
      "UPDATE _pt_columns SET name = 'time' WHERE col_id = 1",
      "UPDATE _pt_columns SET name = 'V' WHERE col_id = 1",
      "UPDATE _pt_columns SET name = '_pt_v' WHERE col_id = 1",
      "UPDATE _pt_columns SET kind = 1 WHERE col_id = 1",
      "UPDATE _pt_columns SET type = 4 WHERE col_id = 1",
      "UPDATE _pt_columns SET kind = 0 WHERE col_id = 1"};

  for (const std::string &mutation : mutations) {
    test_dir_t dir;
    store *st = nullptr;
    REQUIRE(store::open(dir.m_path, 24, &st).ok());

    test_catalog_t cat;
    ingest_field_t field = field_int("v", 1);
    ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
    store_write_t write = prepare_catalog_write(cat, 1, "m", &p, 1);
    CHECK(st->append(&write, 1).ok());
    CHECK(pt_duckdb_exec(st->connection(), mutation).ok());

    CHECK(pt_duckdb_exec(st->connection(), "BEGIN TRANSACTION").ok());

    store_catalog_t snapshot;
    const db_status_t read_status =
        store_catalog_reader(st->connection(), snapshot).read();
    CHECK(read_status.code() == DB_CORRUPTION);
    CHECK(!read_status.message().empty());
    CHECK(pt_duckdb_exec(st->connection(), "ROLLBACK").ok());

    store::destroy(st);

    const db_status_t open_status = store::open(dir.m_path, 24, &st);
    CHECK(open_status.code() == DB_CORRUPTION);
    CHECK(!open_status.message().empty());
    CHECK(st == nullptr);

    store::destroy(st);
  }

  return;
}

/** Table id exhaustion rolls back a flush without wrapping the counter;
existing tables remain writable and the catalog can still be reopened. */
static void test_table_id_exhaustion() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());

  const uint64_t exhausted = std::numeric_limits<uint64_t>::max();
  CHECK(
      pt_duckdb_exec(st->connection(), "UPDATE _pt_state SET next_table_id = " +
                                           std::to_string(exhausted - 1))
          .ok());

  store::destroy(st);
  CHECK(store::open(dir.m_path, 24, &st).ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &f, 1);
  store_write_t writes[] = {prepare_catalog_write(cat, 1, "last", &p, 1),
                            prepare_catalog_write(cat, 2, "extra", &p, 1)};

  const db_status_t exhausted_status = st->append(writes, 2);
  CHECK(exhausted_status.code() == DB_LIMIT_EXCEEDED);

  if (exhausted_status.code() != DB_LIMIT_EXCEEDED) {
    store::destroy(st);

    return;
  }

  CHECK(st->catalog().m_flushed_seq == 0);
  CHECK(st->catalog().m_next_table_id == exhausted - 1);
  CHECK(st->catalog().m_tables.empty());
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 0);

  CHECK(st->append(writes, 1).ok());
  CHECK(st->catalog().m_next_table_id == exhausted);
  CHECK(st->catalog().m_tables.at("last").m_table_id == exhausted - 1);

  store_write_t existing = prepare_catalog_write(cat, 2, "last", &p, 1);
  CHECK(st->append(&existing, 1).ok());

  store_write_t extra = prepare_catalog_write(cat, 3, "extra", &p, 1);
  CHECK(st->append(&extra, 1).code() == DB_LIMIT_EXCEEDED);
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(st->catalog().m_next_table_id == exhausted);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 1);

  store::destroy(st);

  CHECK(store::open(dir.m_path, 24, &st).ok());
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(st->catalog().m_next_table_id == exhausted);
  CHECK(st->catalog().m_tables.size() == 1);

  const std::string physical = st->catalog()
                                   .m_tables.at("last")
                                   .m_partitions.begin()
                                   ->second.m_table_name;
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);

  store::destroy(st);

  return;
}

/** Flushed writes, added columns and exact times survive a reload, which
gives the same state. */
static void test_flush_and_reload() {
  db_status_t reload_status;

  test_dir_t dir;
  store *st = nullptr;
  reload_status = store::open(dir.m_path, 24, &st);
  CHECK(reload_status.ok());

  test_catalog_t cat;

  /* Three cpu points, then one that adds a column, then a mem point. */
  ingest_tag_t host[] = {tag("host", "a")};
  ingest_field_t usage[] = {field_double("usage", 0.5)};
  ingest_field_t more[] = {field_double("usage", 0.75), field_int("cores", 8)};
  ingest_field_t used[] = {field_int("used", 1024)};

  ingest_point_t p1[] = {point(TEST_BASE_TIME_US, host, 1, usage, 1),
                         point(TEST_BASE_TIME_US + 1, host, 1, usage, 1),
                         point(TEST_BASE_TIME_US + 2, host, 1, usage, 1)};

  ingest_point_t p2 = point(TEST_BASE_TIME_US + 3, host, 1, more, 2);
  ingest_point_t p3 = point(TEST_BASE_TIME_US, nullptr, 0, used, 1);

  store_write_t writes[3];
  writes[0] = prepare_catalog_write(cat, 1, "cpu", p1, 3);
  writes[1] = prepare_catalog_write(cat, 2, "cpu", &p2, 1);
  writes[2] = prepare_catalog_write(cat, 3, "mem", &p3, 1);

  /* The first write alone, then the other two. */
  CHECK(st->append(writes, 1).ok());
  CHECK(st->catalog().m_flushed_seq == 1);
  CHECK(st->append(writes + 1, 2).ok());

  const store_catalog_t &state = st->catalog();
  CHECK(state.m_flushed_seq == 3);
  CHECK(state.m_tables.size() == 2);
  const store_table_t &cpu_table = state.m_tables.at("cpu");
  CHECK(cpu_table.m_schema.m_columns.size() == 4);
  CHECK(cpu_table.m_partitions.size() == 1);
  CHECK(state.m_tables.at("mem").m_partitions.size() == 1);

  /* cpu: time, host, usage, cores. The partition was widened for cores;
  the first three rows have NULL there. */
  const std::string cpu =
      store_partition_name(1, ut_utc_hour(TEST_BASE_TIME_US));

  const store_partition_t &part =
      cpu_table.m_partitions.at(ut_utc_hour(TEST_BASE_TIME_US));
  CHECK(part.m_table_name == cpu && part.m_column_count == 4);

  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu) == 4);
  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu + " WHERE c3 IS NULL") ==
        3);
  CHECK(query_int(*st, "SELECT max(c3) FROM " + cpu) == 8);
  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu + " WHERE c1 = 'a'") == 4);
  CHECK(query_int(*st, "SELECT max(_pt_seq) FROM " + cpu) == 2);
  CHECK(query_int(*st, "SELECT max(_pt_row) FROM " + cpu +
                           " WHERE _pt_seq = 1") == 2);

  /* The exact microseconds survive. */
  CHECK(query_int(*st, "SELECT epoch_us(max(c0)) FROM " + cpu) ==
        TEST_BASE_TIME_US + 3);

  store_catalog_t before = state;
  store::destroy(st);

  /* Reloading gives the same state and latest column directory. */

  reload_status = store::open(dir.m_path, 24, &st);
  CHECK(reload_status.ok());
  CHECK(reload_status.message().empty());
  CHECK(states_equal(before, st->catalog()));

  const schema_t &schema = st->catalog().m_tables.find("cpu")->second.m_schema;
  CHECK(schema.m_columns.size() == 4);
  CHECK(schema.m_columns[3].m_name == "cores" &&
        schema.m_columns[3].m_data_type == data_type_t::BIGINT);

  store::destroy(st);

  return;
}

/** One write spanning three UTC days, one before 1970, lands in three
partitions. */
static void test_day_partitions() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());

  /* One write across three UTC days, one of them before 1970. */
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t points[] = {point(-1, nullptr, 0, f, 1),
                             point(0, nullptr, 0, f, 1),
                             point(UT_DAY_US - 1, nullptr, 0, f, 1),
                             point(UT_DAY_US, nullptr, 0, f, 1)};

  test_catalog_t cat;
  store_write_t w = prepare_catalog_write(cat, 1, "m", points, 4);
  CHECK(st->append(&w, 1).ok());

  const store_catalog_t &state = st->catalog();
  CHECK(state.m_tables.at("m").m_partitions.size() == 3);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_n24") == 1);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_p0") == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_p24") == 1);

  store::destroy(st);

  return;
}

/** Broken append contracts abort before changing the stored state. */
static void test_append_invariants() {
  test_dir_t dir;
  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};

  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, f, 1);

  store_write_t w = prepare_catalog_write(cat, 1, "m", &p, 1);

  const auto expect_abort = [&](const store_write_t *writes, size_t count) {
    const pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0) {
      static_cast<void>(st->append(writes, count));
      _exit(0);
    }

    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);

    return;
  };

  store_write_t skipped = w;
  skipped.m_seq = 2;
  expect_abort(&skipped, 1);
  expect_abort(&w, 0);

  schema_t schema = *w.m_schema;
  schema.m_name = "other";
  store_write_t mismatched = w;
  mismatched.m_schema = &schema;
  expect_abort(&mismatched, 1);

  CHECK_EQ(query_int(*st, "SELECT flushed_seq FROM _pt_state"), int64_t{0});
  CHECK_EQ(query_int(*st, "SELECT count(*) FROM _pt_tables"), int64_t{0});
  REQUIRE(st->append(&w, 1).ok());

  /* A committed sequence cannot be appended again. */
  expect_abort(&w, 1);
  CHECK_EQ(query_int(*st, "SELECT flushed_seq FROM _pt_state"), int64_t{1});
  const auto &partitions = st->catalog().m_tables.at("m").m_partitions;
  REQUIRE(partitions.size() == 1);
  const std::string &partition_name = partitions.begin()->second.m_table_name;
  CHECK_EQ(query_int(*st, "SELECT count(*) FROM " + partition_name),
           int64_t{1});

  store::destroy(st);

  return;
}

/** Snapshot catalog reads share partition decoding with open, without loading
ordinary-table names; reopening still performs complete physical catalog
checks. */
static void test_snapshot_catalog() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());

  test_catalog_t cat;
  ingest_field_t field = field_int("v", 1);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  store_write_t w = prepare_catalog_write(cat, 1, "m", &p, 1);
  CHECK(st->append(&w, 1).ok());

  pt_duckdb_session session(st->connection());
  CHECK(
      pt_duckdb_exec(session.connection(), "CREATE TABLE user_table (v BIGINT)")
          .ok());
  CHECK(session.begin().ok());

  store_catalog_t snapshot;
  CHECK(store_catalog_reader(session.connection(), snapshot).read().ok());
  CHECK(snapshot.m_flushed_seq == 1);
  CHECK(snapshot.m_tables.size() == 1);
  CHECK(snapshot.m_tables.at("m").m_partitions.size() == 1);
  CHECK(pt_duckdb_exec(session.connection(), "COMMIT").ok());

  store::destroy(st);

  CHECK(store::open(dir.m_path, 24, &st).ok());
  CHECK(query_int(
            *st,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'user_table'") == 1);
  CHECK(st->catalog().m_tables.at("m").m_partitions.size() == 1);

  bool exists = false;
  CHECK(st->ordinary_table_exists("user_table", exists).ok());
  CHECK(exists);
  CHECK(st->ordinary_table_exists("m", exists).ok());
  CHECK(!exists);

  store::destroy(st);

  return;
}

/** An appender constraint failure rolls back every row and catalog change;
the same sequence can be retried after removing the test constraint. */
static void test_appender_failure() {
  db_status_t test_status;

  test_dir_t dir;
  store *st = nullptr;
  test_status = store::open(dir.m_path, 24, &st);
  CHECK(test_status.ok());

  test_catalog_t cat;
  ingest_field_t field = field_int("v", 1);
  ingest_point_t point_value = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  store_write_t first = prepare_catalog_write(cat, 1, "m", &point_value, 1);
  test_status = st->append(&first, 1);
  CHECK(test_status.ok());

  const std::string physical =
      st->catalog().m_tables.at("m").m_partitions.begin()->second.m_table_name;
  CHECK(pt_duckdb_exec(
            st->connection(),
            "CREATE UNIQUE INDEX _pt_test_unique ON " + physical + " (c1)")
            .ok());

  store_write_t next = prepare_catalog_write(cat, 2, "m", &point_value, 1);
  test_status = st->append(&next, 1);
  CHECK(test_status.code() == DB_ERROR);
  CHECK(!test_status.message().empty());
  CHECK(st->catalog().m_flushed_seq == 1);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 1);

  CHECK(pt_duckdb_exec(st->connection(), "DROP INDEX _pt_test_unique").ok());

  test_status = st->append(&next, 1);
  CHECK(test_status.ok());
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);

  store::destroy(st);

  test_status = store::open(dir.m_path, 24, &st);
  CHECK(test_status.ok());
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);

  store::destroy(st);

  return;
}

/** COMMIT and the best-effort ROLLBACK can both fail. Preserve the first
diagnostic and the unknown-outcome classification. */
static void test_commit_rollback_error() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());

  pt_duckdb_session session(st->connection());

  const db_status_t status = session.commit();
  CHECK(status.code() == DB_OUTCOME_UNKNOWN);
  CHECK(status.message().find("commit") != std::string::npos);
  CHECK(status.message().find("rollback:") != std::string::npos);

  const std::string original = status.message();
  CHECK(session.begin().ok());
  CHECK(session.rollback().ok());
  CHECK(status.message() == original);

  store::destroy(st);

  return;
}

/** An expiry that cannot begin must leave the existing transaction intact. */
static void test_expire_begin_failure() {
  db_status_t test_status;

  test_dir_t dir;
  store *st = nullptr;
  test_status = store::open(dir.m_path, 24, &st);
  CHECK(test_status.ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &f, 1);
  store_write_t w = prepare_catalog_write(cat, 1, "m", &p, 1);
  test_status = st->append(&w, 1);
  CHECK(test_status.ok());

  CHECK(pt_duckdb_exec(st->connection(), "BEGIN TRANSACTION").ok());

  uint64_t removed = 0;
  test_status =
      test_store_expire(*st, ut_utc_day(TEST_BASE_TIME_US) + 1, removed);
  CHECK(test_status.code() == DB_ERROR);
  CHECK(removed == 0);
  CHECK(st->catalog().m_tables.at("m").m_partitions.size() == 1);
  CHECK(test_status.message().find("rollback:") == std::string::npos);
  CHECK(pt_duckdb_exec(st->connection(), "ROLLBACK").ok());

  store::destroy(st);

  return;
}

/** An expiry failure undoes earlier drops and leaves the catalog unchanged. */
static void test_expire_rollback() {
  for (const bool fail_delete : {false, true}) {
    test_dir_t dir;
    store *st = nullptr;
    REQUIRE(store::open(dir.m_path, 24, &st).ok());

    test_catalog_t cat;
    const ingest_field_t field = field_int("v", 1);
    const ingest_point_t points[] = {
        point(TEST_BASE_TIME_US, nullptr, 0, &field, 1),
        point(TEST_BASE_TIME_US + UT_DAY_US, nullptr, 0, &field, 1)};

    const store_write_t write = prepare_catalog_write(cat, 1, "m", points, 2);
    CHECK(st->append(&write, 1).ok());

    const store_catalog_t before = st->catalog();
    const std::string first =
        store_partition_name(1, ut_utc_hour(TEST_BASE_TIME_US));

    const std::string second =
        store_partition_name(1, ut_utc_hour(TEST_BASE_TIME_US) + 24);

    /* Missing metadata fails after a DROP; a missing second partition fails
    after both the first DROP and its catalog DELETE. */
    const std::string renamed = fail_delete ? "_pt_partitions" : second;
    CHECK(pt_duckdb_exec(st->connection(), "ALTER TABLE " + renamed +
                                               " RENAME TO saved_partition")
              .ok());

    uint64_t removed = 99;
    const db_status_t err =
        test_store_expire(*st, ut_utc_day(TEST_BASE_TIME_US) + 2, removed);
    CHECK(err.code() == DB_ERROR);
    CHECK(err.message().find(renamed) != std::string::npos);
    CHECK(removed == 0);
    CHECK(states_equal(before, st->catalog()));
    CHECK(query_int(*st, "SELECT count(*) FROM " + first) == 1);
    CHECK(query_int(*st, std::string("SELECT count(*) FROM ") +
                             (fail_delete ? "saved_partition"
                                          : "_pt_partitions")) == 2);

    /* Restore the deliberately renamed table and retry on the same session. */
    CHECK(pt_duckdb_exec(st->connection(),
                         "ALTER TABLE saved_partition RENAME TO " + renamed)
              .ok());
    CHECK(test_store_expire(*st, ut_utc_day(TEST_BASE_TIME_US) + 2, removed)
              .ok());
    CHECK(removed == 2);
    CHECK(st->catalog().m_tables.at("m").m_partitions.empty());
    CHECK(query_int(*st, "SELECT count(*) FROM _pt_partitions") == 0);

    store::destroy(st);
  }

  return;
}

/** A failed rollback retains the operation's diagnostic; a successful one
leaves it intact for the caller. */
static void test_rollback_diagnostic() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());

  pt_duckdb_session session(st->connection());

  const db_status_t cause(DB_ERROR, "original failure");
  const db_status_t failed = session.rollback(cause);
  CHECK(failed.code() == DB_OUTCOME_UNKNOWN);
  CHECK(failed.message().find("original failure; rollback:") == 0);

  CHECK(session.begin().ok());

  const db_status_t restored = session.rollback(cause);
  CHECK(restored.code() == DB_ERROR);
  CHECK(restored.message() == "original failure");

  store::destroy(st);

  return;
}

/** A failed flush rollback reports an unknown outcome; closing discards its
still-open transaction, and reopening reads the committed catalog. */
static void test_flush_rollback_failure() {
  db_status_t test_status;

  test_dir_t dir;
  store *st = nullptr;
  test_status = store::open(dir.m_path, 24, &st);
  CHECK(test_status.ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &f, 1);
  store_write_t w = prepare_catalog_write(cat, 1, "m", &p, 1);

  ut_debug_global().arm("store.append", DB_ERROR);
  ut_debug_global().arm("store.rollback", DB_ERROR);
  test_status = st->append(&w, 1);
  CHECK(test_status.code() == DB_OUTCOME_UNKNOWN);
  CHECK(test_status.message().find("injected store.append") !=
        std::string::npos);
  CHECK(test_status.message().find("rollback: injected store.rollback") !=
        std::string::npos);
  CHECK(st->catalog().m_flushed_seq == 0);

  store::destroy(st);

  test_status = store::open(dir.m_path, 24, &st);
  CHECK(test_status.ok());
  CHECK(st->catalog().m_flushed_seq == 0);
  CHECK(st->catalog().m_tables.empty());

  store::destroy(st);

  return;
}

/** A flush failing before commit leaves nothing, and the same write succeeds
afterwards. */
static void test_rollback() {
  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};

  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, f, 1);
  store_write_t w = prepare_catalog_write(cat, 1, "m", &p, 1);

  /* Fails after the table, partition and rows were written. */
  ut_debug_global().arm("store.append", DB_ERROR);
  CHECK(st->append(&w, 1).code() == DB_ERROR);
  CHECK(st->catalog().m_flushed_seq == 0);

  /* Nothing of it is left: no rows, no partition table, no catalog rows.
  This shows that appender rows belong to the transaction. */
  CHECK(query_int(*st,
                  "SELECT count(*) FROM information_schema.tables "
                  "WHERE table_schema = 'main'") == 4);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 0);

  /* The same write succeeds afterwards. */
  CHECK(st->append(&w, 1).ok());

  store::destroy(st);

  CHECK(store::open(dir.m_path, 24, &st).ok());
  CHECK(st->catalog().m_flushed_seq == 1);

  store::destroy(st);

  return;
}

/** Match the native default and share explicit budgets across connections. */
static void test_memory_budget() {
  duckdb_database native = nullptr;
  duckdb_connection connection = nullptr;
  REQUIRE(duckdb_open(nullptr, &native) == DuckDBSuccess);
  REQUIRE(duckdb_connect(native, &connection) == DuckDBSuccess);
  const uint64_t default_limit = pt_duckdb_memory_limit_bytes(connection);
  duckdb_disconnect(&connection);
  duckdb_close(&native);

  test_dir_t dir;
  const uint64_t limit = (uint64_t{256} << 20) + 123;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st, PT_DUCKDB_MAX_MEMORY_LIMIT_BYTES + 1)
            .code() == DB_INVALID_ARGUMENT);
  CHECK(st == nullptr);
  CHECK(access(dir.file(STORE_FILE_NAME).c_str(), F_OK) != 0);

  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  CHECK(st->memory_limit_bytes() == default_limit);
  store::destroy(st);

  REQUIRE(
      store::open(dir.m_path, 24, &st, PT_DUCKDB_MAX_MEMORY_LIMIT_BYTES).ok());
  CHECK(st->memory_limit_bytes() == PT_DUCKDB_MAX_MEMORY_LIMIT_BYTES);
  store::destroy(st);

  CHECK(store::open(dir.m_path, 24, &st, limit).ok());

  REQUIRE(st != nullptr);

  CHECK(st->memory_limit_bytes() == limit);

  duckdb_connection other = nullptr;
  CHECK(duckdb_connect(st->database(), &other) == DuckDBSuccess);

  for (duckdb_connection conn : {st->connection(), other}) {
    pt_duckdb_rows_t rows;
    CHECK(pt_duckdb_exec(
              conn, "SELECT memory_limit FROM pragma_database_size()", rows)
              .ok());
    CHECK(rows.size() == 1 && rows[0].size() == 1);

    /* Compare connections through the setting too; its display is rounded. */
    CHECK(pt_duckdb_exec(conn, "SELECT current_setting('memory_limit')", rows)
              .ok());
    CHECK(rows.size() == 1 && rows[0][0].m_string == "256.0 MiB");
  }

  duckdb_disconnect(&other);
  store::destroy(st);
}

/** Default threads match native DuckDB; explicit settings cover every
connection. */
static void test_threads() {
  duckdb_database native = nullptr;
  duckdb_connection connection = nullptr;
  REQUIRE(duckdb_open(nullptr, &native) == DuckDBSuccess);
  REQUIRE(duckdb_connect(native, &connection) == DuckDBSuccess);

  pt_duckdb_rows_t rows;
  CHECK(pt_duckdb_exec(connection, "SELECT current_setting('threads')", rows)
            .ok());
  REQUIRE(rows.size() == 1 && rows[0].size() == 1);

  const int64_t default_threads = rows[0][0].m_integer;
  duckdb_disconnect(&connection);
  duckdb_close(&native);

  test_dir_t dir;
  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st, 0, PT_DUCKDB_MAX_THREADS + 1).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(st == nullptr);
  CHECK(access(dir.file(STORE_FILE_NAME).c_str(), F_OK) != 0);

  for (uint32_t threads : {0, 1, 2}) {
    REQUIRE(store::open(dir.m_path, 24, &st, 0, threads).ok());

    const int64_t expected = threads == 0 ? default_threads : threads;
    CHECK_EQ(st->threads(), expected);
    CHECK_EQ(query_int(*st, "SELECT current_setting('threads')"), expected);

    REQUIRE(duckdb_connect(st->database(), &connection) == DuckDBSuccess);
    CHECK(pt_duckdb_exec(connection, "SELECT current_setting('threads')", rows)
              .ok());
    CHECK(rows.size() == 1 && rows[0].size() == 1 &&
          rows[0][0].m_integer == expected);
    duckdb_disconnect(&connection);
    store::destroy(st);
  }
}

/** A flush preserves untouched catalog entries, including other partitions
of a table it writes. */
static void test_catalog_delta() {
  test_dir_t dir;
  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());

  test_catalog_t cat;
  ingest_field_t field = field_int("v", 1);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  ingest_point_t points[] = {
      p, point(TEST_BASE_TIME_US + UT_DAY_US, nullptr, 0, &field, 1)};
  store_write_t first[] = {prepare_catalog_write(cat, 1, "a", points, 2),
                           prepare_catalog_write(cat, 2, "b", &p, 1)};
  CHECK(st->append(first, 2).ok());

  const store_table_t *untouched = &st->catalog().m_tables.at("b");
  const schema_column_t *columns = untouched->m_schema.m_columns.data();
  const int64_t hour = ut_utc_hour(p.m_time_us);
  const store_partition_t *partition = &untouched->m_partitions.at(hour);
  const auto &updated_partitions = st->catalog().m_tables.at("a").m_partitions;
  const store_partition_t *later = &updated_partitions.at(hour + 24);
  store_write_t next = prepare_catalog_write(cat, 3, "a", &p, 1);
  CHECK(st->append(&next, 1).ok());
  CHECK(&st->catalog().m_tables.at("b") == untouched);
  CHECK(st->catalog().m_tables.at("b").m_schema.m_columns.data() == columns);
  CHECK(&untouched->m_partitions.at(hour) == partition);
  CHECK(query_int(*st, "SELECT count(*) FROM " + partition->m_table_name) == 1);
  CHECK(updated_partitions.size() == 2);
  CHECK(&updated_partitions.at(hour + 24) == later);
  CHECK(query_int(*st, "SELECT count(*) FROM " + later->m_table_name) == 1);
  CHECK(st->catalog().m_flushed_seq == 3);

  store::destroy(st);

  return;
}

/** Hour boundaries, including negative timestamps, survive physical checks. */
static void test_hour_partitions() {
  const int64_t h = UT_HOUR_US;

  for (uint32_t span : {1U, 5U, 24U}) {
    test_dir_t dir;
    store *st = nullptr;
    REQUIRE(store::open(dir.m_path, span, &st).ok());

    test_catalog_t cat;
    ingest_field_t f = field_int("v", 1);
    const int64_t times[] = {-5 * h - 1, -5 * h, -h - 1, -h,        -1,
                             0,          h - 1,  h,      5 * h - 1, 5 * h};

    std::vector<ingest_point_t> points;
    for (int64_t t : times)
      points.push_back(point(t, nullptr, 0, &f, 1));

    store_write_t w =
        prepare_catalog_write(cat, 1, "m", points.data(), points.size());
    REQUIRE(st->append(&w, 1).ok());
    CHECK(st->catalog().m_tables.at("m").m_initialized);

    uint64_t total = 0;

    for (const auto &[hour, part] :
         st->catalog().m_tables.at("m").m_partitions) {
      const int64_t first = part.m_start_hour * h;
      const int64_t end = (part.m_start_hour + span) * h;
      uint64_t expected = 0;
      for (int64_t t : times)
        if (t >= first && t < end)
          ++expected;

      const int64_t count =
          query_int(*st, "SELECT count(*) FROM " + part.m_table_name);
      CHECK_EQ(count, static_cast<int64_t>(expected));
      CHECK(query_int(*st, "SELECT count(*) FROM " + part.m_table_name +
                               " WHERE c0 < make_timestamp(" +
                               std::to_string(first) +
                               ") OR c0 >= make_timestamp(" +
                               std::to_string(end) + ")") == 0);
      total += count;
    }

    CHECK_EQ(total, uint64_t{10});

    const store_catalog_t before = st->catalog();
    store::destroy(st);
    REQUIRE(store::open(dir.m_path, 168, &st).ok());
    CHECK_EQ(st->catalog().m_partition_duration_hours, span);
    CHECK(states_equal(before, st->catalog()));
    CHECK(query_int(*st,
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_name = '_pt_partitions'") == 3);
    store::destroy(st);
  }

  CHECK_EQ(ut_utc_hour(-1), int64_t{-1});
  CHECK_EQ(ut_utc_hour(-h), int64_t{-1});
  CHECK_EQ(ut_utc_hour(-h - 1), int64_t{-2});
  CHECK(store_partition_name(1, INT64_MIN) == "_pt_t1_n9223372036854775808");
}

/** A declaration is durable; its first append initializes atomically. */
static void test_declared_table() {
  test_dir_t dir;
  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  test_store_declare(*st, {{"m", 5}});
  store::destroy(st);
  REQUIRE(store::open(dir.m_path, 1, &st).ok());
  CHECK(!st->catalog().m_tables.at("m").m_initialized);
  CHECK(st->catalog().m_tables.at("m").m_schema.m_columns.empty());
  CHECK(st->catalog().m_tables.at("m").m_partitions.empty());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 7);
  ingest_point_t points[] = {
      point(TEST_BASE_TIME_US, nullptr, 0, &f, 1),
      point(TEST_BASE_TIME_US + 5 * UT_HOUR_US, nullptr, 0, &f, 1)};

  store_write_t w = prepare_catalog_write(cat, 1, "m", points, 2);
  const store_catalog_t before = st->catalog();
  ut_debug_global().arm("store.append", DB_IO_ERROR);
  CHECK_EQ(st->append(&w, 1).code(), DB_IO_ERROR);
  CHECK(states_equal(before, st->catalog()));
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_columns") == 0);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables WHERE initialized") ==
        0);
  REQUIRE(st->append(&w, 1).ok());
  CHECK(st->catalog().m_tables.at("m").m_initialized);
  CHECK_EQ(st->catalog().m_tables.at("m").m_partition_duration_hours,
           uint32_t{5});
  CHECK_EQ(st->catalog().m_tables.at("m").m_partitions.size(), size_t{2});
  store::destroy(st);
  REQUIRE(store::open(dir.m_path, 168, &st).ok());
  CHECK(st->catalog().m_tables.at("m").m_initialized);
  CHECK_EQ(st->catalog().m_tables.at("m").m_partition_duration_hours,
           uint32_t{5});
  store::destroy(st);
}

/** Expiry uses each table's own end hour, including exact cutoff equality. */
static void test_mixed_span_expiry() {
  test_dir_t dir;
  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  test_store_declare(*st, {{"a", 1}, {"b", 5}, {"c", 24}});
  store::destroy(st);
  REQUIRE(store::open(dir.m_path, 24, &st).ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t points[] = {point(0, nullptr, 0, &f, 1),
                             point(UT_HOUR_US, nullptr, 0, &f, 1)};

  batch_seq_t seq = 1;

  for (const char *name : {"a", "b", "c"}) {
    store_write_t w = prepare_catalog_write(cat, seq++, name, points, 2);
    REQUIRE(st->append(&w, 1).ok());
  }

  std::vector<store_partition_key_t> expired_partitions;
  st->collect_expired_partitions((0 + 1) * UT_HOUR_US, 1, expired_partitions);
  CHECK(expired_partitions.empty());
  st->collect_expired_partitions((1 + 1) * UT_HOUR_US, 1, expired_partitions);
  REQUIRE(expired_partitions.size() == 1);
  CHECK(expired_partitions[0] == store_partition_key_t(1, 0));

  uint64_t removed = 0;
  REQUIRE(st->expire_partitions(expired_partitions, removed).ok());
  CHECK_EQ(removed, uint64_t{1});
  st->collect_expired_partitions((5 + 1) * UT_HOUR_US, 1, expired_partitions);
  CHECK_EQ(expired_partitions.size(), size_t{2});
  REQUIRE(st->expire_partitions(expired_partitions, removed).ok());
  CHECK(st->catalog().m_tables.at("a").m_partitions.empty());
  CHECK(st->catalog().m_tables.at("b").m_partitions.empty());
  const auto &remaining = st->catalog().m_tables.at("c").m_partitions;
  CHECK_EQ(remaining.size(), size_t{1});
  CHECK(remaining.count(0) == 1);
  CHECK_EQ(query_int(*st, "SELECT count(*) FROM _pt_t3_p0"), int64_t{2});
  st->collect_expired_partitions((24 + 1) * UT_HOUR_US, 1, expired_partitions);
  REQUIRE(st->expire_partitions(expired_partitions, removed).ok());
  CHECK(remaining.empty());
  store::destroy(st);
  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  CHECK_EQ(st->catalog().m_tables.size(), size_t{3});
  for (const auto &[name, table] : st->catalog().m_tables) {
    CHECK(table.m_partitions.empty());
  }
  store::destroy(st);
}

/** Reject inconsistent declaration flags, durations and untrusted partition
keys. */
static void test_hour_catalog_corruption() {
  for (const char *sql :
       {"UPDATE _pt_tables SET initialized = false",
        "UPDATE _pt_tables SET partition_duration_hours = 0",
        "UPDATE _pt_tables SET partition_duration_hours = 4294967295",
        "UPDATE _pt_partitions SET n_cols = 4294967295",
        "UPDATE _pt_partitions SET start_hour = -9223372036854775808",
        "UPDATE _pt_partitions SET start_hour = 9223372036854775807",
        "UPDATE _pt_partitions SET start_hour = start_hour + 1"}) {
    test_dir_t dir;
    store *st = nullptr;
    REQUIRE(store::open(dir.m_path, 5, &st).ok());

    test_catalog_t cat;
    ingest_field_t f = field_int("v", 1);
    ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &f, 1);
    store_write_t w = prepare_catalog_write(cat, 1, "m", &p, 1);
    REQUIRE(st->append(&w, 1).ok());
    REQUIRE(pt_duckdb_exec(st->connection(), sql).ok());
    store::destroy(st);
    CHECK_EQ(store::open(dir.m_path, 5, &st).code(), DB_CORRUPTION);
    CHECK(st == nullptr);
  }
}

/** Run storage metadata, append, recovery and failure tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"open_new", test_open_new},
      {"hour_partitions", test_hour_partitions},
      {"mixed_span_expiry", test_mixed_span_expiry},
      {"declared_table", test_declared_table},
      {"hour_catalog_corruption", test_hour_catalog_corruption},
      {"catalog_delta", test_catalog_delta},
      {"open_foreign_database", test_open_foreign_database},
      {"duckdb_ownership", test_duckdb_ownership},
      {"memory_budget", test_memory_budget},
      {"threads", test_threads},
      {"pt_duckdb_exec", test_pt_duckdb_exec},

      {"unknown_format", test_unknown_format},
      {"zero_table_id", test_zero_table_id},
      {"invalid_stored_schemas", test_invalid_stored_schemas},
      {"table_id_exhaustion", test_table_id_exhaustion},

      {"flush_and_reload", test_flush_and_reload},
      {"day_partitions", test_day_partitions},
      {"append_invariants", test_append_invariants},
      {"snapshot_catalog", test_snapshot_catalog},

      {"appender_failure", test_appender_failure},
      {"commit_rollback_error", test_commit_rollback_error},

      {"expire_begin_failure", test_expire_begin_failure},
      {"expire_rollback", test_expire_rollback},

      {"rollback_diagnostic", test_rollback_diagnostic},
      {"flush_rollback_failure", test_flush_rollback_failure},
      {"rollback", test_rollback},
  };

  return (test_main("store-t", argc, argv, cases));
}
