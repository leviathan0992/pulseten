/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage-t.cc
 *    Tests for the DuckDB storage: creating the file, flushing writes with
 *    schema changes and day partitions, reloading, rollback, and SIGKILL
 *    during flushes.
 *
 *-------------------------------------------------------------------------
 */

#include "storage.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <map>
#include <signal.h>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "duckdb_database.h"
#include "ingest.h"
#include "os_memory.h"
#include "schema.h"
#include "test_point.h"
#include "test_storage.h"
#include "test_storage_access.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_time.h"

/** Current directories and active writes owned by a storage test. */
struct test_catalog_t {
  std::map<std::string, schema_ref_t> m_schemas;
  std::vector<schema_ref_t> m_write_schemas;
  std::vector<ingest_plan_t *> m_plans;

  ~test_catalog_t() {
    for (ingest_plan_t *plan : m_plans) {
      delete plan;
    }
  }
};

/** Start planning from only the latest stored column directories. */
static void catalog_load(test_catalog_t &cat, const storage_catalog_t &state) {
  for (const auto &entry : state.m_tables) {
    cat.m_schemas[entry.first] =
        std::make_shared<schema_t>(entry.second.m_schema);
  }

  return;
}

/** Plan a write and retain its directory while storage borrows it. */
static storage_write_t catalog_write(test_catalog_t &cat, batch_seq_t seq,
                                     const char *table_name,
                                     const ingest_point_t *points, uint32_t n) {
  schema_ref_t &schema = cat.m_schemas[table_name];
  ingest_plan_t *plan = new ingest_plan_t;
  cat.m_plans.push_back(plan);
  CHECK(plan_for(schema.get(), table_name, points, n, *plan).ok());
  if (!plan->m_new_cols.empty()) {
    schema = std::make_shared<schema_t>(schema_after(schema.get(), *plan));
  }

  cat.m_write_schemas.push_back(schema);
  storage_write_t w;
  w.m_seq = seq;
  w.m_batch = &plan->m_batch;
  w.m_schema = schema.get();
  return (w);
}

/** Execute a query and read its single integer result.
@return the single integer a query returns, or -1 */
static int64_t query_int(storage &st, const std::string &sql) {
  int64_t v = -1;
  CHECK(test_storage_query_int(st, sql, v).ok());

  return (v);
}

/** Compare the metadata describing two stored database states.
@return true if two states describe the same file contents */
static bool states_equal(const storage_catalog_t &a,
                         const storage_catalog_t &b) {
  if (a.m_flushed_seq != b.m_flushed_seq ||
      a.m_next_table_id != b.m_next_table_id ||
      a.m_tables.size() != b.m_tables.size() ||
      a.m_partitions.size() != b.m_partitions.size()) {
    return (false);
  }

  std::map<std::string, storage_table_t>::const_iterator t;
  for (t = a.m_tables.begin(); t != a.m_tables.end(); ++t) {
    if (b.m_tables.count(t->first) == 0) {
      return (false);
    }

    const storage_table_t &x = t->second;
    const storage_table_t &y = b.m_tables.find(t->first)->second;

    if (x.m_table_id != y.m_table_id ||
        x.m_schema.m_cols.size() != y.m_schema.m_cols.size()) {
      return (false);
    }

    for (size_t c = 0; c < x.m_schema.m_cols.size(); ++c) {
      const col_def_t &p = x.m_schema.m_cols[c];
      const col_def_t &q = y.m_schema.m_cols[c];
      if (p.m_name != q.m_name || p.m_kind != q.m_kind ||
          p.m_type != q.m_type) {
        return (false);
      }
    }
  }

  std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator p;
  for (p = a.m_partitions.begin(); p != a.m_partitions.end(); ++p) {
    if (b.m_partitions.count(p->first) == 0) {
      return (false);
    }

    const storage_partition_t &q = b.m_partitions.find(p->first)->second;
    if (p->second.m_phys_name != q.m_phys_name ||
        p->second.m_n_cols != q.m_n_cols || p->second.m_rows != q.m_rows) {
      return (false);
    }
  }

  return (true);
}

/** A new file has the internal tables and an empty state. */
static void test_open_new() {
  db_status_t status;

  test_dir_t dir;
  storage *st = nullptr;

  status = storage::open(dir.m_path, 1, &st);
  CHECK(status.ok());
  CHECK(status.message().empty());

  const storage_catalog_t &state = st->catalog();
  CHECK(state.m_flushed_seq == 0 && state.m_next_table_id == 1);
  CHECK(state.m_tables.empty() && state.m_partitions.empty());
  storage::destroy(st);

  status = storage::open(dir.m_path, 1, &st);
  CHECK(status.ok());
  CHECK(st->catalog().m_flushed_seq == 0);
  storage::destroy(st);

  return;
}

/** A nonempty foreign database fails catalog loading without initialization
or changes to its existing table. */
static void test_open_foreign_database() {
  test_dir_t dir;
  const std::string path = dir.file(STORAGE_FILE_NAME);
  duckdb_backend *backend = nullptr;
  CHECK(duckdb_backend::open(path.c_str(), &backend).ok());
  CHECK(duckdb_exec(backend->connection(),
                    "CREATE TABLE user_table AS SELECT 7::BIGINT AS v")
            .ok());
  duckdb_backend::destroy(backend);

  storage *st = nullptr;
  const db_status_t err = storage::open(dir.m_path, 1, &st);
  CHECK(err.code() == DB_ERROR && st == nullptr);
  CHECK(err.message().find("_pt_state") != std::string::npos);

  CHECK(duckdb_backend::open(path.c_str(), &backend).ok());
  duckdb_rows_t rows;
  CHECK(duckdb_exec(backend->connection(), "SELECT v FROM user_table", rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 7);
  CHECK(duckdb_exec(backend->connection(),
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'main'",
                    rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 1);
  duckdb_backend::destroy(backend);

  return;
}

/** A borrowed session leaves its owner's connection alive; closing an owner
twice and destroying a partially opened owner are safe. */
static void test_duckdb_ownership() {
  test_dir_t dir;
  duckdb_backend *backend = nullptr;
  CHECK(
      duckdb_backend::open(dir.file("session.duckdb").c_str(), &backend).ok());
  if (backend == nullptr) {
    return;
  }

  CHECK(duckdb_exec(backend->connection(), "CREATE TABLE t (v BIGINT)").ok());
  {
    duckdb_session session(backend->connection());
    CHECK(session.begin().ok());
    CHECK(duckdb_exec(session.connection(), "INSERT INTO t VALUES (1)").ok());
    CHECK(session.commit().ok());
  }

  duckdb_rows_t rows;
  CHECK(duckdb_exec(backend->connection(), "SELECT v FROM t", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 1);
  backend->close();
  backend->close();
  duckdb_backend::destroy(backend);

  backend = nullptr;
  CHECK(!duckdb_backend::open(dir.file("missing/session.duckdb").c_str(),
                              &backend)
             .ok());
  CHECK(backend == nullptr);

  return;
}

/** Both execution overloads handle statements by their actual result type,
including writes that return rows. Failed execution clears old output. */
static void test_duckdb_exec() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());
  const duckdb_connection conn = st->connection();
  duckdb_rows_t rows;

  CHECK(duckdb_exec(conn, "CREATE TABLE user_table (v BIGINT)").ok());
  CHECK(duckdb_exec(conn, "INSERT INTO user_table VALUES (1)").ok());
  CHECK(duckdb_exec(conn, "SELECT v FROM user_table", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 1);

  /* A changed-row count is not a returned row set. */
  CHECK(duckdb_exec(conn, "INSERT INTO user_table VALUES (2)", rows).ok());
  CHECK(rows.empty());
  CHECK(duckdb_exec(conn, "INSERT INTO user_table VALUES (3) RETURNING v", rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 3);
  CHECK(duckdb_exec(conn, "UPDATE user_table SET v = 4 WHERE v = 3 RETURNING v",
                    rows)
            .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 4);
  CHECK(
      duckdb_exec(conn, "DELETE FROM user_table WHERE v = 4 RETURNING v", rows)
          .ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 4);

  CHECK(duckdb_exec(conn, "CREATE TABLE other (v BIGINT)", rows).ok());
  CHECK(rows.empty());
  CHECK(duckdb_exec(conn, "SELECT v FROM user_table WHERE false", rows).ok());
  CHECK(rows.empty());
  CHECK(duckdb_exec(conn, "SELECT i FROM range(5000) AS t(i)", rows).ok());
  CHECK(rows.size() == 5000);
  if (rows.size() == 5000) {
    CHECK(rows[0][0].m_i == 0 && rows[4999][0].m_i == 4999);
  }

  const db_status_t failed =
      duckdb_exec(conn, "SELECT * FROM missing_exec_table", rows);
  CHECK(failed.code() == DB_ERROR && !failed.message().empty());
  CHECK(rows.empty());
  const std::string diagnostic = failed.message();
  CHECK(duckdb_exec(conn, "SELECT * FROM missing_exec_table").code() ==
        DB_ERROR);
  CHECK(duckdb_exec(conn, "SELECT v FROM user_table ORDER BY v", rows).ok());
  CHECK(rows.size() == 2);
  if (rows.size() == 2) {
    CHECK(rows[0][0].m_i == 1 && rows[1][0].m_i == 2);
  }

  CHECK(failed.message() == diagnostic);

  /* Discarding RETURNING output still completes the write. */
  CHECK(
      duckdb_exec(conn, "INSERT INTO user_table VALUES (5) RETURNING v").ok());
  CHECK(duckdb_exec(conn, "SELECT count(*) FROM user_table", rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 3);
  storage::destroy(st);

  return;
}

/** Invalid metadata is rejected before the next write can use table id zero. */
static void test_zero_table_id() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());
  CHECK(duckdb_exec(st->connection(), "UPDATE _pt_state SET next_table_id = 0")
            .ok());
  storage::destroy(st);
  CHECK(storage::open(dir.m_path, 1, &st).code() == DB_CORRUPTION);
  CHECK(st == nullptr);
  storage::destroy(st);

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
      "UPDATE _pt_columns SET type = 2 WHERE col_id = 0"};

  for (const std::string &mutation : mutations) {
    test_dir_t dir;
    storage *st = nullptr;
    CHECK(storage::open(dir.m_path, 1, &st).ok());
    if (st == nullptr) {
      return;
    }

    test_catalog_t cat;
    ingest_field_t field = field_int("v", 1);
    ingest_point_t p = point(T0, nullptr, 0, &field, 1);
    storage_write_t write = catalog_write(cat, 1, "m", &p, 1);
    CHECK(st->flush(&write, 1).ok());
    CHECK(duckdb_exec(st->connection(), mutation).ok());

    CHECK(duckdb_exec(st->connection(), "BEGIN TRANSACTION").ok());
    storage_catalog_t snapshot;
    const db_status_t read_status =
        storage_catalog_reader(st->connection(), snapshot).read();
    CHECK(read_status.code() == DB_CORRUPTION);
    CHECK(!read_status.message().empty());
    CHECK(duckdb_exec(st->connection(), "ROLLBACK").ok());
    storage::destroy(st);

    const db_status_t open_status = storage::open(dir.m_path, 1, &st);
    CHECK(open_status.code() == DB_CORRUPTION);
    CHECK(!open_status.message().empty());
    CHECK(st == nullptr);
    storage::destroy(st);
  }

  return;
}

/** Table id exhaustion rolls back a flush without wrapping the counter;
existing tables remain writable and the catalog can still be reopened. */
static void test_table_id_exhaustion() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());
  const uint64_t exhausted = std::numeric_limits<uint64_t>::max();
  CHECK(duckdb_exec(st->connection(), "UPDATE _pt_state SET next_table_id = " +
                                          std::to_string(exhausted - 1))
            .ok());
  storage::destroy(st);
  CHECK(storage::open(dir.m_path, 1, &st).ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  storage_write_t writes[] = {catalog_write(cat, 1, "last", &p, 1),
                              catalog_write(cat, 2, "extra", &p, 1)};
  const db_status_t exhausted_status = st->flush(writes, 2);
  CHECK(exhausted_status.code() == DB_LIMIT_EXCEEDED);
  if (exhausted_status.code() != DB_LIMIT_EXCEEDED) {
    storage::destroy(st);

    return;
  }

  CHECK(st->catalog().m_flushed_seq == 0);
  CHECK(st->catalog().m_next_table_id == exhausted - 1);
  CHECK(st->catalog().m_tables.empty());
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 0);

  CHECK(st->flush(writes, 1).ok());
  CHECK(st->catalog().m_next_table_id == exhausted);
  CHECK(st->catalog().m_tables.at("last").m_table_id == exhausted - 1);
  storage_write_t existing = catalog_write(cat, 2, "last", &p, 1);
  CHECK(st->flush(&existing, 1).ok());
  storage_write_t extra = catalog_write(cat, 3, "extra", &p, 1);
  CHECK(st->flush(&extra, 1).code() == DB_LIMIT_EXCEEDED);
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(st->catalog().m_next_table_id == exhausted);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 1);
  storage::destroy(st);

  CHECK(storage::open(dir.m_path, 1, &st).ok());
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(st->catalog().m_next_table_id == exhausted);
  CHECK(st->catalog().m_tables.size() == 1);
  const std::string physical =
      st->catalog().m_partitions.begin()->second.m_phys_name;
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);
  storage::destroy(st);

  return;
}

/** Flushed writes, added columns and exact times survive a reload, which
gives the same state. */
static void test_flush_and_reload() {
  db_status_t reload_status;

  test_dir_t dir;
  storage *st = nullptr;
  reload_status = storage::open(dir.m_path, 1, &st);
  CHECK(reload_status.ok());

  test_catalog_t cat;

  /* Three cpu points, then one that adds a column, then a mem point. */
  ingest_tag_t host[] = {tag("host", "a")};
  ingest_field_t usage[] = {field_double("usage", 0.5)};
  ingest_field_t more[] = {field_double("usage", 0.75), field_int("cores", 8)};
  ingest_field_t used[] = {field_int("used", 1024)};

  ingest_point_t p1[] = {point(T0, host, 1, usage, 1),
                         point(T0 + 1, host, 1, usage, 1),
                         point(T0 + 2, host, 1, usage, 1)};
  ingest_point_t p2 = point(T0 + 3, host, 1, more, 2);
  ingest_point_t p3 = point(T0, nullptr, 0, used, 1);

  storage_write_t writes[3];
  writes[0] = catalog_write(cat, 1, "cpu", p1, 3);
  writes[1] = catalog_write(cat, 2, "cpu", &p2, 1);
  writes[2] = catalog_write(cat, 3, "mem", &p3, 1);

  /* The first write alone, then the other two. */
  CHECK(st->flush(writes, 1).ok());
  CHECK(st->catalog().m_flushed_seq == 1);
  CHECK(st->flush(writes + 1, 2).ok());

  const storage_catalog_t &state = st->catalog();
  CHECK(state.m_flushed_seq == 3);
  CHECK(state.m_tables.size() == 2);
  CHECK(state.m_tables.find("cpu")->second.m_schema.m_cols.size() == 4);
  CHECK(state.m_partitions.size() == 2);

  /* cpu: time, host, usage, cores. The partition was widened for cores;
  the first three rows have NULL there. */
  const std::string cpu = storage_partition_name(1, ut_utc_day(T0));
  const storage_partition_t &part =
      state.m_partitions
          .find(std::make_pair(static_cast<uint64_t>(1), ut_utc_day(T0)))
          ->second;
  CHECK(part.m_phys_name == cpu && part.m_n_cols == 4 && part.m_rows == 4);

  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu) == 4);
  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu + " WHERE c3 IS NULL") ==
        3);
  CHECK(query_int(*st, "SELECT max(c3) FROM " + cpu) == 8);
  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu + " WHERE c1 = 'a'") == 4);
  CHECK(query_int(*st, "SELECT max(_pt_seq) FROM " + cpu) == 2);
  CHECK(query_int(*st, "SELECT max(_pt_row) FROM " + cpu +
                           " WHERE _pt_seq = 1") == 2);

  /* The exact microseconds survive. */
  CHECK(query_int(*st, "SELECT epoch_us(max(c0)) FROM " + cpu) == T0 + 3);

  storage_catalog_t before = state;
  storage::destroy(st);

  /* Reloading gives the same state and latest column directory. */

  reload_status = storage::open(dir.m_path, 1, &st);
  CHECK(reload_status.ok());
  CHECK(reload_status.message().empty());
  CHECK(states_equal(before, st->catalog()));

  const schema_t &schema = st->catalog().m_tables.find("cpu")->second.m_schema;
  CHECK(schema.m_cols.size() == 4);
  CHECK(schema.m_cols[3].m_name == "cores" &&
        schema.m_cols[3].m_type == col_type_t::BIGINT);

  storage::destroy(st);

  return;
}

/** One write spanning three UTC days, one before 1970, lands in three
partitions. */
static void test_day_partitions() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());

  /* One write across three UTC days, one of them before 1970. */
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t points[] = {point(-1, nullptr, 0, f, 1),
                             point(0, nullptr, 0, f, 1),
                             point(UT_DAY_US - 1, nullptr, 0, f, 1),
                             point(UT_DAY_US, nullptr, 0, f, 1)};

  test_catalog_t cat;
  storage_write_t w = catalog_write(cat, 1, "m", points, 4);
  CHECK(st->flush(&w, 1).ok());

  const storage_catalog_t &state = st->catalog();
  CHECK(state.m_partitions.size() == 3);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_n1") == 1);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_p0") == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_p1") == 1);

  storage::destroy(st);

  return;
}

/** A flush whose sequence numbers do not continue from F is refused. */
static void test_bad_arguments() {
  db_status_t status;

  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);

  /* Sequence numbers must continue from F. */
  storage_write_t w = catalog_write(cat, 2, "m", &p, 1);

  status = st->flush(&w, 1);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(!status.message().empty());
  status = st->flush(&w, 0);
  CHECK(status.code() == DB_INVALID_ARGUMENT);

  w.m_seq = 1;
  status = st->flush(&w, 1);
  CHECK(status.ok());
  status = st->flush(&w, 1);
  CHECK(status.code() == DB_INVALID_ARGUMENT);

  storage::destroy(st);

  return;
}

/** Snapshot catalog reads share partition decoding with open, without loading
user-table names; reopening still performs complete physical catalog checks. */
static void test_snapshot_catalog() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());
  test_catalog_t cat;
  ingest_field_t field = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &field, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);
  CHECK(st->flush(&w, 1).ok());

  duckdb_session session(st->connection());
  CHECK(duckdb_exec(session.connection(), "CREATE TABLE user_table (v BIGINT)")
            .ok());
  CHECK(session.begin().ok());
  storage_catalog_t snapshot;
  CHECK(storage_catalog_reader(session.connection(), snapshot).read().ok());
  CHECK(snapshot.m_flushed_seq == 1);
  CHECK(snapshot.m_tables.size() == 1);
  CHECK(snapshot.m_partitions.size() == 1);
  CHECK(duckdb_exec(session.connection(), "COMMIT").ok());
  storage::destroy(st);

  CHECK(storage::open(dir.m_path, 1, &st).ok());
  CHECK(query_int(
            *st,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'user_table'") == 1);
  CHECK(st->catalog().m_partitions.size() == 1);
  storage::destroy(st);

  return;
}

/** An appender constraint failure rolls back every row and catalog change;
the same sequence can be retried after removing the test constraint. */
static void test_appender_failure() {
  db_status_t test_status;

  test_dir_t dir;
  storage *st = nullptr;
  test_status = storage::open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  test_catalog_t cat;
  ingest_field_t field = field_int("v", 1);
  ingest_point_t point_value = point(T0, nullptr, 0, &field, 1);
  storage_write_t first = catalog_write(cat, 1, "m", &point_value, 1);
  test_status = st->flush(&first, 1);
  CHECK(test_status.ok());

  const std::string physical =
      st->catalog().m_partitions.begin()->second.m_phys_name;
  CHECK(duckdb_exec(
            st->connection(),
            "CREATE UNIQUE INDEX _pt_test_unique ON " + physical + " (c1)")
            .ok());
  storage_write_t next = catalog_write(cat, 2, "m", &point_value, 1);
  test_status = st->flush(&next, 1);
  CHECK(test_status.code() == DB_ERROR);
  CHECK(!test_status.message().empty());
  CHECK(st->catalog().m_flushed_seq == 1);
  CHECK(st->catalog().m_partitions.begin()->second.m_rows == 1);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 1);

  CHECK(duckdb_exec(st->connection(), "DROP INDEX _pt_test_unique").ok());
  test_status = st->flush(&next, 1);
  CHECK(test_status.ok());
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);
  storage::destroy(st);

  test_status = storage::open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  CHECK(st->catalog().m_flushed_seq == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);
  storage::destroy(st);

  return;
}

/** COMMIT and the best-effort ROLLBACK can both fail. Preserve the first
diagnostic and the unknown-outcome classification. */
static void test_commit_rollback_error() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());
  duckdb_session session(st->connection());

  const db_status_t status = session.commit();
  CHECK(status.code() == DB_OUTCOME_UNKNOWN);
  CHECK(status.message().find("commit") != std::string::npos);
  CHECK(status.message().find("rollback:") != std::string::npos);
  const std::string original = status.message();
  CHECK(session.begin().ok());
  CHECK(session.rollback().ok());
  CHECK(status.message() == original);
  storage::destroy(st);

  return;
}

/** An expiry that cannot begin must leave the existing transaction intact. */
static void test_expire_begin_failure() {
  db_status_t test_status;

  test_dir_t dir;
  storage *st = nullptr;
  test_status = storage::open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);
  test_status = st->flush(&w, 1);
  CHECK(test_status.ok());

  CHECK(duckdb_exec(st->connection(), "BEGIN TRANSACTION").ok());
  uint64_t removed = 0;
  test_status = st->expire(ut_utc_day(T0) + 1, removed);
  CHECK(test_status.code() == DB_ERROR);
  CHECK(removed == 0);
  CHECK(st->catalog().m_partitions.size() == 1);
  CHECK(test_status.message().find("rollback:") == std::string::npos);
  CHECK(duckdb_exec(st->connection(), "ROLLBACK").ok());
  storage::destroy(st);

  return;
}

/** An expiry failure restores earlier drops and leaves the catalog unchanged.
 */
static void test_expire_rollback() {
  for (const bool fail_delete : {false, true}) {
    test_dir_t dir;
    storage *st = nullptr;
    CHECK(storage::open(dir.m_path, 1, &st).ok());
    if (st == nullptr) {
      return;
    }

    test_catalog_t cat;
    const ingest_field_t field = field_int("v", 1);
    const ingest_point_t points[] = {
        point(T0, nullptr, 0, &field, 1),
        point(T0 + UT_DAY_US, nullptr, 0, &field, 1)};
    const storage_write_t write = catalog_write(cat, 1, "m", points, 2);
    CHECK(st->flush(&write, 1).ok());
    const storage_catalog_t before = st->catalog();
    const std::string first = storage_partition_name(1, ut_utc_day(T0));
    const std::string second = storage_partition_name(1, ut_utc_day(T0) + 1);

    /* Missing metadata fails after a DROP; a missing second partition fails
    after both the first DROP and its catalog DELETE. */
    const std::string renamed = fail_delete ? "_pt_partitions" : second;
    CHECK(duckdb_exec(st->connection(),
                      "ALTER TABLE " + renamed + " RENAME TO saved_partition")
              .ok());

    uint64_t removed = 99;
    const db_status_t err = st->expire(ut_utc_day(T0) + 2, removed);
    CHECK(err.code() == DB_ERROR);
    CHECK(err.message().find(renamed) != std::string::npos);
    CHECK(removed == 0);
    CHECK(states_equal(before, st->catalog()));
    CHECK(query_int(*st, "SELECT count(*) FROM " + first) == 1);
    CHECK(query_int(*st, std::string("SELECT count(*) FROM ") +
                             (fail_delete ? "saved_partition"
                                          : "_pt_partitions")) == 2);

    /* Restore the deliberately renamed table and retry on the same session. */
    CHECK(duckdb_exec(st->connection(),
                      "ALTER TABLE saved_partition RENAME TO " + renamed)
              .ok());
    CHECK(st->expire(ut_utc_day(T0) + 2, removed).ok());
    CHECK(removed == 2);
    CHECK(st->catalog().m_partitions.empty());
    CHECK(query_int(*st, "SELECT count(*) FROM _pt_partitions") == 0);
    storage::destroy(st);
  }

  return;
}

/** A failed rollback retains the operation's diagnostic; a successful one
leaves it intact for the caller. */
static void test_rollback_diagnostic() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());

  duckdb_session session(st->connection());

  const db_status_t cause(DB_ERROR, "original failure");
  const db_status_t failed = session.rollback(cause);
  CHECK(failed.code() == DB_OUTCOME_UNKNOWN);
  CHECK(failed.message().find("original failure; rollback:") == 0);

  CHECK(session.begin().ok());
  const db_status_t restored = session.rollback(cause);
  CHECK(restored.code() == DB_ERROR);
  CHECK(restored.message() == "original failure");
  storage::destroy(st);

  return;
}

/** A failed flush rollback reports an unknown outcome; closing discards its
still-open transaction, and reopening reads the committed catalog. */
static void test_flush_rollback_failure() {
  db_status_t test_status;

  test_dir_t dir;
  storage *st = nullptr;
  test_status = storage::open(dir.m_path, 1, &st);
  CHECK(test_status.ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);

  storage_test_access::fail_next_flush(*st);
  storage_test_access::fail_next_rollback(*st);
  test_status = st->flush(&w, 1);
  CHECK(test_status.code() == DB_OUTCOME_UNKNOWN);
  CHECK(test_status.message().find("injected flush failure") !=
        std::string::npos);
  CHECK(test_status.message().find("rollback: injected rollback failure") !=
        std::string::npos);
  CHECK(st->catalog().m_flushed_seq == 0);
  storage::destroy(st);

  test_status = storage::open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  CHECK(st->catalog().m_flushed_seq == 0);
  CHECK(st->catalog().m_tables.empty());
  CHECK(st->catalog().m_partitions.empty());
  storage::destroy(st);

  return;
}

/** A flush failing before commit leaves nothing, and the same write succeeds
afterwards. */
static void test_rollback() {
  test_dir_t dir;
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, 1, &st).ok());

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);

  /* Fails after the table, partition and rows were written. */
  storage_test_access::fail_next_flush(*st);
  CHECK(st->flush(&w, 1).code() == DB_ERROR);
  CHECK(st->catalog().m_flushed_seq == 0);

  /* Nothing of it is left: no rows, no partition table, no catalog rows.
  This shows that appender rows belong to the transaction. */
  CHECK(query_int(*st,
                  "SELECT count(*) FROM information_schema.tables "
                  "WHERE table_schema = 'main'") == 4);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 0);

  /* The same write succeeds afterwards. */
  CHECK(st->flush(&w, 1).ok());
  storage::destroy(st);

  CHECK(storage::open(dir.m_path, 1, &st).ok());
  CHECK(st->catalog().m_flushed_seq == 1);
  storage::destroy(st);

  return;
}

/** Rows of write seq in the kill test.
@return number of rows generated for that write */
static uint32_t kill_rows(batch_seq_t seq) {
  return (static_cast<uint32_t>(seq % 5) + 1);
}

/** Child process: flush writes after F, one per flush, until killed. Every
10th write adds a column. Reports each committed F on the pipe. */
static void kill_child(const char *dir, int fd) {
  storage *st = nullptr;
  if (!storage::open(dir, 1, &st).ok()) {
    _exit(2);
  }

  test_catalog_t cat;
  catalog_load(cat, st->catalog());

  for (;;) {
    const batch_seq_t seq = st->catalog().m_flushed_seq + 1;

    char extra[32];
    snprintf(extra, sizeof(extra), "x%llu",
             static_cast<unsigned long long>(seq));
    ingest_field_t f[] = {field_int("seq", static_cast<int64_t>(seq)),
                          field_int(extra, 1)};
    const uint32_t n_fields = seq % 10 == 0 ? 2 : 1;

    std::vector<ingest_point_t> points;
    for (uint32_t r = 0; r < kill_rows(seq); ++r) {
      points.push_back(point(T0 + static_cast<int64_t>(seq) * 1000 + r, nullptr,
                             0, f, n_fields));
    }

    storage_write_t w = catalog_write(cat, seq, "k", &points[0],
                                      static_cast<uint32_t>(points.size()));

    if (!st->flush(&w, 1).ok()) {
      _exit(3);
    }

    if (write(fd, &seq, sizeof(seq)) != static_cast<ssize_t>(sizeof(seq))) {
      _exit(4);
    }
  }

  return;
}

/** Kill a process while it flushes, over and over: F covers every committed
flush and nothing else is stored. */
static void test_kill_during_flush() {
  db_status_t status;

  test_dir_t dir;
  batch_seq_t acked = 0;

  for (int round = 0; round < 8; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.m_path, fds[1]);
    }

    close(fds[1]);

    const int wanted = 5 + round * 3;
    int received = 0;
    batch_seq_t seq = 0;

    while (received < wanted && read(fds[0], &seq, sizeof(seq)) ==
                                    static_cast<ssize_t>(sizeof(seq))) {
      acked = seq;
      ++received;
    }

    kill(pid, SIGKILL);
    int child_status = 0;
    waitpid(pid, &child_status, 0);

    while (read(fds[0], &seq, sizeof(seq)) ==
           static_cast<ssize_t>(sizeof(seq))) {
      acked = seq;
    }

    close(fds[0]);
    CHECK(received == wanted);

    /* F covers every committed flush, and the stored rows are exactly
    those of writes 1 .. F. */
    storage *st = nullptr;

    status = storage::open(dir.m_path, 1, &st);
    CHECK(status.ok());
    if (st == nullptr) {
      fprintf(stderr, "reopen failed: %s\n", status.message().c_str());

      return;
    }

    const storage_catalog_t &state = st->catalog();
    CHECK(state.m_flushed_seq >= acked);

    uint64_t expected = 0;
    for (batch_seq_t s = 1; s <= state.m_flushed_seq; ++s) {
      expected += kill_rows(s);
    }

    uint64_t counted = 0;
    uint64_t actual = 0;
    std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
        it;
    for (it = state.m_partitions.begin(); it != state.m_partitions.end();
         ++it) {
      counted += it->second.m_rows;
      actual += static_cast<uint64_t>(
          query_int(*st, "SELECT count(*) FROM " + it->second.m_phys_name));
    }

    CHECK(counted == expected);
    CHECK(actual == expected);
    storage::destroy(st);
  }

  printf("kill test: %llu flushes committed across 8 kills\n",
         static_cast<unsigned long long>(acked));

  return;
}

/** All connections of one backend use its configured buffer manager. */
static void test_memory_budget() {
  uint64_t effective = 0, resolved = 0;
  CHECK(os_effective_memory(effective).ok());
  CHECK(duckdb_resolve_memory_limit(resolved).ok());
  CHECK(resolved == std::min(effective / 8, uint64_t{2} << 30));
  resolved = DUCKDB_MAX_MEMORY_LIMIT_BYTES;
  CHECK(duckdb_resolve_memory_limit(resolved).ok());
  CHECK(resolved == DUCKDB_MAX_MEMORY_LIMIT_BYTES);
  resolved = DUCKDB_MAX_MEMORY_LIMIT_BYTES + 1;
  CHECK(duckdb_resolve_memory_limit(resolved).code() == DB_INVALID_ARGUMENT);
  CHECK(resolved == DUCKDB_MAX_MEMORY_LIMIT_BYTES + 1);

  test_dir_t dir;
  const uint64_t limit = (uint64_t{256} << 20) + 123;
  duckdb_backend *backend = nullptr;
  CHECK(duckdb_backend::open(dir.file("budget.duckdb").c_str(), &backend, limit)
            .ok());
  if (backend == nullptr) {
    return;
  }
  duckdb_connection other = nullptr;
  CHECK(duckdb_connect(backend->database(), &other) == DuckDBSuccess);
  for (duckdb_connection conn : {backend->connection(), other}) {
    duckdb_rows_t rows;
    CHECK(duckdb_exec(conn, "SELECT memory_limit FROM pragma_database_size()",
                      rows)
              .ok());
    CHECK(rows.size() == 1 && rows[0].size() == 1);
    /* Compare connections through the setting as well; its display is rounded.
     */
    CHECK(
        duckdb_exec(conn, "SELECT current_setting('memory_limit')", rows).ok());
    CHECK(rows.size() == 1 && rows[0][0].m_s == "256.0 MiB");
  }
  duckdb_disconnect(&other);
  duckdb_backend::destroy(backend);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_open_new();
  test_open_foreign_database();
  test_duckdb_ownership();
  test_memory_budget();
  test_duckdb_exec();
  test_zero_table_id();
  test_invalid_stored_schemas();
  test_table_id_exhaustion();
  test_flush_and_reload();
  test_day_partitions();
  test_bad_arguments();
  test_snapshot_catalog();
  test_appender_failure();
  test_commit_rollback_error();
  test_expire_begin_failure();
  test_expire_rollback();
  test_rollback_diagnostic();
  test_flush_rollback_failure();
  test_rollback();
  test_kill_during_flush();

  return (test_result("storage-t"));
}
