/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * exec-t.cc
 *    Tests for SQL execution through exec: ordinary tables beside time-series
 *    data, names that exec protects, ordinary objects created, renamed and
 *    dropped while open, hints for missing time-series sources, and namespace
 *    violations made behind exec's back that the next open detects.
 *
 * Covers: src/db/db.cc, src/query/query.cc
 *
 *-------------------------------------------------------------------------
 */

#include <fstream>
#include <string>
#include <vector>

#include "db.h"
#include "ingest.h"
#include "pt_duckdb.h"
#include "query.h"
#include "store.h"
#include "test_db.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_util.h"

/** Native SQL tables join hot and stored data without changing WAL progress. */
static void test_native_sql() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 60000;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(database
            ->exec("CREATE SCHEMA app; "
                   "CREATE TABLE app.devices (id INTEGER, label VARCHAR); "
                   "INSERT INTO app.devices VALUES (1, 'old'); "
                   "CREATE VIEW devices AS SELECT * FROM app.devices; "
                   "CREATE TABLE \"device notes\" (note VARCHAR)")
            .ok());
  CHECK(write_id(*database, 1).ok());

  const char *join =
      "SELECT d.label FROM main.k AS k "
      "JOIN app.devices AS d ON k.id = d.id";

  db_cursor *old = nullptr;
  CHECK(database->open_cursor(join, false, &old).ok());
  CHECK(database->exec("UPDATE app.devices SET label = 'new'").ok());

  query_rows_t rows;

  if (old != nullptr) {
    CHECK(old->fetch(rows).ok());
    CHECK_EQ(rows.size(), 1);
    CHECK_EQ(rows[0][0].m_string, "old");
  }

  db_cursor::destroy(old);

  test_query_t result;
  CHECK(query_all(*database, join, result).ok());
  CHECK_EQ(result.m_rows.size(), 1);
  CHECK_EQ(result.m_rows[0][0].m_string, "new");
  CHECK(query_all(*database, "SELECT count(*) FROM devices", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, 1);
  CHECK(query_all(*database, "SELECT count(*) FROM \"device notes\"", result)
            .ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, 0);

  /* A qualified ordinary table must not fall back to a time-series name. */
  CHECK(database
            ->exec("CREATE TABLE app.k (id INTEGER); "
                   "INSERT INTO app.k VALUES (2)")
            .ok());
  CHECK(query_all(*database, "SELECT id FROM app.k", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, 2);
  CHECK(query_all(*database, "SELECT * FROM app.devices_missing", result)
            .code() == DB_ERROR);

  db::stats_t stats;
  database->stats(stats);
  CHECK_EQ(stats.m_visible_seq, 1);
  CHECK_EQ(stats.m_flushed_seq, 0);

  db::flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(write_id(*database, 1).ok());
  CHECK(query_all(*database, join, result).ok());
  CHECK_EQ(result.m_rows.size(), 2);

  db_close(database);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  if (database != nullptr) {
    CHECK(query_all(*database, join, result).ok());
    CHECK_EQ(result.m_rows.size(), 2);
    CHECK(query_all(*database, "SELECT count(*) FROM devices", result).ok());
    CHECK_EQ(result.m_rows[0][0].m_integer, 1);

    db_close(database);
  }
}

/** New, renamed and dropped ordinary objects are checked without reopening. */
static void test_native_table_names() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 60000;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(database->exec("CREATE TABLE K (id BIGINT)").ok());
  CHECK_EQ(write_id(*database, 1).code(), DB_INVALID_ARGUMENT);
  CHECK(database
            ->exec("ALTER TABLE K RENAME TO devices; "
                   "CREATE VIEW k AS SELECT * FROM devices")
            .ok());
  CHECK_EQ(write_id(*database, 1).code(), DB_INVALID_ARGUMENT);
  CHECK(database->exec("DROP VIEW k").ok());
  CHECK(write_id(*database, 1).ok());

  db_close(database);
}

#ifndef PT_EXEC_NAMES_FILE
#error "PT_EXEC_NAMES_FILE must name tests/sql/exec_names.test"
#endif /* PT_EXEC_NAMES_FILE */

/** Split one line of the names file at its tabs.
@return the fields, in order */
static std::vector<std::string> split_fields(const std::string &line) {
  std::vector<std::string> fields;
  size_t start = 0;

  for (;;) {
    const size_t tab = line.find('\t', start);
    fields.push_back(line.substr(start, tab - start));

    if (tab == std::string::npos) {
      break;
    }

    start = tab + 1;
  }

  return (fields);
}

/** Describe the outcome of an exec call for a failure message.
@return the error code and message */
static std::string describe(const db_status_t &err) {
  return ("got " + std::to_string(static_cast<int>(err.code())) + ": " +
          err.message());
}

/** Run one line of the names file.
@param[in]   fields  the line's tab-separated fields
@param[out]  what    what the line expects, and what happened instead
@return true if the line's expectation holds */
static bool run_names_line(const test_dir_t &dir, db *&database,
                           const std::vector<std::string> &fields,
                           std::string &what) {
  const std::string &verb = fields[0];
  const size_t wanted =
      verb == "reopen" ? 1 : (verb == "reject" || verb == "value" ? 3 : 2);

  if (fields.size() != wanted) {
    what = "\"" + verb + "\" takes " + std::to_string(wanted) + " fields";

    return (false);
  }

  if (verb == "setup" || verb == "accept") {
    const db_status_t err = database->exec(fields[1]);
    what = "exec succeeds: " + fields[1] + " (" + describe(err) + ")";

    return (err.ok());
  }

  if (verb == "reject") {
    const db_status_t err = database->exec(fields[1]);
    what = "rejected for \"" + fields[2] + "\": " + fields[1] + " (" +
           describe(err) + ")";

    return (err.code() == DB_INVALID_ARGUMENT &&
            err.message().find(fields[2]) != std::string::npos);
  }

  if (verb == "value" || verb == "error") {
    test_query_t result;
    const db_status_t err = query_all(*database, fields[1], result);

    if (verb == "error") {
      what = "fails with DB_ERROR: " + fields[1] + " (" + describe(err) + ")";

      return (err.code() == DB_ERROR);
    }

    what =
        "returns " + fields[2] + ": " + fields[1] + " (" + describe(err) + ")";

    return (err.ok() && result.m_rows.size() == 1 &&
            result.m_rows[0].size() == 1 && !result.m_rows[0][0].m_is_null &&
            std::to_string(result.m_rows[0][0].m_integer) == fields[2]);
  }

  if (verb == "reopen") {
    db::flush_info_t info;
    const db_status_t flushed = database->flush(info);

    db_close(database);
    database = nullptr;

    const db_status_t err =
        db_open(dir.m_path, deferred_flush_options(), &database);
    what = "flush and reopen succeed (" + describe(flushed) + ", " +
           describe(err) + ")";

    return (flushed.ok() && err.ok());
  }

  what = "unknown verb \"" + verb + "\"";

  return (false);
}

/** Every line of exec_names.test holds: protected names are rejected for the
stated reason, ordinary names are accepted, and nothing rejected changes the
time-series data. */
static void test_exec_names() {
  std::ifstream file(PT_EXEC_NAMES_FILE);
  REQUIRE(file.is_open());

  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  REQUIRE(write_id(*database, 1).ok());

  std::string line;
  int line_no = 0;
  int lines_run = 0;

  while (database != nullptr && std::getline(file, line)) {
    ++line_no;

    if (line.empty() || line[0] == '#') {
      continue;
    }

    std::string what;
    const bool ok = run_names_line(dir, database, split_fields(line), what);
    test_check(ok, what.c_str(), PT_EXEC_NAMES_FILE, line_no);
    ++lines_run;
  }

  CHECK(database != nullptr);
  CHECK(lines_run > 0);

  db_close(database);

  return;
}

/** Exec explains missing time-series sources after native CTE/table lookup. */
static void test_exec_time_series_diagnostic() {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, nullptr, &database).ok());

  CHECK(write_id(*database, 1).ok());
  CHECK(database
            ->exec("CREATE TABLE sink (id BIGINT); CREATE SCHEMA app; "
                   "CREATE TABLE app.k (id BIGINT); "
                   "ATTACH ':memory:' AS aux; CREATE TABLE aux.k (id BIGINT)")
            .ok());

  for (const char *sql :
       {"SELECT * FROM k", "SELECT * FROM main.k", "SELECT * FROM data.main.k",
        "SELECT * FROM data.k", "INSERT INTO sink SELECT id FROM k",
        "CREATE TABLE copied AS SELECT * FROM k",
        "CREATE VIEW v AS SELECT * FROM k", "PREPARE p AS SELECT * FROM k",
        "EXPLAIN SELECT * FROM k"}) {
    const db_status_t error = database->exec(sql);
    CHECK_EQ(error.code(), DB_ERROR);
    CHECK(error.message().find(
              "cannot read time-series table 'k' through exec") !=
          std::string::npos);

    if (error.message().find("use the query API") == std::string::npos) {
      fprintf(stderr, "missing exec hint: %s: %s\n", sql,
              error.message().c_str());
      CHECK(false);
    }
  }

  for (const char *sql :
       {"WITH k AS (SELECT 2 AS id) SELECT * FROM k",
        "WITH RECURSIVE k(id) AS (SELECT 1 UNION ALL "
        "SELECT id + 1 FROM k WHERE id < 3) SELECT * FROM k",
        "CREATE VIEW ordinary_view AS "
        "WITH k AS (SELECT 2 AS id) SELECT * FROM k",
        "SELECT * FROM app.k", "SELECT * FROM aux.k",
        "USE app; SELECT * FROM k", "USE aux; SELECT * FROM k"}) {
    CHECK(database->exec(sql).ok());
  }

  CHECK(database->exec("DROP TABLE aux.k").ok());
  for (const char *sql :
       {"USE aux; SELECT * FROM k", "SELECT * FROM aux.main.k",
        "SELECT * FROM missing_table",
        "SELECT 1; SELECT * FROM missing_table"}) {
    const db_status_t error = database->exec(sql);
    CHECK_EQ(error.code(), DB_ERROR);
    CHECK_EQ(error.message().find("use the query API"), std::string::npos);
  }

  db_close(database);

  return;
}

/** Catalog tampering cannot hide stored or replayed time-series schemas. */
static void test_invalid_stored_objects() {
  const char *statements[] = {"CREATE TABLE _pt_orphan (v BIGINT)",
                              "CREATE TABLE m (v BIGINT)",
                              "CREATE TABLE hot (v BIGINT)"};

  for (const char *sql : statements) {
    test_dir_t dir;
    db::options_t options;
    options.m_flush_interval_ms = 60000;

    db *database = nullptr;
    CHECK(db_open(dir.m_path, &options, &database).ok());

    if (database == nullptr) {
      continue;
    }

    const ingest_field_t field = field_int("v", 1);
    const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
    batch_seq_t seq = 0;
    CHECK(database->write("m", 1, &p, 1, seq).ok());

    db::flush_info_t info;
    CHECK(database->flush(info).ok());
    CHECK(database->write("hot", 3, &p, 1, seq).ok());

    db_close(database);

    store *st = nullptr;
    CHECK(store::open(dir.m_path, 24, &st).ok());

    if (st == nullptr) {
      continue;
    }

    CHECK(pt_duckdb_exec(st->connection(), sql).ok());

    store::destroy(st);
    CHECK(db_open(dir.m_path, &options, &database).code() == DB_CORRUPTION);
    CHECK(database == nullptr);

    db_close(database);
  }

  return;
}

/** Run exec, ordinary table and name protection tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"native_sql", test_native_sql},
      {"native_table_names", test_native_table_names},
      {"exec_names", test_exec_names},
      {"exec_time_series_diagnostic", test_exec_time_series_diagnostic},
      {"invalid_stored_objects", test_invalid_stored_objects},
  };

  return (test_main("exec-t", argc, argv, cases));
}
