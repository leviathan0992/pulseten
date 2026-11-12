/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_query-t.cc
 *    Read-only SQL boundaries and snapshot preservation.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <cstdint>
#include <string>

#include "duckdb_database.h"
#include "ingest.h"
#include "query.h"
#include "storage.h"
#include "test_db.h"
#include "test_point.h"
#include "test_util.h"

/** Rejected SQL never writes data, changes schemas or advances WAL sequences.
 */
static void test_query_read_only() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 60000;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return;
  }

  CHECK(write_id(*database, 1).ok());

  db_stats_t before;
  database->read_stats(before);
  const char *statements[] = {"CREATE TABLE copied (id BIGINT)",
                              "CREATE TABLE copied AS SELECT * FROM k",
                              "DROP TABLE k",
                              "DROP TABLE _pt_state",
                              "INSERT INTO k VALUES (2)",
                              "UPDATE k SET id = 2",
                              "DELETE FROM k",
                              "ALTER TABLE k ADD COLUMN extra BIGINT",
                              "SELECT 1; DROP TABLE k"};
  for (const char *sql : statements) {
    db_cursor *cursor = nullptr;
    const db_status_t err = database->cursor_open(sql, false, &cursor);
    CHECK(err.code() == DB_INVALID_ARGUMENT);
    CHECK(!err.message().empty());
    CHECK(cursor == nullptr);
    db_cursor::destroy(cursor);
  }

  const ingest_field_t invalid_field = field_str("id", "wrong type");
  const ingest_point_t invalid = point(T0, nullptr, 0, &invalid_field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &invalid, 1, seq).code() == DB_SCHEMA_CONFLICT);
  CHECK(seq == 0);

  db_stats_t after;
  database->read_stats(after);
  CHECK(after.m_visible_seq == before.m_visible_seq);
  CHECK(after.m_flushed_seq == before.m_flushed_seq);
  test_query_t result;
  CHECK(query_all(*database, "SELECT id FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_i == 1);
  db_close(database);

  CHECK(db_open(dir.m_path, &options, &database).ok());
  CHECK(query_all(*database, "SELECT id FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_i == 1);
  db_close(database);

  return;
}

/** An old cursor reads its original rows after rejected SQL, writes and flush.
 */
static void test_snapshot_after_rejection() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 60000;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return;
  }

  CHECK(write_id(*database, 1).ok());
  db_cursor *cursor = nullptr;
  CHECK(database->cursor_open("SELECT id FROM k", false, &cursor).ok());

  db_cursor *rejected = nullptr;
  CHECK(database->cursor_open("DROP TABLE k", false, &rejected).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(rejected == nullptr);
  CHECK(write_id(*database, 2).ok());
  db_flush_info_t info;
  CHECK(database->flush(info).ok());

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 1);
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.empty());
  db_cursor::destroy(cursor);

  test_query_t current;
  CHECK(query_all(*database, "SELECT count(*) FROM k", current).ok());
  CHECK(current.m_rows.size() == 1 && current.m_rows[0][0].m_i == 2);
  db_close(database);

  return;
}

/** Catalog tampering cannot hide stored or replayed time-series schemas. */
static void test_invalid_stored_objects() {
  const char *statements[] = {"CREATE TABLE _pt_orphan (v BIGINT)",
                              "CREATE VIEW escaped AS SELECT * FROM _pt_state",
                              "CREATE TABLE m (v BIGINT)",
                              "CREATE TABLE hot (v BIGINT)"};
  for (const char *sql : statements) {
    test_dir_t dir;
    db_options_t options;
    options.m_flush_interval_ms = 60000;
    db *database = nullptr;
    CHECK(db_open(dir.m_path, &options, &database).ok());
    if (database == nullptr) {
      continue;
    }

    const ingest_field_t field = field_int("v", 1);
    const ingest_point_t p = point(T0, nullptr, 0, &field, 1);
    batch_seq_t seq = 0;
    CHECK(database->write("m", 1, &p, 1, seq).ok());
    db_flush_info_t info;
    CHECK(database->flush(info).ok());
    CHECK(database->write("hot", 3, &p, 1, seq).ok());
    db_close(database);

    duckdb_backend *backend = nullptr;
    CHECK(duckdb_backend::open(dir.file(STORAGE_FILE_NAME).c_str(), &backend)
              .ok());
    if (backend == nullptr) {
      continue;
    }

    CHECK(duckdb_exec(backend->connection(), sql).ok());
    duckdb_backend::destroy(backend);
    CHECK(db_open(dir.m_path, &options, &database).code() == DB_CORRUPTION);
    CHECK(database == nullptr);
    db_close(database);
  }

  return;
}

/** Run read-only query boundary tests. */
int main() {
  test_query_read_only();
  test_snapshot_after_rejection();
  test_invalid_stored_objects();

  return (test_result("db_query-t"));
}
