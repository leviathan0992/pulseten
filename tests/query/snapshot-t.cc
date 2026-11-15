/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * snapshot-t.cc
 *    Tests for queries through database cursors: rejected statements change
 *    nothing, an open cursor keeps its snapshot across later writes and
 *    flushes, deferred scans cross chunk boundaries, and selective snapshots
 *    resolve aliases, CTEs and repeated tables.
 *
 * Covers: src/db/db.cc, src/query/query.cc
 *
 *-------------------------------------------------------------------------
 */

#include <cstdint>
#include <cstring>
#include <string>

#include "db.h"
#include "ingest.h"
#include "query.h"
#include "test_db.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_util.h"

/** Rejected SQL never writes data, changes schemas or advances the WAL. */
static void test_query_read_only() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 60000;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_id(*database, 1).ok());

  db::stats_t before;
  database->stats(before);

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

    const db_status_t err = database->open_cursor(sql, false, &cursor);
    CHECK_EQ(err.code(), DB_INVALID_ARGUMENT);
    CHECK(!err.message().empty());
    CHECK(cursor == nullptr);

    db_cursor::destroy(cursor);
  }

  const ingest_field_t invalid_field = field_str("id", "wrong type");
  const ingest_point_t invalid =
      point(TEST_BASE_TIME_US, nullptr, 0, &invalid_field, 1);

  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &invalid, 1, seq).code() == DB_SCHEMA_CONFLICT);
  CHECK_EQ(seq, 0);

  db::stats_t after;
  database->stats(after);
  CHECK_EQ(after.m_visible_seq, before.m_visible_seq);
  CHECK_EQ(after.m_flushed_seq, before.m_flushed_seq);

  test_query_t result;
  CHECK(query_all(*database, "SELECT id FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 1);

  db_close(database);

  CHECK(db_open(dir.m_path, &options, &database).ok());
  CHECK(query_all(*database, "SELECT id FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 1);

  db_close(database);

  return;
}

/** An old cursor keeps its rows after rejected SQL, later writes and flush. */
static void test_snapshot_after_rejection() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 60000;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_id(*database, 1).ok());

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT id FROM k", false, &cursor).ok());

  db_cursor *rejected = nullptr;
  CHECK(database->open_cursor("DROP TABLE k", false, &rejected).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(rejected == nullptr);
  CHECK(write_id(*database, 2).ok());

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.empty());

  db_cursor::destroy(cursor);

  test_query_t current;
  CHECK(query_all(*database, "SELECT count(*) FROM k", current).ok());
  CHECK(current.m_rows.size() == 1 && current.m_rows[0][0].m_integer == 2);

  db_close(database);

  return;
}

/** A deferred scan crosses pointer-cache and output-chunk boundaries after
flush. Other tables and later writes must not change its rows. */
static void test_streaming_interval_after_flush() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 60000;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  for (int i = 0; i < 1100; ++i) {
    ingest_field_t field = field_int("n", i);
    ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
    batch_seq_t seq = 0;
    CHECK(database->write("other", 5, &p, 1, seq).ok());
  }

  const int count = 4400;

  for (int start = 0; start < count; start += 4) {
    ingest_field_t fields[4];
    ingest_point_t points[4];

    for (int i = 0; i < 4; ++i) {
      fields[i] = field_int("id", start + i);
      points[i] = point(TEST_BASE_TIME_US, nullptr, 0, &fields[i], 1);
    }

    batch_seq_t seq = 0;
    CHECK(database->write("k", 1, points, 4, seq).ok());
  }

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT * FROM k", false, &cursor).ok());
  CHECK(cursor->columns().size() == 2);

  db_cursor *limited = nullptr;
  CHECK(
      database->open_cursor("SELECT id FROM k LIMIT 1", false, &limited).ok());

  ingest_field_t field = field_int("id", count);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  /* A new reader sees the flushed suffix while both old cursors keep their
  original view, including batches that have not been fetched yet. */
  test_query_t latest;
  CHECK(query_all(*database, "SELECT count(*) FROM k", latest).ok());
  CHECK(latest.m_rows.size() == 1 &&
        latest.m_rows[0][0].m_integer == count + 1);

  /* Early close must stop execution before releasing its interval. */
  query_rows_t rows;
  CHECK(limited->fetch(rows).ok());
  CHECK_EQ(rows.size(), 1);

  db_cursor::destroy(limited);

  int64_t total = 0;
  int seen = 0;

  for (;;) {
    CHECK(cursor->fetch(rows).ok());

    if (rows.empty()) {
      break;
    }

    for (const auto &row : rows) {
      CHECK_EQ(row.size(), 2);
      CHECK(row[1].m_integer >= 0 && row[1].m_integer < count);

      total += row[1].m_integer;
      ++seen;
    }

    field = field_int("id", count + seen);
    CHECK(database->write("k", 1, &p, 1, seq).ok());
    CHECK(database->flush(info).ok());
  }

  CHECK_EQ(seen, count);
  CHECK_EQ(total, int64_t{count} * (count - 1) / 2);
  CHECK(cursor->columns().size() == 2);

  db_cursor::destroy(cursor);

  /* Two scans of one hot table must have independent read positions. */
  CHECK(write_id(*database, -1).ok());

  test_query_t result;
  CHECK(
      query_all(*database,
                "SELECT count(*) FROM k a JOIN k b ON a.id=b.id WHERE a.id=-1",
                result)
          .ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 1);

  db_close(database);
}

/** Selective snapshots still resolve aliases, CTEs and repeated tables. */
static void test_selected_tables() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 3600000;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  const ingest_field_t field = field_int("v", 1);
  const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;

  for (const char *name : {"a", "b", "unrelated"}) {
    CHECK(database->write(name, strlen(name), &p, 1, seq).ok());
  }

  db::flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(database->write("a", 1, &p, 1, seq).ok());

  for (const char *sql :
       {"SELECT count(*) FROM a AS picked",
        "SELECT count(*) FROM a JOIN b USING (v)",
        "WITH picked AS (SELECT * FROM a) SELECT count(*) FROM picked"}) {
    test_query_t result;
    CHECK(query_all(*database, sql, result).ok());
    CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 2);
  }

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM a x JOIN a y USING (v)",
                  result)
            .ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 4);

  db_close(database);

  return;
}

/** Run cursor snapshot and read-only query tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"query_read_only", test_query_read_only},
      {"snapshot_after_rejection", test_snapshot_after_rejection},
      {"streaming_interval_after_flush", test_streaming_interval_after_flush},
      {"selected_tables", test_selected_tables},
  };

  return (test_main("snapshot-t", argc, argv, cases));
}
