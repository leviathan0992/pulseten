/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine0ttl-t.cc
 *    Tests for the retention period: old writes rejected, expired
 *    partitions dropped whole and for good, queries that started before a
 *    drop, and the background thread dropping by the wall clock.
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include <cstdio>
#include <string>

#include "test0engine.h"
#include "ut0time.h"

/** Options: no background thread, one-day partitions, a retention.
@param[in]  retention_days  retention period in days; 0 keeps all data
@return options with background flushing disabled */
static engine_options_t daily(uint32_t retention_days) {
  engine_options_t options;
  options.background_flush = false;
  options.partition_days = 1;
  options.retention_days = retention_days;

  return (options);
}

/** Write one point of table m with v at a time.
@param[in,out]  engine   database engine
@param[in]      time_us  UTC timestamp in microseconds since 1970-01-01
@param[in]      v        value of field v
@return the status from engine_write() */
static dberr_t write_at(engine_t *engine, int64_t time_us, int64_t v) {
  ingest_field_t f[] = {field_int("v", v)};
  ingest_point_t p = point(time_us, nullptr, 0, f, 1);
  batch_seq_t seq = 0;

  return (engine_write(*engine, "m", 1, &p, 1, seq, test_error));
}

/** Execute a query and read its single integer result.
@param[in,out]  engine  database engine
@param[in]      sql     SQL statement to check or execute
@return the single BIGINT a query returns, or -1 */
static int64_t query_int(engine_t *engine, const std::string &sql) {
  test_query_t r;
  std::string error;

  if (query_all(*engine, sql, r, error) != DB_SUCCESS || r.rows.size() != 1 ||
      r.rows[0][0].is_null) {
    fprintf(stderr, "query failed: %s: %s\n", sql.c_str(), error.c_str());

    return (-1);
  }

  return (r.rows[0][0].i);
}

/** Flush, checking that it succeeds.
@param[in,out]  engine  database engine */
static void flush(engine_t *engine) {
  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
}

/** Start of the UTC day of the current time.
@return start of the current UTC day in microseconds since 1970-01-01 */
static int64_t today_us() {
  return (ut_utc_day(ut_now_us()) * DAY);
}

/** Writes older than the retention period are rejected, a whole batch at a
time. */
static void test_reject_old_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = daily(10);

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  const int64_t now = ut_now_us();
  std::string error;

  CHECK(write_at(engine, now - 9 * DAY, 1) == DB_SUCCESS);
  CHECK(write_at(engine, now - 11 * DAY, 2) == DB_INVALID_ARGUMENT);

  /* One old point rejects the whole batch. */
  ingest_field_t f[] = {field_int("v", 3)};
  ingest_point_t points[] = {point(now, nullptr, 0, f, 1),
                             point(now - 20 * DAY, nullptr, 0, f, 1)};
  batch_seq_t seq = 0;

  CHECK(engine_write(*engine, "m", 1, points, 2, seq, error) ==
        DB_INVALID_ARGUMENT);
  CHECK(error.find("retention") != std::string::npos);

  /* An empty batch is refused. */
  CHECK(engine_write(*engine, "m", 1, nullptr, 0, seq, error) ==
        DB_INVALID_ARGUMENT);
  CHECK(error == "empty batch");

  CHECK(query_int(engine, "SELECT count(*) FROM m") == 1);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Too long a retention period. */
  options.retention_days = ENGINE_MAX_RETENTION_DAYS + 1;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_INVALID_ARGUMENT);
}

/** Partitions entirely past the cutoff are dropped, for good; others stay. */
static void test_expire() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = daily(30);

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* One point a day at noon for the last 20 days, all stored. */
  const int64_t today = today_us();

  for (int d = 0; d < 20; ++d) {
    CHECK(write_at(engine, today - d * DAY + DAY / 2, d) == DB_SUCCESS);
  }
  flush(engine);

  const storage_state_t *state =
      storage_get_state(*(engine_get_storage(*engine)));
  CHECK(state->partitions.size() == 20);

  /* Now: nothing has passed 30 days. */
  uint64_t removed = 0;
  CHECK(engine_expire(*engine, ut_now_us(), removed, test_error) == DB_SUCCESS);
  CHECK(removed == 0);

  /* 15 days later the cutoff is 15 days ago, a quarter into that day:
  days 16 .. 19 ago end before it and go; day 15 ago does not. */
  CHECK(engine_expire(*engine, today + DAY / 4 + 15 * DAY, removed,
                      test_error) == DB_SUCCESS);
  CHECK(removed == 4);
  CHECK(state->partitions.size() == 16);
  CHECK(query_int(engine, "SELECT count(*) FROM m") == 16);
  CHECK(query_int(engine, "SELECT max(v) FROM m") == 15);

  /* The physical tables are gone too. */
  int64_t tables = -1;
  CHECK(storage_query_int(*(engine_get_storage(*engine)),
                          "SELECT count(*) FROM information_schema.tables "
                          "WHERE regexp_matches(table_name, '^_pt_t[0-9]+_')",
                          tables) == DB_SUCCESS);
  CHECK(tables == 16);

  engine_stats_t stats;
  engine_get_stats(*engine, stats);
  CHECK(stats.expired_partitions == 4);

  /* Again at the same time: nothing more. */
  CHECK(engine_expire(*engine, today + DAY / 4 + 15 * DAY, removed,
                      test_error) == DB_SUCCESS);
  CHECK(removed == 0);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* The drop is durable, and opening checks the file is consistent. */
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(query_int(engine, "SELECT count(*) FROM m") == 16);
  CHECK(storage_get_state(*(engine_get_storage(*engine)))->partitions.size() ==
        16);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** A query started before an expiry reads every partition it started with. */
static void test_open_cursor() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = daily(30);

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  const int64_t today = today_us();

  for (int d = 0; d < 10; ++d) {
    for (int i = 0; i < 500; ++i) {
      CHECK(write_at(engine, today - d * DAY + i, d) == DB_SUCCESS);
    }
  }
  flush(engine);

  /* A query started before the drop reads every partition it started
  with. */
  engine_cursor_t *cursor = nullptr;
  CHECK(engine_cursor_open(*engine, "SELECT v FROM m", false, &cursor,
                           test_error) == DB_SUCCESS);

  uint64_t removed = 0;
  CHECK(engine_expire(*engine, today + 25 * DAY, removed, test_error) ==
        DB_SUCCESS);
  CHECK(removed == 4);

  size_t rows = 0;
  query_rows_t chunk;
  std::string error;

  for (;;) {
    const dberr_t err = engine_cursor_fetch(*cursor, chunk, error);
    CHECK(err == DB_SUCCESS);
    if (err != DB_SUCCESS || chunk.empty()) {
      if (err != DB_SUCCESS) {
        fprintf(stderr, "fetch: %s\n", error.c_str());
      }
      break;
    }
    rows += chunk.size();
  }

  CHECK(rows == 5000);
  engine_cursor_close(cursor);

  /* A query started after it does not. */
  CHECK(query_int(engine, "SELECT count(*) FROM m") == 6 * 500);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Without a retention period nothing is rejected or dropped. */
static void test_no_retention() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = daily(0);

  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* Anything in the time range is accepted and kept. */
  CHECK(write_at(engine, T0 - 3650 * DAY, 1) == DB_SUCCESS);
  flush(engine);

  uint64_t removed = 0;
  CHECK(engine_expire(*engine, ut_now_us() + 100000 * DAY, removed,
                      test_error) == DB_SUCCESS);
  CHECK(removed == 0);
  CHECK(query_int(engine, "SELECT count(*) FROM m") == 1);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** The background thread drops expired partitions by the wall clock. */
static void test_background() {
  test_dir_t dir;
  engine_t *engine = nullptr;

  /* Old data written while there was no retention period. */
  engine_options_t options = daily(0);
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  const int64_t now = ut_now_us();
  for (int d = 0; d < 6; ++d) {
    CHECK(write_at(engine, now - d * DAY, d) == DB_SUCCESS);
  }
  flush(engine);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Reopened with 3 days and a background thread, by the wall clock:
  the points 4 and 5 days ago are in days that ended before the cutoff;
  the one 3 days ago may or may not be. */
  options.background_flush = true;
  options.flush_interval_ms = 10;
  options.retention_days = 3;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  engine_stats_t stats;
  for (int waited = 0; waited < 5000; waited += 10) {
    engine_get_stats(*engine, stats);
    if (stats.expired_partitions >= 2) {
      break;
    }
    usleep(10 * 1000);
  }

  CHECK(stats.expired_partitions >= 2 && stats.expired_partitions <= 3);
  CHECK(stats.background_errors == 0);
  CHECK(query_int(engine, "SELECT max(v) FROM m") <= 3);
  CHECK(query_int(engine, "SELECT count(*) FROM m WHERE v <= 2") == 3);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_reject_old_writes();
  test_expire();
  test_open_cursor();
  test_no_retention();
  test_background();

  return (test_result("engine0ttl-t"));
}
