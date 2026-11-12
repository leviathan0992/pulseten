/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_ttl-t.cc
 *    Tests for the retention period: old writes rejected, expired
 *    partitions dropped whole and for good, queries that started before a
 *    drop, and the background thread dropping by the wall clock.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <cstdint>
#include <cstdio>
#include <string>

#include <unistd.h>

#include "ingest.h"
#include "query.h"
#include "storage.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_point.h"
#include "test_storage.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_time.h"

/** Options: a long background interval, one-day partitions and retention.
@param[in]  retention_days  retention period in days; 0 keeps all data
@return options with a long background interval and no capacity limit */
static db_options_t daily(uint32_t retention_days) {
  db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_partition_duration_days = 1;
  options.m_retention_days = retention_days;

  return (options);
}

/** Write one point of table m with v at a time.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@return the status from db::write() */
static db_status_t write_at(db &database, int64_t time_us, int64_t v) {
  ingest_field_t f[] = {field_int("v", v)};
  ingest_point_t p = point(time_us, nullptr, 0, f, 1);
  batch_seq_t seq = 0;

  return (database.write("m", 1, &p, 1, seq));
}

/** Execute a query and read its single integer result.
@return the single BIGINT a query returns, or -1 */
static int64_t query_int(db &database, const std::string &sql) {
  db_status_t status;

  test_query_t r;

  status = query_all(database, sql, r);
  if (!status.ok() || r.m_rows.size() != 1 || r.m_rows[0][0].m_is_null) {
    fprintf(stderr, "query failed: %s: %s\n", sql.c_str(),
            status.message().c_str());

    return (-1);
  }

  return (r.m_rows[0][0].m_i);
}

/** Flush, checking that it succeeds. */
static void flush(db &database) {
  db_flush_info_t info;
  CHECK(database.flush(info).ok());

  return;
}

/** Start of the UTC day of the current time.
@return start of the current UTC day in microseconds since 1970-01-01 */
static int64_t today_us() {
  return (ut_utc_day(ut_now_us()) * DAY);
}

/** Writes older than the retention period are rejected, a whole batch at a
time. */
static void test_reject_old_writes() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;
  db_options_t options = daily(10);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  const int64_t now = ut_now_us();

  CHECK(write_at(*database, now - 9 * DAY, 1).ok());
  CHECK(write_at(*database, now - 11 * DAY, 2).code() == DB_INVALID_ARGUMENT);

  /* One old point rejects the whole batch. */
  ingest_field_t f[] = {field_int("v", 3)};
  ingest_point_t points[] = {point(now, nullptr, 0, f, 1),
                             point(now - 20 * DAY, nullptr, 0, f, 1)};
  batch_seq_t seq = 0;

  status = database->write("m", 1, points, 2, seq);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(status.message().find("retention") != std::string::npos);

  /* An empty batch is refused. */
  status = database->write("m", 1, nullptr, 0, seq);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(status.message() == "empty batch");

  CHECK(query_int(*database, "SELECT count(*) FROM m") == 1);
  db_close(database);

  /* Too long a retention period. */
  options.m_retention_days = DB_MAX_RETENTION_DAYS + 1;
  CHECK(db_open(dir.m_path, &options, &database).code() == DB_INVALID_ARGUMENT);

  return;
}

/** Partitions entirely past the cutoff are dropped, for good; others stay. */
static void test_expire() {
  test_dir_t dir;
  db *database = nullptr;
  db_options_t options = daily(30);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* One point a day at noon for the last 20 days, all stored. */
  const int64_t today = today_us();

  for (int d = 0; d < 20; ++d) {
    CHECK(write_at(*database, today - d * DAY + DAY / 2, d).ok());
  }

  flush(*database);

  db_close(database);
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, options.m_partition_duration_days, &st).ok());
  if (st == nullptr) {
    return;
  }

  CHECK(st->catalog().m_partitions.size() == 20);
  storage::destroy(st);
  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* Now: nothing has passed 30 days. */
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 20);
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 20);
  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, ut_now_us(), removed).ok());
  CHECK(removed == 0);

  /* 15 days later the cutoff is 15 days ago, a quarter into that day:
  days 16 .. 19 ago end before it and go; day 15 ago does not. */
  CHECK(db_test_access::expire(*database, today + DAY / 4 + 15 * DAY, removed)
            .ok());
  CHECK(removed == 4);
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 16);
  CHECK(query_int(*database, "SELECT max(v) FROM m") == 15);

  db_stats_t stats;
  database->read_stats(stats);
  CHECK(stats.m_expired_partitions == 4);

  /* Again at the same time: nothing more. */
  CHECK(db_test_access::expire(*database, today + DAY / 4 + 15 * DAY, removed)
            .ok());
  CHECK(removed == 0);
  db_close(database);

  /* The drop is durable, and opening checks the file is consistent. */
  CHECK(db_open(dir.m_path, &options, &database).ok());
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 16);
  db_close(database);

  /* The catalog and physical tables both retain only the surviving days. */
  CHECK(storage::open(dir.m_path, options.m_partition_duration_days, &st).ok());
  if (st == nullptr) {
    return;
  }

  CHECK(st->catalog().m_partitions.size() == 16);
  int64_t tables = -1;
  CHECK(test_storage_query_int(
            *st,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE regexp_matches(table_name, '^_pt_t[0-9]+_')",
            tables)
            .ok());
  CHECK(tables == 16);
  storage::destroy(st);

  return;
}

/** A query started before an expiry reads every partition it started with. */
static void test_open_cursor() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;
  db_options_t options = daily(30);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  const int64_t today = today_us();

  for (int d = 0; d < 10; ++d) {
    for (int i = 0; i < 500; ++i) {
      CHECK(write_at(*database, today - d * DAY + i, d).ok());
    }
  }

  flush(*database);

  /* A query started before the drop reads every partition it started
  with. */
  db_cursor *cursor = nullptr;
  CHECK(database->cursor_open("SELECT v FROM m", false, &cursor).ok());

  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, today + 25 * DAY, removed).ok());
  CHECK(removed == 4);

  size_t rows = 0;
  query_rows_t chunk;

  for (;;) {
    status = cursor->fetch(chunk);
    const db_status_t err = status;
    CHECK(err.ok());
    if (!err.ok() || chunk.empty()) {
      if (!err.ok()) {
        fprintf(stderr, "fetch: %s\n", status.message().c_str());
      }

      break;
    }

    rows += chunk.size();
  }

  CHECK(rows == 5000);
  db_cursor::destroy(cursor);

  /* A query started after it does not. */
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 6 * 500);
  db_close(database);

  return;
}

/** Without a retention period nothing is rejected or dropped. */
static void test_no_retention() {
  test_dir_t dir;
  db *database = nullptr;
  db_options_t options = daily(0);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* Anything in the time range is accepted and kept. */
  CHECK(write_at(*database, T0 - 3650 * DAY, 1).ok());
  flush(*database);

  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, ut_now_us() + 100000 * DAY, removed)
            .ok());
  CHECK(removed == 0);
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 1);
  db_close(database);

  return;
}

/** The background thread drops expired partitions by the wall clock. */
static void test_background() {
  test_dir_t dir;
  db *database = nullptr;

  /* Old data written while there was no retention period. */
  db_options_t options = daily(0);
  CHECK(db_open(dir.m_path, &options, &database).ok());

  const int64_t now = ut_now_us();
  for (int d = 0; d < 6; ++d) {
    CHECK(write_at(*database, now - d * DAY, d).ok());
  }

  flush(*database);
  db_close(database);

  /* Reopened with 3 days and a background thread, by the wall clock:
  the points 4 and 5 days ago are in days that ended before the cutoff;
  the one 3 days ago may or may not be. */

  options.m_flush_interval_ms = 10;
  options.m_retention_days = 3;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  db_stats_t stats;
  for (int waited = 0; waited < 5000; waited += 10) {
    database->read_stats(stats);
    if (stats.m_expired_partitions >= 2) {
      break;
    }

    usleep(10 * 1000);
  }

  CHECK(stats.m_expired_partitions >= 2 && stats.m_expired_partitions <= 3);
  CHECK(stats.m_background_errors == 0);
  CHECK(query_int(*database, "SELECT max(v) FROM m") <= 3);
  CHECK(query_int(*database, "SELECT count(*) FROM m WHERE v <= 2") == 3);
  db_close(database);

  return;
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

  return (test_result("db_ttl-t"));
}
