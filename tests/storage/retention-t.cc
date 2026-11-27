/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * retention-t.cc
 *    Tests for the retention period: old writes rejected, expired partitions
 *    dropped whole and for good, queries that started before a drop, and the
 *    background thread dropping by the wall clock.
 *
 * Covers: src/db/db.cc, src/store/store.cc
 *
 *-------------------------------------------------------------------------
 */

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "db.h"
#include "ingest.h"
#include "query.h"
#include "store.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_store.h"
#include "test_util.h"
#include "ut.h"

/** Write one point of table m with v at a time.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@return the status from db::write() */
static db_status_t write_at(db &database, int64_t time_us, int64_t v) {
  ingest_field_t f[] = {field_int("v", v)};

  ingest_point_t p = point(time_us, nullptr, 0, f, 1);

  return (database.write("m", 1, &p, 1));
}

/** Flush, checking that it succeeds. */
static void flush(db &database) {
  db::flush_info_t info;
  CHECK(database.flush(info).ok());

  return;
}

/** Start of the UTC day of the current time.
@return start of the current UTC day in microseconds since 1970-01-01 */
static int64_t today_us() {
  return (ut_utc_day(ut_now_us()) * TEST_DAY_US);
}

/** Writes older than the retention period are rejected, a whole batch at a
time. */
static void test_reject_old_writes() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;

  db::options_t options = daily_partition_options(10);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  const int64_t now = ut_now_us();

  CHECK(write_at(*database, now - 9 * TEST_DAY_US, 1).ok());
  CHECK(write_at(*database, now - 11 * TEST_DAY_US, 2).code() ==
        DB_INVALID_ARGUMENT);

  /* One old point rejects the whole batch. */
  ingest_field_t f[] = {field_int("v", 3)};
  ingest_point_t points[] = {point(now, nullptr, 0, f, 1),
                             point(now - 20 * TEST_DAY_US, nullptr, 0, f, 1)};

  status = database->write("m", 1, points, 2);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(status.message().find("retention") != std::string::npos);

  /* An empty batch is refused. */
  status = database->write("m", 1, nullptr, 0);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(status.message() == "empty batch");

  CHECK(query_int(*database, "SELECT count(*) FROM m") == 1);

  db_close(database);

  /* Too long a retention period. */
  options.m_retention_hours = STORE_MAX_RETENTION_HOURS + 1;
  CHECK(db_open(dir.m_path, &options, &database).code() == DB_INVALID_ARGUMENT);

  return;
}

/** Partitions entirely past the cutoff are dropped, for good; others stay. */
static void test_expire() {
  test_dir_t dir;
  db *database = nullptr;

  db::options_t options = daily_partition_options(30);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* One point a day at noon for the last 20 days, all stored. */
  const int64_t today = today_us();

  for (int d = 0; d < 20; ++d) {
    CHECK(
        write_at(*database, today - d * TEST_DAY_US + TEST_DAY_US / 2, d).ok());
  }

  flush(*database);

  db_close(database);

  store *st = nullptr;
  REQUIRE(
      store::open(dir.m_path, options.m_partition_duration_hours, &st).ok());

  CHECK(st->catalog().m_tables.at("m").m_partitions.size() == 20);

  store::destroy(st);
  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* Now: nothing has passed 30 days. */
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 20);
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 20);

  CHECK(db_test_access::expire(*database, ut_now_us()).ok());
  db::stats_t stats;
  database->stats(stats);
  CHECK(stats.m_expired_partitions == 0);

  /* 15 days later the cutoff is 15 days ago, a quarter into that day:
  days 16 .. 19 ago end before it and go; day 15 ago does not. */
  CHECK(db_test_access::expire(*database,
                               today + TEST_DAY_US / 4 + 15 * TEST_DAY_US)
            .ok());
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 16);
  CHECK(query_int(*database, "SELECT max(v) FROM m") == 15);

  database->stats(stats);
  CHECK(stats.m_expired_partitions == 4);

  /* Again at the same time: nothing more. */
  CHECK(db_test_access::expire(*database,
                               today + TEST_DAY_US / 4 + 15 * TEST_DAY_US)
            .ok());
  database->stats(stats);
  CHECK(stats.m_expired_partitions == 4);

  db_close(database);

  /* The drop is durable, and opening checks the file is consistent. */
  CHECK(db_open(dir.m_path, &options, &database).ok());
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 16);

  db_close(database);

  /* The catalog and physical tables both retain only the surviving days. */
  REQUIRE(
      store::open(dir.m_path, options.m_partition_duration_hours, &st).ok());

  CHECK(st->catalog().m_tables.at("m").m_partitions.size() == 16);

  int64_t tables = -1;
  CHECK(
      test_store_query_int(*st,
                           "SELECT count(*) FROM information_schema.tables "
                           "WHERE regexp_matches(table_name, '^_pt_t[0-9]+_')",
                           tables)
          .ok());
  CHECK(tables == 16);

  store::destroy(st);

  return;
}

/** A query started before an expiry reads every partition it started with. */
static void test_snapshot_survives_expiry() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;

  db::options_t options = daily_partition_options(30);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  const int64_t today = today_us();

  for (int d = 0; d < 10; ++d) {
    for (int i = 0; i < 500; ++i) {
      CHECK(write_at(*database, today - d * TEST_DAY_US + i, d).ok());
    }
  }

  flush(*database);

  /* A query started before the drop reads every partition it started with. */
  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT v FROM m", false, &cursor).ok());

  CHECK(db_test_access::expire(*database, today + 25 * TEST_DAY_US).ok());
  db::stats_t stats;
  database->stats(stats);
  CHECK(stats.m_expired_partitions == 4);

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

  db::options_t options = daily_partition_options(0);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  /* Anything in the time range is accepted and kept. */
  CHECK(write_at(*database, TEST_BASE_TIME_US - 3650 * TEST_DAY_US, 1).ok());

  flush(*database);

  CHECK(db_test_access::expire(*database, ut_now_us() + 100000 * TEST_DAY_US)
            .ok());
  CHECK(query_int(*database, "SELECT count(*) FROM m") == 1);

  db_close(database);

  return;
}

/** The background thread drops expired partitions by the wall clock. */
static void test_background_expiry() {
  test_dir_t dir;
  db *database = nullptr;

  /* Old data written while there was no retention period. */
  db::options_t options = daily_partition_options(0);
  CHECK(db_open(dir.m_path, &options, &database).ok());

  const int64_t now = ut_now_us();

  for (int d = 0; d < 6; ++d) {
    CHECK(write_at(*database, now - d * TEST_DAY_US, d).ok());
  }

  flush(*database);

  db_close(database);

  /* Reopened with 3 days and a background thread, by the wall clock:
  the points 4 and 5 days ago are in days that ended before the cutoff;
  the one 3 days ago may or may not be. */

  options.m_flush_interval_ms = 10;
  options.m_retention_hours = 3 * 24;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  db::stats_t stats;

  for (int waited = 0; waited < 5000; waited += 10) {
    database->stats(stats);

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

/** Open with maintenance stopped, leaving flush and expiry under test
control. */
static db *open_manual(test_dir_t &dir, const db::options_t &options) {
  db *database = nullptr;
  const db_status_t opened = db_open(dir.m_path, &options, &database);
  ut_a(opened.ok());
  db_test_access::stop_flusher(*database);

  return (database);
}

/** Write one integer point to the named table. */
static db_status_t write_named(db &database, const char *name, int64_t time,
                               int64_t value) {
  ingest_field_t f = field_int("v", value);
  ingest_point_t p = point(time, nullptr, 0, &f, 1);

  return (database.write(name, strlen(name), &p, 1));
}

/** A table override filters cold and hot rows, including joins and
aggregates. */
static void test_table_cutoff() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  db *database = open_manual(dir, options);
  const int64_t now = ut_now_us();

  for (const char *name : {"a", "b"}) {
    CHECK(write_named(*database, name, now - 2 * UT_HOUR_US, 10).ok());
    CHECK(write_named(*database, name, now - UT_HOUR_US / 2, 1).ok());
  }

  flush(*database);
  CHECK(write_named(*database, "a", now - 2 * UT_HOUR_US, 20).ok());
  CHECK(write_named(*database, "a", now - UT_HOUR_US / 2, 2).ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM a"), int64_t{4});
  CHECK(database->set_table_options("A", {1}).ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM a"), int64_t{2});
  CHECK_EQ(query_int(*database, "SELECT sum(v) FROM a"), int64_t{3});
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM b"), int64_t{2});
  CHECK_EQ(
      query_int(*database, "SELECT count(*) FROM a JOIN b ON a.time=b.time"),
      int64_t{2});
  CHECK_EQ(query_int(*database,
                     "SELECT count(*) FROM a WHERE time < make_timestamp(" +
                         std::to_string(now - UT_HOUR_US) + ")"),
           int64_t{0});

  CHECK(write_named(*database, "a", now - 2 * UT_HOUR_US, 30).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(database->set_table_options("a", {0}).ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM a"), int64_t{4});
  CHECK(database->set_table_options("a", {1}).ok());
  flush(*database);

  CHECK(db_test_access::expire(*database, now + 25 * UT_HOUR_US).ok());

  /* Global retention is unlimited; only a is eligible. */
  CHECK(database->set_table_options("a", {0}).ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM a"), int64_t{0});
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM b"), int64_t{2});

  db_close(database);
}

/** Retention does not depend on user macros shadowing timestamp functions. */
static void test_cutoff_literal() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  db *database = open_manual(dir, options);
  const int64_t now = ut_now_us();
  CHECK(write_at(*database, now - 2 * UT_HOUR_US, 1).ok());
  CHECK(write_at(*database, now, 2).ok());
  flush(*database);
  CHECK(write_at(*database, now - 2 * UT_HOUR_US, 3).ok());
  CHECK(write_at(*database, now, 4).ok());
  CHECK(database->set_table_options("m", {1}).ok());
  CHECK(
      database->exec("CREATE MACRO make_timestamp(x) AS TIMESTAMP '1900-01-01'")
          .ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{2});

  db_close(database);
}

/** Set persists a declaration without flushing WAL rows or their columns. */
static void test_set_unflushed() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  db *database = open_manual(dir, options);
  const int64_t now = ut_now_us();
  CHECK(write_at(*database, now - 2 * UT_HOUR_US, 1).ok());
  CHECK(write_at(*database, now, 2).ok());
  CHECK(database->set_table_options("m", {1}).ok());

  const store_catalog_t &catalog =
      db_test_access::store_handle(*database).catalog();
  CHECK_EQ(catalog.m_flushed_seq, batch_seq_t{0});
  REQUIRE(catalog.m_tables.count("m") == 1);
  CHECK(catalog.m_tables.at("m").m_partitions.empty());
  CHECK(!catalog.m_tables.at("m").initialized());
  CHECK(catalog.m_tables.at("m").m_schema.m_columns.empty());
  CHECK(catalog.m_tables.at("m").m_retention_hours == 1);

  db_close(database);

  options.m_retention_hours = 24;

  /* Reopen retains the durable duration. */
  options.m_partition_duration_hours = 1;
  database = open_manual(dir, options);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{1});
  CHECK(database->set_table_options("m", {}).ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{2});
  CHECK(!db_test_access::store_handle(*database)
             .catalog()
             .m_tables.at("m")
             .m_retention_hours);
  flush(*database);
  CHECK_EQ(db_test_access::store_handle(*database)
               .catalog()
               .m_tables.at("m")
               .m_partition_duration_hours,
           uint32_t{24});

  db_close(database);
  options.m_retention_hours = 1;
  database = open_manual(dir, options);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{1});
  CHECK(database->set_table_options("m", {0}).ok());

  db_close(database);
  database = open_manual(dir, options);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{2});

  db_close(database);
}

/** Invalid options and failed transactions leave the previous policy intact. */
static void test_set_failures() {
  for (int failure = 0; failure < 3; ++failure) {
    ut_debug_scope debug_scope;

    test_dir_t dir;
    db::options_t options = daily_partition_options();
    db *database = open_manual(dir, options);
    CHECK(write_at(*database, ut_now_us(), 1).ok());
    CHECK(database->set_table_options("missing", {1}).code() ==
          DB_INVALID_ARGUMENT);
    CHECK(database->set_table_options("_pt_state", {1}).code() ==
          DB_INVALID_ARGUMENT);
    CHECK(database->set_table_options("m", {STORE_MAX_RETENTION_HOURS + 1})
              .code() == DB_INVALID_ARGUMENT);
    CHECK(database->exec("CREATE TABLE ordinary(v INT)").ok());
    CHECK(database->set_table_options("ordinary", {1}).code() ==
          DB_INVALID_ARGUMENT);

    if (failure == 1) {
      ut_debug_global().arm("store.commit", DB_IO_ERROR);
    } else {
      ut_debug_global().arm("store.table_options", DB_IO_ERROR);
    }

    if (failure == 2) {
      ut_debug_global().arm("store.rollback", DB_IO_ERROR);
    }

    const db_status_t err = database->set_table_options("m", {1});
    CHECK(err.code() == (failure == 0 ? DB_IO_ERROR : DB_OUTCOME_UNKNOWN));
    CHECK(db_test_access::store_handle(*database).catalog().m_tables.empty());

    if (failure == 0) {
      CHECK(database->set_table_options("m", {0}).ok());
    } else {
      CHECK(database->set_table_options("m", {0}).code() == DB_FENCED);
      CHECK(write_at(*database, ut_now_us(), 2).code() == DB_FENCED);
    }

    db_close(database);
    database = open_manual(dir, options);
    CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{1});
    CHECK(database->set_table_options("m", {1}).ok());
    flush(*database);

    db_close(database);
  }
}

/** A query holds a stable policy; Set respects its table-lock deadline. */
static void test_set_query_lock() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  options.m_write_lock_timeout_ms = 20;

  db *database = open_manual(dir, options);
  CHECK(write_at(*database, ut_now_us() - 2 * UT_HOUR_US, 1).ok());

  db_cursor *cursor = nullptr;
  REQUIRE(database->open_cursor("SELECT count(*) FROM m", false, &cursor).ok());

  db_status_t result;
  std::thread setter([&] { result = database->set_table_options("m", {1}); });
  setter.join();
  CHECK(result.code() == DB_LOCK_TIMEOUT);

  query_rows_t rows;
  REQUIRE(cursor->fetch(rows).ok());
  REQUIRE(rows.size() == 1);
  CHECK_EQ(rows[0][0].m_integer, int64_t{1});
  db_cursor::destroy(cursor);
  CHECK(database->set_table_options("m", {1}).ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{0});

  db_close(database);
}

/** Writes can grow beyond the former partition cap and recover from WAL. */
static void test_partition_growth() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_partition_duration_hours = 1;

  db *database = open_manual(dir, options);
  const int64_t hour = ut_utc_hour(ut_now_us()) * UT_HOUR_US;
  ingest_field_t f = field_int("v", 1);
  std::vector<ingest_point_t> points;

  /* Keep each batch within the separate limit on distinct UTC days. */
  for (uint32_t first = 0; first < 4097; first += 256) {
    points.clear();

    for (uint32_t i = first; i < first + 256 && i < 4097; ++i) {
      points.push_back(point(hour + i * UT_HOUR_US, nullptr, 0, &f, 1));
    }

    REQUIRE(database->write("m", 1, points.data(), points.size()).ok());
  }

  db::stats_t stats;
  database->stats(stats);
  CHECK_EQ(stats.m_visible_seq, batch_seq_t{17});
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{4097});
  CHECK(database->set_table_options("m", {5000}).ok());
  CHECK_EQ(db_test_access::store_handle(*database).catalog().m_flushed_seq,
           batch_seq_t{0});

  db_close(database);
  database = open_manual(dir, options);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{4097});
  CHECK(write_at(*database, hour + 4097 * UT_HOUR_US, 2).ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{4098});

  db_close(database);

  return;
}

/** Pending rows can flush after physical expiry and are cleaned up again. */
static void test_expiry_with_pending_writes() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();

  db *database = open_manual(dir, options);
  const int64_t now = ut_now_us();
  const int64_t old = now - 3 * TEST_DAY_US;
  CHECK(write_at(*database, old, 1).ok());
  CHECK(write_at(*database, now + TEST_DAY_US, 2).ok());
  flush(*database);

  /* Pending row retains the old identity. */
  CHECK(write_at(*database, old, 3).ok());
  CHECK(database->set_table_options("m", {1}).ok());

  CHECK(db_test_access::expire(*database, now).ok());
  db::stats_t stats;
  database->stats(stats);
  CHECK_EQ(stats.m_expired_partitions, uint64_t{1});

  /* A new partition leaves pending writes for the normal flush path. */
  CHECK(write_at(*database, now + 3 * TEST_DAY_US, 4).ok());
  CHECK_EQ(db_test_access::store_handle(*database).catalog().m_flushed_seq,
           batch_seq_t{2});
  flush(*database);
  CHECK(db_test_access::expire(*database, now).ok());
  database->stats(stats);
  CHECK_EQ(stats.m_expired_partitions, uint64_t{2});
  CHECK(write_at(*database, now + 3 * TEST_DAY_US, 5).ok());

  db_close(database);
}

/** Concurrent partition growth preserves writes while Set and flush run. */
static void test_concurrent_writes_and_set() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_partition_duration_hours = 1;

  db *database = open_manual(dir, options);
  const int64_t hour = ut_utc_hour(ut_now_us()) * UT_HOUR_US;
  CHECK(write_at(*database, hour, 1).ok());

  std::atomic<int> accepted{0}, rejected{0}, errors{0};
  std::atomic<bool> start{false};
  std::vector<std::thread> writers;

  for (int i = 1; i <= 12; ++i) {
    writers.emplace_back([&, i] {
      while (!start.load()) {
        std::this_thread::yield();
      }

      const db_status_t err = write_at(*database, hour + i * UT_HOUR_US, i);
      if (err.ok()) {
        ++accepted;
      } else if (err.code() == DB_LIMIT_EXCEEDED) {
        ++rejected;
      } else {
        ++errors;
      }
    });
  }

  start = true;

  for (auto &writer : writers) {
    writer.join();
  }

  CHECK_EQ(accepted.load(), 12);
  CHECK_EQ(rejected.load(), 0);
  CHECK_EQ(errors.load(), 0);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{13});

  std::thread writer([&] {
    for (int i = 0; i < 50; ++i) {
      if (!write_at(*database, hour, 1).ok()) {
        ++errors;
      }
    }
  });

  std::thread storage([&] {
    for (int i = 0; i < 20; ++i) {
      db::flush_info_t info;
      if (!database->flush(info).ok()) {
        ++errors;
      }
    }
  });

  for (int i = 0; i < 20; ++i) {
    if (!database->set_table_options("m", {i % 2 == 0 ? 1U : 0U}).ok()) {
      ++errors;
    }
  }

  writer.join();
  storage.join();
  CHECK_EQ(errors.load(), 0);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{63});

  db_close(database);
  database = open_manual(dir, options);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{63});

  db_close(database);
}

/** A cursor retains its cutoff even after a row ages out during fetch. */
static void test_frozen_cutoff() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  options.m_retention_hours = 1;

  db *database = open_manual(dir, options);
  const int64_t boundary = ut_now_us() + 2000000LL;
  CHECK(write_at(*database, boundary - UT_HOUR_US, 1).ok());

  db_cursor *cursor = nullptr;
  REQUIRE(database->open_cursor("SELECT count(*) FROM m", false, &cursor).ok());

  while (ut_now_us() <= boundary) {
    usleep(10000);
  }

  query_rows_t rows;
  REQUIRE(cursor->fetch(rows).ok());
  REQUIRE(rows.size() == 1);
  CHECK_EQ(rows[0][0].m_integer, int64_t{1});
  db_cursor::destroy(cursor);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{0});

  db_close(database);
}

/** Disabled future checks preserve supported timestamps; enabled checks reject
whole batches without changing schemas, WAL or durable recovery. */
static void test_future_skew() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  CHECK_EQ(options.m_max_future_skew_seconds, uint32_t{0});

  db *database = open_manual(dir, options);
  const int64_t now = ut_now_us();
  CHECK(write_at(*database, now + 100 * TEST_DAY_US, 1).ok());

  db_close(database);
  options.m_max_future_skew_seconds = 300;
  database = open_manual(dir, options);

  /* New policy does not reject already accepted WAL-only points on reopen. */
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{1});
  CHECK(write_at(*database, now + 60 * 1000000LL, 2).ok());
  wal::stats_t before, after;
  db_test_access::wal_stats(*database, before);

  ingest_field_t f = field_int("extra", 9);
  ingest_point_t points[] = {point(now, nullptr, 0, &f, 1),
                             point(now + UT_HOUR_US, nullptr, 0, &f, 1)};

  CHECK(database->write("m", 1, points, 2).code() == DB_INVALID_ARGUMENT);
  CHECK(write_at(*database, INGEST_MAX_TIME_US, 3).code() ==
        DB_INVALID_ARGUMENT);
  db_test_access::wal_stats(*database, after);
  CHECK_EQ(before.m_records, after.m_records);

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);
  REQUIRE(tables.size() == 1);
  CHECK_EQ(tables[0].m_schema->m_columns.size(), size_t{2});
  flush(*database);

  db_close(database);
  database = open_manual(dir, options);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{2});

  db_close(database);

  /* Retention and future skew remain independent of partition counts. */
  test_dir_t policies;
  options.m_partition_duration_hours = 1;
  database = open_manual(policies, options);
  CHECK(database->create_table("m", {1, 5000}).ok());
  CHECK(database->set_table_options("m", {1}).ok());
  CHECK(write_at(*database, ut_now_us(), 1).ok());
  CHECK(write_at(*database, ut_now_us() + UT_HOUR_US, 2).code() ==
        DB_INVALID_ARGUMENT);

  db_close(database);
}

/** Invalid time input never waits for memtable capacity or schema X. */
static void test_early_time_rejection() {
  for (bool future : {false, true}) {
    ut_debug_scope debug_scope;

    test_dir_t dir;
    db::options_t options = daily_partition_options(future ? 0 : 1);
    options.m_max_future_skew_seconds = future ? 300 : 0;
    options.m_memtable_limit_bytes = 1;
    options.m_write_lock_timeout_ms = 20;

    db *database = open_manual(dir, options);
    const int64_t now = ut_now_us();
    CHECK(write_at(*database, now, 1).ok());

    db_cursor *cursor = nullptr;
    REQUIRE(database->open_cursor("SELECT * FROM m", false, &cursor).ok());

    int waits = 0;

    ut_debug_global().set("db.before_space_wait", [&] {
      ++waits;

      /* Allow a regressed implementation to unwind. */
      flush(*database);
    });

    ingest_field_t f = field_int("extra", 2);
    ingest_point_t p = point(future ? now + UT_HOUR_US : now - 2 * TEST_DAY_US,
                             nullptr, 0, &f, 1);

    CHECK(database->write("m", 1, &p, 1).code() == DB_INVALID_ARGUMENT);
    CHECK(database->write("m", 1, nullptr, 1).code() == DB_INVALID_ARGUMENT);
    CHECK(database->write("m", 1, &p, INGEST_MAX_POINTS + 1).code() ==
          DB_LIMIT_EXCEEDED);
    CHECK_EQ(waits, 0);
    db_cursor::destroy(cursor);

    db_close(database);
  }
}

/** Policy changes during S-to-X upgrade still reject the batch at admission. */
static void test_time_recheck_after_upgrade() {
  test_dir_t dir;
  db *database = open_manual(dir, daily_partition_options());
  const int64_t now = ut_now_us();
  CHECK(write_at(*database, now, 1).ok());
  ut_debug_global().set(
      "db.before_write_schema_lock",
      [&] { CHECK(database->set_table_options("m", {1}).ok()); }, 1);

  ingest_field_t f = field_int("extra", 2);
  ingest_point_t points[] = {point(now, nullptr, 0, &f, 1),
                             point(now - 2 * UT_HOUR_US, nullptr, 0, &f, 1)};

  const auto rejected = database->write("m", 1, points, 2);
  CHECK(rejected.code() == DB_INVALID_ARGUMENT);
  CHECK(rejected.message().find("point 1") != std::string::npos);
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{1});

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);
  REQUIRE(tables.size() == 1);
  CHECK_EQ(tables[0].m_schema->m_columns.size(), size_t{2});

  db_close(database);
}

/** A paused prepared write does not hold the global lock against another
writer. */
static void test_write_preparation_progress() {
  test_dir_t dir;
  db *database = open_manual(dir, daily_partition_options());
  const int64_t now = ut_now_us();
  CHECK(write_at(*database, now, 1).ok());

  std::atomic<bool> start{false}, done{false};
  db_status_t status;

  std::thread other([&] {
    while (!start.load()) {
      std::this_thread::yield();
    }

    status = write_at(*database, now, 2);
    done = true;
  });
  ut_debug_global().set(
      "db.before_reserve_write",
      [&] {
        start = true;

        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);

        while (!done.load() && std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }

        CHECK(done.load());
      },
      1);
  CHECK(write_at(*database, now, 3).ok());
  other.join();
  CHECK(status.ok());
  CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{3});

  db_close(database);
}

/** Partition growth does not flush or expire existing data on admission. */
static void test_partition_growth_without_flush() {
  for (bool stored : {false, true}) {
    ut_debug_scope debug_scope;

    test_dir_t dir;
    auto options = *deferred_flush_options();
    options.m_partition_duration_hours = 1;

    db *database = open_manual(dir, options);
    const int64_t now = ut_now_us();
    CHECK(write_at(*database, now - 3 * UT_HOUR_US, 1).ok());
    CHECK(write_at(*database, now - 2 * UT_HOUR_US, 2).ok());

    if (stored) {
      flush(*database);
    }

    CHECK(database->set_table_options("m", {1}).ok());

    db_cursor *old = nullptr;
    REQUIRE(database->open_cursor("SELECT count(*) FROM m", false, &old).ok());

    ut_debug_global().arm("store.append", DB_OUT_OF_MEMORY);
    CHECK(write_at(*database, now, 3).ok());
    CHECK_EQ(ut_debug_global().failures("store.append"), uint64_t{0});
    CHECK_EQ(db_test_access::store_handle(*database).catalog().m_flushed_seq,
             stored ? batch_seq_t{2} : batch_seq_t{0});
    ut_debug_global().disarm("store.append");

    query_rows_t rows;
    REQUIRE(old->fetch(rows).ok());
    REQUIRE(rows.size() == 1);
    CHECK_EQ(rows[0][0].m_integer, int64_t{0});
    db_cursor::destroy(old);
    CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{1});

    db_close(database);
    database = open_manual(dir, options);
    CHECK_EQ(query_int(*database, "SELECT count(*) FROM m"), int64_t{1});
    CHECK(database->drop_table("m").ok());
    CHECK(database->create_table("m", {5, 0}).ok());

    ingest_field_t f = field_str("v", "new");
    ingest_point_t p = point(now, nullptr, 0, &f, 1);
    CHECK(database->write("m", 1, &p, 1).ok());

    db_close(database);
    database = open_manual(dir, options);

    test_query_t result;
    REQUIRE(query_all(*database, "SELECT v FROM m", result).ok());
    REQUIRE(result.m_rows.size() == 1);
    CHECK_EQ(result.m_rows[0][0].m_string, "new");

    db_close(database);
  }
}

/** Cached time bounds are rechecked after waiting for memtable capacity. */
static void test_time_bounds_after_waits() {
  test_dir_t dir;
  auto options = *deferred_flush_options();
  options.m_partition_duration_hours = 1;
  options.m_memtable_limit_bytes = 1;

  db *database = open_manual(dir, options);
  CHECK(write_at(*database, ut_now_us(), 1).ok());
  CHECK(database->set_table_options("m", {1}).ok());

  const int64_t boundary = ut_now_us() + 1000000;
  bool waited = false;
  ut_debug_global().set(
      "db.before_space_wait",
      [&] {
        waited = true;

        while (ut_now_us() <= boundary + 10000) {
          usleep(10000);
        }

        flush(*database);
      },
      1);

  ingest_field_t f = field_int("v", 3);
  ingest_point_t p = point(boundary - UT_HOUR_US, nullptr, 0, &f, 1);
  CHECK(database->write("m", 1, &p, 1).code() == DB_INVALID_ARGUMENT);
  CHECK(waited);

  db_close(database);

  return;
}

/** Run retention boundary, partition expiry and snapshot tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"partition_growth_without_flush", test_partition_growth_without_flush},
      {"time_bounds_after_waits", test_time_bounds_after_waits},
      {"write_preparation_progress", test_write_preparation_progress},
      {"future_skew", test_future_skew},
      {"early_time_rejection", test_early_time_rejection},
      {"time_recheck_after_upgrade", test_time_recheck_after_upgrade},
      {"reject_old_writes", test_reject_old_writes},
      {"expire", test_expire},
      {"snapshot_survives_expiry", test_snapshot_survives_expiry},
      {"no_retention", test_no_retention},
      {"background_expiry", test_background_expiry},
      {"table_cutoff", test_table_cutoff},
      {"cutoff_literal", test_cutoff_literal},
      {"set_unflushed", test_set_unflushed},
      {"set_failures", test_set_failures},
      {"set_query_lock", test_set_query_lock},
      {"partition_growth", test_partition_growth},
      {"expiry_with_pending_writes", test_expiry_with_pending_writes},
      {"frozen_cutoff", test_frozen_cutoff},
      {"concurrent_writes_and_set", test_concurrent_writes_and_set},
  };

  return (test_main("retention-t", argc, argv, cases));
}
