/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * flusher-t.cc
 *    Tests for background flushing: by interval, WAL files deleted after
 *    flushes, writers held back by the memtable limit, and failures that stop
 *    maintenance.
 *
 * Covers: src/db/db.cc
 *
 *-------------------------------------------------------------------------
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <duckdb.h>
#include <sys/types.h>
#include <unistd.h>

#include "db.h"
#include "ingest.h"
#include "memtable.h"
#include "pt_duckdb.h"
#include "store.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"

/** Wait up to timeout_ms for the memtable to be empty.
@return true if it became empty */
static bool wait_until_flushed(db &database, int timeout_ms) {
  for (int waited = 0; waited < timeout_ms; waited += 10) {
    memtable::stats_t stats;
    db_test_access::memtable_handle(database).stats(stats);

    if (stats.m_live_writes == 0) {
      return (true);
    }

    usleep(10 * 1000);
  }

  return (false);
}

/** The background thread flushes on its interval, with no explicit flush. */
static void test_interval_flush() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 20;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  for (int64_t v = 1; v <= 10; ++v) {
    CHECK(write_id(*database, v).ok());
  }

  /* No db::flush() call: the interval stores everything. */
  CHECK(wait_until_flushed(*database, 3000));

  db::stats_t stats;
  database->stats(stats);
  CHECK(stats.m_background_flushes >= 1 && stats.m_background_errors == 0);

  db_close(database);

  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  memtable::stats_t mt_stats;
  db_test_access::memtable_handle(*database).stats(mt_stats);
  CHECK(mt_stats.m_flushed_seq == 10 && mt_stats.m_live_writes == 0);
  CHECK(all_ids(*database).size() == 10);

  db_close(database);

  return;
}

/** A committed background flush is counted even if the following expiry
fails; expiry failures are counted separately. */
static void test_flush_count_when_expiry_fails() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us() - 3 * UT_DAY_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  /* Before starting the flusher, make expiry of the old partition fail.
  The foreign key blocks its catalog deletion, but not a later flush. */
  db_close(database);

  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());

  CHECK(pt_duckdb_exec(st->connection(),
                       "CREATE TABLE test_expiry_block (table_id UBIGINT, "
                       "start_hour BIGINT, FOREIGN KEY(table_id, start_hour) "
                       "REFERENCES _pt_partitions(table_id, start_hour))")
            .ok());
  CHECK(pt_duckdb_exec(st->connection(),
                       "INSERT INTO test_expiry_block "
                       "SELECT table_id, start_hour FROM _pt_partitions")
            .ok());
  store::destroy(st);

  options.m_retention_hours = 1 * 24;
  options.m_flush_interval_ms = 20;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  p.m_time_us = ut_now_us();
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db::stats_t stats;

  for (int waited = 0; waited < 3000; waited += 10) {
    database->stats(stats);

    if (stats.m_background_flushes > 0 && stats.m_background_errors > 0) {
      break;
    }

    usleep(10 * 1000);
  }

  memtable::stats_t mt;
  db_test_access::memtable_handle(*database).stats(mt);
  CHECK(mt.m_flushed_seq == 2 && mt.m_live_writes == 0);
  CHECK(stats.m_background_flushes == 1);
  CHECK(stats.m_background_errors >= 1);

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

  /** Points per write. */
  uint32_t m_points_per_write{1};

  /** Length of the padding string of each point. */
  size_t m_padding_bytes{2};

  /** Calls that failed. */
  int m_errors{0};
};

/** Write count batches with unique ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  const std::string pad(arg->m_padding_bytes, 'p');

  for (int i = 0; i < arg->m_write_count; ++i) {
    std::vector<ingest_field_t> fields;
    std::vector<ingest_point_t> points;

    /* Ids unique across threads, writes and points. */
    const int64_t base =
        (static_cast<int64_t>(arg->m_thread_index) * 100000 + i) *
        static_cast<int64_t>(arg->m_points_per_write);

    fields.resize(2 * arg->m_points_per_write);

    for (uint32_t k = 0; k < arg->m_points_per_write; ++k) {
      fields[2 * k] = field_int("id", base + k);
      fields[2 * k + 1] = field_str("pad", pad.c_str());
    }

    for (uint32_t k = 0; k < arg->m_points_per_write; ++k) {
      points.push_back(
          point(TEST_BASE_TIME_US + k, nullptr, 0, &fields[2 * k], 2));
    }

    batch_seq_t seq = 0;
    if (!arg->m_db->write("k", 1, &points[0], arg->m_points_per_write, seq)
             .ok()) {
      arg->m_errors++;
    }
  }

  return;
}

/** Run writer threads to completion. */
static void run_writers(db &database, int threads, int count, uint32_t points,
                        size_t pad) {
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_db = &database;
    args[t].m_thread_index = t;
    args[t].m_write_count = count;
    args[t].m_points_per_write = points;
    args[t].m_padding_bytes = pad;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
    CHECK(args[t].m_errors == 0);
  }

  return;
}

/** Store an old partition, then reopen with retention and a long interval
so the test can prepare a failure before waking the flusher.
@return the reopened db, or NULL if opening failed */
static db *open_expiry_db(const test_dir_t &dir) {
  db::options_t options = daily_partition_options();
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  if (database == nullptr) {
    return (nullptr);
  }

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us() - 3 * UT_DAY_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  db_close(database);

  options.m_retention_hours = 1 * 24;
  options.m_flush_interval_ms = 10 * 60 * 1000;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  return (database);
}

/** Block deletion of the old partition's catalog row with a foreign key. */
static void block_expiry(db &database) {
  duckdb_connection backend_conn = nullptr;
  CHECK(duckdb_connect((db_test_access::store_handle(database)).database(),
                       &backend_conn) == DuckDBSuccess);
  CHECK(pt_duckdb_exec(backend_conn,
                       "CREATE TABLE _pt_test_expiry_block (table_id UBIGINT, "
                       "start_hour BIGINT, FOREIGN KEY(table_id, start_hour) "
                       "REFERENCES _pt_partitions(table_id, start_hour))")
            .ok());
  CHECK(pt_duckdb_exec(backend_conn,
                       "INSERT INTO _pt_test_expiry_block "
                       "SELECT table_id, start_hour FROM _pt_partitions")
            .ok());
  duckdb_disconnect(&backend_conn);

  return;
}

/** Write a current point, then wake the background flusher. */
static void wake_expiry_db(db &database) {
  ingest_field_t field = field_int("id", 2);
  ingest_point_t p = point(ut_now_us(), nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database.write("k", 1, &p, 1, seq).ok());

  db_test_access::wake_flusher(database);

  return;
}

/** Wait for the triggered background pass to report its failure.
@return counters after the failure, or the last counters on timeout */
static db::stats_t wait_background_error(db &database) {
  db::stats_t stats;

  for (int waited = 0; waited < 5000; waited += 10) {
    database.stats(stats);

    if (stats.m_background_errors > 0) {
      return (stats);
    }

    usleep(10 * 1000);
  }

  CHECK(stats.m_background_errors > 0);

  return (stats);
}

/** A rolled-back flush stops maintenance and retains its first diagnostic.
@param[in] fail_expiry also arrange an expiry failure that must not run */
static void test_flush_failure_skips_expiry(bool fail_expiry) {
  test_dir_t dir;
  db *database = open_expiry_db(dir);
  REQUIRE(database != nullptr);

  if (fail_expiry) {
    block_expiry(*database);
  }

  ut_debug_global().arm("store.append", DB_ERROR, -1);
  wake_expiry_db(*database);

  const db::stats_t stats = wait_background_error(*database);
  CHECK(stats.m_background_flushes == 0);
  CHECK(stats.m_background_errors == 1);
  CHECK(stats.m_expired_partitions == 0);

  memtable::stats_t mt;
  db_test_access::memtable_handle(*database).stats(mt);
  CHECK(mt.m_flushed_seq == 1 && mt.m_live_writes == 1);

  const db_status_t message_status = database->background_status();
  const std::string &message = message_status.message();
  CHECK(message.find("flush: injected store.append") == 0);
  CHECK(message.find("; expire:") == std::string::npos);

  if (fail_expiry) {
    CHECK(pt_duckdb_exec(db_test_access::store_handle(*database).connection(),
                         "DROP TABLE _pt_test_expiry_block")
              .ok());
  }

  db::flush_info_t info;
  const db_status_t fenced = database->flush(info);
  CHECK(fenced.code() == DB_FENCED);
  CHECK(fenced.message() == "injected store.append");

  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, ut_now_us(), removed).code() ==
        DB_FENCED);
  CHECK(removed == 0);

  db_close(database);
  CHECK(reopen_ids(dir).size() == 2);

  return;
}

/** A failed rollback or corrupt stored boundary stops the database and leaves
expired partitions untouched.
@param[in]  corrupt  change the stored boundary instead of failing rollback */
static void test_unsafe_flush_stops_expiry(bool corrupt) {
  test_dir_t dir;
  db *database = open_expiry_db(dir);
  REQUIRE(database != nullptr);

  store &st = db_test_access::store_handle(*database);

  if (corrupt) {
    CHECK(
        pt_duckdb_exec(st.connection(), "UPDATE _pt_state SET flushed_seq = 2")
            .ok());
  } else {
    ut_debug_global().arm("store.rollback", DB_ERROR);
    ut_debug_global().arm("store.append", DB_ERROR);
  }

  wake_expiry_db(*database);

  const db::stats_t stats = wait_background_error(*database);
  CHECK(stats.m_background_flushes == 0);
  CHECK(stats.m_background_errors == 1);
  CHECK(stats.m_expired_partitions == 0);

  db::flush_info_t probe_info;
  const db_status_t reason_status = database->flush(probe_info);
  CHECK(reason_status.code() == DB_FENCED);

  const std::string &reason = reason_status.message();
  CHECK(!reason.empty());
  CHECK(reason.find(corrupt ? "flushed_seq" : "rollback:") !=
        std::string::npos);

  const db_status_t message_status = database->background_status();
  const std::string &message = message_status.message();
  CHECK(message.find("flush:") == 0);
  CHECK(message.find("expire:") == std::string::npos);

  std::vector<int64_t> partitions;
  CHECK(store_ints(st, "SELECT count(*) FROM _pt_partitions", partitions));
  CHECK(partitions.size() == 1 && partitions[0] == 1);

  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, ut_now_us(), removed).code() ==
        DB_FENCED);

  db::flush_info_t info;
  CHECK(database->flush(info).code() == DB_FENCED);

  ingest_field_t field = field_int("id", 3);
  ingest_point_t p = point(ut_now_us(), nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).code() == DB_FENCED);
  CHECK(seq == 0);

  if (corrupt) {
    /* Restore the test's deliberate corruption before checking recovery. */
    CHECK(
        pt_duckdb_exec(st.connection(), "UPDATE _pt_state SET flushed_seq = 1")
            .ok());
  }

  db_close(database);

  const std::vector<int64_t> ids = reopen_ids(dir);
  CHECK(ids.size() == 2);

  return;
}

/** A committed flush is counted even when expiry cannot roll back; the
db must be reopened and reopening recovers both committed points. */
static void test_expiry_rollback_failure() {
  test_dir_t dir;
  db *database = open_expiry_db(dir);
  REQUIRE(database != nullptr);

  block_expiry(*database);
  ut_debug_global().arm("store.rollback", DB_ERROR);
  ut_debug_global().disarm("store.append");
  wake_expiry_db(*database);

  const db::stats_t stats = wait_background_error(*database);
  CHECK(stats.m_background_flushes == 1);
  CHECK(stats.m_background_errors == 1);
  CHECK(stats.m_expired_partitions == 0);

  db::flush_info_t probe_info;
  const db_status_t reason_status = database->flush(probe_info);
  CHECK(reason_status.code() == DB_FENCED);

  const std::string &reason = reason_status.message();
  CHECK(reason.find("rollback: injected store.rollback") != std::string::npos);

  const db_status_t message_status = database->background_status();
  const std::string &message = message_status.message();
  CHECK(message.find("expire:") == 0);
  CHECK(message.find("rollback: injected store.rollback") != std::string::npos);

  /* Clean up the deliberately failed transaction and test-only constraint.
  The db remains stopped throughout; recovery still needs a reopen. */
  CHECK(pt_duckdb_exec((db_test_access::store_handle(*database)).connection(),
                       "ROLLBACK")
            .ok());
  CHECK(pt_duckdb_exec(db_test_access::store_handle(*database).connection(),
                       "DROP TABLE _pt_test_expiry_block")
            .ok());

  db_close(database);

  const std::vector<int64_t> ids = reopen_ids(dir);
  CHECK(ids.size() == 2);

  return;
}

/** A failed background flush keeps its message for the caller. */
static void test_background_error() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 10;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  ut_debug_global().arm("store.append", DB_ERROR);

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us(), nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db::stats_t stats;

  for (int waited = 0; waited < 10 * 1000; waited += 10) {
    database->stats(stats);

    if (stats.m_background_errors > 0) {
      break;
    }

    usleep(10 * 1000);
  }

  CHECK(stats.m_background_errors > 0);

  const db_status_t message_status = database->background_status();
  const std::string &message = message_status.message();
  CHECK(!message.empty());

  db_close(database);

  return;
}

/** A flush failure wakes capacity waiters and fences subsequent operations. */
static void test_throttle_flush_failure() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 60000;
  options.m_memtable_limit_bytes = 1;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  CHECK(database->exec("CREATE TABLE user_table (id BIGINT)").ok());
  ut_debug_global().arm("store.append", DB_ERROR, -1);

  /* Reject the SQL-table target before checking the reserved field. */
  ingest_field_t reserved = field_int("time", 1);
  ingest_point_t invalid = point(ut_now_us(), nullptr, 0, &reserved, 1);
  batch_seq_t rejected_seq = 99;
  db_status_t rejected_write =
      database->write("user_table", 10, &invalid, 1, rejected_seq);
  CHECK(rejected_write.code() == DB_INVALID_ARGUMENT);
  CHECK(rejected_write.message() ==
        "SQL table cannot receive time-series writes: user_table");
  CHECK(rejected_seq == 0);

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us(), nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  std::atomic<bool> returned{false};
  db_status_t written = DB_SUCCESS;

  std::thread writer([&] {
    written = database->write("k", 1, &p, 1, seq);
    returned = true;
  });

  for (int waited = 0; waited < 5000 && !returned.load(); waited += 5) {
    usleep(5000);
  }

  const bool rejected = returned.load();
  CHECK(rejected);

  db::flush_info_t info;
  const db_status_t flushed = database->flush(info);
  CHECK(flushed.code() == DB_FENCED);
  CHECK(flushed.message() == "injected store.append");

  rejected_seq = 99;
  rejected_write = database->write("user_table", 10, &p, 1, rejected_seq);
  CHECK(rejected_write.code() == DB_FENCED);
  CHECK(rejected_seq == 0);

  writer.join();
  CHECK(written.code() == DB_FENCED);
  CHECK(seq == 0);
  CHECK(written.message() == "injected store.append");

  CHECK(database->write("k", 1, &p, 1, seq).code() == DB_FENCED);

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM k", result).code() ==
        DB_FENCED);

  db_close(database);

  return;
}

/** OOM preserves backpressure, delays writer-triggered retries and recovers. */
static void test_throttle_flush_oom() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 500;
  options.m_memtable_limit_bytes = 1;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  ut_debug_global().arm("store.append", DB_OUT_OF_MEMORY, -1);
  CHECK(write_id(*database, 1).ok());

  std::atomic<bool> returned{false};
  db_status_t written;

  std::thread writer([&] {
    written = write_id(*database, 2);
    returned = true;
  });

  const db::stats_t failed = wait_background_error(*database);
  CHECK(failed.m_background_errors == 1);
  CHECK(database->background_status().code() == DB_OUT_OF_MEMORY);
  CHECK(!returned.load());

  /* Repeated writer signals must not bypass the retry interval. */
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(150);

  do {
    db_test_access::wake_flusher(*database);
    usleep(2000);
  } while (std::chrono::steady_clock::now() < deadline);

  db::stats_t stats;
  database->stats(stats);
  CHECK(stats.m_background_errors == 1 && stats.m_flushed_seq == 0);
  CHECK(!returned.load());

  test_query_t result;
  CHECK(query_all(*database, "SELECT id FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 1);
  ut_debug_global().disarm("store.append");

  for (int waited = 0; waited < 5000 && !returned.load(); waited += 5) {
    usleep(5000);
  }

  CHECK(returned.load());

  /* Ensure a failing test still releases the capacity waiter before joining. */
  db::flush_info_t info;
  CHECK(database->flush(info).ok());
  writer.join();
  CHECK(written.ok());
  CHECK(database->flush(info).ok());
  CHECK(query_all(*database, "SELECT count(*) FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 2);

  db_close(database);

  return;
}

/** close interrupts an OOM retry delay without waiting for its deadline. */
static void test_close_after_flush_oom() {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  ut_debug_global().arm("store.append", DB_OUT_OF_MEMORY, -1);
  CHECK(write_id(*database, 1).ok());
  db_test_access::wake_flusher(*database);
  CHECK(wait_background_error(*database).m_background_errors == 1);

  const auto started = std::chrono::steady_clock::now();

  db_close(database);
  CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(2));
  CHECK(reopen_ids(dir).size() == 1);

  return;
}

/** Background flushes delete stored WAL files; reopening replays nothing. */
static void test_wal_purged() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 10;
  options.m_wal_file_size_bytes = 4096;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  run_writers(*database, 8, 100, 1, 2);
  CHECK(wait_until_flushed(*database, 5000));

  /* Hundreds of small files were written; flushed ones are gone. */
  db::stats_t stats;

  /* Memtable retirement precedes WAL deletion; wait for deletion too. */
  for (int waited = 0; waited < 5000; waited += 10) {
    database->stats(stats);

    if (stats.m_wal_file_count <= 2) {
      break;
    }

    usleep(10 * 1000);
  }

  printf("wal purged: %llu files left after %llu background flushes\n",
         static_cast<unsigned long long>(stats.m_wal_file_count),
         static_cast<unsigned long long>(stats.m_background_flushes));
  CHECK(stats.m_wal_file_count <= 2);

  db_close(database);

  /* Reopening finds every write in storage and replays nothing. */
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  memtable::stats_t mt_stats;
  db_test_access::memtable_handle(*database).stats(mt_stats);

  const std::vector<int64_t> ids = all_ids(*database);

  CHECK(ids.size() == 800);
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == 800);
  CHECK(mt_stats.m_live_writes == 0 && mt_stats.m_flushed_seq == 800);

  database->stats(stats);
  CHECK(stats.m_wal_file_count <= 2);

  db_close(database);

  return;
}

struct sampler_arg_t {
  /** Database under test. */
  db *m_db{nullptr};

  /** Set when the thread should stop; protected by m_mutex. */
  bool *m_stop{nullptr};

  /** Protects *m_stop. */
  std::mutex *m_mutex{nullptr};

  /** Largest memtable size seen. */
  uint64_t m_max_bytes{0};
};

/** Record the largest memtable size seen. */
static void sampler_main(sampler_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->m_mutex);

      if (*arg->m_stop) {
        break;
      }
    }

    memtable::stats_t stats;
    db_test_access::memtable_handle(*arg->m_db).stats(stats);

    if (stats.m_live_bytes > arg->m_max_bytes) {
      arg->m_max_bytes = stats.m_live_bytes;
    }

    usleep(500);
  }

  return;
}

/** Writers wait while the memtable is over its limit, so it stays near the
limit. */
static void test_throttle() {
  test_dir_t dir;

  /* Writes of 100 points with 200-byte strings, about 25 KiB each, against
  a 64 KiB limit; the interval alone would not flush in time. */
  db::options_t options;
  options.m_flush_interval_ms = 60 * 1000;
  options.m_memtable_limit_bytes = 64 * 1024;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  bool stop = false;
  std::mutex mutex;
  sampler_arg_t sampler;
  sampler.m_db = database;
  sampler.m_stop = &stop;
  sampler.m_mutex = &mutex;

  std::thread sampler_thread(sampler_main, &sampler);

  const int threads = 4;
  run_writers(*database, threads, 30, 100, 200);

  {
    std::lock_guard<std::mutex> lock(mutex);
    stop = true;
  }

  sampler_thread.join();

  db::stats_t stats;
  database->stats(stats);

  /* Each writer checks the limit before writing, so the memtable can pass
  it by at most one write per writer. */
  const uint64_t one_write = 100 * (8 + 8 + 200 + 4 + 3) + 1024;
  printf("throttle: %llu writes waited, largest memtable %llu bytes\n",
         static_cast<unsigned long long>(stats.m_throttled_writes),
         static_cast<unsigned long long>(sampler.m_max_bytes));
  CHECK(stats.m_throttled_writes > 0);
  CHECK(sampler.m_max_bytes <=
        options.m_memtable_limit_bytes + threads * one_write);

  db_close(database);

  CHECK(reopen_ids(dir).size() == static_cast<size_t>(threads * 30 * 100));

  return;
}

/** Completion and fatal failure before wait registration are both observed. */
static void test_progress_before_wait() {
  for (bool fatal : {false, true}) {
    ut_debug_scope debug_scope;

    test_dir_t dir;
    db::options_t options = *deferred_flush_options();
    options.m_memtable_limit_bytes = 1;

    db *database = nullptr;
    CHECK(db_open(dir.m_path, &options, &database).ok());

    const ingest_field_t f = field_int("v", 1);
    const ingest_point_t p = point(ut_now_us(), nullptr, 0, &f, 1);
    batch_seq_t seq = 0;
    CHECK(database->write("m", 1, &p, 1, seq).ok());

    if (fatal) {
      ut_debug_global().set(
          "db.before_space_wait",
          [database] {
            ut_debug_global().arm("store.append", DB_ERROR);
            ut_debug_global().arm("store.rollback", DB_ERROR);

            db::flush_info_t info;
            CHECK(database->flush(info).code() == DB_OUTCOME_UNKNOWN);
          },
          1);
    } else {
      ut_debug_global().set(
          "db.before_space_wait",
          [database] {
            db::flush_info_t info;
            CHECK(database->flush(info).ok() && info.m_writes == 1);
          },
          1);
    }

    const db_status_t result = database->write("m", 1, &p, 1, seq);
    CHECK(fatal ? result.code() == DB_FENCED && seq == 0
                : result.ok() && seq == 2);

    db_close(database);
  }
}

/** Run background flush, backpressure and maintenance failure tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"interval_flush", test_interval_flush},
      {"flush_count_when_expiry_fails", test_flush_count_when_expiry_fails},
      {"flush_failure_skips_expiry/plain",
       [] { test_flush_failure_skips_expiry(false); }},
      {"flush_failure_skips_expiry/expiry_fails",
       [] { test_flush_failure_skips_expiry(true); }},
      {"unsafe_flush_stops_expiry/rollback_fails",
       [] { test_unsafe_flush_stops_expiry(false); }},
      {"unsafe_flush_stops_expiry/corrupt_boundary",
       [] { test_unsafe_flush_stops_expiry(true); }},
      {"expiry_rollback_failure", test_expiry_rollback_failure},
      {"background_error", test_background_error},
      {"throttle_flush_failure", test_throttle_flush_failure},
      {"throttle_flush_oom", test_throttle_flush_oom},
      {"close_after_flush_oom", test_close_after_flush_oom},
      {"wal_purged", test_wal_purged},
      {"progress_before_wait", test_progress_before_wait},
      {"throttle", test_throttle},
  };

  return (test_main("flusher-t", argc, argv, cases));
}
