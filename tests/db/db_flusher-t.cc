/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_flusher-t.cc
 *    Tests for background flushing: by interval, WAL segments
 *    deleted after flushes, writers held back by the memtable limit, and
 *    SIGKILL while all of it runs.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <signal.h>
#include <string>
#include <thread>
#include <vector>

#include <duckdb.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "duckdb_database.h"
#include "ingest.h"
#include "memtable.h"
#include "storage.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_point.h"
#include "test_storage_access.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_time.h"

/** A long background interval keeps internal flush boundaries under test
control.
@return options with a long background interval and no capacity limit */
static const db_options_t *deferred_flush() {
  static db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;

  return (&options);
}

/** Wait up to timeout_ms for the memtable to be empty.
@return true if it became empty */
static bool wait_until_flushed(db &database, int timeout_ms) {
  for (int waited = 0; waited < timeout_ms; waited += 10) {
    memtable_stats_t stats;
    db_test_access::memtable_handle(database).read_stats(stats);

    if (stats.m_live_writes == 0) {
      return (true);
    }

    usleep(10 * 1000);
  }

  return (false);
}

/** Reopen with a long background interval and return every recovered id.
@return ids read from storage and replayed memtable batches */
static std::vector<int64_t> reopen_ids(const test_dir_t &dir) {
  db *database = nullptr;
  std::vector<int64_t> ids;

  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  if (database != nullptr) {
    ids = all_ids(*database);
    db_close(database);
  }

  return (ids);
}

/** The background thread flushes on its interval, with no explicit flush. */
static void test_interval_flush() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 20;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  for (int64_t v = 1; v <= 10; ++v) {
    CHECK(write_id(*database, v).ok());
  }

  /* No db::flush() call: the interval stores everything. */
  CHECK(wait_until_flushed(*database, 3000));

  db_stats_t stats;
  database->read_stats(stats);
  CHECK(stats.m_background_flushes >= 1 && stats.m_background_errors == 0);
  db_close(database);

  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  memtable_stats_t mt_stats;
  db_test_access::memtable_handle(*database).read_stats(mt_stats);
  CHECK(mt_stats.m_flushed_seq == 10 && mt_stats.m_live_writes == 0);
  CHECK(all_ids(*database).size() == 10);
  db_close(database);

  return;
}

/** A committed background flush is counted even if the following expiry
fails; expiry failures are counted separately. */
static void test_flush_count_when_expiry_fails() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_partition_duration_days = 1;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us() - 3 * UT_DAY_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db_flush_info_t info;
  CHECK(database->flush(info).ok());

  /* Before starting the flusher, make expiry of the old partition fail.
  The foreign key blocks its catalog deletion, but not a later flush. */
  db_close(database);
  duckdb_backend *backend = nullptr;
  CHECK(
      duckdb_backend::open(dir.file(STORAGE_FILE_NAME).c_str(), &backend).ok());
  if (backend == nullptr) {
    return;
  }

  CHECK(duckdb_exec(backend->connection(),
                    "CREATE TABLE test_expiry_block (table_id UBIGINT, "
                    "first_day BIGINT, FOREIGN KEY(table_id, first_day) "
                    "REFERENCES _pt_partitions(table_id, first_day))")
            .ok());
  CHECK(duckdb_exec(backend->connection(),
                    "INSERT INTO test_expiry_block "
                    "SELECT table_id, first_day FROM _pt_partitions")
            .ok());
  duckdb_backend::destroy(backend);

  options.m_retention_days = 1;
  options.m_flush_interval_ms = 20;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  p.m_time_us = ut_now_us();
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db_stats_t stats;
  for (int waited = 0; waited < 3000; waited += 10) {
    database->read_stats(stats);
    if (stats.m_background_flushes > 0 && stats.m_background_errors > 0) {
      break;
    }

    usleep(10 * 1000);
  }

  memtable_stats_t mt;
  db_test_access::memtable_handle(*database).read_stats(mt);
  CHECK(mt.m_flushed_seq == 2 && mt.m_live_writes == 0);
  CHECK(stats.m_background_flushes == 1);
  CHECK(stats.m_background_errors >= 1);
  db_close(database);

  return;
}

struct writer_arg_t {
  /** Engine under test. */
  db *m_db{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Writes to make. */
  int m_count{0};

  /** Points per write. */
  uint32_t m_points{1};

  /** Length of the padding string of each point. */
  size_t m_pad{2};

  /** Calls that failed. */
  int m_errors{0};
};

/** Write count batches with unique ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  const std::string pad(arg->m_pad, 'p');

  for (int i = 0; i < arg->m_count; ++i) {
    std::vector<ingest_field_t> fields;
    std::vector<ingest_point_t> points;

    /* Ids unique across threads, writes and points. */
    const int64_t base = (static_cast<int64_t>(arg->m_thread_no) * 100000 + i) *
                         static_cast<int64_t>(arg->m_points);

    fields.resize(2 * arg->m_points);
    for (uint32_t k = 0; k < arg->m_points; ++k) {
      fields[2 * k] = field_int("id", base + k);
      fields[2 * k + 1] = field_str("pad", pad.c_str());
    }

    for (uint32_t k = 0; k < arg->m_points; ++k) {
      points.push_back(point(T0 + k, nullptr, 0, &fields[2 * k], 2));
    }

    batch_seq_t seq = 0;
    if (!arg->m_db->write("k", 1, &points[0], arg->m_points, seq).ok()) {
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
    args[t].m_thread_no = t;
    args[t].m_count = count;
    args[t].m_points = points;
    args[t].m_pad = pad;
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
  db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_partition_duration_days = 1;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return (nullptr);
  }

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us() - 3 * UT_DAY_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());
  db_flush_info_t info;
  CHECK(database->flush(info).ok());
  db_close(database);

  options.m_retention_days = 1;
  options.m_flush_interval_ms = 10 * 60 * 1000;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  return (database);
}

/** Block deletion of the old partition's catalog row with a foreign key. */
static void block_expiry(db &database) {
  duckdb_connection backend_conn = nullptr;
  CHECK(duckdb_connect((db_test_access::storage_handle(database)).database(),
                       &backend_conn) == DuckDBSuccess);
  CHECK(duckdb_exec(backend_conn,
                    "CREATE TABLE _pt_test_expiry_block (table_id UBIGINT, "
                    "first_day BIGINT, FOREIGN KEY(table_id, first_day) "
                    "REFERENCES _pt_partitions(table_id, first_day))")
            .ok());
  CHECK(duckdb_exec(backend_conn,
                    "INSERT INTO _pt_test_expiry_block "
                    "SELECT table_id, first_day FROM _pt_partitions")
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
static db_stats_t wait_background_error(db &database) {
  db_stats_t stats;
  for (int waited = 0; waited < 5000; waited += 10) {
    database.read_stats(stats);
    if (stats.m_background_errors > 0) {
      return (stats);
    }

    usleep(10 * 1000);
  }

  CHECK(stats.m_background_errors > 0);

  return (stats);
}

/** A rolled-back flush does not suppress expiry. If both tasks fail, count
one failed pass and retain both diagnostics.
@param[in]  fail_expiry  block expiry after the flush has rolled back */
static void test_expiry_after_flush_failure(bool fail_expiry) {
  test_dir_t dir;
  db *database = open_expiry_db(dir);
  if (database == nullptr) {
    return;
  }

  if (fail_expiry) {
    block_expiry(*database);
  }

  db_test_access::set_flush_failure(*database, true);
  wake_expiry_db(*database);
  const db_stats_t stats = wait_background_error(*database);
  CHECK(stats.m_background_flushes == 0);
  CHECK(stats.m_background_errors == 1);
  CHECK(stats.m_expired_partitions == (fail_expiry ? 0U : 1U));

  memtable_stats_t mt;
  db_test_access::memtable_handle(*database).read_stats(mt);
  CHECK(mt.m_flushed_seq == 1 && mt.m_live_writes == 1);
  const db_status_t message_status = database->background_status();
  const std::string &message = message_status.message();
  CHECK(message.find("flush: injected flush failure") == 0);
  CHECK((message.find("; expire:") != std::string::npos) == fail_expiry);

  if (fail_expiry) {
    CHECK(duckdb_exec(db_test_access::storage_handle(*database).connection(),
                      "DROP TABLE _pt_test_expiry_block")
              .ok());
  }

  db_test_access::set_flush_failure(*database, false);
  db_flush_info_t info;
  CHECK(database->flush(info).ok());
  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, ut_now_us(), removed).ok());
  CHECK(removed == (fail_expiry ? 1U : 0U));
  const std::vector<int64_t> ids = all_ids(*database);
  CHECK(ids.size() == 1 && ids[0] == 2);
  db_close(database);

  return;
}

/** A failed rollback or corrupt stored boundary stops the database and leaves
expired partitions untouched.
@param[in]  corrupt  change the stored boundary instead of failing rollback */
static void test_unsafe_flush_stops_expiry(bool corrupt) {
  test_dir_t dir;
  db *database = open_expiry_db(dir);
  if (database == nullptr) {
    return;
  }

  storage &store = db_test_access::storage_handle(*database);
  if (corrupt) {
    CHECK(
        duckdb_exec(store.connection(), "UPDATE _pt_state SET flushed_seq = 2")
            .ok());
  } else {
    storage_test_access::fail_next_rollback(store);
    db_test_access::fail_next_flush(*database);
  }

  wake_expiry_db(*database);
  const db_stats_t stats = wait_background_error(*database);
  CHECK(stats.m_background_flushes == 0);
  CHECK(stats.m_background_errors == 1);
  CHECK(stats.m_expired_partitions == 0);
  db_flush_info_t probe_info;
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
  CHECK(storage_ints(store, "SELECT count(*) FROM _pt_partitions", partitions));
  CHECK(partitions.size() == 1 && partitions[0] == 1);
  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, ut_now_us(), removed).code() ==
        DB_FENCED);
  db_flush_info_t info;
  CHECK(database->flush(info).code() == DB_FENCED);
  ingest_field_t field = field_int("id", 3);
  ingest_point_t p = point(ut_now_us(), nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).code() == DB_FENCED);
  CHECK(seq == 0);

  if (corrupt) {
    /* Restore the test's deliberate corruption before checking recovery. */
    CHECK(
        duckdb_exec(store.connection(), "UPDATE _pt_state SET flushed_seq = 1")
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
  if (database == nullptr) {
    return;
  }

  block_expiry(*database);
  storage_test_access::fail_next_rollback(
      db_test_access::storage_handle(*database));
  db_test_access::set_flush_failure(*database, false);
  wake_expiry_db(*database);

  const db_stats_t stats = wait_background_error(*database);
  CHECK(stats.m_background_flushes == 1);
  CHECK(stats.m_background_errors == 1);
  CHECK(stats.m_expired_partitions == 0);
  db_flush_info_t probe_info;
  const db_status_t reason_status = database->flush(probe_info);
  CHECK(reason_status.code() == DB_FENCED);
  const std::string &reason = reason_status.message();
  CHECK(reason.find("rollback: injected rollback failure") !=
        std::string::npos);
  const db_status_t message_status = database->background_status();
  const std::string &message = message_status.message();
  CHECK(message.find("expire:") == 0);
  CHECK(message.find("rollback: injected rollback failure") !=
        std::string::npos);

  /* Clean up the deliberately failed transaction and test-only constraint.
  The db remains stopped throughout; recovery still needs a reopen. */
  CHECK(duckdb_exec((db_test_access::storage_handle(*database)).connection(),
                    "ROLLBACK")
            .ok());
  CHECK(duckdb_exec(db_test_access::storage_handle(*database).connection(),
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
  db_options_t options;
  options.m_flush_interval_ms = 10;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  db_test_access::fail_next_flush(*database);

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us(), nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("k", 1, &p, 1, seq).ok());

  db_stats_t stats;

  for (int waited = 0; waited < 10 * 1000; waited += 10) {
    database->read_stats(stats);

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

/** A full memtable returns persistent flush failures, then accepts writes
again after a successful retry without stopping the database. */
static void test_throttle_flush_failure() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 60000;
  options.m_memtable_limit_bytes = 1;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  CHECK(duckdb_exec(db_test_access::storage_handle(*database).connection(),
                    "CREATE TABLE user_table (id BIGINT)")
            .ok());
  db_test_access::set_flush_failure(*database, true);

  /* Reject the user-table target before validating the reserved field name. */
  ingest_field_t reserved = field_int("time", 1);
  ingest_point_t invalid = point(ut_now_us(), nullptr, 0, &reserved, 1);
  batch_seq_t rejected_seq = 99;
  db_status_t rejected_write =
      database->write("user_table", 10, &invalid, 1, rejected_seq);
  CHECK(rejected_write.code() == DB_INVALID_ARGUMENT);
  CHECK(rejected_write.message() ==
        "user tables cannot receive time-series writes");
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

  db_flush_info_t info;
  const db_status_t flushed = database->flush(info);
  CHECK(flushed.code() == DB_ERROR);
  CHECK(flushed.message() == "injected flush failure");

  /* Reject the user-table target before checking the failed flush for room. */
  rejected_seq = 99;
  rejected_write = database->write("user_table", 10, &p, 1, rejected_seq);
  CHECK(rejected_write.code() == DB_INVALID_ARGUMENT);
  CHECK(rejected_write.message() ==
        "user tables cannot receive time-series writes");
  CHECK(rejected_seq == 0);

  /* Release even a regressed writer so a failure does not hang the suite. */
  db_test_access::set_flush_failure(*database, false);
  CHECK(database->flush(info).ok());
  writer.join();
  if (rejected) {
    CHECK(written.code() == DB_ERROR);
    CHECK(seq == 0);
    CHECK(written.message() == "injected flush failure");
  }

  CHECK(database->write("k", 1, &p, 1, seq).ok());
  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM k", result).ok());
  CHECK(result.m_rows.size() == 1);
  if (result.m_rows.size() == 1) {
    CHECK(result.m_rows[0][0].m_i == (rejected ? 2 : 3));
  }

  db_close(database);

  return;
}

/** Background flushes delete stored WAL segments; reopening replays nothing. */
static void test_wal_purged() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 10;
  options.m_wal_segment_size_bytes = 4096;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  run_writers(*database, 8, 100, 1, 2);
  CHECK(wait_until_flushed(*database, 5000));

  /* Hundreds of small segments were written; flushed ones are gone. */
  db_stats_t stats;
  /* Memtable retirement precedes WAL deletion; wait for deletion too. */
  for (int waited = 0; waited < 5000; waited += 10) {
    database->read_stats(stats);
    if (stats.m_wal_segment_count <= 2) {
      break;
    }

    usleep(10 * 1000);
  }

  printf("wal purged: %llu segments left after %llu background flushes\n",
         static_cast<unsigned long long>(stats.m_wal_segment_count),
         static_cast<unsigned long long>(stats.m_background_flushes));
  CHECK(stats.m_wal_segment_count <= 2);
  db_close(database);

  /* Reopening finds every write in storage and replays nothing. */
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  memtable_stats_t mt_stats;
  db_test_access::memtable_handle(*database).read_stats(mt_stats);
  const std::vector<int64_t> ids = all_ids(*database);

  CHECK(ids.size() == 800);
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == 800);
  CHECK(mt_stats.m_live_writes == 0 && mt_stats.m_flushed_seq == 800);
  database->read_stats(stats);
  CHECK(stats.m_wal_segment_count <= 2);
  db_close(database);

  return;
}

struct sampler_arg_t {
  /** Engine under test. */
  db *m_db{nullptr};

  /** Set when the thread should stop; protected by the m_mutex. */
  bool *m_stop{nullptr};

  /** Protects *stop. */
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

    memtable_stats_t stats;
    db_test_access::memtable_handle(*arg->m_db).read_stats(stats);

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
  db_options_t options;
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

  db_stats_t stats;
  database->read_stats(stats);

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

/** Writer in the kill test: report each acknowledged id. */
struct kill_writer_t {
  /** Engine under test. */
  db *m_db{nullptr};

  /** First id to write. */
  int64_t m_base{0};

  /** Pipe end the acknowledgements are written to. */
  int m_fd{-1};
};

/** Write ids from base until killed; report each acknowledged one. */
static void kill_writer_main(kill_writer_t *w) {
  for (int64_t i = 0;; ++i) {
    const int64_t id = w->m_base + i;

    if (!write_id(*w->m_db, id).ok()) {
      _exit(3);
    }

    if (write(w->m_fd, &id, sizeof(id)) != static_cast<ssize_t>(sizeof(id))) {
      _exit(4);
    }
  }

  return;
}

/** Child process: write from four threads; the background thread flushes
often and deletes small WAL segments. */
static void kill_child(const char *dir, int round, int fd) {
  db_options_t options;
  options.m_flush_interval_ms = 2;
  options.m_wal_segment_size_bytes = 8192;

  db *database = nullptr;
  if (!db_open(dir, &options, &database).ok()) {
    _exit(2);
  }

  kill_writer_t writers[4];
  std::thread threads[4];

  for (int t = 0; t < 4; ++t) {
    writers[t].m_db = database;
    writers[t].m_base = static_cast<int64_t>(round) * 100000000 + t * 1000000;
    writers[t].m_fd = fd;
    threads[t] = std::thread(kill_writer_main, &writers[t]);
  }

  threads[0].join();

  return;
}

/** Kill a process whose background thread flushes and purges the WAL: every
acknowledged id survives, none twice. */
static void test_kill_with_background() {
  test_dir_t dir;
  std::set<int64_t> acked;

  for (int round = 0; round < 8; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.m_path, round, fds[1]);
    }

    close(fds[1]);

    const int wanted = 50 + round * 25;
    int received = 0;
    int64_t id = 0;

    while (received < wanted &&
           read(fds[0], &id, sizeof(id)) == static_cast<ssize_t>(sizeof(id))) {
      acked.insert(id);
      ++received;
    }

    kill(pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    while (read(fds[0], &id, sizeof(id)) == static_cast<ssize_t>(sizeof(id))) {
      acked.insert(id);
    }

    close(fds[0]);
    CHECK(received == wanted);

    /* Every acknowledged id survives, none twice, even though segments
    were being deleted when the process died. */
    const std::vector<int64_t> ids = reopen_ids(dir);
    const std::set<int64_t> unique(ids.begin(), ids.end());
    CHECK(unique.size() == ids.size());

    std::set<int64_t>::iterator it;
    for (it = acked.begin(); it != acked.end(); ++it) {
      CHECK(unique.count(*it) == 1);
    }

    if (round == 7) {
      printf("kill test: %zu acknowledged writes, %zu recovered rows\n",
             acked.size(), ids.size());
    }
  }

  return;
}

/** Completion and fatal failure before wait registration are both observed. */
static void test_progress_before_wait() {
  for (bool fatal : {false, true}) {
    test_dir_t dir;
    db_options_t options = *deferred_flush();
    options.m_memtable_limit_bytes = 1;
    db *database = nullptr;
    CHECK(db_open(dir.m_path, &options, &database).ok());
    const ingest_field_t f = field_int("v", 1);
    const ingest_point_t p = point(ut_now_us(), nullptr, 0, &f, 1);
    batch_seq_t seq = 0;
    CHECK(database->write("m", 1, &p, 1, seq).ok());
    if (fatal) {
      db_test_access::before_space_wait(*database, [](db &d) {
        db_test_access::fail_next_flush(d);
        storage_test_access::fail_next_rollback(
            db_test_access::storage_handle(d));
        db_flush_info_t info;
        CHECK(d.flush(info).code() == DB_OUTCOME_UNKNOWN);
      });
    } else {
      db_test_access::before_space_wait(*database, [](db &d) {
        db_flush_info_t info;
        CHECK(d.flush(info).ok() && info.m_writes == 1);
      });
    }
    const db_status_t result = database->write("m", 1, &p, 1, seq);
    CHECK(fatal ? result.code() == DB_FENCED && seq == 0
                : result.ok() && seq == 2);
    db_close(database);
  }
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_interval_flush();
  test_flush_count_when_expiry_fails();
  test_expiry_after_flush_failure(false);
  test_expiry_after_flush_failure(true);
  test_unsafe_flush_stops_expiry(false);
  test_unsafe_flush_stops_expiry(true);
  test_expiry_rollback_failure();
  test_background_error();
  test_throttle_flush_failure();
  test_wal_purged();
  test_progress_before_wait();
  test_throttle();
  test_kill_with_background();

  return (test_result("db_flusher-t"));
}
