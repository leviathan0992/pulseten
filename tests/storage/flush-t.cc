/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * flush-t.cc
 *    Tests for flushing: writes move from the memtable to DuckDB, reopening
 *    replays only what is not stored, flushes run alongside writes, and
 *    injected failures of a flush commit or of opening are recovered.
 *
 * Covers: src/db/db.cc, src/store/store.cc
 *
 *-------------------------------------------------------------------------
 */

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "db.h"
#include "ingest.h"
#include "memtable.h"
#include "store.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_store.h"
#include "test_util.h"
#include "ut.h"
#include "wal.h"

/** A flush stores every visible write, and reopening replays only the writes
after F. */
static void test_flush_and_reopen() {
  test_dir_t dir;
  db *database = nullptr;

  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  for (int64_t v = 1; v <= 3; ++v) {
    CHECK(write_id(*database, v).ok());
  }

  /* Everything visible goes to storage and leaves the memtable. */
  db::flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 3 && info.m_rows == 3 && info.m_flushed_seq == 3);

  memtable::stats_t stats;
  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == 3 && stats.m_live_writes == 0);
  CHECK(stats.m_retired_write_count == 0);

  /* Nothing left: a flush with nothing to do. */
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 0 && info.m_flushed_seq == 3);

  CHECK(write_id(*database, 4).ok());
  CHECK(all_ids(*database).size() == 4);

  db_close(database);

  /* Reopening replays only the write after F. */
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == 3 && stats.m_visible_seq == 4 &&
        stats.m_live_writes == 1);

  std::vector<int64_t> ids = all_ids(*database);
  CHECK(ids.size() == 4);
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == 4);

  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 1 && info.m_flushed_seq == 4);

  db_close(database);

  return;
}

/** A WAL batch using a column prefix fills the missing columns with NULL
through recovery, flush and reopen. */
static void test_recover_short_prefix() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  ingest_field_t a[] = {field_int("a", 1)};

  ingest_point_t p1 = point(TEST_BASE_TIME_US, nullptr, 0, a, 1);
  ingest_plan_t create;
  CHECK(plan_for(nullptr, "m", &p1, 1, create).ok());

  const schema_t base = schema_after(nullptr, create);
  ingest_plan_t prefix;
  CHECK(plan_for(&base, "m", &p1, 1, prefix).ok());

  batch_seq_t seq = 0;
  CHECK(database->write("m", 1, &p1, 1, seq).ok());

  ingest_field_t b[] = {field_int("a", 2), field_double("b", 0.5)};

  ingest_point_t p2 = point(TEST_BASE_TIME_US, nullptr, 0, b, 2);
  CHECK(database->write("m", 1, &p2, 1, seq).ok());

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  db_close(database);

  /* Encode a valid current-format batch that omits the trailing column. */
  std::string payload;
  ingest_encode(prefix, payload);

  wal *log = nullptr;

  wal_recovery_t recovery;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recovery).ok());

  wal_record_t record;
  record.m_seq = recovery.m_last_seq + 1;
  record.m_payload = reinterpret_cast<const unsigned char *>(payload.data());
  record.m_payload_bytes = payload.size();
  CHECK(record.m_seq == 3 && log->append(&record, 1).ok());
  CHECK(wal::destroy(log).ok());

  for (int round = 0; round < 2; ++round) {
    CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

    test_query_t result;
    CHECK(
        query_all(*database, "SELECT count(*), count(b) FROM m", result).ok());
    CHECK(result.m_rows.size() == 1);

    if (result.m_rows.size() == 1) {
      CHECK(result.m_rows[0][0].m_integer == 3 &&
            result.m_rows[0][1].m_integer == 1);
    }

    CHECK(database->flush(info).ok());

    db_close(database);
  }

  return;
}

/** A stored column directory and additions only in the WAL are recovered
together. */
static void test_schema_across_flush() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  ingest_field_t f1[] = {field_int("a", 1)};
  ingest_field_t f2[] = {field_int("a", 2), field_double("b", 0.5)};

  ingest_point_t p1 = point(TEST_BASE_TIME_US, nullptr, 0, f1, 1);
  ingest_point_t p2 = point(TEST_BASE_TIME_US, nullptr, 0, f2, 2);
  batch_seq_t seq = 0;

  /* The initial columns are stored; the new column only lives in the WAL. */
  CHECK(database->write("m", 1, &p1, 1, seq).ok());

  db::flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(database->write("m", 1, &p2, 1, seq).ok());

  db_close(database);

  /* Replay appends the WAL column to the stored directory, and later writes
  plan against that directory. */
  memtable::stats_t stats;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == 1 && stats.m_visible_seq == 2 &&
        stats.m_live_writes == 1);
  CHECK(database->write("m", 1, &p1, 1, seq).ok());
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 2);

  std::vector<schema_ref_t> schemas;
  database->collect_schemas(schemas);
  CHECK(schemas.size() == 1 && schemas[0]->m_columns.size() == 3);

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM m WHERE b IS NULL", result)
            .ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 2);

  db_close(database);

  /* Check the latest stored column directory after releasing the database. */
  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, STORE_DEFAULT_PARTITION_HOURS, &st).ok());

  const store_catalog_t &state = st->catalog();
  const store_table_t &m = state.m_tables.find("m")->second;
  CHECK(m.m_schema.m_columns.size() == 3);

  int64_t nulls = -1;
  CHECK(test_store_query_int(*st,
                             "SELECT count(*) FROM " +
                                 m.m_partitions.begin()->second.m_table_name +
                                 " WHERE c2 IS NULL",
                             nulls)
            .ok());
  CHECK(nulls == 2);

  store::destroy(st);

  return;
}

/** A rolled-back flush fences the database; reopening recovers the write. */
static void test_flush_failure() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  CHECK(write_id(*database, 1).ok());

  /* Rollback preserves the batch, but the instance cannot continue. */
  db::flush_info_t info;
  ut_debug_global().arm("store.append", DB_ERROR);

  const db_status_t status = database->flush(info);
  CHECK(status.code() == DB_ERROR);
  CHECK(!status.message().empty() && info.m_writes == 0);

  memtable::stats_t stats;
  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == 0 && stats.m_live_writes == 1);

  CHECK(write_id(*database, 2).code() == DB_FENCED);

  const db_status_t fenced = database->flush(info);
  CHECK(fenced.code() == DB_FENCED);
  CHECK(fenced.message() == status.message());

  db_close(database);
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 1);

  db_close(database);

  return;
}

/** A failed COMMIT has an unknown outcome: the database is fenced, and
reopening recovers every write exactly once from the WAL. */
static void test_flush_commit_failure() {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  CHECK(write_id(*database, 1).ok());
  CHECK(write_id(*database, 2).ok());

  db::flush_info_t info;
  ut_debug_global().arm("store.commit", DB_IO_ERROR);
  CHECK_EQ(database->flush(info).code(), DB_OUTCOME_UNKNOWN);
  CHECK_EQ(info.m_writes, uint64_t{0});
  CHECK_EQ(write_id(*database, 3).code(), DB_FENCED);

  db_close(database);

  CHECK(reopen_ids(dir) == std::vector<int64_t>({1, 2}));

  return;
}

/** Configured failure points fail opening: creating the WAL of a
new database, or committing its first catalog. The next open succeeds. */
static void test_open_failure(const char *point) {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  ut_debug_global().arm(point, DB_IO_ERROR);

  db *database = nullptr;
  const db_status_t failed = db_open(dir.m_path, &options, &database);
  CHECK(!failed.ok());
  CHECK(database == nullptr);
  CHECK_EQ(ut_debug_global().failures(point), uint64_t{1});

  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_id(*database, 1).ok());

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  db_close(database);

  CHECK(reopen_ids(dir) == std::vector<int64_t>({1}));

  return;
}

/** OOM can retry only when rollback confirms that nothing was committed. */
static void test_flush_oom(bool fail_rollback) {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  CHECK(write_id(*database, 1).ok());
  ut_debug_global().arm("store.append", DB_OUT_OF_MEMORY, -1);

  if (fail_rollback) {
    ut_debug_global().arm("store.rollback", DB_ERROR);
  }

  db::flush_info_t info;
  const db_status_t failed = database->flush(info);
  CHECK(failed.code() ==
        (fail_rollback ? DB_OUTCOME_UNKNOWN : DB_OUT_OF_MEMORY));
  CHECK(info.m_writes == 0);

  memtable::stats_t stats;
  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == 0 && stats.m_live_writes == 1);

  if (fail_rollback) {
    CHECK(write_id(*database, 2).code() == DB_FENCED);
    CHECK(database->flush(info).code() == DB_FENCED);

    db_close(database);

    return;
  }

  test_query_t result;
  CHECK(query_all(*database, "SELECT id FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 1);
  CHECK(write_id(*database, 2).ok());
  ut_debug_global().disarm("store.append");
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 2 && info.m_flushed_seq == 2);

  db_close(database);

  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  if (database != nullptr) {
    CHECK(query_all(*database, "SELECT id FROM k ORDER BY id", result).ok());
    CHECK(result.m_rows.size() == 2);

    if (result.m_rows.size() == 2) {
      CHECK(result.m_rows[0][0].m_integer == 1);
      CHECK(result.m_rows[1][0].m_integer == 2);
    }

    db_close(database);
  }

  return;
}

/** A native DuckDB budget failure recovers once SQL restores room. */
static void test_flush_memory_limit() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_duckdb_memory_limit_bytes = 64 * 1024 * 1024;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_id(*database, 1).ok());

  const db_status_t limited = database->exec("SET memory_limit = '1MiB'");
  CHECK(limited.ok());

  if (!limited.ok()) {
    fprintf(stderr, "cannot lower memory limit: %s\n",
            limited.message().c_str());
  }

  db::flush_info_t info;
  const db_status_t failed = database->flush(info);
  CHECK(failed.code() == DB_OUT_OF_MEMORY);
  CHECK(info.m_writes == 0);
  CHECK(database->exec("SET memory_limit = '64MiB'").ok());
  CHECK(write_id(*database, 2).ok());
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 2);

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM k", result).ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_integer == 2);

  db_close(database);

  return;
}

/** WAL synchronization failure still fences the instance before storage. */
static void test_flush_wal_sync_failure() {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  CHECK(write_id(*database, 1).ok());
  ut_debug_global().arm("wal.sync", DB_IO_ERROR);

  db::flush_info_t info;
  CHECK(!database->flush(info).ok());
  CHECK(info.m_writes == 0);
  CHECK(write_id(*database, 2).code() == DB_FENCED);

  db_close(database);

  return;
}

/** A flush syncs the WAL even with WAL sync on: writes recovered from an
earlier unsynced open must be durable before storage holds them. */
static void test_flush_syncs_recovered_writes() {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  CHECK(write_id(*database, 1).ok());
  db_close(database);

  db::options_t options = *deferred_flush_options();
  options.m_wal_sync = true;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  /* The failed sync stops the flush before it reaches storage. */
  ut_debug_global().arm("wal.sync", DB_IO_ERROR);

  db::flush_info_t info;
  CHECK(database->flush(info).code() == DB_IO_ERROR);
  CHECK(info.m_writes == 0);
  CHECK(ut_debug_global().failures("wal.sync") == 1);

  db_close(database);

  /* Reopening recovers the write, and flushing it then succeeds. */
  REQUIRE(db_open(dir.m_path, &options, &database).ok());
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 1);
  CHECK(all_ids(*database) == std::vector<int64_t>{1});

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

  /** Calls that failed. */
  int m_errors{0};
};

/** Write count ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  for (int i = 0; i < arg->m_write_count; ++i) {
    if (!write_id(*arg->m_db, arg->m_thread_index * 100000 + i).ok()) {
      arg->m_errors++;
    }
  }

  return;
}

struct flusher_arg_t {
  /** Database under test. */
  db *m_db{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *m_stop{nullptr};

  /** Protects *m_stop. */
  std::mutex *m_stop_mutex{nullptr};

  /** Flushes that stored something. */
  int m_flushes{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Flush until told to stop, counting flushes and failures.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void flusher_main(flusher_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->m_stop_mutex);

      if (*arg->m_stop) {
        break;
      }
    }

    db::flush_info_t info;
    if (!arg->m_db->flush(info).ok()) {
      arg->m_errors++;
    } else if (info.m_writes > 0) {
      arg->m_flushes++;
    }

    usleep(2000);
  }

  return;
}

/** Flushes running alongside writers lose and duplicate nothing. */
static void test_flush_during_writes() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  bool stop = false;
  std::mutex stop_mutex;
  flusher_arg_t flusher;
  flusher.m_db = database;
  flusher.m_stop = &stop;
  flusher.m_stop_mutex = &stop_mutex;

  std::thread flusher_thread(flusher_main, &flusher);

  const int threads = 4;
  const int count = 150;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_db = database;
    args[t].m_thread_index = t;
    args[t].m_write_count = count;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
    CHECK(args[t].m_errors == 0);
  }

  {
    std::lock_guard<std::mutex> lock(stop_mutex);
    stop = true;
  }

  flusher_thread.join();
  CHECK(flusher.m_errors == 0);

  printf("flush during writes: %d flushes\n", flusher.m_flushes);

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  /* Every write is stored exactly once. */
  std::vector<int64_t> ids = all_ids(*database);
  CHECK(ids.size() == static_cast<size_t>(threads * count));
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == ids.size());

  db_close(database);

  memtable::stats_t stats;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == static_cast<batch_seq_t>(threads * count) &&
        stats.m_live_writes == 0);
  CHECK(all_ids(*database).size() == static_cast<size_t>(threads * count));

  db_close(database);

  return;
}

/** Cleanup failure does not undo a committed flush; a later pass retries it. */
static void test_cleanup_failure() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_wal_file_size_bytes = 1;

  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  const ingest_field_t f = field_int("v", 1);
  const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &f, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("m", 1, &p, 1, seq).ok());
  CHECK(database->write("m", 1, &p, 1, seq).ok());

  const std::string path = std::string(dir.m_path) + "/" + wal_file_name(1);
  const std::string held = path + ".held";
  CHECK(rename(path.c_str(), held.c_str()) == 0);
  CHECK(mkdir(path.c_str(), 0700) == 0);

  db::flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(info.m_flushed_seq == 2 && info.m_writes == 2);
  CHECK(db_test_access::purge_wal(*database).code() == DB_IO_ERROR);
  CHECK(rmdir(path.c_str()) == 0);
  CHECK(rename(held.c_str(), path.c_str()) == 0);
  CHECK(db_test_access::purge_wal(*database).ok());
  CHECK(access(path.c_str(), F_OK) != 0);
  CHECK(database->write("m", 1, &p, 1, seq).ok() && seq == 3);

  db_close(database);
}

/** Run flush ordering, snapshot and failure tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"cleanup_failure", test_cleanup_failure},
      {"flush_and_reopen", test_flush_and_reopen},
      {"schema_across_flush", test_schema_across_flush},
      {"recover_short_prefix", test_recover_short_prefix},
      {"flush_failure", test_flush_failure},
      {"flush_commit_failure", test_flush_commit_failure},
      {"open_failure/wal_write_header",
       [] { test_open_failure("wal.write_header"); }},
      {"open_failure/store_commit", [] { test_open_failure("store.commit"); }},
      {"flush_oom/clean_rollback", [] { test_flush_oom(false); }},
      {"flush_oom/failed_rollback", [] { test_flush_oom(true); }},
      {"flush_memory_limit", test_flush_memory_limit},
      {"flush_wal_sync_failure", test_flush_wal_sync_failure},
      {"flush_syncs_recovered_writes", test_flush_syncs_recovered_writes},
      {"flush_during_writes", test_flush_during_writes},
  };

  return (test_main("flush-t", argc, argv, cases));
}
