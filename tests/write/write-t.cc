/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * write-t.cc
 *    Tests for the write path: writes become visible and survive reopening,
 *    concurrent schema changes agree, and failures stop new work.
 *
 * Covers: src/db/db.cc
 *
 *-------------------------------------------------------------------------
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sys/types.h>
#include <unistd.h>

#include "db.h"
#include "db_schema.h"
#include "ingest.h"
#include "memtable.h"
#include "os_file.h"
#include "pt_duckdb.h"
#include "store.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_memtable.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "wal.h"

/** Format a query value for comparing states.
@return formatted value text, or "NULL" */
static std::string value_text(const query_value_t &value, data_type_t type) {
  if (value.m_is_null) {
    return ("NULL");
  }

  char buf[64];

  switch (type) {
    case data_type_t::BOOLEAN:
      return (value.m_boolean ? "true" : "false");

    case data_type_t::BIGINT:
    case data_type_t::TIMESTAMP:
      snprintf(buf, sizeof(buf), "%lld",
               static_cast<long long>(value.m_integer));

      return (buf);

    case data_type_t::DOUBLE:
      snprintf(buf, sizeof(buf), "%.17g", value.m_double);

      return (buf);

    case data_type_t::VARCHAR:
      return ("'" + value.m_string + "'");
  }

  return ("?");
}

/** Query schemas and rows in a stable order, for comparing recovered states.
@return a deterministic description of visible schemas and rows */
static std::string dump(db &database) {
  memtable::stats_t stats;
  db_test_access::memtable_handle(database).stats(stats);

  std::vector<schema_ref_t> schemas;
  db_test_access::collect_schemas(database, schemas);

  std::string out = "visible " + std::to_string(stats.m_visible_seq) + "\n";

  for (const schema_ref_t &reference : schemas) {
    const schema_t &schema = *reference;

    out += "table " + schema.m_name;

    for (const schema_column_t &col : schema.m_columns) {
      out += " " + col.m_name;
    }

    out += "\n";

    test_query_t result;
    CHECK(query_all(database,
                    "SELECT * FROM \"" + schema.m_name + "\" ORDER BY ALL",
                    result)
              .ok());

    for (const std::vector<query_value_t> &row : result.m_rows) {
      for (size_t c = 0; c < result.m_columns.size(); ++c) {
        out += value_text(row[c], result.m_columns[c].m_data_type) + " ";
      }

      out += "\n";
    }
  }

  return (out);
}

/** Write one point with the given tags and fields; checks success.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@param[in]  tags     input tags; may be NULL when n_tags is 0
@return the visible sequence after the write; a failed write records a test
  failure and returns 0 */
static batch_seq_t write_one(db &database, const char *table_name,
                             int64_t time_us, const ingest_tag_t *tags,
                             uint32_t n_tags, const ingest_field_t *fields,
                             uint32_t n_fields) {
  ingest_point_t p = point(time_us, tags, n_tags, fields, n_fields);
  const db_status_t err = database.write(table_name, strlen(table_name), &p, 1);
  CHECK(err.ok());

  if (!err.ok()) {
    fprintf(stderr, "write failed: %s\n", err.message().c_str());

    return (0);
  }

  db::stats_t stats;
  database.stats(stats);

  return (stats.m_visible_seq);
}

/** Scope exit and repeated explicit close both release the directory lock. */
static void test_object_lifetime() {
  test_dir_t dir;
  const db::options_t options = *deferred_flush_options();

  for (int explicit_close = 0; explicit_close < 2; ++explicit_close) {
    {
      db database;
      CHECK(database.open(dir.m_path, options).ok());

      const ingest_field_t field = field_int("v", explicit_close);
      const ingest_point_t p =
          point(TEST_BASE_TIME_US + explicit_close, nullptr, 0, &field, 1);

      CHECK(database.write("scoped", 6, &p, 1).ok());

      if (explicit_close != 0) {
        database.close();
        database.close();
      }
    }

    db *reopened = nullptr;
    CHECK(db_open(dir.m_path, &options, &reopened).ok());

    test_query_t result;
    CHECK(query_all(*reopened, "SELECT count(*) FROM scoped", result).ok());
    CHECK(result.m_rows.size() == 1);

    if (result.m_rows.size() == 1) {
      CHECK(result.m_rows[0][0].m_integer == explicit_close + 1);
    }

    db_close(reopened);
  }

  return;
}

/** Writes are visible at once, and reopening replays the WAL into the same
state and sequence numbers. */
static void test_write_and_reopen() {
  test_dir_t dir;
  db *database = nullptr;

  memtable::stats_t stats;

  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_visible_seq == 0 && stats.m_live_writes == 0);

  /* Default writes do not execute the WAL group sync. */
  ut_debug_global().arm("wal.sync", DB_IO_ERROR);

  ingest_tag_t host_a[] = {tag("host", "a")};
  ingest_tag_t host_b[] = {tag("host", "b")};
  ingest_field_t cpu1[] = {field_double("usage", 0.25)};
  ingest_field_t cpu2[] = {field_double("usage", 0.5),
                           field_str("note", "\xE6\xB8\xA9")};

  ingest_field_t mem[] = {field_int("used", 1024), field_bool("ok", true)};

  CHECK(write_one(*database, "cpu", TEST_BASE_TIME_US, host_a, 1, cpu1, 1) ==
        1);
  CHECK(write_one(*database, "CPU", TEST_BASE_TIME_US + 1, host_b, 1, cpu2,
                  2) == 2);
  CHECK(write_one(*database, "mem", TEST_BASE_TIME_US, nullptr, 0, mem, 2) ==
        3);

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_visible_seq == 3);

  std::vector<schema_ref_t> schemas;
  db_test_access::collect_schemas(*database, schemas);
  CHECK(schemas.size() == 2 && schemas[0]->m_name == "cpu" &&
        schemas[0]->m_columns.size() == 4);

  const std::string before = dump(*database);

  ut_debug_global().disarm("wal.sync");
  db_close(database);

  /* Reopening replays the WAL into the same state. */
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_visible_seq == 3 && stats.m_live_writes == 3);
  CHECK(dump(*database) == before);

  /* And writing continues after the recovered sequence numbers. */
  CHECK(write_one(*database, "mem", TEST_BASE_TIME_US + 1, nullptr, 0, mem,
                  2) == 4);

  db_close(database);

  return;
}

/** A failed checkpoint sync must not advance durable storage past the WAL. */
static void test_unsynced_flush_failure() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  ingest_field_t fields[] = {field_int("v", 1)};
  CHECK(write_one(*database, "m", TEST_BASE_TIME_US, nullptr, 0, fields, 1) ==
        1);

  const std::string path = dir.file(wal_file_name(1).c_str());
  const std::string hidden = path + ".hidden";
  CHECK(rename(path.c_str(), hidden.c_str()) == 0);

  db::flush_info_t info;
  const db_status_t err = database->flush(info);
  CHECK(err.code() == DB_IO_ERROR);
  CHECK(err.message().find("cannot sync WAL before flush") == 0);

  memtable::stats_t stats;
  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == 0 && stats.m_visible_seq == 1 &&
        stats.m_live_writes == 1);

  CHECK(rename(hidden.c_str(), path.c_str()) == 0);

  const db_status_t fenced = database->flush(info);
  CHECK(fenced.code() == DB_FENCED && fenced.message() == err.message());

  db_close(database);
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  CHECK(database->flush(info).ok());
  CHECK(info.m_rows == 1);

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_flushed_seq == 1 && stats.m_visible_seq == 1);

  db_close(database);

  return;
}

/** Conflicting and invalid writes are rejected with a reason and use no
sequence number. */
static void test_rejected_writes() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  ingest_field_t num[] = {field_int("v", 1)};
  ingest_field_t text[] = {field_str("v", "x")};
  ingest_field_t reserved[] = {field_int("time", 1)};

  CHECK(write_one(*database, "m", TEST_BASE_TIME_US, nullptr, 0, num, 1) == 1);

  /* A conflict and an invalid name: rejected with a reason, no sequence
  number used. */
  const ingest_field_t *bad[] = {text, reserved};
  const dberr_t expected[] = {DB_SCHEMA_CONFLICT, DB_INVALID_ARGUMENT};

  for (int i = 0; i < 2; ++i) {
    ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, bad[i], 1);

    status = database->write("m", 1, &p, 1);
    CHECK(status.code() == expected[i]);
    CHECK(!status.message().empty());
  }

  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, num, 1);
  CHECK(database->write("bad name", 8, &p, 1).code() == DB_INVALID_ARGUMENT);
  CHECK(database->write("_pt_meta", 8, &p, 1).code() == DB_INVALID_ARGUMENT);

  CHECK(write_one(*database, "m", TEST_BASE_TIME_US, nullptr, 0, num, 1) == 2);

  db_close(database);

  memtable::stats_t stats;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_visible_seq == 2 && stats.m_live_writes == 2);

  db_close(database);

  return;
}

/** A second open of the same directory is refused. */
static void test_second_open() {
  test_dir_t dir;
  db *first = nullptr;
  db *second = nullptr;

  CHECK(db_open(dir.m_path, deferred_flush_options(), &first).ok());
  CHECK(db_open(dir.m_path, deferred_flush_options(), &second).code() ==
        DB_LOCKED);
  CHECK(second == nullptr);

  db_close(first);

  return;
}

/** Invalid arguments are rejected before opening resources; failures after
locking release the directory so a later open can succeed. */
static void test_open_failure_cleanup() {
  db_status_t status;

  test_dir_t dir;
  db *database = nullptr;

  db::options_t options;

  status = db_open(nullptr, &options, &database);
  CHECK(status.code() == DB_INVALID_ARGUMENT && database == nullptr);

  status = db_open("", &options, &database);
  CHECK(status.code() == DB_INVALID_ARGUMENT && database == nullptr);

  options.m_flush_interval_ms = 0;
  status = db_open(dir.m_path, &options, &database);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(database == nullptr);
  CHECK(status.message() ==
        "background flush interval must be greater than zero");
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_retention_hours = STORE_MAX_RETENTION_HOURS + 1;
  status = db_open(dir.m_path, &options, &database);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(database == nullptr);
  CHECK(status.message().find("retention period") != std::string::npos);
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  /* Partition options are rejected before taking the directory lock too. */
  options.m_retention_hours = 0;
  options.m_partition_duration_hours = 0;
  status = db_open(dir.m_path, &options, &database);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(database == nullptr && !status.message().empty());
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  options.m_partition_duration_hours = STORE_DEFAULT_PARTITION_HOURS;
  status = db_open(dir.m_path, &options, &database);
  CHECK(status.ok());
  CHECK(database != nullptr && status.message().empty());

  db_close(database);

  /* A bad WAL fails recovery after storage and the memtable are open. */
  const std::string log = dir.file(wal_file_name(1).c_str());
  const std::string saved_wal = dir.file("saved-wal");
  CHECK(rename(log.c_str(), saved_wal.c_str()) == 0);

  FILE *empty_wal = fopen(log.c_str(), "wb");
  CHECK(empty_wal != nullptr);

  if (empty_wal != nullptr) {
    CHECK(fclose(empty_wal) == 0);
  }

  status = db_open(dir.m_path, &options, &database);
  CHECK(status.code() == DB_CORRUPTION);
  CHECK(database == nullptr && !status.message().empty());

  CHECK(unlink(log.c_str()) == 0);
  CHECK(rename(saved_wal.c_str(), log.c_str()) == 0);

  status = db_open(dir.m_path, &options, &database);
  CHECK(status.ok());
  CHECK(database != nullptr && status.message().empty());

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

/** Each write has 10 points. Every 20th write adds a field whose name all
threads share, so threads race to add the same column. Odd threads also write
to a table of their own.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  char shared[32];
  char own[32];
  snprintf(own, sizeof(own), "own_%d", arg->m_thread_index);

  for (int i = 0; i < arg->m_write_count; ++i) {
    snprintf(shared, sizeof(shared), "g%d", i / 20);

    ingest_field_t f[] = {field_int("n", i), field_double(shared, 1.5)};
    ingest_tag_t t[] = {tag("thread", own)};

    std::vector<ingest_point_t> points;

    for (int k = 0; k < 10; ++k) {
      points.push_back(point(TEST_BASE_TIME_US + k, t, 1, f, 2));
    }

    const char *table_name =
        (arg->m_thread_index % 2 == 1 && i % 2 == 1) ? own : "m";

    if (!arg->m_db->write(table_name, strlen(table_name), &points[0], 10)
             .ok()) {
      arg->m_errors++;
    }
  }

  return;
}

struct reader_arg_t {
  /** Database under test. */
  db *m_db{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *m_stop{nullptr};

  /** Protects *m_stop. */
  std::mutex *m_stop_mutex{nullptr};

  /** States inspected. */
  int m_reads{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Check counters while writes run; visibility must only grow. Debug builds
also check that snapshots hold exactly one batch per visible write.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void reader_main(reader_arg_t *arg) {
  batch_seq_t last = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->m_stop_mutex);

      if (*arg->m_stop) {
        break;
      }
    }

    batch_seq_t visible = 0;
    memtable::snapshot_t snapshot;
    snapshot_all(db_test_access::memtable_handle(*arg->m_db), snapshot);

    size_t batches = 0;

    for (const schema_ref_t &schema : snapshot_schemas(
             snapshot, db_test_access::tables_handle(*arg->m_db))) {
      batches += snapshot_batches(snapshot, schema->m_name).size();
    }

    if (batches != snapshot.m_visible_seq) {
      arg->m_errors++;
    }

    visible = snapshot.m_visible_seq;
    (db_test_access::memtable_handle(*arg->m_db)).release_snapshot(snapshot);

    if (visible < last) {
      arg->m_errors++;
    }

    last = visible;
    arg->m_reads++;
  }

  return;
}

/** WAL-only table names are checked against DuckDB after replay. */
static void test_replayed_name_collision() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  ingest_field_t fields[] = {field_int("v", 1)};
  CHECK(write_one(*database, "mixed", TEST_BASE_TIME_US, nullptr, 0, fields,
                  1) == 1);

  db_close(database);

  store *st = nullptr;
  CHECK(store::open(dir.m_path, 24, &st).ok());
  CHECK(pt_duckdb_exec(st->connection(), "CREATE TABLE MiXeD (v BIGINT)").ok());

  store::destroy(st);

  const db_status_t err =
      db_open(dir.m_path, deferred_flush_options(), &database);
  CHECK(err.code() == DB_CORRUPTION);
  CHECK(database == nullptr);

  return;
}

/** Threads writing at once, some racing to add the same columns, lose
nothing. */
static void test_concurrent_writes() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  const int threads = 8;
  const int count = 100;

  bool stop = false;
  std::mutex stop_mutex;
  reader_arg_t reader;
  reader.m_db = database;
  reader.m_stop = &stop;
  reader.m_stop_mutex = &stop_mutex;

  std::thread reader_thread(reader_main, &reader);

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

  reader_thread.join();
  CHECK(reader.m_errors == 0);

  printf("concurrent: %d states inspected during writes\n", reader.m_reads);

  /* Table m: time, thread, n, and g0..g4 added in races. */
  memtable::stats_t stats;
  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_visible_seq == static_cast<batch_seq_t>(threads * count));

  std::vector<schema_ref_t> schemas;
  db_test_access::collect_schemas(*database, schemas);

  bool found = false;

  for (const schema_ref_t &reference : schemas) {
    const schema_t &schema = *reference;

    if (schema.m_name == "m") {
      found = true;
      CHECK(schema.m_columns.size() == 3 + count / 20);
    }
  }

  CHECK(found);

  const std::string before = dump(*database);

  db_close(database);

  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  CHECK(dump(*database) == before);

  db_close(database);

  return;
}

/** A failed WAL sync stops new work; what was visible stays, and
reopening recovers. */
static void test_write_failure() {
  test_dir_t dir;
  db *database = nullptr;

  db::options_t options = *deferred_flush_options();
  options.m_wal_sync = true;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  ingest_field_t f[] = {field_int("v", 1)};
  CHECK(write_one(*database, "m", TEST_BASE_TIME_US, nullptr, 0, f, 1) == 1);

  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, f, 1);

  ut_debug_global().arm("wal.sync", DB_IO_ERROR);
  CHECK(database->write("m", 1, &p, 1).code() == DB_OUTCOME_UNKNOWN);

  const db_status_t reason_status = database->write("m", 1, &p, 1);
  CHECK(reason_status.code() == DB_FENCED);

  /* Every rejected operation retains the original failure diagnostic. */
  const std::string &reason = reason_status.message();
  CHECK(reason.find("cannot sync WAL file") == 0);

  db_cursor *cursor = nullptr;

  const db_status_t query =
      database->open_cursor("SELECT v FROM m", false, &cursor);
  CHECK(query.code() == DB_FENCED && query.message() == reason);
  CHECK(cursor == nullptr);

  db_cursor::destroy(cursor);

  /* Queries without table locks still pass through snapshot admission. */
  const db_status_t constant_query =
      database->open_cursor("SELECT 1", false, &cursor);
  CHECK(constant_query.code() == DB_FENCED &&
        constant_query.message() == reason);
  CHECK(cursor == nullptr);

  db_cursor::destroy(cursor);

  db::flush_info_t info;
  const db_status_t flush = database->flush(info);
  CHECK(flush.code() == DB_FENCED && flush.message() == reason);
  CHECK(info.m_writes == 0);

  /* What was visible before stays visible. */
  memtable::stats_t stats;
  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_visible_seq == 1);

  db_close(database);

  /* Reopening recovers; the unknown write may or may not be there. */
  CHECK(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  db_test_access::memtable_handle(*database).stats(stats);
  CHECK(stats.m_visible_seq >= 1 && stats.m_visible_seq <= 2);
  CHECK(stats.m_live_writes == stats.m_visible_seq);
  CHECK(write_one(*database, "m", TEST_BASE_TIME_US, nullptr, 0, f, 1) ==
        stats.m_visible_seq + 1);

  db_close(database);

  return;
}

/** Selected WAL payloads remain valid while additional writers queue. */
struct write_gate_t {
  /** A leader has selected its group and released the write lock. */
  ut_event_t m_entered;

  /** The test permits WAL I/O to continue. */
  ut_event_t m_release;
};

/** Pause the active group without holding the database write lock. */
static void pause_write(write_gate_t &gate) {
  gate.m_entered.set();
  gate.m_release.wait();

  return;
}

/** A first write reserves its name until publication; waiters can cancel. */
static void test_sql_creation_race() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_memtable_limit_bytes = 1;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_id(*database, 1).ok());

  write_gate_t paused;
  ut_debug_global().set(
      "db.before_space_wait",
      [&paused] {
        paused.m_entered.set();
        paused.m_release.wait();
      },
      1);

  db_status_t write_status;

  std::thread writer([&] {
    const ingest_field_t field = field_int("v", 2);
    const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
    write_status = database->write("fresh", 5, &p, 1);
  });

  CHECK(paused.m_entered.wait_for(std::chrono::seconds(5)));

  query_cancel cancel;
  db_status_t cancelled;

  std::thread waiter([&] {
    cancelled = database->exec("CREATE TABLE fresh (v BIGINT)", &cancel);
  });
  cancel.request();
  waiter.join();
  CHECK(cancelled.code() == DB_INTERRUPTED);

  db_status_t ddl_status;
  ut_event_t started;
  ut_event_t finished;

  std::thread ddl([&] {
    started.set();
    ddl_status = database->exec("CREATE TABLE fresh (v BIGINT)");
    finished.set();
  });
  started.wait();
  CHECK(!finished.wait_for(std::chrono::milliseconds(50)));

  db::flush_info_t info;
  CHECK(database->flush(info).ok());
  paused.m_release.set();
  writer.join();
  ddl.join();
  ut_debug_global().clear("db.before_space_wait");
  CHECK(write_status.ok());
  CHECK(ddl_status.code() == DB_INVALID_ARGUMENT);

  db_close(database);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  if (database != nullptr) {
    test_query_t rows;
    CHECK(query_all(*database, "SELECT v FROM fresh", rows).ok());
    CHECK(rows.m_rows.size() == 1 && rows.m_rows[0][0].m_integer == 2);

    db_close(database);
  }
}

/** A fault introduced while the first selected group is paused. */
enum class write_failure_t {
  /** Every queued group succeeds. */
  NONE,

  /** The active WAL sync fails. */
  WAL,

  /** A concurrent storage rollback fails before the WAL call returns. */
  STORAGE,

  /** A later WAL failure must preserve the earlier storage failure. */
  STORAGE_AND_WAL
};

/** Database leaders publish in order, preserve the first cause and keep active
payloads alive while rejecting later queued work.
@param[in] failure fault injected after all followers have queued */
static void test_queued_writes(write_failure_t failure) {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_wal_sync = true;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  const ingest_field_t field = field_int("v", 1);
  const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  CHECK(database->write("m", 1, &p, 1).ok());

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT v FROM m", false, &cursor).ok());

  write_gate_t gate;
  ut_debug_global().set("db.before_wal", [&gate] { pause_write(gate); });

  const size_t count = 5;
  std::vector<db_status_t> statuses(count, DB_ERROR);
  std::vector<std::thread> writers;
  std::atomic<size_t> finished{0};

  auto write = [&](size_t i) {
    statuses[i] = database->write("m", 1, &p, 1);
    ++finished;
  };

  writers.emplace_back(write, 0);

  CHECK(gate.m_entered.wait_for(std::chrono::seconds(5)));

  for (size_t i = 1; i < count; ++i) {
    writers.emplace_back(write, i);
  }

  memtable::stats_t pending;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);

  do {
    db_test_access::memtable_handle(*database).stats(pending);

    if (pending.m_live_writes == count + 1) {
      break;
    }

    std::this_thread::yield();
  } while (std::chrono::steady_clock::now() < deadline);
  CHECK(pending.m_live_writes == count + 1 && pending.m_visible_seq == 1);
  CHECK(finished.load() == 0);

  db_status_t store_failure;

  if (failure == write_failure_t::WAL) {
    ut_debug_global().arm("wal.sync", DB_IO_ERROR);
  } else if (failure == write_failure_t::STORAGE ||
             failure == write_failure_t::STORAGE_AND_WAL) {
    ut_debug_global().arm("store.rollback", DB_ERROR);
    ut_debug_global().arm("store.append", DB_ERROR);

    db::flush_info_t info;
    store_failure = database->flush(info);
    CHECK(store_failure.code() == DB_OUTCOME_UNKNOWN);
    CHECK(finished.load() == 0);

    if (failure == write_failure_t::STORAGE_AND_WAL) {
      ut_debug_global().arm("wal.sync", DB_IO_ERROR);
    }
  }

  gate.m_release.set();

  for (std::thread &writer : writers) {
    writer.join();
  }

  ut_debug_global().clear("db.before_wal");
  CHECK(finished.load() == count);

  if (failure == write_failure_t::NONE) {
    for (size_t i = 0; i < count; ++i) {
      CHECK(statuses[i].ok());
    }

    db::stats_t stats;
    database->stats(stats);

    wal::stats_t log_stats;
    db_test_access::wal_stats(*database, log_stats);
    CHECK(log_stats.m_records == count + 1);
    CHECK(log_stats.m_groups == 3);
    CHECK(stats.m_visible_seq == count + 1);
  } else {
    CHECK(statuses[0].code() == DB_OUTCOME_UNKNOWN);

    const bool wal_failed = failure == write_failure_t::WAL ||
                            failure == write_failure_t::STORAGE_AND_WAL;
    CHECK(statuses[0].message().find(wal_failed
                                         ? "cannot sync WAL file"
                                         : "rollback:") != std::string::npos);

    /* Queued requests retain the first fatal cause, even if active WAL I/O
    independently fails later with a different diagnostic. */
    const std::string cause = failure == write_failure_t::WAL
                                  ? statuses[0].message()
                                  : store_failure.message();

    for (size_t i = 1; i < count; ++i) {
      CHECK(statuses[i].code() == DB_FENCED);
      CHECK(statuses[i].message() == cause);
    }

    /* Failed queue heads must not append their reserved records to the WAL. */
    wal::stats_t log_stats;
    db_test_access::wal_stats(*database, log_stats);
    CHECK(log_stats.m_records == (wal_failed ? 1 : 2));
    CHECK(log_stats.m_groups == (wal_failed ? 1 : 2));

    db_cursor *rejected = nullptr;

    const db_status_t query =
        database->open_cursor("SELECT v FROM m", false, &rejected);
    CHECK(query.code() == DB_FENCED && query.message() == cause);
    CHECK(rejected == nullptr);

    db_cursor::destroy(rejected);

    db::flush_info_t info;
    const db_status_t flush = database->flush(info);
    CHECK(flush.code() == DB_FENCED && flush.message() == cause);
    CHECK(info.m_writes == 0);

    db_test_access::memtable_handle(*database).stats(pending);
    CHECK(pending.m_visible_seq == 1);
  }

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);

  db_cursor::destroy(cursor);

  db_close(database);

  CHECK(db_open(dir.m_path, &options, &database).ok());

  db::stats_t restored;
  database->stats(restored);

  if (failure == write_failure_t::NONE) {
    CHECK(restored.m_visible_seq == count + 1);
  } else {
    CHECK(restored.m_visible_seq >= 1 && restored.m_visible_seq <= 2);
  }

  CHECK(database->write("m", 1, &p, 1).ok());

  db_close(database);

  return;
}

/** Recovery may discard writes after F, but must never truncate below F. */
static void test_wal_tail_boundary() {
  for (const bool sync : {false, true}) {
    for (const batch_seq_t damaged : {2, 3}) {
      test_dir_t dir;
      db::options_t options = *deferred_flush_options();
      options.m_wal_sync = sync;

      db *database = nullptr;
      CHECK(db_open(dir.m_path, &options, &database).ok());
      CHECK(write_id(*database, 1).ok());
      CHECK(write_id(*database, 2).ok());

      db::flush_info_t flushed;
      CHECK(database->flush(flushed).ok());
      const batch_seq_t flushed_seq = test_flushed_seq(*database);
      CHECK(flushed_seq == 2);
      CHECK(write_id(*database, 3).ok());
      CHECK(write_id(*database, 4).ok());

      db_close(database);

      const std::string path = dir.file(wal_file_name(1).c_str());
      os_file_t file = OS_FILE_CLOSED;
      CHECK(os_file_open(path.c_str(), OS_FILE_OPEN, file).ok());

      uint64_t size = 0;
      CHECK(os_file_size(file, size).ok());

      std::vector<unsigned char> bytes(size);
      CHECK(os_file_read(file, bytes.data(), bytes.size(), 0).ok());

      size_t at = WAL_FILE_HEADER_BYTES;

      for (batch_seq_t seq = 1; seq < damaged; ++seq) {
        batch_seq_t decoded = 0;
        size_t len = 0;
        REQUIRE(at <= bytes.size() &&
                bytes.size() - at >= WAL_RECORD_HEADER_BYTES);
        const unsigned char *record = bytes.data() + at;
        REQUIRE(wal_decode_record_header(record, decoded, len));
        REQUIRE(len <= bytes.size() - at - WAL_RECORD_HEADER_BYTES);
        REQUIRE(wal_record_crc_valid(record, len));
        CHECK(decoded == seq);
        at += WAL_RECORD_HEADER_BYTES + len;
      }

      REQUIRE(at < bytes.size());
      bytes[at] ^= 1;
      CHECK(os_file_write(file, bytes.data(), bytes.size(), 0).ok());
      CHECK(os_file_sync(file).ok());
      CHECK(os_file_close(file).ok());

      const db_status_t status = db_open(dir.m_path, &options, &database);

      if (damaged <= flushed_seq) {
        CHECK(status.code() == DB_CORRUPTION);
        CHECK(status.message().find("before required sequence 2") !=
              std::string::npos);
        CHECK(database == nullptr);
        CHECK(os_file_open(path.c_str(), OS_FILE_OPEN, file).ok());

        uint64_t after_size = 0;
        CHECK(os_file_size(file, after_size).ok());
        CHECK(after_size == size);

        std::vector<unsigned char> after(after_size);
        CHECK(os_file_read(file, after.data(), after.size(), 0).ok());
        CHECK(after == bytes);
        CHECK(os_file_close(file).ok());
      } else {
        CHECK(status.ok());

        db::stats_t stats;
        database->stats(stats);
        CHECK(stats.m_flushed_seq == 2 && stats.m_visible_seq == 2);
        CHECK(all_ids(*database) == std::vector<int64_t>({1, 2}));
        CHECK(write_id(*database, 5).ok());
        database->stats(stats);
        CHECK(stats.m_visible_seq == 3);

        db_close(database);
        CHECK(db_open(dir.m_path, &options, &database).ok());
        CHECK(all_ids(*database) == std::vector<int64_t>({1, 2, 5}));

        db_close(database);
      }
    }
  }

  return;
}

/** A valid WAL checksum cannot admit an out-of-range timestamp or repair it. */
static void test_recovery_timestamp_range() {
  const int64_t times[] = {INGEST_MIN_TIME_US - 1, INGEST_MIN_TIME_US,
                           INGEST_MAX_TIME_US - 1, INGEST_MAX_TIME_US};

  for (int64_t time : times) {
    test_dir_t dir;
    const db::options_t options = *deferred_flush_options();
    db *database = nullptr;
    REQUIRE(db_open(dir.m_path, &options, &database).ok());
    db_close(database);
    database = nullptr;

    ingest_field_t field = field_int("v", 1);
    ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
    ingest_plan_t plan;
    REQUIRE(plan_for(nullptr, "time_bounds", &p, 1, plan).ok());
    plan.m_batch.m_columns[0].m_integers[0] = time;
    std::string payload;
    ingest_encode(plan, payload);

    const std::string path = dir.file(wal_file_name(1).c_str());
    os_file_t file = OS_FILE_CLOSED;
    REQUIRE(os_file_open(path.c_str(), OS_FILE_OPEN, file).ok());
    const size_t valid_bytes =
        WAL_FILE_HEADER_BYTES + WAL_RECORD_HEADER_BYTES + payload.size();
    std::vector<unsigned char> bytes(valid_bytes + 2, 0xff);
    REQUIRE(os_file_read(file, bytes.data(), WAL_FILE_HEADER_BYTES, 0).ok());
    wal_encode_record(bytes.data() + WAL_FILE_HEADER_BYTES, 1,
                      reinterpret_cast<const unsigned char *>(payload.data()),
                      payload.size());
    REQUIRE(os_file_write(file, bytes.data(), bytes.size(), 0).ok());
    REQUIRE(os_file_sync(file).ok());
    REQUIRE(os_file_close(file).ok());

    const bool valid = time >= INGEST_MIN_TIME_US && time < INGEST_MAX_TIME_US;
    for (int attempt = 0; attempt < 2; ++attempt) {
      const db_status_t status = db_open(dir.m_path, &options, &database);
      if (valid) {
        REQUIRE(status.ok());
        test_query_t result;
        REQUIRE(
            query_all(*database, "SELECT time FROM time_bounds", result).ok());
        REQUIRE(result.m_rows.size() == 1);
        CHECK_EQ(result.m_rows[0][0].m_integer, time);
        db_close(database);
        database = nullptr;
      } else {
        CHECK(status.code() == DB_CORRUPTION);
        CHECK(database == nullptr);
      }

      REQUIRE(os_file_open(path.c_str(), OS_FILE_OPEN, file).ok());
      uint64_t size = 0;
      REQUIRE(os_file_size(file, size).ok());
      CHECK_EQ(size, valid ? valid_bytes : bytes.size());
      std::vector<unsigned char> after(size);
      REQUIRE(os_file_read(file, after.data(), after.size(), 0).ok());
      CHECK(after == std::vector<unsigned char>(
                         bytes.begin(),
                         bytes.begin() + (valid ? valid_bytes : bytes.size())));
      REQUIRE(os_file_close(file).ok());
    }
  }

  return;
}

/** Run database lifecycle, write, recovery and concurrency tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"sql_creation_race", test_sql_creation_race},
      {"object_lifetime", test_object_lifetime},
      {"write_and_reopen", test_write_and_reopen},
      {"wal_tail_boundary", test_wal_tail_boundary},
      {"recovery_timestamp_range", test_recovery_timestamp_range},
      {"unsynced_flush_failure", test_unsynced_flush_failure},

      {"rejected_writes", test_rejected_writes},
      {"second_open", test_second_open},
      {"open_failure_cleanup", test_open_failure_cleanup},
      {"replayed_name_collision", test_replayed_name_collision},

      {"concurrent_writes", test_concurrent_writes},

      {"write_failure", test_write_failure},
      {"queued_writes/none", [] { test_queued_writes(write_failure_t::NONE); }},
      {"queued_writes/wal", [] { test_queued_writes(write_failure_t::WAL); }},
      {"queued_writes/store",
       [] { test_queued_writes(write_failure_t::STORAGE); }},
      {"queued_writes/store_and_wal",
       [] { test_queued_writes(write_failure_t::STORAGE_AND_WAL); }},

  };

  return (test_main("write-t", argc, argv, cases));
}
