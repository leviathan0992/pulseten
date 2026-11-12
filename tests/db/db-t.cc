/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db-t.cc
 *    Tests for the write path: writes become visible, survive reopening
 *    and SIGKILL, concurrent schema changes agree, and failures stop new work.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <signal.h>
#include <string>
#include <thread>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "batch.h"
#include "duckdb_database.h"
#include "ingest.h"
#include "memtable.h"
#include "schema.h"
#include "storage.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_memtable.h"
#include "test_point.h"
#include "test_storage_access.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "wal_format.h"

/** A long background interval lets tests control internal flush boundaries.
@return options with a long background interval and no capacity limit */
static const db_options_t *deferred_flush() {
  static db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;

  return (&options);
}

/** Format a query value for comparing states.
@return formatted value text, or "NULL" */
static std::string value_text(const query_value_t &value, col_type_t type) {
  if (value.m_is_null) {
    return ("NULL");
  }

  char buf[64];

  switch (type) {
    case col_type_t::BOOLEAN:
      return (value.m_b ? "true" : "false");
    case col_type_t::BIGINT:
    case col_type_t::TIMESTAMP:
      snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(value.m_i));

      return (buf);
    case col_type_t::DOUBLE:
      snprintf(buf, sizeof(buf), "%.17g", value.m_d);

      return (buf);
    case col_type_t::VARCHAR:
      return ("'" + value.m_s + "'");
  }

  return ("?");
}

/** Query schemas and rows in a stable order, for comparing recovered states.
@return a deterministic description of visible schemas and rows */
static std::string dump(db &database) {
  memtable_stats_t stats;
  db_test_access::memtable_handle(database).read_stats(stats);
  std::vector<schema_t> schemas;
  database.copy_schemas(schemas);

  std::string out = "visible " + std::to_string(stats.m_visible_seq) + "\n";
  for (const schema_t &schema : schemas) {
    out += "table " + schema.m_name;
    for (const col_def_t &col : schema.m_cols) {
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
        out += value_text(row[c], result.m_columns[c].m_type) + " ";
      }

      out += "\n";
    }
  }

  return (out);
}

/** Write one point with the given tags and fields; checks success.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@param[in]  tags     input tags; may be NULL when n_tags is 0
@return the assigned sequence number; a failed write is recorded as a test
  failure */
static batch_seq_t write_one(db &database, const char *table_name,
                             int64_t time_us, const ingest_tag_t *tags,
                             uint32_t n_tags, const ingest_field_t *fields,
                             uint32_t n_fields) {
  db_status_t status;

  ingest_point_t p = point(time_us, tags, n_tags, fields, n_fields);
  batch_seq_t seq = 0;

  status = database.write(table_name, strlen(table_name), &p, 1, seq);
  const db_status_t err = status;
  CHECK(err.ok());
  if (!err.ok()) {
    fprintf(stderr, "write failed: %s: %s\n", err.message().c_str(),
            status.message().c_str());
  }

  return (seq);
}

/** Scope exit and repeated explicit close both release the directory lock. */
static void test_object_lifetime() {
  test_dir_t dir;
  const db_options_t options = *deferred_flush();

  for (int explicit_close = 0; explicit_close < 2; ++explicit_close) {
    {
      db database;
      CHECK(database.open(dir.m_path, options).ok());
      const ingest_field_t field = field_int("v", explicit_close);
      const ingest_point_t p =
          point(T0 + explicit_close, nullptr, 0, &field, 1);
      batch_seq_t seq = 0;
      CHECK(database.write("scoped", 6, &p, 1, seq).ok());
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
      CHECK(result.m_rows[0][0].m_i == explicit_close + 1);
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
  memtable_stats_t stats;

  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_visible_seq == 0 && stats.m_live_writes == 0);
  /* Default writes do not execute the WAL group sync. */
  db_test_access::fail_next_wal_sync(*database);

  ingest_tag_t host_a[] = {tag("host", "a")};
  ingest_tag_t host_b[] = {tag("host", "b")};
  ingest_field_t cpu1[] = {field_double("usage", 0.25)};
  ingest_field_t cpu2[] = {field_double("usage", 0.5),
                           field_str("note", "\xE6\xB8\xA9")};
  ingest_field_t mem[] = {field_int("used", 1024), field_bool("ok", true)};

  CHECK(write_one(*database, "cpu", T0, host_a, 1, cpu1, 1) == 1);
  CHECK(write_one(*database, "CPU", T0 + 1, host_b, 1, cpu2, 2) == 2);
  CHECK(write_one(*database, "mem", T0, nullptr, 0, mem, 2) == 3);

  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_visible_seq == 3);
  std::vector<schema_t> schemas;
  database->copy_schemas(schemas);
  CHECK(schemas.size() == 2 && schemas[0].m_name == "cpu" &&
        schemas[0].m_cols.size() == 4);

  const std::string before = dump(*database);
  db_close(database);

  /* Reopening replays the WAL into the same state. */
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_visible_seq == 3 && stats.m_live_writes == 3);
  CHECK(dump(*database) == before);

  /* And writing continues after the recovered sequence numbers. */
  CHECK(write_one(*database, "mem", T0 + 1, nullptr, 0, mem, 2) == 4);
  db_close(database);

  return;
}

/** A failed checkpoint sync must not advance durable storage past the WAL. */
static void test_unsynced_flush_failure() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  ingest_field_t fields[] = {field_int("v", 1)};
  CHECK(write_one(*database, "m", T0, nullptr, 0, fields, 1) == 1);

  const std::string path = dir.file(wal_segment_name(1).c_str());
  const std::string hidden = path + ".hidden";
  CHECK(rename(path.c_str(), hidden.c_str()) == 0);

  db_flush_info_t info;
  const db_status_t err = database->flush(info);
  CHECK(err.code() == DB_IO_ERROR);
  CHECK(err.message().find("cannot sync WAL before flush") == 0);
  memtable_stats_t stats;
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_flushed_seq == 0 && stats.m_visible_seq == 1 &&
        stats.m_live_writes == 1);

  CHECK(rename(hidden.c_str(), path.c_str()) == 0);
  CHECK(database->flush(info).ok());
  CHECK(info.m_flushed_seq == 1 && info.m_rows == 1);
  db_close(database);
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
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
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());

  ingest_field_t num[] = {field_int("v", 1)};
  ingest_field_t text[] = {field_str("v", "x")};
  ingest_field_t reserved[] = {field_int("time", 1)};

  CHECK(write_one(*database, "m", T0, nullptr, 0, num, 1) == 1);

  /* A conflict and an invalid name: rejected with a reason, no sequence
  number used. */
  const ingest_field_t *bad[] = {text, reserved};
  const dberr_t expected[] = {DB_SCHEMA_CONFLICT, DB_INVALID_ARGUMENT};

  for (int i = 0; i < 2; ++i) {
    ingest_point_t p = point(T0, nullptr, 0, bad[i], 1);
    batch_seq_t seq = 99;

    status = database->write("m", 1, &p, 1, seq);
    CHECK(status.code() == expected[i]);
    CHECK(seq == 0 && !status.message().empty());
  }

  ingest_point_t p = point(T0, nullptr, 0, num, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("bad name", 8, &p, 1, seq).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(database->write("_pt_meta", 8, &p, 1, seq).code() ==
        DB_INVALID_ARGUMENT);

  CHECK(write_one(*database, "m", T0, nullptr, 0, num, 1) == 2);
  db_close(database);

  memtable_stats_t stats;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_visible_seq == 2 && stats.m_live_writes == 2);
  db_close(database);

  return;
}

/** A second open of the same directory is refused. */
static void test_second_open() {
  test_dir_t dir;
  db *first = nullptr;
  db *second = nullptr;

  CHECK(db_open(dir.m_path, deferred_flush(), &first).ok());
  CHECK(db_open(dir.m_path, deferred_flush(), &second).code() == DB_LOCKED);
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
  db_options_t options;

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
  options.m_retention_days = DB_MAX_RETENTION_DAYS + 1;
  status = db_open(dir.m_path, &options, &database);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(database == nullptr);
  CHECK(status.message().find("retention period") != std::string::npos);
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  /* Partition options are rejected before taking the directory lock too. */
  options.m_retention_days = 0;
  options.m_partition_duration_days = 0;
  status = db_open(dir.m_path, &options, &database);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(database == nullptr && !status.message().empty());
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  options.m_partition_duration_days = STORAGE_DEFAULT_PARTITION_DAYS;
  status = db_open(dir.m_path, &options, &database);
  CHECK(status.ok());
  CHECK(database != nullptr && status.message().empty());
  db_close(database);

  /* A bad WAL fails recovery after storage and the memtable are open. */
  const std::string log = dir.file(wal_segment_name(1).c_str());
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
  /** Engine under test. */
  db *m_db{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Writes to make. */
  int m_count{0};

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
  snprintf(own, sizeof(own), "own_%d", arg->m_thread_no);

  for (int i = 0; i < arg->m_count; ++i) {
    snprintf(shared, sizeof(shared), "g%d", i / 20);

    ingest_field_t f[] = {field_int("n", i), field_double(shared, 1.5)};
    ingest_tag_t t[] = {tag("thread", own)};
    std::vector<ingest_point_t> points;

    for (int k = 0; k < 10; ++k) {
      points.push_back(point(T0 + k, t, 1, f, 2));
    }

    const char *table_name =
        (arg->m_thread_no % 2 == 1 && i % 2 == 1) ? own : "m";
    batch_seq_t seq = 0;

    if (!arg->m_db->write(table_name, strlen(table_name), &points[0], 10, seq)
             .ok()) {
      arg->m_errors++;
    }
  }

  return;
}

struct reader_arg_t {
  /** Engine under test. */
  db *m_db{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *m_stop{nullptr};

  /** Protects *stop. */
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
    memtable_snapshot_t snapshot;
    snapshot_all(db_test_access::memtable_handle(*arg->m_db), snapshot);

    size_t batches = 0;
    std::map<std::string, memtable_table_view_t>::iterator it;
    for (it = snapshot.m_tables.begin(); it != snapshot.m_tables.end(); ++it) {
      batches += it->second.m_batches.size();
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
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  ingest_field_t fields[] = {field_int("v", 1)};
  CHECK(write_one(*database, "mixed", T0, nullptr, 0, fields, 1) == 1);
  db_close(database);

  storage *store = nullptr;
  CHECK(storage::open(dir.m_path, 1, &store).ok());
  CHECK(duckdb_exec(store->connection(), "CREATE TABLE MiXeD (v BIGINT)").ok());
  storage::destroy(store);

  const db_status_t err = db_open(dir.m_path, deferred_flush(), &database);
  CHECK(err.code() == DB_CORRUPTION);
  CHECK(database == nullptr);

  return;
}

/** Threads writing at once, some racing to add the same columns, lose
nothing. */
static void test_concurrent_writes() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());

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
    args[t].m_thread_no = t;
    args[t].m_count = count;
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
  memtable_stats_t stats;
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_visible_seq == static_cast<batch_seq_t>(threads * count));
  std::vector<schema_t> schemas;
  database->copy_schemas(schemas);
  bool found = false;
  for (const schema_t &schema : schemas) {
    if (schema.m_name == "m") {
      found = true;
      CHECK(schema.m_cols.size() == 3 + count / 20);
    }
  }

  CHECK(found);

  const std::string before = dump(*database);
  db_close(database);

  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  CHECK(dump(*database) == before);
  db_close(database);

  return;
}

/** A failed WAL sync stops new work; what was visible stays, and
reopening recovers. */
static void test_write_failure() {
  test_dir_t dir;
  db *database = nullptr;
  db_options_t options = *deferred_flush();
  options.m_wal_sync = true;
  CHECK(db_open(dir.m_path, &options, &database).ok());

  ingest_field_t f[] = {field_int("v", 1)};
  CHECK(write_one(*database, "m", T0, nullptr, 0, f, 1) == 1);

  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  batch_seq_t seq = 0;

  db_test_access::fail_next_wal_sync(*database);
  CHECK(database->write("m", 1, &p, 1, seq).code() == DB_OUTCOME_UNKNOWN);
  CHECK(seq == 2);

  const db_status_t reason_status = database->write("m", 1, &p, 1, seq);
  CHECK(reason_status.code() == DB_FENCED);
  CHECK(seq == 0);

  /* Every rejected operation retains the original failure diagnostic. */
  const std::string &reason = reason_status.message();
  CHECK(reason.find("cannot sync WAL segment") == 0);

  db_cursor *cursor = nullptr;
  const db_status_t query =
      database->cursor_open("SELECT v FROM m", false, &cursor);
  CHECK(query.code() == DB_FENCED && query.message() == reason);
  CHECK(cursor == nullptr);
  db_cursor::destroy(cursor);

  db_flush_info_t info;
  const db_status_t flush = database->flush(info);
  CHECK(flush.code() == DB_FENCED && flush.message() == reason);
  CHECK(info.m_writes == 0);

  /* What was visible before stays visible. */
  memtable_stats_t stats;
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_visible_seq == 1);
  db_close(database);

  /* Reopening recovers; the unknown write may or may not be there. */
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_visible_seq >= 1 && stats.m_visible_seq <= 2);
  CHECK(stats.m_live_writes == stats.m_visible_seq);
  CHECK(write_one(*database, "m", T0, nullptr, 0, f, 1) ==
        stats.m_visible_seq + 1);
  db_close(database);

  return;
}

/** A selected WAL group remains borrowed while additional writers queue. */
struct write_gate_t {
  /** Protects callback progress. */
  std::mutex m_mutex;

  /** Signals callback entry and permission to continue. */
  std::condition_variable m_changed;

  /** A leader has selected its group and released the write lock. */
  bool m_entered{false};

  /** The test permits WAL I/O to continue. */
  bool m_release{false};
};

/** Pause the active group without holding the database write lock. */
static void pause_write(void *context) {
  write_gate_t &gate = *static_cast<write_gate_t *>(context);
  std::unique_lock<std::mutex> lock(gate.m_mutex);
  gate.m_entered = true;
  gate.m_changed.notify_all();
  gate.m_changed.wait(lock, [&] { return (gate.m_release); });

  return;
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
  db_options_t options = *deferred_flush();
  options.m_wal_sync = true;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return;
  }

  const ingest_field_t field = field_int("v", 1);
  const ingest_point_t p = point(T0, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("m", 1, &p, 1, seq).ok());
  db_cursor *cursor = nullptr;
  CHECK(database->cursor_open("SELECT v FROM m", false, &cursor).ok());

  write_gate_t gate;
  db_test_access::set_before_wal(*database, pause_write, &gate);
  const size_t count = 5;
  std::vector<db_status_t> statuses(count, DB_ERROR);
  std::vector<batch_seq_t> sequences(count, 0);
  std::vector<std::thread> writers;
  std::atomic<size_t> finished{0};
  auto write = [&](size_t i) {
    statuses[i] = database->write("m", 1, &p, 1, sequences[i]);
    ++finished;
  };
  writers.emplace_back(write, 0);
  {
    std::unique_lock<std::mutex> lock(gate.m_mutex);
    CHECK(gate.m_changed.wait_for(lock, std::chrono::seconds(5),
                                  [&] { return (gate.m_entered); }));
  }

  for (size_t i = 1; i < count; ++i) {
    writers.emplace_back(write, i);
  }

  memtable_stats_t pending;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  do {
    db_test_access::memtable_handle(*database).read_stats(pending);
    if (pending.m_live_writes == count + 1) {
      break;
    }

    std::this_thread::yield();
  } while (std::chrono::steady_clock::now() < deadline);
  CHECK(pending.m_live_writes == count + 1 && pending.m_visible_seq == 1);
  CHECK(finished.load() == 0);

  db_status_t storage_failure;
  if (failure == write_failure_t::WAL) {
    db_test_access::fail_next_wal_sync(*database);
  } else if (failure == write_failure_t::STORAGE ||
             failure == write_failure_t::STORAGE_AND_WAL) {
    storage_test_access::fail_next_rollback(
        db_test_access::storage_handle(*database));
    db_test_access::fail_next_flush(*database);
    db_flush_info_t info;
    storage_failure = database->flush(info);
    CHECK(storage_failure.code() == DB_OUTCOME_UNKNOWN);
    CHECK(finished.load() == 0);
    if (failure == write_failure_t::STORAGE_AND_WAL) {
      db_test_access::fail_next_wal_sync(*database);
    }
  }

  {
    std::lock_guard<std::mutex> lock(gate.m_mutex);
    gate.m_release = true;
    gate.m_changed.notify_all();
  }

  for (std::thread &writer : writers) {
    writer.join();
  }

  db_test_access::set_before_wal(*database, nullptr, nullptr);
  CHECK(finished.load() == count);
  if (failure == write_failure_t::NONE) {
    for (size_t i = 0; i < count; ++i) {
      CHECK(statuses[i].ok() && sequences[i] >= 2);
    }

    db_stats_t stats;
    database->read_stats(stats);
    wal_stats_t log_stats;
    db_test_access::read_wal_stats(*database, log_stats);
    CHECK(log_stats.m_records == count + 1);
    CHECK(log_stats.m_groups == 3);
    CHECK(stats.m_visible_seq == count + 1);
  } else {
    CHECK(statuses[0].code() == DB_OUTCOME_UNKNOWN && sequences[0] == 2);
    const bool wal_failed = failure == write_failure_t::WAL ||
                            failure == write_failure_t::STORAGE_AND_WAL;
    CHECK(statuses[0].message().find(wal_failed
                                         ? "cannot sync WAL segment"
                                         : "rollback:") != std::string::npos);
    /* Queued requests retain the first fatal cause, even if active WAL I/O
    independently fails later with a different diagnostic. */
    const std::string cause = failure == write_failure_t::WAL
                                  ? statuses[0].message()
                                  : storage_failure.message();

    for (size_t i = 1; i < count; ++i) {
      CHECK(statuses[i].code() == DB_FENCED && sequences[i] == 0);
      CHECK(statuses[i].message() == cause);
    }

    /* Failed queue heads must not append their reserved records to the WAL. */
    wal_stats_t log_stats;
    db_test_access::read_wal_stats(*database, log_stats);
    CHECK(log_stats.m_records == (wal_failed ? 1 : 2));
    CHECK(log_stats.m_groups == (wal_failed ? 1 : 2));

    db_cursor *rejected = nullptr;
    const db_status_t query =
        database->cursor_open("SELECT v FROM m", false, &rejected);
    CHECK(query.code() == DB_FENCED && query.message() == cause);
    CHECK(rejected == nullptr);
    db_cursor::destroy(rejected);
    db_flush_info_t info;
    const db_status_t flush = database->flush(info);
    CHECK(flush.code() == DB_FENCED && flush.message() == cause);
    CHECK(info.m_writes == 0);
    db_test_access::memtable_handle(*database).read_stats(pending);
    CHECK(pending.m_visible_seq == 1);
  }

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 1);
  db_cursor::destroy(cursor);
  db_close(database);

  CHECK(db_open(dir.m_path, &options, &database).ok());
  db_stats_t restored;
  database->read_stats(restored);
  if (failure == write_failure_t::NONE) {
    CHECK(restored.m_visible_seq == count + 1);
  } else {
    CHECK(restored.m_visible_seq >= 1 && restored.m_visible_seq <= 2);
  }

  CHECK(database->write("m", 1, &p, 1, seq).ok());
  db_close(database);

  return;
}

/** Acknowledgement from the writer process. */
struct kill_ack_t {
  /** Id of an acknowledged write. */
  int64_t m_id{0};
};

struct kill_writer_t {
  /** Engine under test. */
  db *m_db{nullptr};

  /** Kill round; keeps ids apart across rounds. */
  int m_round{0};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Pipe end the acknowledgements are written to. */
  int m_fd{-1};
};

/** Write points with unique ids until killed; report each acknowledged write.
 */
static void kill_writer_main(kill_writer_t *w) {
  for (int i = 0;; ++i) {
    kill_ack_t ack;
    ack.m_id = static_cast<int64_t>(w->m_round) * 100000000 +
               w->m_thread_no * 1000000 + i;

    ingest_field_t f[] = {field_int("id", ack.m_id), field_str("pad", "xyz")};
    ingest_point_t p = point(T0 + i, nullptr, 0, f, 2);
    batch_seq_t seq = 0;

    if (!w->m_db->write("k", 1, &p, 1, seq).ok()) {
      _exit(3);
    }

    if (write(w->m_fd, &ack, sizeof(ack)) !=
        static_cast<ssize_t>(sizeof(ack))) {
      _exit(4);
    }
  }

  return;
}

/** Child process: open the directory and write from four threads until
killed. */
static void kill_child(const char *dir, int round, int fd) {
  db *database = nullptr;
  if (!db_open(dir, deferred_flush(), &database).ok()) {
    _exit(2);
  }

  kill_writer_t writers[4];
  std::thread threads[4];

  for (int t = 0; t < 4; ++t) {
    writers[t].m_db = database;
    writers[t].m_round = round;
    writers[t].m_thread_no = t;
    writers[t].m_fd = fd;
    threads[t] = std::thread(kill_writer_main, &writers[t]);
  }

  for (int t = 0; t < 4; ++t) {
    threads[t].join();
  }

  _exit(0);
}

/** Kill a writing process, over and over: every acknowledged write is
recovered, none twice. */
static void test_kill_during_writes() {
  test_dir_t dir;
  std::set<int64_t> acked;

  for (int round = 0; round < 10; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.m_path, round, fds[1]);
    }

    close(fds[1]);

    /* Let some writes finish, then kill the writer mid-write. */
    const int wanted = 20 + round * 13;
    int received = 0;
    kill_ack_t ack;

    while (received < wanted && read(fds[0], &ack, sizeof(ack)) ==
                                    static_cast<ssize_t>(sizeof(ack))) {
      acked.insert(ack.m_id);
      ++received;
    }

    kill(pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    while (read(fds[0], &ack, sizeof(ack)) ==
           static_cast<ssize_t>(sizeof(ack))) {
      acked.insert(ack.m_id);
    }

    close(fds[0]);
    CHECK(received == wanted);

    /* Every acknowledged write is recovered, and nothing twice. */
    db *database = nullptr;
    CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());

    const std::vector<int64_t> ids = all_ids(*database);
    const std::set<int64_t> unique(ids.begin(), ids.end());
    CHECK(unique.size() == ids.size());

    std::set<int64_t>::iterator it;
    for (it = acked.begin(); it != acked.end(); ++it) {
      CHECK(unique.count(*it) == 1);
    }

    db_close(database);
  }

  printf("kill test: %zu acknowledged writes survived 10 kills\n",
         acked.size());

  return;
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_object_lifetime();
  test_write_and_reopen();
  test_unsynced_flush_failure();
  test_rejected_writes();
  test_second_open();
  test_open_failure_cleanup();
  test_replayed_name_collision();
  test_concurrent_writes();
  test_write_failure();
  test_queued_writes(write_failure_t::NONE);
  test_queued_writes(write_failure_t::WAL);
  test_queued_writes(write_failure_t::STORAGE);
  test_queued_writes(write_failure_t::STORAGE_AND_WAL);
  test_kill_during_writes();

  return (test_result("db-t"));
}
