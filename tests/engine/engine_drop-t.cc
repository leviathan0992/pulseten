/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine_drop-t.cc
 *    Rejected drops preserve managed time-series tables: stored and memtable
 *    rows, schemas, query snapshots, concurrent writes and WAL recovery.
 *
 *-------------------------------------------------------------------------
 */

#include "engine.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <duckdb.h>

#include "duckdb_database.h"
#include "ingest.h"
#include "memtable.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "test_engine.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_time.h"

/** Options: a long background interval and one-day partitions.
@return options with a long background interval and no capacity limit */
static engine_options_t deferred_flush() {
  engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_partition_duration_days = 1;

  return (options);
}

/** Write n points of table name with v = from .. from + n - 1, one a minute
from T0 + from minutes.
@param[in]  from  first row or id to generate, inclusive
@return the status from engine_write() */
static db_status_t write_rows(engine_t &engine, const char *name, int64_t from,
                              int64_t n) {
  std::vector<ingest_field_t> f(n);
  std::vector<ingest_point_t> p(n);

  for (int64_t i = 0; i < n; ++i) {
    f[i] = field_int("v", from + i);
    p[i] = point(T0 + (from + i) * 60 * 1000000LL, nullptr, 0, &f[i], 1);
  }

  batch_seq_t seq = 0;

  return (engine_write(engine, name, strlen(name), p.data(),
                       static_cast<uint32_t>(n), seq));
}

/** Execute a query and read its single integer result.
@return the single BIGINT a query returns, or -1 */
static int64_t query_int(engine_t &engine, const std::string &sql) {
  db_status_t status;

  test_query_t r;

  status = query_all(engine, sql, r);
  if (!status.ok() || r.m_rows.size() != 1 || r.m_rows[0][0].m_is_null) {
    fprintf(stderr, "query failed: %s: %s\n", sql.c_str(),
            status.message().c_str());

    return (-1);
  }

  return (r.m_rows[0][0].m_i);
}

/** Flush, checking that it succeeds. */
static void flush(engine_t &engine) {
  engine_flush_info_t info;
  CHECK(engine_flush(engine, info).ok());
}

/** Count the physical time-partition tables in storage.
@return physical partition tables in the DuckDB file */
static int64_t physical_tables(storage_t &st) {
  int64_t n = -1;
  CHECK(storage_query_int(st,
                          "SELECT count(*) FROM information_schema.tables "
                          "WHERE regexp_matches(table_name, '^_pt_t[0-9]+_')",
                          n)
            .ok());

  return (n);
}

/** List the logical table names in sorted order.
@return the names of the tables */
static std::vector<std::string> table_names(engine_t &engine) {
  std::vector<schema_t> schemas;
  std::vector<std::string> names;

  engine_copy_schemas(engine, schemas);
  for (size_t i = 0; i < schemas.size(); ++i) {
    names.push_back(schemas[i].m_name);
  }

  return (names);
}

/** Rejected drops preserve stored and unflushed rows without flushing them. */
static void test_drop() {
  db_status_t test_status;

  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();

  test_status = engine_open(dir.m_path, &options, &engine);
  CHECK(test_status.ok());

  /* m: three days stored, more rows only in the memtable; k stays. */
  CHECK(write_rows(*engine, "m", 0, 3 * 1440).ok());
  CHECK(write_rows(*engine, "k", 0, 10).ok());
  flush(*engine);
  CHECK(write_rows(*engine, "m", 3 * 1440, 100).ok());
#ifdef PT_DEBUG
  CHECK(physical_tables(engine_dbg_storage(*engine)) == 4);
#endif /* PT_DEBUG */

  memtable_stats_t before;
  engine_get_memtable_stats(*engine, before);
  test_status = engine_exec_ddl(*engine, "DROP TABLE M");
  CHECK(test_status.code() == DB_INVALID_ARGUMENT);
  CHECK(test_status.message() == "time-series tables cannot be dropped");
  memtable_stats_t after;
  engine_get_memtable_stats(*engine, after);
  CHECK(after.m_flushed_seq == before.m_flushed_seq);
  CHECK(after.m_visible_seq == before.m_visible_seq);
  CHECK(after.m_live_writes == before.m_live_writes);
  CHECK(after.m_live_rows == before.m_live_rows);
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 3 * 1440 + 100);
  CHECK(query_int(*engine, "SELECT count(*) FROM k") == 10);
#ifdef PT_DEBUG
  CHECK(physical_tables(engine_dbg_storage(*engine)) == 4);
#endif /* PT_DEBUG */
  CHECK(table_names(*engine) == (std::vector<std::string>{"k", "m"}));

  /* Missing and reserved names still report their own errors. */
  CHECK(engine_exec_ddl(*engine, "DROP TABLE nothing").code() == DB_NOT_FOUND);
  CHECK(engine_exec_ddl(*engine, "DROP TABLE _pt_x").code() == DB_INVALID_ARGUMENT);

  /* A table only in the memtable. */
  CHECK(write_rows(*engine, "hot", 0, 5).ok());
  CHECK(engine_exec_ddl(*engine, "DROP TABLE hot").code() == DB_INVALID_ARGUMENT);
  CHECK(query_int(*engine, "SELECT count(*) FROM hot") == 5);
  CHECK(table_names(*engine) == (std::vector<std::string>{"hot", "k", "m"}));

  /* The original schema remains, and later valid writes still succeed. */
  ingest_field_t f[] = {field_str("v", "text")};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  batch_seq_t seq = 0;
  test_status = engine_write(*engine, "m", 1, &p, 1, seq);
  CHECK(test_status.code() == DB_SCHEMA_CONFLICT);
  CHECK(write_rows(*engine, "m", 3 * 1440 + 100, 5).ok());
  CHECK(engine_close(engine).ok());

  /* WAL recovery retains every row and schema after a rejected drop. */
  test_status = engine_open(dir.m_path, &options, &engine);
  CHECK(test_status.ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 3 * 1440 + 105);
  CHECK(query_int(*engine, "SELECT count(*) FROM k") == 10);
  CHECK(query_int(*engine, "SELECT count(*) FROM hot") == 5);
  CHECK(table_names(*engine) == (std::vector<std::string>{"hot", "k", "m"}));

  /* Flushing and reopening continue to preserve the original tables. */
  flush(*engine);
#ifdef PT_DEBUG
  CHECK(physical_tables(engine_dbg_storage(*engine)) == 6);
#endif /* PT_DEBUG */
  CHECK(engine_close(engine).ok());

  test_status = engine_open(dir.m_path, &options, &engine);
  CHECK(test_status.ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 3 * 1440 + 105);
  CHECK(engine_close(engine).ok());

  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, options.m_partition_duration_days, &st).ok());
  if (st != nullptr) {
    CHECK(physical_tables(*st) == 6);
    storage_close(st);
  }
}

/** Rejected drops and later writes preserve an existing query snapshot. */
static void test_open_cursor() {
  db_status_t status;

  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();

  CHECK(engine_open(dir.m_path, &options, &engine).ok());

  CHECK(write_rows(*engine, "m", 0, 4000).ok());
  flush(*engine);
  CHECK(write_rows(*engine, "m", 4000, 1000).ok());

  /* The cursor retains its snapshot while later writes add more rows. */
  engine_cursor_t *cursor = nullptr;
  CHECK(engine_cursor_open(*engine, "SELECT v FROM m", false, &cursor).ok());

  CHECK(engine_exec_ddl(*engine, "DROP TABLE m").code() == DB_INVALID_ARGUMENT);
  CHECK(write_rows(*engine, "m", 0, 7).ok());
  flush(*engine);

  size_t rows = 0;
  query_rows_t chunk;

  for (;;) {
    status = engine_cursor_fetch(*cursor, chunk);
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
  engine_cursor_close(cursor);

  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 5007);
  CHECK(engine_close(engine).ok());
}

/** Rejected drops do not interrupt writers or background flushing. */
static void test_concurrent_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();

  options.m_flush_interval_ms = 5;

  CHECK(engine_open(dir.m_path, &options, &engine).ok());

  std::atomic<bool> stop{false};
  std::atomic<int64_t> k_written{0};
  std::atomic<int64_t> m_written{0};
  std::atomic<int64_t> failures{0};

  std::thread k_writer([&] {
    for (int64_t i = 0; !stop.load(); ++i) {
      if (write_rows(*engine, "k", i % 1000, 3).ok()) {
        k_written += 3;
      } else {
        failures++;
      }
    }
  });

  std::thread m_writer([&] {
    for (int64_t i = 0; !stop.load(); ++i) {
      if (!write_rows(*engine, "m", i % 1000, 2).ok()) {
        failures++;
      } else {
        m_written += 2;
      }
    }
  });

  int rejected = 0;
  for (int i = 0; i < 20; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const db_status_t err = engine_exec_ddl(*engine, "DROP TABLE m");
    CHECK(err.code() == DB_INVALID_ARGUMENT || err.code() == DB_NOT_FOUND);
    rejected += err.code() == DB_INVALID_ARGUMENT;
  }

  stop = true;
  k_writer.join();
  m_writer.join();

  CHECK(failures == 0);
  CHECK(rejected > 0);

  engine_stats_t stats;
  engine_get_stats(*engine, stats);
  CHECK(stats.m_background_errors == 0);

  /* Every accepted write to both tables remains. */
  CHECK(query_int(*engine, "SELECT count(*) FROM k") == k_written.load());
  const int64_t m_rows = query_int(*engine, "SELECT count(*) FROM m");
  CHECK(m_rows == m_written.load());
  CHECK(engine_close(engine).ok());

  /* Recovery agrees. */
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM k") == k_written.load());
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == m_rows);
  CHECK(engine_close(engine).ok());
}

/** A schema conflict and a rejected drop preserve the original table. */
static void test_rejected_plan_preserves_schema() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(write_rows(*engine, "m", 0, 1).ok());
  ingest_field_t f = field_str("v", "wrong type");
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "m", 1, &p, 1, seq).code() == DB_SCHEMA_CONFLICT);
  CHECK(engine_exec_ddl(*engine, "DROP TABLE m").code() == DB_INVALID_ARGUMENT);
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 1);
  memtable_stats_t stats;
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_garbage_writes == 0);
  CHECK(engine_close(engine).ok());
}

/** Ordinary DDL persists, shares query snapshots, and cannot change managed
tables or bypass the SQL read boundary. */
static void test_ordinary_tables() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(write_rows(*engine, "m", 0, 3).ok());
  flush(*engine);
  CHECK(write_rows(*engine, "m", 3, 2).ok());
  memtable_stats_t before;
  engine_get_memtable_stats(*engine, before);

  CHECK(
      engine_exec_ddl(*engine, "CREATE TABLE copied AS SELECT * FROM m").ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM copied") == 5);
  CHECK(query_int(*engine,
                  "SELECT count(*) FROM copied c JOIN m ON c.v = m.v") == 5);
  memtable_stats_t after;
  engine_get_memtable_stats(*engine, after);
  CHECK(after.m_flushed_seq == before.m_flushed_seq);
  CHECK(after.m_visible_seq == before.m_visible_seq);

  engine_cursor_t *cursor = nullptr;
  CHECK(
      engine_cursor_open(*engine, "SELECT v FROM copied", false, &cursor).ok());
  CHECK(engine_exec_ddl(*engine, "DROP TABLE COPIED").ok());
  size_t rows = 0;
  query_rows_t chunk;
  do {
    CHECK(engine_cursor_fetch(*cursor, chunk).ok());
    rows += chunk.size();
  } while (!chunk.empty());
  CHECK(rows == 5);
  engine_cursor_close(cursor);

  CHECK(engine_exec_ddl(*engine, "CREATE TABLE copied AS SELECT 'text' AS v")
            .ok());
  CHECK(engine_exec_ddl(*engine, "CREATE TABLE IF NOT EXISTS copied (v BIGINT)")
            .ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM copied WHERE v = 'text'") ==
        1);
  CHECK(write_rows(*engine, "copied", 0, 1).code() == DB_INVALID_ARGUMENT);

  const char *refused[] = {
      "CREATE TABLE m (v BIGINT)",
      "CREATE OR REPLACE TABLE m AS SELECT 1 AS v",
      "DROP TABLE m",
      "DROP TABLE IF EXISTS m",
      "CREATE TABLE _pt_x (v BIGINT)",
      "DROP TABLE main._pt_state",
      "CREATE TEMP TABLE temp_rows (v BIGINT)",
      "CREATE VIEW hidden AS SELECT * FROM _pt_state",
      "CREATE TABLE hidden AS SELECT * FROM _pt_state",
      "CREATE TABLE hidden AS SELECT * FROM information_schema.tables",
      "CREATE TABLE hidden AS SELECT * FROM read_csv('/etc/passwd')",
      "CREATE TABLE hidden AS SELECT * FROM main.copied",
      "CREATE TABLE fk (v BIGINT REFERENCES _pt_tables(table_id))",
      "CREATE TABLE other_schema.hidden (v BIGINT)",
      "DROP TABLE copied CASCADE",
      "DROP VIEW copied",
      "BEGIN",
      "SELECT 1",
      "DELETE FROM copied",
      "ATTACH 'other.db'",
      "CREATE TABLE first (v BIGINT); DROP TABLE m",
      "CREATE TABLE",
      ("CREATE TABLE hidden AS WITH x AS (DELETE FROM copied RETURNING *) "
       "SELECT * FROM x")};
  for (const char *sql : refused) {
    CHECK(engine_exec_ddl(*engine, sql).code() == DB_INVALID_ARGUMENT);
  }
  CHECK(engine_exec_ddl(*engine, "CREATE TABLE broken AS SELECT absent FROM m")
            .code() == DB_ERROR);
  CHECK(write_rows(*engine, "broken", 0, 1).ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 5);
  CHECK(engine_exec_ddl(*engine, "DROP TABLE missing").code() == DB_NOT_FOUND);
  CHECK(engine_exec_ddl(*engine, "DROP TABLE IF EXISTS missing").ok());
  CHECK(engine_close(engine).ok());

  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM copied WHERE v = 'text'") ==
        1);
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 5);
  CHECK(write_rows(*engine, "COPIED", 0, 1).code() == DB_INVALID_ARGUMENT);
  CHECK(engine_exec_ddl(*engine,
                        "CREATE OR REPLACE TABLE copied AS SELECT 7 AS v")
            .ok());
  flush(*engine);
  CHECK(engine_close(engine).ok());

  options.m_retention_days = 1;
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  uint64_t removed = 0;
  CHECK(engine_expire(*engine, T0 + 2 * UT_DAY_US, removed).ok());
  CHECK(removed > 0);
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 0);
  CHECK(query_int(*engine, "SELECT v FROM copied") == 7);
  CHECK(engine_exec_ddl(*engine, "DROP TABLE copied").ok());
  CHECK(engine_close(engine).ok());
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(engine_exec_ddl(*engine, "DROP TABLE copied").code() == DB_NOT_FOUND);
  CHECK(engine_close(engine).ok());
}

/** Concurrent ordinary and time-series creation cannot claim the same name. */
static void test_concurrent_table_kinds() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  for (int i = 0; i < 20; ++i) {
    const std::string name = "race_" + std::to_string(i);
    std::atomic<bool> start{false};
    db_status_t written = DB_ERROR;
    db_status_t created = DB_ERROR;
    std::thread writer([&] {
      while (!start.load()) {
        std::this_thread::yield();
      }
      written = write_rows(*engine, name.c_str(), 0, 1);
    });
    std::thread ddl([&] {
      while (!start.load()) {
        std::this_thread::yield();
      }
      created = engine_exec_ddl(*engine,
                                "CREATE TABLE " + name + " AS SELECT 1 AS v");
    });
    start = true;
    writer.join();
    ddl.join();
    CHECK((written.ok() && created.code() == DB_INVALID_ARGUMENT) ||
          (created.ok() && written.code() == DB_INVALID_ARGUMENT));
    CHECK(query_int(*engine, "SELECT count(*) FROM " + name) == 1);
    CHECK(engine_exec_ddl(*engine, "DROP TABLE " + name).code() ==
          (created.ok() ? DB_SUCCESS : DB_INVALID_ARGUMENT));
  }
  CHECK(engine_close(engine).ok());
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(engine_close(engine).ok());
}

#ifdef PT_DEBUG
/** Pause ordinary DDL while testing other operations against its snapshot. */
struct ddl_pause_t {
  /** Protects the two callback flags. */
  std::mutex m_mutex;

  /** Signals callback entry and permission to continue. */
  std::condition_variable m_cv;

  /** The DDL has fixed its snapshot and is about to execute. */
  bool m_entered{false};

  /** The test allows DDL execution to continue. */
  bool m_resume{false};
};

/** Wait until the test has checked concurrent progress. */
static void pause_ddl(void *context) {
  ddl_pause_t &pause = *static_cast<ddl_pause_t *>(context);
  std::unique_lock<std::mutex> lock(pause.m_mutex);
  pause.m_entered = true;
  pause.m_cv.notify_all();
  pause.m_cv.wait(lock, [&] { return (pause.m_resume); });
}

/** Pause one reader between its memtable and DuckDB snapshots. */
struct snapshot_pause_t {
  /** Protects callback and DDL progress. */
  std::mutex m_mutex;

  /** Signals snapshot pinning, DDL completion and permission to continue. */
  std::condition_variable m_cv;

  /** The first cursor has pinned its memtable snapshot. */
  bool m_pinned{false};

  /** The reader may fix its DuckDB snapshot. */
  bool m_resume{false};

  /** The concurrent DDL has returned. */
  bool m_ddl_done{false};
};

/** Let later cursors proceed while the first waits for the test. */
static void pause_snapshot(void *context) {
  snapshot_pause_t &pause = *static_cast<snapshot_pause_t *>(context);
  std::unique_lock<std::mutex> lock(pause.m_mutex);
  if (pause.m_pinned) {
    return;
  }

  pause.m_pinned = true;
  pause.m_cv.notify_all();
  pause.m_cv.wait(lock, [&] { return (pause.m_resume); });
}

/** A reader cannot see ordinary DDL committed after a write its pinned
memtable snapshot excludes. */
static void test_ddl_snapshot_commit() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(write_rows(*engine, "m", 0, 1).ok());
  flush(*engine);
  CHECK(engine_exec_ddl(*engine, "CREATE TABLE marker AS SELECT 0 AS v").ok());

  snapshot_pause_t pause;
  engine_dbg_set_snapshot_callback(*engine, pause_snapshot, &pause);
  test_query_t result;
  db_status_t queried = DB_ERROR;
  std::thread reader([&] {
    queried = query_all(*engine,
                        "SELECT (SELECT count(*) FROM m), "
                        "(SELECT max(v) FROM marker)",
                        result);
  });
  {
    std::unique_lock<std::mutex> lock(pause.m_mutex);
    CHECK(pause.m_cv.wait_for(lock, std::chrono::seconds(5),
                              [&] { return (pause.m_pinned); }));
  }

  CHECK(write_rows(*engine, "m", 1, 1).ok());
  db_status_t replaced = DB_ERROR;
  std::thread ddl([&] {
    replaced = engine_exec_ddl(
        *engine, "CREATE OR REPLACE TABLE marker AS SELECT 1 AS v");
    std::lock_guard<std::mutex> lock(pause.m_mutex);
    pause.m_ddl_done = true;
    pause.m_cv.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(pause.m_mutex);
    const bool committed = pause.m_cv.wait_for(
        lock, std::chrono::seconds(1), [&] { return (pause.m_ddl_done); });
    CHECK(!committed);
    pause.m_resume = true;
    pause.m_cv.notify_all();
  }
  reader.join();
  ddl.join();
  engine_dbg_set_snapshot_callback(*engine, nullptr, nullptr);

  CHECK(queried.ok());
  CHECK(replaced.ok());
  CHECK(result.m_rows.size() == 1);
  if (result.m_rows.size() == 1 && result.m_rows[0].size() == 2) {
    CHECK(result.m_rows[0][0].m_i == 1);
    CHECK(result.m_rows[0][1].m_i == 0);
  } else {
    CHECK(false);
  }
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 2);
  CHECK(query_int(*engine, "SELECT max(v) FROM marker") == 1);
  CHECK(engine_close(engine).ok());
}

/** A running CTAS reserves its name while unrelated writes, new queries and
flushes continue; it retains the memtable rows of its original snapshot. */
static void test_ddl_concurrent_progress() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(write_rows(*engine, "m", 0, 3).ok());
  flush(*engine);
  CHECK(write_rows(*engine, "m", 3, 2).ok());

  ddl_pause_t pause;
  engine_dbg_set_ddl_callback(*engine, pause_ddl, &pause);
  db_status_t created = DB_ERROR;
  std::string ddl_error;
  std::thread ddl([&] {
    created =
        engine_exec_ddl(*engine, "CREATE TABLE copied AS SELECT * FROM m");
  });
  {
    std::unique_lock<std::mutex> lock(pause.m_mutex);
    CHECK(pause.m_cv.wait_for(lock, std::chrono::seconds(5),
                              [&] { return (pause.m_entered); }));
  }

  /* The paused DDL has copied its callback; let another DDL execute. */
  engine_dbg_set_ddl_callback(*engine, nullptr, nullptr);
  db_status_t other_ddl = DB_ERROR;
  std::atomic<bool> progressed{false};
  db_status_t written = DB_ERROR;
  db_status_t queried = DB_ERROR;
  db_status_t flushed = DB_ERROR;
  db_status_t reserved = DB_SUCCESS;
  test_query_t result;
  std::thread operations([&] {
    other_ddl = engine_exec_ddl(*engine, "CREATE TABLE other_table (v BIGINT)");
    ingest_field_t field = field_int("v", 5);
    ingest_point_t p = point(T0, nullptr, 0, &field, 1);
    batch_seq_t seq = 0;
    written = engine_write(*engine, "m", 1, &p, 1, seq);
    queried = query_all(*engine, "SELECT count(*) FROM m", result);
    engine_flush_info_t info;
    flushed = engine_flush(*engine, info);
    reserved = engine_write(*engine, "copied", 6, &p, 1, seq);
    progressed = true;
  });
  for (int waited = 0; waited < 5000 && !progressed.load(); waited += 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  CHECK(progressed.load());
  {
    std::lock_guard<std::mutex> lock(pause.m_mutex);
    pause.m_resume = true;
    pause.m_cv.notify_all();
  }
  operations.join();
  ddl.join();
  CHECK(other_ddl.ok());
  CHECK(written.ok());
  CHECK(queried.ok());
  CHECK(flushed.ok());
  CHECK(reserved.code() == DB_INVALID_ARGUMENT);
  CHECK(result.m_rows.size() == 1);
  if (result.m_rows.size() == 1) {
    CHECK(result.m_rows[0][0].m_i == 6);
  }
  if (!created.ok()) {
    fprintf(stderr, "concurrent CTAS: %s\n", ddl_error.c_str());
  }
  CHECK(created.ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM copied") == 5);
  CHECK(query_int(*engine, "SELECT count(*) FROM m") == 6);
  CHECK(engine_close(engine).ok());
}

/** A failed DDL releases only its own reservation on a shared target. */
static void test_ddl_shared_reservation() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(write_rows(*engine, "m", 0, 1).ok());

  ddl_pause_t first;
  ddl_pause_t second;
  engine_dbg_set_ddl_callback(*engine, pause_ddl, &first);
  db_status_t failed = DB_SUCCESS;
  db_status_t created = DB_ERROR;
  std::thread failing([&] {
    failed = engine_exec_ddl(
        *engine, "CREATE TABLE pending AS SELECT no_such_column FROM m");
  });
  {
    std::unique_lock<std::mutex> lock(first.m_mutex);
    CHECK(first.m_cv.wait_for(lock, std::chrono::seconds(5),
                              [&] { return (first.m_entered); }));
  }

  engine_dbg_set_ddl_callback(*engine, pause_ddl, &second);
  std::thread creating([&] {
    created = engine_exec_ddl(*engine, "CREATE TABLE pending (v BIGINT)");
  });
  {
    std::unique_lock<std::mutex> lock(second.m_mutex);
    CHECK(second.m_cv.wait_for(lock, std::chrono::seconds(5),
                               [&] { return (second.m_entered); }));
  }
  {
    std::lock_guard<std::mutex> lock(first.m_mutex);
    first.m_resume = true;
    first.m_cv.notify_all();
  }
  failing.join();
  CHECK(failed.code() == DB_ERROR);
  CHECK(write_rows(*engine, "pending", 0, 1).code() == DB_INVALID_ARGUMENT);

  engine_dbg_set_ddl_callback(*engine, nullptr, nullptr);
  {
    std::lock_guard<std::mutex> lock(second.m_mutex);
    second.m_resume = true;
    second.m_cv.notify_all();
  }
  creating.join();
  CHECK(created.ok());
  CHECK(write_rows(*engine, "pending", 0, 1).code() == DB_INVALID_ARGUMENT);
  CHECK(engine_exec_ddl(*engine, "DROP TABLE pending").ok());
  CHECK(write_rows(*engine, "pending", 0, 1).ok());
  CHECK(engine_close(engine).ok());
}

/** An old no-op DROP must not erase a concurrently committed CREATE. */
static void test_ddl_noop_catalog_state() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());

  ddl_pause_t pause;
  engine_dbg_set_ddl_callback(*engine, pause_ddl, &pause);
  db_status_t dropped = DB_ERROR;
  std::thread dropping([&] {
    dropped = engine_exec_ddl(*engine, "DROP TABLE IF EXISTS pending");
  });
  {
    std::unique_lock<std::mutex> lock(pause.m_mutex);
    CHECK(pause.m_cv.wait_for(lock, std::chrono::seconds(5),
                              [&] { return (pause.m_entered); }));
  }
  engine_dbg_set_ddl_callback(*engine, nullptr, nullptr);
  CHECK(engine_exec_ddl(*engine, "CREATE TABLE pending (v BIGINT)").ok());
  {
    std::lock_guard<std::mutex> lock(pause.m_mutex);
    pause.m_resume = true;
    pause.m_cv.notify_all();
  }
  dropping.join();
  /* DuckDB may reject a catalog conflict; either outcome must preserve the
  committed ordinary name and keep time-series writes out of it. */
  CHECK(dropped.ok() || dropped.code() == DB_ERROR);
  CHECK(write_rows(*engine, "pending", 0, 1).code() == DB_INVALID_ARGUMENT);
  CHECK(query_int(*engine, "SELECT count(*) FROM pending") == 0);
  CHECK(engine_close(engine).ok());
}

/** A fence raised during CTAS prevents its commit, rolls back the new table,
and releases its name for use after recovery. */
static void test_ddl_fence_before_commit() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  options.m_wal_sync = true;
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(write_rows(*engine, "m", 0, 1).ok());
  ddl_pause_t pause;
  engine_dbg_set_ddl_callback(*engine, pause_ddl, &pause);
  db_status_t created = DB_SUCCESS;
  std::thread ddl([&] {
    created =
        engine_exec_ddl(*engine, "CREATE TABLE refused AS SELECT * FROM m");
  });
  {
    std::unique_lock<std::mutex> lock(pause.m_mutex);
    CHECK(pause.m_cv.wait_for(lock, std::chrono::seconds(5),
                              [&] { return (pause.m_entered); }));
  }
  engine_dbg_fail_next_wal_sync(*engine);
  std::atomic<bool> fenced{false};
  db_status_t written = DB_SUCCESS;
  std::thread writer([&] {
    written = write_rows(*engine, "m", 1, 1);
    fenced = true;
  });
  for (int waited = 0; waited < 5000 && !fenced.load(); waited += 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  CHECK(fenced.load());
  {
    std::lock_guard<std::mutex> lock(pause.m_mutex);
    pause.m_resume = true;
    pause.m_cv.notify_all();
  }
  writer.join();
  ddl.join();
  CHECK(written.code() == DB_OUTCOME_UNKNOWN);
  CHECK(created.code() == DB_FENCED);
  CHECK(engine_close(engine).ok());

  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  test_query_t result;
  CHECK(query_all(*engine, "SELECT * FROM refused", result).code() == DB_ERROR);
  CHECK(write_rows(*engine, "refused", 0, 1).ok());
  CHECK(engine_close(engine).ok());
}
#endif /* PT_DEBUG */

/** Ordinary names retain their case in DuckDB but match case-insensitively. */
static void test_ordinary_name_case() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = deferred_flush();
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(engine_exec_ddl(*engine, "CREATE TABLE MixedCase (v BIGINT)").ok());
  CHECK(engine_exec_ddl(*engine, "DROP TABLE mixedcase").ok());
  CHECK(write_rows(*engine, "mixedcase", 0, 1).ok());

  CHECK(engine_exec_ddl(*engine, "CREATE TABLE OtherCase (v BIGINT)").ok());
  CHECK(engine_exec_ddl(*engine, "DROP TABLE IF EXISTS othercase").ok());
  CHECK(write_rows(*engine, "othercase", 0, 1).ok());
  CHECK(engine_close(engine).ok());
  CHECK(engine_open(dir.m_path, &options, &engine).ok());
  CHECK(query_int(*engine, "SELECT count(*) FROM mixedcase") == 1);
  CHECK(query_int(*engine, "SELECT count(*) FROM othercase") == 1);
  CHECK(engine_close(engine).ok());
}

/** Reopening still rejects unknown internal tables, views and name collisions.
 */
static void test_invalid_stored_objects() {
  const char *statements[] = {"CREATE TABLE _pt_orphan (v BIGINT)",
                              "CREATE VIEW escaped AS SELECT * FROM _pt_state",
                              "CREATE TABLE m (v BIGINT)",
                              "CREATE TABLE hot (v BIGINT)"};
  for (const char *sql : statements) {
    test_dir_t dir;
    engine_t *engine = nullptr;
    engine_options_t options = deferred_flush();
    CHECK(engine_open(dir.m_path, &options, &engine).ok());
    CHECK(write_rows(*engine, "m", 0, 1).ok());
    flush(*engine);
    CHECK(write_rows(*engine, "hot", 0, 1).ok());
    CHECK(engine_close(engine).ok());

    /* Change the closed file directly, then test engine recovery. */
    duckdb_database_t *database = nullptr;
    CHECK(duckdb_database_open(dir.file(STORAGE_FILE_NAME).c_str(), &database)
              .ok());
    if (database == nullptr) {
      continue;
    }
    CHECK(duckdb_database_exec(*database, sql).ok());
    duckdb_database_close(database);
    CHECK(engine_open(dir.m_path, &options, &engine).code() == DB_CORRUPTION);
    CHECK(engine == nullptr);
  }
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_drop();
  test_rejected_plan_preserves_schema();
  test_open_cursor();
  test_concurrent_writes();
  test_ordinary_tables();
  test_concurrent_table_kinds();
#ifdef PT_DEBUG
  test_ddl_snapshot_commit();
  test_ddl_concurrent_progress();
  test_ddl_shared_reservation();
  test_ddl_noop_catalog_state();
  test_ddl_fence_before_commit();
#endif /* PT_DEBUG */
  test_ordinary_name_case();
  test_invalid_stored_objects();

  return (test_result("engine_drop-t"));
}
