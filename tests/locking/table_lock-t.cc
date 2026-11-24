/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * table_lock-t.cc
 *    Tests for table locks: stable table entries, S/X modes and writer
 *    preference, timeouts and cancelled waits, and the lock order of queries
 *    and writes.
 *
 * Covers: src/db/db_table.cc, src/db/db.cc
 *
 *-------------------------------------------------------------------------
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "db.h"
#include "db_table.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_db_tables_access.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_store.h"
#include "test_util.h"
#include "ut.h"

/** One guard releases the right mode across reuse and scope exit. */
static void test_table_lock_guard() {
  db database;
  db_table table;

  {
    db_table_lock lock;
    CHECK(lock.acquire(database, table, db_table_lock_mode_t::SHARED).ok());
    CHECK(lock.holds(&table));
    CHECK(!lock.exclusive());

    {
      db_table_lock reader;
      CHECK(reader.acquire(database, table, db_table_lock_mode_t::SHARED).ok());
    }

    CHECK(!db_tables_test_access::lock(table).try_lock());

    lock.release();
    lock.release();
    CHECK(!lock.holds(&table));
    CHECK(lock.acquire(database, table, db_table_lock_mode_t::EXCLUSIVE).ok());
    CHECK(lock.exclusive());
    CHECK(!db_tables_test_access::lock(table).try_lock_shared());
  }

  db_table_lock reader;
  CHECK(reader.acquire(database, table, db_table_lock_mode_t::SHARED).ok());

  reader.release();

  {
    db_table_lock writer;
    CHECK(
        writer.acquire(database, table, db_table_lock_mode_t::EXCLUSIVE).ok());
  }

  return;
}

/** First users converge on one object; later insertions never invalidate it. */
static void test_concurrent_creation() {
  db_tables tables;
  const size_t workers = 16;
  std::vector<db_table *> results(workers, nullptr);
  std::vector<std::thread> threads;
  std::atomic<bool> start{false};

  for (size_t i = 0; i < workers; ++i) {
    threads.emplace_back([&, i] {
      while (!start.load()) {
        std::this_thread::yield();
      }

      if (!tables.find_or_create("cpu", results[i]).ok()) {
        return;
      }

      for (size_t n = 0; n < 100; ++n) {
        db_table *table = nullptr;
        if (!tables
                 .find_or_create("table_" + std::to_string(i * 100 + n), table)
                 .ok()) {
          results[i] = nullptr;

          return;
        }
      }
    });
  }

  start = true;

  for (std::thread &thread : threads) {
    thread.join();
  }

  CHECK(results[0] != nullptr);

  for (const db_table *table : results) {
    CHECK(table == results[0]);
  }

  CHECK(tables.find("cpu") == results[0]);
  CHECK(db_tables_test_access::size(tables) == workers * 100 + 1);

  return;
}

/** A busy table does not prevent lookup of its stable cache entry. */
static void test_shared_lookup() {
  db_tables tables;
  db_table *original = nullptr;
  CHECK(tables.find_or_create("cpu", original).ok());

  std::atomic<bool> done{false};
  db_table *found = nullptr;

  std::unique_lock<ut_rwlock> held(db_tables_test_access::lock(*original));
  std::thread lookup([&] { done = tables.find_or_create("cpu", found).ok(); });
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);

  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  CHECK(done.load());

  held.unlock();
  lookup.join();
  CHECK(found == original);

  return;
}

/** Empty slots remain stable and can later receive a time-series schema. */
static void test_slot_lifetime() {
  db_tables tables;
  db_table *first = nullptr;
  CHECK(tables.find_or_create("first", first).ok());
  CHECK(first->schema() == nullptr);

  for (size_t i = 0; i < 1000; ++i) {
    db_table *other = nullptr;
    CHECK(tables.find_or_create("slot_" + std::to_string(i), other).ok());
  }

  db_table *again = nullptr;
  CHECK(tables.find_or_create("first", again).ok());
  CHECK(again == first);

  schema_t schema;
  schema.m_name = "first";

  schema_column_t time;
  time.m_name = SCHEMA_TIME_COLUMN_NAME;
  time.m_column_type = column_type_t::TIME;
  time.m_data_type = data_type_t::TIMESTAMP;
  schema.m_columns.push_back(time);
  first->publish_schema(std::make_shared<schema_t>(schema));
  CHECK(tables.find("first")->schema()->m_name == "first");
  CHECK(db_tables_test_access::size(tables) == 1001);

  return;
}

/** Versions survive batch reclamation, and replay reconstructs their identities
without storing process-local identifiers in the WAL. */
static void test_schema_versions() {
  db_tables tables;
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());

  ingest_field_t first[] = {field_int("a", 1)};

  ingest_point_t point1 = point(TEST_BASE_TIME_US, nullptr, 0, first, 1);
  ingest_plan_t plan;
  CHECK(plan_for(nullptr, "m", &point1, 1, plan).ok());

  ingest_plan_t replay_first = plan;
  CHECK(tables.reserve_write(*mt, 1, plan).ok());

  mt->publish(1);

  db_table *table = tables.find("m");
  CHECK(table != nullptr);

  const schema_ref_t v2 = table->schema(2);
  CHECK(v2 != nullptr && table->schema() == v2);

  ingest_field_t second[] = {field_int("a", 2), field_double("b", 3)};

  ingest_point_t point2 = point(TEST_BASE_TIME_US, nullptr, 0, second, 2);
  CHECK(plan_for(v2.get(), "m", &point2, 1, plan).ok());

  ingest_plan_t replay_second = plan;
  CHECK(tables.reserve_write(*mt, 2, plan).ok());

  mt->publish(2);
  CHECK(table->schema(2) == v2);
  CHECK(table->schema(3) == table->schema());
  CHECK(table->schema(0) == nullptr && table->schema(4) == nullptr);

  std::vector<const memtable::entry_t *> entries;
  mt->collect_unflushed(entries);
  CHECK(entries.size() == 2);
  CHECK(entries[0]->m_batch.m_schema_version == 2 &&
        entries[1]->m_batch.m_schema_version == 3);

  mt->retire(2);
  CHECK(table->schema(2) == v2);
  CHECK(tables.reserve_write(*mt, 3, replay_first).code() == DB_CORRUPTION);
  CHECK(table->schema() == table->schema(3));

  memtable::destroy(mt);

  db_tables recovered;
  CHECK(memtable::create(0, 0, &mt).ok());
  CHECK(recovered.reserve_write(*mt, 1, replay_first).ok());
  CHECK(recovered.reserve_write(*mt, 2, replay_second).ok());

  const db_table *restored = recovered.find("m");
  CHECK(restored->schema(2)->m_columns[1].m_name == "a");
  CHECK(restored->schema(3)->m_columns[2].m_name == "b");

  memtable::destroy(mt);
}

/** Shared readers coexist, X excludes both modes, and release may change
threads. */
static void test_table_lock_modes() {
  db_table table;
  db_tables_test_access::lock(table).lock_shared();

  bool shared = false;
  bool exclusive = true;

  std::thread reader([&] {
    shared = db_tables_test_access::lock(table).try_lock_shared();
    if (shared) {
      db_tables_test_access::lock(table).unlock_shared();
    }

    exclusive = db_tables_test_access::lock(table).try_lock();
    if (exclusive) {
      db_tables_test_access::lock(table).unlock();
    }
  });

  reader.join();
  CHECK(shared && !exclusive);

  std::thread closer(
      [&] { db_tables_test_access::lock(table).unlock_shared(); });
  closer.join();
  CHECK(db_tables_test_access::lock(table).try_lock());

  bool blocked = false;

  std::thread other([&] {
    const bool read = db_tables_test_access::lock(table).try_lock_shared();
    if (read) {
      db_tables_test_access::lock(table).unlock_shared();
    }

    const bool write = db_tables_test_access::lock(table).try_lock();
    if (write) {
      db_tables_test_access::lock(table).unlock();
    }

    blocked = !read && !write;
    db_tables_test_access::lock(table).unlock();
  });

  other.join();
  CHECK(blocked);
  CHECK(db_tables_test_access::lock(table).try_lock_shared());

  db_tables_test_access::lock(table).unlock_shared();

  return;
}

/** A queued writer excludes later readers until it has completed. */
static void test_writer_progress() {
  db_table table;
  db_tables_test_access::lock(table).lock_shared();

  std::atomic<bool> entered{false};
  std::atomic<bool> finish{false};

  std::thread writer([&] {
    db_tables_test_access::lock(table).lock();
    entered = true;

    while (!finish.load()) {
      std::this_thread::yield();
    }

    db_tables_test_access::lock(table).unlock();
  });

  bool queued = false;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);

  while (std::chrono::steady_clock::now() < deadline) {
    if (!db_tables_test_access::lock(table).try_lock_shared()) {
      queued = true;
      break;
    }

    db_tables_test_access::lock(table).unlock_shared();

    std::this_thread::yield();
  }

  CHECK(queued);
  CHECK(!entered.load());

  db_tables_test_access::lock(table).unlock_shared();

  while (!entered.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  CHECK(entered.load());

  const bool unexpected_reader =
      db_tables_test_access::lock(table).try_lock_shared();
  CHECK(!unexpected_reader);

  if (unexpected_reader) {
    db_tables_test_access::lock(table).unlock_shared();
  }

  finish = true;
  writer.join();
  CHECK(db_tables_test_access::lock(table).try_lock_shared());

  db_tables_test_access::lock(table).unlock_shared();

  return;
}

/** Wait for a worker, with a deadline so that a lock failure unwinds. */
static bool wait_for_worker(const std::atomic<bool> &done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);

  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  return (done.load());
}

/** The caller holds S, so a refused reader reveals a queued X request. */
static bool wait_for_schema_writer(db_table &table,
                                   const std::atomic<bool> &done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);

  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    if (!db_tables_test_access::lock(table).try_lock_shared()) {
      return (true);
    }

    db_tables_test_access::lock(table).unlock_shared();

    std::this_thread::yield();
  }

  return (false);
}

/** A cursor allows existing-column writes but delays new columns until close.
Waiting for X must not prevent another table from writing and flushing. */
static void test_write_lock_modes() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_id(*database, 1).ok());

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT id FROM k", false, &cursor).ok());
  CHECK(write_id(*database, 2).ok());

  db_table *table = db_test_access::tables_handle(*database).find("k");
  CHECK(table != nullptr);

  const schema_ref_t schema = table->schema();
  const ingest_field_t field = field_int("extra", 30);
  const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  db_status_t status = DB_ERROR;
  batch_seq_t seq = 0;
  std::atomic<bool> done{false};

  std::thread writer([&] {
    status = database->write("k", 1, &p, 1, seq);
    done = true;
  });

  CHECK(wait_for_schema_writer(*table, done));
  CHECK(!done.load());
  CHECK(table->schema() == schema);

  db_status_t other_status = DB_ERROR;
  db_status_t flush_status = DB_ERROR;
  std::atomic<bool> other_done{false};

  std::thread other([&] {
    batch_seq_t other_seq = 0;
    other_status = database->write("other", 5, &p, 1, other_seq);

    db::flush_info_t info;
    flush_status = database->flush(info);
    other_done = true;
  });

  CHECK(wait_for_worker(other_done));

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);

  db_cursor::destroy(cursor);
  writer.join();
  other.join();
  CHECK(status.ok() && seq != 0);
  CHECK(other_status.ok() && flush_status.ok());

  db_close(database);
  CHECK(db_open(dir.m_path, &options, &database).ok());

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*), count(extra), max(extra) FROM k",
                  result)
            .ok());
  CHECK(result.m_rows.size() == 1);

  if (result.m_rows.size() == 1) {
    CHECK(result.m_rows[0][0].m_integer == 3);
    CHECK(result.m_rows[0][1].m_integer == 1);
    CHECK(result.m_rows[0][2].m_integer == 30);
  }

  db_close(database);

  return;
}

/** First writes need X too; concurrent creators resolve the latest column ids.
Each row omits the columns introduced by the other creators. */
static void test_creation_lock() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  db_table *table = nullptr;
  CHECK(db_test_access::tables_handle(*database)
            .find_or_create("created", table)
            .ok());

  std::shared_lock<ut_rwlock> held(db_tables_test_access::lock(*table));
  const size_t count = 4;
  std::vector<db_status_t> statuses(count, DB_ERROR);
  std::vector<batch_seq_t> seqs(count, 0);
  std::vector<std::thread> writers;
  std::atomic<size_t> completed{0};
  std::atomic<bool> done{false};

  for (size_t i = 0; i < count; ++i) {
    writers.emplace_back([&, i] {
      const std::string name = "v" + std::to_string(i);
      const ingest_field_t field = field_int(name.c_str(), i + 1);
      const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
      statuses[i] = database->write("created", 7, &p, 1, seqs[i]);

      if (++completed == count) {
        done = true;
      }
    });
  }

  CHECK(wait_for_schema_writer(*table, done));
  CHECK(completed.load() == 0);
  CHECK(table->schema() == nullptr);

  held.unlock();

  for (std::thread &writer : writers) {
    writer.join();
  }

  for (size_t i = 0; i < count; ++i) {
    CHECK(statuses[i].ok());
    CHECK(seqs[i] != 0);
  }

  const schema_ref_t schema = table->schema();
  CHECK(schema != nullptr && schema->m_columns.size() == count + 1);

  if (schema != nullptr) {
    for (size_t i = 0; i < schema->m_columns.size(); ++i) {
      CHECK(schema->m_columns[i].m_id == i);
    }
  }

  db_close(database);
  CHECK(db_open(dir.m_path, &options, &database).ok());

  test_query_t result;
  CHECK(query_all(*database,
                  "SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) "
                  "FROM created",
                  result)
            .ok());
  CHECK(result.m_rows.size() == 1);

  if (result.m_rows.size() == 1) {
    CHECK(result.m_rows[0][0].m_integer == count);

    for (size_t i = 0; i < count; ++i) {
      CHECK(result.m_rows[0][i + 1].m_integer == static_cast<int64_t>(i + 1));
    }
  }

  db_close(database);

  return;
}

/** Write one field to a named table, creating it when absent. */
static db_status_t write_table_value(db &database, const char *name,
                                     const char *column) {
  const ingest_field_t field = field_int(column, 1);
  const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;

  return (database.write(name, strlen(name), &p, 1, seq));
}

/** A failed database rejects writes without waiting for an existing cursor.
@param[in] fail_before_wait fail before starting the write, or while it waits */
static void test_failed_schema_write(bool fail_before_wait) {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_wal_sync = true;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_table_value(*database, "m", "v").ok());

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT v FROM m", false, &cursor).ok());

  if (fail_before_wait) {
    ut_debug_global().arm("wal.sync", DB_IO_ERROR);
    CHECK(write_table_value(*database, "m", "v").code() == DB_OUTCOME_UNKNOWN);
  }

  const ingest_field_t added = field_int("extra", 2);
  const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &added, 1);
  db_status_t result;
  batch_seq_t seq = 0;
  std::atomic<bool> done{false};

  std::thread writer([&] {
    result = database->write("m", 1, &p, 1, seq);
    done = true;
  });

  if (!fail_before_wait) {
    db_table *table = db_test_access::tables_handle(*database).find("m");
    CHECK(wait_for_schema_writer(*table, done));

    ut_debug_global().arm("store.rollback", DB_ERROR);
    ut_debug_global().arm("store.append", DB_ERROR);

    db::flush_info_t info;
    CHECK(database->flush(info).code() == DB_OUTCOME_UNKNOWN);
  }

  /* Keep the S holder alive: failure, not cursor close, must unblock X. */
  CHECK(wait_for_worker(done));

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);

  db_cursor::destroy(cursor);
  writer.join();
  CHECK(result.code() == DB_FENCED && seq == 0);

  db_table *table = db_test_access::tables_handle(*database).find("m");

  for (const bool exclusive : {false, true}) {
    db_table_lock lock;
    const db_status_t rejected =
        lock.acquire(*database, *table,
                     exclusive ? db_table_lock_mode_t::EXCLUSIVE
                               : db_table_lock_mode_t::SHARED);
    CHECK(rejected.code() == DB_FENCED);
    CHECK(rejected.message() == result.message());
    CHECK(!lock.holds(table));
    CHECK(!lock.exclusive());

    lock.release();
  }

  const bool reusable = db_tables_test_access::lock(*table).try_lock();
  CHECK(reusable);

  if (reusable) {
    db_tables_test_access::lock(*table).unlock();
  }

  db_close(database);

  return;
}

/** Wait for an observed synchronization state, with a bounded deadline. */
template <typename Predicate>
static bool wait_for_state(Predicate ready) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);

  while (std::chrono::steady_clock::now() < deadline) {
    if (ready()) {
      return (true);
    }

    std::this_thread::yield();
  }

  return (false);
}

/** Fail promptly if a regression leaves threads that cannot be joined. */
static void require_progress(bool ready) {
  CHECK(ready);

  if (!ready) {
    std::abort();
  }

  return;
}

/** Publish only after the mixed snapshot is fixed; WAL failure must still
finish while that snapshot is paused. */
static void test_mixed_snapshot_publication(bool stored, bool fail_wal) {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  db_test_access::stop_flusher(*database);

  REQUIRE(write_id(*database, 0).ok());
  REQUIRE(database
              ->exec("CREATE TABLE ordinary_state(v BIGINT); "
                     "INSERT INTO ordinary_state VALUES (0)")
              .ok());

  db::flush_info_t flushed;
  if (stored) {
    REQUIRE(database->flush(flushed).ok());
  }

  wal::stats_t before;
  db_test_access::wal_stats(*database, before);

  std::atomic<bool> pinned{false};
  ut_event_t resume_snapshot;
  ut_debug_global().set(
      "db.after_snapshot_pin",
      [&] {
        pinned = true;
        resume_snapshot.wait();
      },
      1);

  const char *sql =
      "SELECT count(k.id), s.v FROM k "
      "CROSS JOIN ordinary_state s GROUP BY s.v";
  db_cursor *cursor = nullptr;
  db_status_t opened;
  std::atomic<bool> reader_done{false};
  std::thread reader([&] {
    opened = database->open_cursor(sql, false, &cursor);
    reader_done = true;
  });
  require_progress(wait_for_worker(pinned));

  if (fail_wal) {
    ut_debug_global().arm("wal.partial_write", DB_IO_ERROR);
  }

  db_status_t written, updated;
  ut_event_t writer_finished;
  std::atomic<bool> writer_done{false};
  std::thread writer([&] {
    written = write_id(*database, 1);
    if (written.ok()) {
      updated = database->exec("UPDATE ordinary_state SET v = 1");
    }

    writer_done = true;
    writer_finished.set();
  });

  if (fail_wal) {
    require_progress(wait_for_worker(writer_done));
  } else {
    /* WAL I/O finishes without waiting for the snapshot or holding write. */
    require_progress(wait_for_state([&] {
      wal::stats_t current;
      db_test_access::wal_stats(*database, current);

      return (current.m_records == before.m_records + 1);
    }));
    CHECK(!writer_finished.wait_for(std::chrono::milliseconds(30)));

    db::stats_t current;
    database->stats(current);
    CHECK_EQ(current.m_visible_seq, 1);
  }

  resume_snapshot.set();
  require_progress(wait_for_worker(reader_done));
  require_progress(wait_for_worker(writer_done));
  reader.join();
  writer.join();
  ut_debug_global().clear("db.after_snapshot_pin");

  CHECK(opened.ok());
  if (fail_wal) {
    CHECK(written.code() == DB_OUTCOME_UNKNOWN);
    CHECK(write_id(*database, 2).code() == DB_FENCED);
  } else {
    CHECK(written.ok());
    CHECK(updated.ok());

    /* Keeping the result open must not prevent later writes or flushes. */
    CHECK(write_id(*database, 2).ok());
    CHECK(database->exec("UPDATE ordinary_state SET v = 2").ok());
    CHECK(database->flush(flushed).ok());
    CHECK_EQ(flushed.m_flushed_seq, 3);
  }

  if (opened.ok()) {
    query_rows_t rows;
    CHECK(cursor->fetch(rows).ok());
    CHECK(rows.size() == 1 && rows[0][0].m_integer == 1 &&
          rows[0][1].m_integer == 0);
    CHECK(cursor->fetch(rows).ok());
    CHECK(rows.empty());
  }

  db_cursor::destroy(cursor);
  db_close(database);

  /* The successful prefix and SQL-table version agree after recovery. */
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());
  db_test_access::stop_flusher(*database);
  test_query_t recovered;
  CHECK(query_all(*database, sql, recovered).ok());
  CHECK(recovered.m_rows.size() == 1 &&
        recovered.m_rows[0][0].m_integer == (fail_wal ? 1 : 3) &&
        recovered.m_rows[0][1].m_integer == (fail_wal ? 0 : 2));
  db_close(database);
}

/** A late-created table must restart the complete sorted lock acquisition.
Two queries and two schema writers formerly formed a four-operation cycle. */
static void test_query_lock_order() {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, deferred_flush_options(), &database).ok());

  CHECK(write_table_value(*database, "b", "v").ok());

  db_table *b = db_test_access::tables_handle(*database).find("b");

  std::atomic<bool> snapshot_waiting{false};
  ut_event_t resume_snapshot;
  ut_debug_global().set(
      "db.before_query_snapshot",
      [&] {
        snapshot_waiting = true;
        resume_snapshot.wait();
      },
      1);

  const char *sql = "SELECT count(*) FROM a CROSS JOIN b";
  db_status_t statuses[4];
  test_query_t results[2];

  std::atomic<size_t> completed{0};
  std::atomic<bool> b_done{false};
  std::atomic<bool> a_done{false};

  std::thread first([&] {
    statuses[0] = query_all(*database, sql, results[0]);
    ++completed;
  });

  require_progress(wait_for_worker(snapshot_waiting));

  db_status_t created;
  std::thread creator(
      [&] { created = write_table_value(*database, "a", "v"); });
  creator.join();
  CHECK(created.ok());

  db_table *a = db_test_access::tables_handle(*database).find("a");

  std::thread write_b([&] {
    statuses[2] = write_table_value(*database, "b", "extra");
    b_done = true;
    ++completed;
  });

  require_progress(wait_for_schema_writer(*b, b_done));

  std::thread second([&] {
    statuses[1] = query_all(*database, sql, results[1]);
    ++completed;
  });

  require_progress(wait_for_state([&] {
    if (!db_tables_test_access::lock(*a).try_lock()) {
      return (true);
    }

    db_tables_test_access::lock(*a).unlock();

    return (false);
  }));

  std::thread write_a([&] {
    statuses[3] = write_table_value(*database, "a", "extra");
    a_done = true;
    ++completed;
  });

  require_progress(wait_for_schema_writer(*a, a_done));

  resume_snapshot.set();
  require_progress(wait_for_state([&] { return (completed.load() == 4); }));
  first.join();
  second.join();
  write_a.join();
  write_b.join();
  ut_debug_global().clear("db.before_query_snapshot");

  for (const db_status_t &status : statuses) {
    CHECK(status.ok());
  }

  CHECK(results[0].m_rows.size() == 1 &&
        results[0].m_rows[0][0].m_integer == 4);
  CHECK(results[1].m_rows.size() == 1 &&
        results[1].m_rows[0][0].m_integer == 2);

  db_close(database);

  return;
}

/** Cancellation releases waiter accounting, preserving writer preference.
The predicate also reenters the lock API to verify it runs outside state locks.
@param[in] exclusive cancel an X waiter, or an S waiter */
static void test_cancelled_table_wait(bool exclusive) {
  db_table table;

  if (exclusive) {
    db_tables_test_access::lock(table).lock_shared();
  } else {
    db_tables_test_access::lock(table).lock();
  }

  std::atomic<bool> allowed{true};
  std::atomic<size_t> checks{0};
  std::atomic<bool> done{false};
  bool acquired = true;

  std::thread waiter([&] {
    auto keep_waiting = [&] {
      const bool shared = db_tables_test_access::lock(table).try_lock_shared();
      if (shared) {
        db_tables_test_access::lock(table).unlock_shared();
      }

      ++checks;

      return (allowed.load());
    };

    acquired = exclusive
                   ? db_tables_test_access::lock(table).lock_while(keep_waiting)
                   : db_tables_test_access::lock(table).lock_shared_while(
                         keep_waiting);

    if (acquired) {
      if (exclusive) {
        db_tables_test_access::lock(table).unlock();
      } else {
        db_tables_test_access::lock(table).unlock_shared();
      }
    }

    done = true;
  });

  require_progress(wait_for_state([&] { return (checks.load() >= 2); }));

  if (exclusive) {
    const bool bypassed = db_tables_test_access::lock(table).try_lock_shared();
    CHECK(!bypassed);

    if (bypassed) {
      db_tables_test_access::lock(table).unlock_shared();
    }
  }

  allowed = false;
  require_progress(wait_for_worker(done));
  waiter.join();
  CHECK(!acquired);

  if (exclusive) {
    const bool released = db_tables_test_access::lock(table).try_lock_shared();
    CHECK(released);

    if (released) {
      db_tables_test_access::lock(table).unlock_shared();
    }

    db_tables_test_access::lock(table).unlock_shared();
  } else {
    db_tables_test_access::lock(table).unlock();
  }

  const bool reusable = db_tables_test_access::lock(table).try_lock();
  CHECK(reusable);

  if (reusable) {
    db_tables_test_access::lock(table).unlock();
  }

  return;
}

/** Table-lock waits can be cancelled immediately. Snapshot-lock waits finish
acquiring the lock before the query reports cancellation. */
static void test_cancelled_query_open(bool snapshot_wait) {
  test_dir_t dir;
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, nullptr, &database).ok());

  CHECK(write_table_value(*database, "m", "v").ok());

  db_table *table = db_test_access::tables_handle(*database).find("m");

  if (snapshot_wait) {
    db_test_access::snapshot_mutex(*database).lock();
  } else {
    db_tables_test_access::lock(*table).lock();
  }

  query_cancel cancel;
  db_cursor *cursor = nullptr;

  db_status_t status;
  std::atomic<bool> started{false};
  std::atomic<bool> done{false};

  std::thread reader([&] {
    started = true;
    status = database->open_cursor("SELECT v FROM m", false, &cursor, &cancel);
    done = true;
  });

  CHECK(wait_for_worker(started));

  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  CHECK(!done.load());

  cancel.request();

  if (snapshot_wait) {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CHECK(!done.load());

    db_test_access::snapshot_mutex(*database).unlock();
    CHECK(wait_for_worker(done));
  } else {
    CHECK(wait_for_worker(done));

    db_tables_test_access::lock(*table).unlock();
  }

  reader.join();
  CHECK(status.code() == DB_INTERRUPTED);
  CHECK(cursor == nullptr);

  db_cursor::destroy(cursor);

  CHECK(database->open_cursor("SELECT v FROM m", false, &cursor).ok());

  db_cursor::destroy(cursor);

  db_close(database);
}

/** A cursor holder can reject a schema write and retry after closing it. */
static void test_cursor_write_timeout() {
  test_dir_t dir;
  db::options_t options;
  options.m_flush_interval_ms = 3600000;
  options.m_write_lock_timeout_ms = 20;

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());

  CHECK(write_id(*database, 1).ok());

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT id FROM k", false, &cursor).ok());

  db_table *table = db_test_access::tables_handle(*database).find("k");
  const schema_ref_t before = table->schema();
  const ingest_field_t field = field_int("extra", 30);
  const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 99;
  const db_status_t status = database->write("k", 1, &p, 1, seq);
  CHECK(status.code() == DB_LOCK_TIMEOUT);
  CHECK(status.message().find("20 ms") != std::string::npos);
  CHECK(seq == 0);
  CHECK(table->schema() == before);

  db::stats_t stats;
  database->stats(stats);
  CHECK(stats.m_visible_seq == 1);

  /* The rejected X waiter must not keep excluding shared operations. */
  CHECK(write_id(*database, 2).ok());

  db_cursor *other = nullptr;
  CHECK(database->open_cursor("SELECT id FROM k", false, &other).ok());
  db_cursor::destroy(other);

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_integer == 1);
  db_cursor::destroy(cursor);

  CHECK(database->write("k", 1, &p, 1, seq).ok());
  CHECK(seq == 3);

  db_close(database);

  return;
}

/** Both lock modes leave an empty guard when their wait expires. */
static void test_table_lock_timeout() {
  db database;
  db_table table;
  db_table_lock holder;
  CHECK(holder.acquire(database, table, db_table_lock_mode_t::EXCLUSIVE).ok());
  for (const auto mode :
       {db_table_lock_mode_t::SHARED, db_table_lock_mode_t::EXCLUSIVE}) {
    db_table_lock waiter;
    CHECK(waiter.acquire(database, table, mode, nullptr, 10).code() ==
          DB_LOCK_TIMEOUT);
    CHECK(waiter.table() == nullptr);
  }

  holder.release();
  CHECK(holder.acquire(database, table, db_table_lock_mode_t::SHARED).ok());

  return;
}

/** Open with maintenance stopped so declarations and WAL-only schemas stay
distinct. */
static db *open_declarations(test_dir_t &dir, const db::options_t &options) {
  db *database = nullptr;
  ut_a(db_open(dir.m_path, &options, &database).ok());
  db_test_access::stop_flusher(*database);

  return (database);
}

/** Write a valid current point, optionally with a second field. */
static db_status_t write_declared(db &database, const char *name,
                                  int64_t time) {
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(time, nullptr, 0, &f, 1);
  batch_seq_t seq = 0;

  return (database.write(name, strlen(name), &p, 1, seq));
}

/** Declarations survive reopening; schema initialization occurs on accepted
writes. */
static void test_explicit_table_lifecycle() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  db *database = open_declarations(dir, options);
  test_query_t primed;

  /* Cache the empty catalog at F=0. */
  CHECK(query_all(*database, "SELECT 1", primed).ok());
  CHECK(database->create_table("CPU", {5, 1}).ok());
  CHECK(database->create_table("pending", {}).ok());

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);
  REQUIRE(tables.size() == 2);
  CHECK(tables[0].m_name == "cpu" && tables[0].m_schema == nullptr);
  CHECK_EQ(tables[0].m_partition_duration_hours, uint32_t{5});
  CHECK(tables[0].m_retention_hours == 1);
  CHECK(tables[1].m_name == "pending" && !tables[1].m_retention_hours);
  CHECK_EQ(tables[1].m_partition_duration_hours, uint32_t{24});

  const store_catalog_t &catalog =
      db_test_access::store_handle(*database).catalog();
  CHECK_EQ(catalog.m_flushed_seq, batch_seq_t{0});
  CHECK_EQ(catalog.m_tables.size(), size_t{2});
  CHECK(catalog.m_tables.at("cpu").m_partitions.empty());
  CHECK(catalog.m_tables.at("pending").m_partitions.empty());
  CHECK(!catalog.m_tables.at("cpu").m_initialized);
  CHECK(catalog.m_tables.at("cpu").m_schema.m_columns.empty());

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT * FROM cpu", false, &cursor).code() ==
        DB_TABLE_NOT_INITIALIZED);
  CHECK(cursor == nullptr);

  std::string plan;
  CHECK(database->explain("SELECT * FROM cpu", plan).code() ==
        DB_TABLE_NOT_INITIALIZED);
  CHECK(database->open_cursor("SELECT * FROM missing", false, &cursor).code() ==
        DB_ERROR);

  test_query_t result;
  CHECK(
      query_all(*database, "WITH cpu AS (SELECT 9 v) SELECT v FROM cpu", result)
          .ok());
  REQUIRE(result.m_rows.size() == 1);
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{9});
  for (const char *sql :
       {"CREATE TABLE cpu(v INT)", "CREATE OR REPLACE TABLE cpu(v INT)",
        "CREATE VIEW cpu AS SELECT 1", "DROP TABLE cpu"}) {
    CHECK(!database->exec(sql).ok());
  }

  batch_seq_t seq = 99;
  CHECK(database->write("cpu", 3, nullptr, 0, seq).code() ==
        DB_INVALID_ARGUMENT);
  CHECK_EQ(seq, batch_seq_t{0});
  CHECK(write_declared(*database, "cpu", ut_now_us() - 2 * UT_HOUR_US).code() ==
        DB_INVALID_ARGUMENT);
  database->collect_tables(tables);
  CHECK(tables[0].m_schema == nullptr);
  CHECK(database->set_table_options("cpu", {0}).ok());
  CHECK(write_declared(*database, "cpu", TEST_BASE_TIME_US).ok());
  database->collect_tables(tables);
  REQUIRE(tables[0].m_schema != nullptr);
  CHECK_EQ(tables[0].m_schema->m_columns.size(), size_t{2});
  CHECK(!catalog.m_tables.at("cpu").m_initialized);
  CHECK_EQ(catalog.m_flushed_seq, batch_seq_t{0});
  CHECK(database->create_table("cpu", {1, 24}).code() == DB_INVALID_ARGUMENT);

  db_close(database);

  /* A new Open value never changes the durable default or declared table
  durations. */
  options.m_partition_duration_hours = 1;
  database = open_declarations(dir, options);
  database->collect_tables(tables);
  REQUIRE(tables.size() == 2);
  CHECK(tables[0].m_schema != nullptr && tables[1].m_schema == nullptr);
  CHECK_EQ(tables[0].m_partition_duration_hours, uint32_t{5});
  CHECK(tables[0].m_retention_hours == 0);
  CHECK_EQ(tables[1].m_partition_duration_hours, uint32_t{24});
  CHECK(database->create_table("inherited", {}).ok());
  CHECK_EQ(db_test_access::store_handle(*database)
               .catalog()
               .m_tables.at("inherited")
               .m_partition_duration_hours,
           uint32_t{24});

  db::flush_info_t info;
  ut_debug_global().arm("store.append", DB_OUT_OF_MEMORY);
  CHECK(database->flush(info).code() == DB_OUT_OF_MEMORY);
  CHECK(!db_test_access::store_handle(*database)
             .catalog()
             .m_tables.at("cpu")
             .m_initialized);
  CHECK(database->flush(info).ok());
  CHECK(db_test_access::store_handle(*database)
            .catalog()
            .m_tables.at("cpu")
            .m_initialized);
  CHECK(database->set_table_options("cpu", {1}).ok());

  uint64_t removed = 0;
  CHECK(db_test_access::expire(*database, ut_now_us(), removed).ok());
  CHECK(removed >= 1);
  CHECK(query_all(*database, "SELECT count(*) FROM cpu", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{0});

  db_close(database);
  database = open_declarations(dir, options);
  CHECK(query_all(*database, "SELECT count(*) FROM cpu", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{0});
  CHECK(database->open_cursor("SELECT * FROM pending", false, &cursor).code() ==
        DB_TABLE_NOT_INITIALIZED);

  db_close(database);
}

/** Rejections reserve neither names nor schema, and creation never flushes
other rows. */
static void test_create_validation() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  options.m_write_lock_timeout_ms = 20;

  db *database = open_declarations(dir, options);

  for (const char *name : {"", "_pt_hidden", "bad.name"}) {
    CHECK(database->create_table(name, {}).code() == DB_INVALID_ARGUMENT);
  }

  CHECK(database->create_table("bad_span", {STORE_MAX_PARTITION_HOURS + 1, 0})
            .code() == DB_INVALID_ARGUMENT);
  CHECK(database->create_table("bad_retention", {1, DB_MAX_RETENTION_HOURS + 1})
            .code() == DB_INVALID_ARGUMENT);
  CHECK(database->create_table("wide_window", {1, 5000}).ok());
  CHECK(database->exec("CREATE TABLE ordinary(v INT)").ok());
  CHECK(database->create_table("ordinary", {}).code() == DB_INVALID_ARGUMENT);
  CHECK(write_declared(*database, "auto_table", ut_now_us()).ok());
  CHECK(database->create_table("AUTO_TABLE", {1, 0}).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(database->create_table("declared", {1, 1}).ok());
  CHECK(database->create_table("declared", {24, 0}).code() ==
        DB_INVALID_ARGUMENT);

  const store_catalog_t &catalog =
      db_test_access::store_handle(*database).catalog();
  CHECK_EQ(catalog.m_flushed_seq, batch_seq_t{0});
  CHECK(catalog.m_tables.count("auto_table") == 0);
  CHECK(catalog.m_tables.at("declared").m_retention_hours == 1);
  CHECK_EQ(catalog.m_tables.at("declared").m_partition_duration_hours,
           uint32_t{1});

  db_table *slot = nullptr;
  CHECK(db_test_access::tables_handle(*database)
            .find_or_create("wait", slot)
            .ok());

  db_table_lock holder;
  CHECK(holder.acquire(*database, *slot, db_table_lock_mode_t::SHARED).ok());

  db_status_t timeout;
  std::thread waiter([&] { timeout = database->create_table("wait", {}); });
  waiter.join();
  CHECK(timeout.code() == DB_LOCK_TIMEOUT);
  CHECK(!slot->exists());
  holder.release();
  CHECK(database->create_table("wait", {}).ok());

  db_close(database);
}

/** Rollback leaves an unused slot reusable; uncertain metadata outcomes fence
the database. */
static void test_create_failures() {
  for (int failure = 0; failure < 3; ++failure) {
    ut_debug_scope debug_scope;

    test_dir_t dir;
    db::options_t options = daily_partition_options();
    db *database = open_declarations(dir, options);

    if (failure == 1) {
      ut_debug_global().arm("store.commit", DB_IO_ERROR);
    } else {
      ut_debug_global().arm("store.table_options", DB_IO_ERROR);
    }

    if (failure == 2) {
      ut_debug_global().arm("store.rollback", DB_IO_ERROR);
    }

    const db_status_t err = database->create_table("m", {5, 1});
    CHECK(err.code() == (failure == 0 ? DB_IO_ERROR : DB_OUTCOME_UNKNOWN));
    CHECK(db_test_access::store_handle(*database).catalog().m_tables.empty());

    std::vector<db_table_info_t> tables;
    database->collect_tables(tables);
    CHECK(tables.empty());

    if (failure == 0) {
      CHECK(database->exec("CREATE TABLE m(v INT)").ok());
      CHECK(database->exec("DROP TABLE m").ok());
      CHECK(database->create_table("m", {5, 1}).ok());
    } else {
      CHECK(database->create_table("next", {}).code() == DB_FENCED);
      CHECK(write_declared(*database, "m", ut_now_us()).code() == DB_FENCED);
    }

    db_close(database);
    database = open_declarations(dir, options);
    database->collect_tables(tables);
    CHECK_EQ(tables.size(), failure == 0 ? size_t{1} : size_t{0});

    if (failure != 0) {
      CHECK(database->create_table("m", {5, 1}).ok());
    }

    CHECK(write_declared(*database, "m", ut_now_us()).ok());

    db_close(database);
  }
}

/** Catalog reads remain available while a cursor delays a pending Set. */
static void test_catalog_with_waiting_set() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  options.m_write_lock_timeout_ms = 5000;

  db *database = open_declarations(dir, options);
  CHECK(database->create_table("m", {5, 0}).ok());
  CHECK(write_declared(*database, "m", ut_now_us()).ok());

  db_cursor *cursor = nullptr;
  REQUIRE(database->open_cursor("SELECT * FROM m", false, &cursor).ok());

  std::atomic<bool> done{false};
  db_status_t set;

  std::thread setter([&] {
    set = database->set_table_options("m", {1});
    done = true;
  });

  db_table *table = db_test_access::tables_handle(*database).find("m");
  CHECK(wait_for_schema_writer(*table, done));

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);
  CHECK(!done.load());
  CHECK(tables.size() == 1);

  if (!tables.empty()) {
    CHECK(tables[0].m_schema != nullptr && tables[0].m_retention_hours == 0);
  }

  db_cursor::destroy(cursor);
  setter.join();
  CHECK(set.ok());
  database->collect_tables(tables);
  CHECK(tables[0].m_retention_hours == 1);

  db_close(database);
}

/** Duplicate declarations and first writes serialize on the same stable
slot. */
static void test_create_races() {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  options.m_write_lock_timeout_ms = 5000;

  db *database = open_declarations(dir, options);
  std::atomic<bool> start{false};
  std::atomic<int> created{0}, duplicates{0}, errors{0};
  std::vector<std::thread> workers;

  for (int i = 0; i < 8; ++i) {
    workers.emplace_back([&] {
      while (!start.load()) {
        std::this_thread::yield();
      }

      const db_status_t err = database->create_table("same", {5, 0});
      if (err.ok()) {
        ++created;
      } else if (err.code() == DB_INVALID_ARGUMENT) {
        ++duplicates;
      } else {
        ++errors;
      }
    });
  }

  start = true;

  for (auto &worker : workers) {
    worker.join();
  }

  CHECK_EQ(created.load(), 1);
  CHECK_EQ(duplicates.load(), 7);
  CHECK_EQ(errors.load(), 0);

  for (int i = 0; i < 6; ++i) {
    const std::string name = "race" + std::to_string(i);
    db_status_t declared, written, flushed;

    std::thread creator(
        [&] { declared = database->create_table(name, {5, 0}); });

    std::thread writer([&] {
      written = write_declared(*database, name.c_str(), ut_now_us());
    });

    std::thread storage([&] {
      db::flush_info_t info;
      flushed = database->flush(info);
    });
    creator.join();
    writer.join();
    storage.join();
    CHECK(written.ok() && flushed.ok());
    CHECK(declared.ok() || declared.code() == DB_INVALID_ARGUMENT);

    std::vector<db_table_info_t> tables;
    database->collect_tables(tables);

    for (const auto &table : tables) {
      if (table.m_name == name) {
        CHECK(table.m_schema != nullptr);
        CHECK_EQ(table.m_partition_duration_hours,
                 declared.ok() ? uint32_t{5} : uint32_t{24});
      }
    }
  }

  db_close(database);

  for (int i = 0; i < 4; ++i) {
    test_dir_t ordinary_dir;
    database = open_declarations(ordinary_dir, options);
    db_status_t declared, ordinary;

    std::thread creator(
        [&] { declared = database->create_table("conflict", {}); });

    std::thread ddl(
        [&] { ordinary = database->exec("CREATE TABLE conflict(v INT)"); });
    creator.join();
    ddl.join();
    CHECK(declared.ok() != ordinary.ok());

    db_close(database);
    database = open_declarations(ordinary_dir, options);

    db_close(database);
  }
}

/** Write the same column count with a different type after recreating a
name. */
static db_status_t write_replacement(db &database, const char *name,
                                     int64_t time) {
  ingest_field_t f = field_str("v", "new");
  ingest_point_t p = point(time, nullptr, 0, &f, 1);
  batch_seq_t seq = 0;

  return (database.write(name, strlen(name), &p, 1, seq));
}

/** Drop flushes old WAL rows; a pinned unrelated snapshot cannot leak them
back. */
static void test_drop_recreate() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_partition_duration_hours = 1;

  db *database = open_declarations(dir, options);
  CHECK(database->create_table("M", {1, 0}).ok());
  CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).ok());

  db::flush_info_t flushed;
  CHECK(database->flush(flushed).ok());

  const uint64_t old_id = db_test_access::store_handle(*database)
                              .catalog()
                              .m_tables.at("m")
                              .m_table_id;
  CHECK(write_declared(*database, "m", TEST_BASE_TIME_US + UT_HOUR_US).ok());
  CHECK(
      write_declared(*database, "m", TEST_BASE_TIME_US + 2 * UT_HOUR_US).ok());
  CHECK(write_declared(*database, "other", TEST_BASE_TIME_US).ok());

  std::vector<schema_ref_t> old_schemas;
  database->collect_schemas(old_schemas);

  db_cursor *other = nullptr;
  REQUIRE(database->open_cursor("SELECT v FROM other", false, &other).ok());
  CHECK(database->drop_table("M").ok());

  const store_catalog_t &catalog =
      db_test_access::store_handle(*database).catalog();
  CHECK_EQ(catalog.m_flushed_seq, batch_seq_t{4});
  CHECK(catalog.m_tables.count("m") == 0);

  db_cursor *missing = nullptr;
  CHECK(database->open_cursor("SELECT * FROM m", false, &missing).code() ==
        DB_ERROR);
  CHECK(missing == nullptr);
  CHECK(database->set_table_options("m", {1}).code() == DB_INVALID_ARGUMENT);
  CHECK(database->create_table("m", {5, 0}).ok());
  CHECK(write_replacement(*database, "m", TEST_BASE_TIME_US).ok());
  CHECK(write_replacement(*database, "m", TEST_BASE_TIME_US + 5 * UT_HOUR_US)
            .ok());

  test_query_t result;
  REQUIRE(query_all(*database, "SELECT v FROM m", result).ok());
  CHECK_EQ(result.m_rows.size(), size_t{2});

  for (const auto &row : result.m_rows) {
    CHECK(row[0].m_string == "new");
  }

  CHECK(old_schemas[0]->m_columns[1].m_data_type == data_type_t::BIGINT);

  query_rows_t rows;
  REQUIRE(other->fetch(rows).ok());
  REQUIRE(rows.size() == 1);
  CHECK_EQ(rows[0][0].m_integer, int64_t{1});
  db_cursor::destroy(other);
  CHECK(database->flush(flushed).ok());

  const uint64_t new_id = catalog.m_tables.at("m").m_table_id;
  CHECK(new_id > old_id);

  int64_t count = -1;
  CHECK(
      test_store_query_int(db_test_access::store_handle(*database),
                           "SELECT count(*) FROM _pt_columns WHERE table_id=" +
                               std::to_string(old_id),
                           count)
          .ok());
  CHECK_EQ(count, int64_t{0});
  CHECK(test_store_query_int(db_test_access::store_handle(*database),
                             "SELECT count(*) FROM information_schema.tables "
                             "WHERE table_name LIKE '_pt_t" +
                                 std::to_string(old_id) + "_%'",
                             count)
            .ok());
  CHECK_EQ(count, int64_t{0});

  db_close(database);
  database = open_declarations(dir, options);
  REQUIRE(query_all(*database, "SELECT v FROM m", result).ok());
  CHECK_EQ(result.m_rows.size(), size_t{2});
  CHECK(database->drop_table("m").ok());

  /* Automatic recreation uses the database defaults. */
  CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).ok());

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);
  REQUIRE(tables.size() == 2);
  CHECK_EQ(tables[0].m_partition_duration_hours, uint32_t{1});
  CHECK(!tables[0].m_retention_hours);
  CHECK(tables[0].m_schema->m_columns[1].m_data_type == data_type_t::BIGINT);

  db_close(database);
  database = open_declarations(dir, options);
  REQUIRE(query_all(*database, "SELECT v FROM m", result).ok());
  CHECK_EQ(result.m_rows.size(), size_t{1});
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{1});
  CHECK(database->flush(flushed).ok());
  CHECK(db_test_access::store_handle(*database)
            .catalog()
            .m_tables.at("m")
            .m_table_id > new_id);

  db_close(database);
}

/** Uninitialized declarations can be dropped without leaving stale catalog
views. */
static void test_drop_empty_and_validation() {
  test_dir_t dir;
  db *database = open_declarations(dir, daily_partition_options());
  CHECK(database->create_table("empty", {5, 1}).ok());

  db_cursor *cursor = nullptr;
  CHECK(database->open_cursor("SELECT * FROM empty", false, &cursor).code() ==
        DB_TABLE_NOT_INITIALIZED);
  CHECK(database->drop_table("empty").ok());
  CHECK(database->open_cursor("SELECT * FROM empty", false, &cursor).code() ==
        DB_ERROR);

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);
  CHECK(tables.empty());
  CHECK(database->create_table("empty", {1, 0}).ok());
  CHECK(database->open_cursor("SELECT * FROM empty", false, &cursor).code() ==
        DB_TABLE_NOT_INITIALIZED);
  CHECK(database->drop_table("empty").ok());
  CHECK(database->exec("CREATE TABLE empty(v INT)").ok());
  CHECK(write_declared(*database, "buffered", TEST_BASE_TIME_US).ok());
  for (const char *name :
       {"empty", "missing", "", "_pt_hidden", "main.empty"}) {
    CHECK(database->drop_table(name).code() == DB_INVALID_ARGUMENT);
  }

  CHECK_EQ(db_test_access::store_handle(*database).catalog().m_flushed_seq,
           batch_seq_t{0});

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM empty", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{0});
  CHECK(database->exec("DROP TABLE empty").ok());
  CHECK(write_declared(*database, "empty", TEST_BASE_TIME_US).ok());
  CHECK(database->drop_table("empty").ok());

  db_close(database);
}

/** Readers finish before deletion, while other tables keep accepting writes. */
static void test_drop_reader_wait(bool timeout) {
  test_dir_t dir;
  db::options_t options = daily_partition_options();
  options.m_write_lock_timeout_ms = timeout ? 20 : 5000;

  db *database = open_declarations(dir, options);
  CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).ok());

  db_cursor *cursor = nullptr;
  REQUIRE(database->open_cursor("SELECT v FROM m", false, &cursor).ok());

  std::atomic<bool> done{false};
  db_status_t dropped;

  std::thread worker([&] {
    dropped = database->drop_table("m");
    done = true;
  });

  db_table *table = db_test_access::tables_handle(*database).find("m");

  if (timeout) {
    worker.join();
    CHECK(dropped.code() == DB_LOCK_TIMEOUT);
    CHECK_EQ(db_test_access::store_handle(*database).catalog().m_flushed_seq,
             batch_seq_t{0});
  } else {
    CHECK(wait_for_schema_writer(*table, done));
    CHECK(write_declared(*database, "other", TEST_BASE_TIME_US).ok());
    CHECK(!done.load());
  }

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK_EQ(rows.size(), size_t{1});
  db_cursor::destroy(cursor);

  if (!timeout) {
    worker.join();
    CHECK(dropped.ok());
  } else {
    CHECK(database->drop_table("m").ok());
  }

  CHECK(!table->exists());

  db_close(database);
}

/** Flush and catalog failures preserve the old incarnation or fence all
operations. */
static void test_drop_failures() {
  for (int failure = 0; failure < 5; ++failure) {
    ut_debug_scope debug_scope;

    test_dir_t dir;
    db::options_t options = daily_partition_options();
    db *database = open_declarations(dir, options);
    CHECK(database->create_table("m", {5, 0}).ok());
    CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).ok());

    db::flush_info_t flushed;

    if (failure >= 1 && failure <= 3) {
      CHECK(database->flush(flushed).ok());
    }

    db_table *table = db_test_access::tables_handle(*database).find("m");
    const uint64_t generation = table->generation();

    if (failure == 0) {
      ut_debug_global().arm("store.append", DB_OUT_OF_MEMORY);
    } else if (failure == 2 || failure == 4) {
      ut_debug_global().arm("store.commit", DB_IO_ERROR);
    } else {
      ut_debug_global().arm("store.drop_table", DB_IO_ERROR);
    }

    if (failure == 3) {
      ut_debug_global().arm("store.rollback", DB_IO_ERROR);
    }

    const db_status_t dropped = database->drop_table("m");
    CHECK(dropped.code() == (failure == 0   ? DB_OUT_OF_MEMORY
                             : failure == 1 ? DB_IO_ERROR
                                            : DB_OUTCOME_UNKNOWN));
    CHECK(table->exists());
    CHECK_EQ(table->generation(), generation);

    if (failure < 2) {
      test_query_t result;
      CHECK(query_all(*database, "SELECT count(*) FROM m", result).ok());
      CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{1});
      CHECK(database->drop_table("m").ok());
      CHECK(database->create_table("m", {1, 0}).ok());
      CHECK(write_replacement(*database, "m", TEST_BASE_TIME_US).ok());
    } else {
      CHECK(database->drop_table("m").code() == DB_FENCED);
      CHECK(database->set_table_options("m", {0}).code() == DB_FENCED);
      CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).code() ==
            DB_FENCED);
    }

    db_close(database);
    database = open_declarations(dir, options);

    test_query_t result;
    CHECK(query_all(*database, "SELECT v FROM m", result).ok());
    CHECK_EQ(result.m_rows.size(), size_t{1});

    if (failure < 2) {
      CHECK(result.m_rows[0][0].m_string == "new");
    } else {
      CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{1});
    }

    db_close(database);
  }
}

/** A request selected on an older incarnation cannot touch its replacement. */
static void test_drop_stale_request(int operation, bool ordinary_replacement) {
  test_dir_t dir;
  db *database = open_declarations(dir, daily_partition_options());
  CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).ok());

  const char *hooks[] = {
      "db.before_write_table_lock", "db.before_write_schema_lock",
      "db.before_existing_table_lock", "db.before_existing_table_lock"};

  std::atomic<bool> paused{false}, resume{false};
  ut_debug_global().set(
      hooks[operation],
      [&] {
        paused = true;

        while (!resume.load()) {
          std::this_thread::yield();
        }
      },
      1);

  db_status_t pending;

  std::thread worker([&] {
    if (operation == 0) {
      pending = write_declared(*database, "m", TEST_BASE_TIME_US);
    } else if (operation == 1) {
      pending = write_table_value(*database, "m", "extra");
    } else if (operation == 2) {
      pending = database->set_table_options("m", {1});
    } else {
      pending = database->drop_table("m");
    }
  });
  CHECK(wait_for_worker(paused));
  CHECK(database->drop_table("m").ok());

  if (ordinary_replacement) {
    CHECK(database->exec("CREATE TABLE m(v INT)").ok());
  } else {
    CHECK(database->create_table("m", {5, 0}).ok());
    /* Keep the same schema so a stale write needs the generation check. */
    CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).ok());
  }

  resume = true;
  worker.join();
  ut_debug_global().clear(hooks[operation]);
  CHECK(pending.code() == DB_INVALID_ARGUMENT);

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM m", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer,
           ordinary_replacement ? int64_t{0} : int64_t{1});

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);

  if (ordinary_replacement) {
    CHECK(tables.empty());
  } else {
    REQUIRE(tables.size() == 1);
    CHECK_EQ(tables[0].m_partition_duration_hours, uint32_t{5});
    CHECK(tables[0].m_retention_hours == 0);
  }

  db_close(database);
}

/** Repeated deletion and recreation serialize with other-table writes and
flushes. */
static void test_drop_under_flush() {
  test_dir_t dir;
  db *database = open_declarations(dir, daily_partition_options());
  std::atomic<int> errors{0};

  std::thread writer([&] {
    for (int i = 0; i < 50; ++i) {
      if (!write_declared(*database, "other", TEST_BASE_TIME_US).ok()) {
        ++errors;
      }
    }
  });

  std::thread storage([&] {
    for (int i = 0; i < 20; ++i) {
      db::flush_info_t f;
      if (!database->flush(f).ok()) {
        ++errors;
      }
    }
  });

  for (int i = 0; i < 8; ++i) {
    CHECK(database->create_table("m", {5, 0}).ok());
    CHECK(write_declared(*database, "m", TEST_BASE_TIME_US).ok());
    CHECK(database->drop_table("m").ok());
  }

  writer.join();
  storage.join();
  CHECK_EQ(errors.load(), 0);

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM other", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{50});

  db_close(database);
  database = open_declarations(dir, daily_partition_options());
  CHECK(query_all(*database, "SELECT count(*) FROM other", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{50});

  std::vector<db_table_info_t> tables;
  database->collect_tables(tables);
  CHECK_EQ(tables.size(), size_t{1});

  db_close(database);
}

/** Default table and namespace waits continue until their holders release. */
static void test_default_lock_wait() {
  test_dir_t dir;
  db::options_t options;
  CHECK(options.m_write_lock_timeout_ms == 0);

  db *database = open_declarations(dir, options);
  CHECK(write_id(*database, 1).ok());

  db_cursor *cursor = nullptr;
  REQUIRE(database->open_cursor("SELECT id FROM k", false, &cursor).ok());

  db_table *table = db_test_access::tables_handle(*database).find("k");
  std::unique_lock<ut_rwlock> names(db_test_access::namespace_lock(*database));

  std::atomic<bool> create_started{false};
  std::atomic<bool> create_done{false};
  std::atomic<bool> write_done{false};
  db_status_t created;
  db_status_t written;

  std::thread creator([&] {
    create_started = true;
    created = database->create_table("next", {});
    create_done = true;
  });

  std::thread writer([&] {
    const ingest_field_t field = field_int("extra", 30);
    const ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, &field, 1);
    batch_seq_t seq = 0;
    written = database->write("k", 1, &p, 1, seq);
    write_done = true;
  });

  CHECK(wait_for_worker(create_started));
  CHECK(wait_for_schema_writer(*table, write_done));
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  CHECK(!create_done.load());
  CHECK(!write_done.load());

  names.unlock();
  db_cursor::destroy(cursor);
  CHECK(wait_for_worker(create_done));
  CHECK(wait_for_worker(write_done));
  creator.join();
  writer.join();
  CHECK(created.ok());
  CHECK(written.ok());

  db_close(database);
}

/** Create and Drop honor the namespace deadline; Set requires no namespace. */
static void test_namespace_timeout() {
  test_dir_t dir;
  auto options = daily_partition_options();
  options.m_write_lock_timeout_ms = 20;

  db *database = open_declarations(dir, options);
  CHECK(database->create_table("m", {}).ok());
  {
    std::unique_lock<ut_rwlock> names(
        db_test_access::namespace_lock(*database));

    const db_status_t created = database->create_table("next", {});
    const db_status_t dropped = database->drop_table("m");
    CHECK(created.code() == DB_LOCK_TIMEOUT);
    CHECK(dropped.code() == DB_LOCK_TIMEOUT);
    CHECK(created.message().find("namespace") != std::string::npos);
    CHECK(dropped.message().find("namespace") != std::string::npos);
    CHECK(database->set_table_options("m", {0}).ok());
  }

  CHECK(database->drop_table("m").ok());
  CHECK(database->create_table("next", {}).ok());

  db_close(database);
}

/** Exercise cache lifetime and S/X behavior. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"default_lock_wait", test_default_lock_wait},
      {"namespace_timeout", test_namespace_timeout},
      {"drop_recreate", test_drop_recreate},
      {"drop_empty_and_validation", test_drop_empty_and_validation},
      {"drop_reader_wait", [] { test_drop_reader_wait(false); }},
      {"drop_reader_timeout", [] { test_drop_reader_wait(true); }},
      {"drop_failures", test_drop_failures},
      {"drop_stale_write", [] { test_drop_stale_request(0, true); }},
      {"drop_stale_write_recreated", [] { test_drop_stale_request(0, false); }},
      {"drop_stale_upgrade", [] { test_drop_stale_request(1, true); }},
      {"drop_stale_set", [] { test_drop_stale_request(2, false); }},
      {"drop_stale_drop", [] { test_drop_stale_request(3, false); }},
      {"drop_under_flush", test_drop_under_flush},
      {"explicit_table_lifecycle", test_explicit_table_lifecycle},
      {"create_validation", test_create_validation},
      {"create_failures", test_create_failures},
      {"create_races", test_create_races},
      {"catalog_with_waiting_set", test_catalog_with_waiting_set},
      {"table_lock_guard", test_table_lock_guard},
      {"table_lock_timeout", test_table_lock_timeout},
      {"cursor_write_timeout", test_cursor_write_timeout},
      {"cancelled_query_open/table_wait",
       [] { test_cancelled_query_open(false); }},
      {"cancelled_query_open/snapshot_wait",
       [] { test_cancelled_query_open(true); }},

      {"concurrent_creation", test_concurrent_creation},
      {"shared_lookup", test_shared_lookup},
      {"slot_lifetime", test_slot_lifetime},
      {"schema_versions", test_schema_versions},

      {"table_lock_modes", test_table_lock_modes},
      {"writer_progress", test_writer_progress},
      {"write_lock_modes", test_write_lock_modes},
      {"creation_lock", test_creation_lock},

      {"failed_schema_write/before_wait",
       [] { test_failed_schema_write(true); }},
      {"failed_schema_write/during_wait",
       [] { test_failed_schema_write(false); }},

      {"query_lock_order", test_query_lock_order},
      {"mixed_snapshot/hot",
       [] { test_mixed_snapshot_publication(false, false); }},
      {"mixed_snapshot/stored",
       [] { test_mixed_snapshot_publication(true, false); }},
      {"mixed_snapshot/wal_failure",
       [] { test_mixed_snapshot_publication(false, true); }},
      {"cancelled_table_wait/exclusive",
       [] { test_cancelled_table_wait(true); }},
      {"cancelled_table_wait/shared", [] { test_cancelled_table_wait(false); }},
  };

  return (test_main("table_lock-t", argc, argv, cases));
}
