/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_table-t.cc
 *    Stable cache entries and operation-owned table S/X locks.
 *
 *-------------------------------------------------------------------------
 */

#include "db_table.h"

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
#include "test_db.h"
#include "test_db_access.h"
#include "test_db_tables_access.h"
#include "test_point.h"
#include "test_storage_access.h"
#include "test_util.h"
#include "ut_shared_mutex.h"

/** One guard releases the right mode across reuse and scope exit. */
static void test_table_lock_guard() {
  db database;
  db_table_t table;
  {
    db_table_lock lock;
    CHECK(lock.acquire(database, table, db_table_lock_mode_t::SHARED).ok());
    CHECK(lock.holds(&table));
    CHECK(!lock.exclusive());
    {
      db_table_lock reader;
      CHECK(reader.acquire(database, table, db_table_lock_mode_t::SHARED).ok());
    }
    CHECK(!table.m_mutex.try_lock());

    lock.release();
    lock.release();
    CHECK(!lock.holds(&table));
    CHECK(lock.acquire(database, table, db_table_lock_mode_t::EXCLUSIVE).ok());
    CHECK(lock.exclusive());
    CHECK(!table.m_mutex.try_lock_shared());
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
  std::vector<db_table_t *> results(workers, nullptr);
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
        db_table_t *table = nullptr;
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
  for (const db_table_t *table : results) {
    CHECK(table == results[0]);
  }

  CHECK(tables.find("cpu") == results[0]);
  CHECK(db_tables_test_access::size(tables) == workers * 100 + 1);

  return;
}

/** A busy table does not prevent lookup of its stable cache entry. */
static void test_shared_lookup() {
  db_tables tables;
  db_table_t *original = nullptr;
  CHECK(tables.find_or_create("cpu", original).ok());
  std::atomic<bool> done{false};
  db_table_t *found = nullptr;
  std::unique_lock<ut_rwlock_t> held(original->m_mutex);
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
  db_table_t *first = nullptr;
  CHECK(tables.find_or_create("first", first).ok());
  CHECK(first->schema() == nullptr);
  for (size_t i = 0; i < 1000; ++i) {
    db_table_t *other = nullptr;
    CHECK(tables.find_or_create("slot_" + std::to_string(i), other).ok());
  }

  db_table_t *again = nullptr;
  CHECK(tables.find_or_create("first", again).ok());
  CHECK(again == first);
  schema_t schema;
  schema.m_name = "first";
  first->set_schema(std::make_shared<schema_t>(schema));
  CHECK(tables.find("first")->schema()->m_name == "first");
  CHECK(db_tables_test_access::size(tables) == 1001);

  return;
}

/** Shared readers coexist, X excludes both modes, and release may change
 * threads. */
static void test_table_lock_modes() {
  db_table_t table;
  table.m_mutex.lock_shared();
  bool shared = false;
  bool exclusive = true;
  std::thread reader([&] {
    shared = table.m_mutex.try_lock_shared();
    if (shared)
      table.m_mutex.unlock_shared();
    exclusive = table.m_mutex.try_lock();
    if (exclusive)
      table.m_mutex.unlock();
  });
  reader.join();
  CHECK(shared && !exclusive);
  std::thread closer([&] { table.m_mutex.unlock_shared(); });
  closer.join();
  CHECK(table.m_mutex.try_lock());
  bool blocked = false;
  std::thread other([&] {
    const bool read = table.m_mutex.try_lock_shared();
    if (read)
      table.m_mutex.unlock_shared();
    const bool write = table.m_mutex.try_lock();
    if (write)
      table.m_mutex.unlock();
    blocked = !read && !write;
    table.m_mutex.unlock();
  });
  other.join();
  CHECK(blocked);
  CHECK(table.m_mutex.try_lock_shared());
  table.m_mutex.unlock_shared();

  return;
}

/** A queued writer excludes later readers until it has completed. */
static void test_writer_progress() {
  db_table_t table;
  table.m_mutex.lock_shared();
  std::atomic<bool> entered{false};
  std::atomic<bool> finish{false};
  std::thread writer([&] {
    table.m_mutex.lock();
    entered = true;
    while (!finish.load())
      std::this_thread::yield();
    table.m_mutex.unlock();
  });
  bool queued = false;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (!table.m_mutex.try_lock_shared()) {
      queued = true;
      break;
    }

    table.m_mutex.unlock_shared();
    std::this_thread::yield();
  }

  CHECK(queued);
  CHECK(!entered.load());
  table.m_mutex.unlock_shared();
  while (!entered.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  CHECK(entered.load());
  const bool unexpected_reader = table.m_mutex.try_lock_shared();
  CHECK(!unexpected_reader);
  if (unexpected_reader)
    table.m_mutex.unlock_shared();
  finish = true;
  writer.join();
  CHECK(table.m_mutex.try_lock_shared());
  table.m_mutex.unlock_shared();

  return;
}

/** Wait until a worker finishes, with a deadline so a lock failure can unwind.
 */
static bool wait_for_worker(const std::atomic<bool> &done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  return (done.load());
}

/** The caller holds S, so a refused reader identifies a queued X acquisition.
 */
static bool wait_for_schema_writer(db_table_t &table,
                                   const std::atomic<bool> &done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    if (!table.m_mutex.try_lock_shared()) {
      return (true);
    }

    table.m_mutex.unlock_shared();
    std::this_thread::yield();
  }

  return (false);
}

/** A cursor allows existing-column writes but delays new columns until close.
Waiting for X must not prevent another table from writing and flushing. */
static void test_write_lock_modes() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return;
  }

  CHECK(write_id(*database, 1).ok());
  db_cursor *cursor = nullptr;
  CHECK(database->cursor_open("SELECT id FROM k", false, &cursor).ok());
  CHECK(write_id(*database, 2).ok());

  db_table_t *table = db_test_access::tables_handle(*database).find("k");
  CHECK(table != nullptr);
  const schema_ref_t schema = table->schema();
  const ingest_field_t field = field_int("extra", 30);
  const ingest_point_t p = point(T0, nullptr, 0, &field, 1);
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
    db_flush_info_t info;
    flush_status = database->flush(info);
    other_done = true;
  });
  CHECK(wait_for_worker(other_done));

  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 1);
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
    CHECK(result.m_rows[0][0].m_i == 3);
    CHECK(result.m_rows[0][1].m_i == 1);
    CHECK(result.m_rows[0][2].m_i == 30);
  }

  db_close(database);

  return;
}

/** First writes need X too; concurrent creators resolve the latest column IDs.
Each row omits the columns introduced by the other creators. */
static void test_creation_lock() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return;
  }

  db_table_t *table = nullptr;
  CHECK(db_test_access::tables_handle(*database)
            .find_or_create("created", table)
            .ok());
  std::shared_lock<ut_rwlock_t> held(table->m_mutex);
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
      const ingest_point_t p = point(T0, nullptr, 0, &field, 1);
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
  CHECK(schema != nullptr && schema->m_cols.size() == count + 1);
  if (schema != nullptr) {
    for (size_t i = 0; i < schema->m_cols.size(); ++i) {
      CHECK(schema->m_cols[i].m_id == i);
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
    CHECK(result.m_rows[0][0].m_i == count);
    for (size_t i = 0; i < count; ++i) {
      CHECK(result.m_rows[0][i + 1].m_i == static_cast<int64_t>(i + 1));
    }
  }

  db_close(database);

  return;
}

/** Write one field to a named table, creating it when absent. */
static db_status_t write_table_value(db &database, const char *name,
                                     const char *column) {
  const ingest_field_t field = field_int(column, 1);
  const ingest_point_t p = point(T0, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;

  return (database.write(name, strlen(name), &p, 1, seq));
}

/** A failed database rejects writes without waiting for an existing cursor.
@param[in] fail_before_wait fail before starting the write, or while it waits */
static void test_failed_schema_write(bool fail_before_wait) {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_wal_sync = true;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return;
  }

  CHECK(write_table_value(*database, "m", "v").ok());
  db_cursor *cursor = nullptr;
  CHECK(database->cursor_open("SELECT v FROM m", false, &cursor).ok());
  if (fail_before_wait) {
    db_test_access::fail_next_wal_sync(*database);
    CHECK(write_table_value(*database, "m", "v").code() == DB_OUTCOME_UNKNOWN);
  }

  const ingest_field_t added = field_int("extra", 2);
  const ingest_point_t p = point(T0, nullptr, 0, &added, 1);
  db_status_t result;
  batch_seq_t seq = 0;
  std::atomic<bool> done{false};
  std::thread writer([&] {
    result = database->write("m", 1, &p, 1, seq);
    done = true;
  });
  if (!fail_before_wait) {
    db_table_t *table = db_test_access::tables_handle(*database).find("m");
    CHECK(wait_for_schema_writer(*table, done));
    storage_test_access::fail_next_rollback(
        db_test_access::storage_handle(*database));
    db_test_access::fail_next_flush(*database);
    db_flush_info_t info;
    CHECK(database->flush(info).code() == DB_OUTCOME_UNKNOWN);
  }

  /* Keep the S holder alive: failure, rather than cursor close, must unblock X.
   */
  CHECK(wait_for_worker(done));
  query_rows_t rows;
  CHECK(cursor->fetch(rows).ok());
  CHECK(rows.size() == 1 && rows[0][0].m_i == 1);
  db_cursor::destroy(cursor);
  writer.join();
  CHECK(result.code() == DB_FENCED && seq == 0);
  db_table_t *table = db_test_access::tables_handle(*database).find("m");
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
  const bool reusable = table->m_mutex.try_lock();
  CHECK(reusable);
  if (reusable) {
    table->m_mutex.unlock();
  }

  db_close(database);

  return;
}

/** Wait for one observed synchronization state, with a bounded test deadline.
 */
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

/** A late-created table must restart the complete sorted lock acquisition.
Two queries and two schema writers formerly formed a four-operation cycle. */
static void test_query_lock_order() {
  test_dir_t dir;
  db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  if (database == nullptr) {
    return;
  }

  CHECK(write_table_value(*database, "b", "v").ok());
  db_table_t *b = db_test_access::tables_handle(*database).find("b");
  std::unique_lock<ut_shared_mutex_t> snapshot_gate(
      db_test_access::snapshot_mutex(*database));
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
  require_progress(wait_for_state([&] {
    if (!b->m_mutex.try_lock()) {
      return (true);
    }

    b->m_mutex.unlock();
    return (false);
  }));

  db_status_t created;
  std::thread creator(
      [&] { created = write_table_value(*database, "a", "v"); });
  creator.join();
  CHECK(created.ok());
  db_table_t *a = db_test_access::tables_handle(*database).find("a");
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
    if (!a->m_mutex.try_lock()) {
      return (true);
    }

    a->m_mutex.unlock();
    return (false);
  }));
  std::thread write_a([&] {
    statuses[3] = write_table_value(*database, "a", "extra");
    a_done = true;
    ++completed;
  });
  require_progress(wait_for_schema_writer(*a, a_done));

  snapshot_gate.unlock();
  require_progress(wait_for_state([&] { return (completed.load() == 4); }));
  first.join();
  second.join();
  write_a.join();
  write_b.join();
  for (const db_status_t &status : statuses) {
    CHECK(status.ok());
  }

  CHECK(results[0].m_rows.size() == 1 && results[0].m_rows[0][0].m_i == 4);
  CHECK(results[1].m_rows.size() == 1 && results[1].m_rows[0][0].m_i == 2);
  db_close(database);

  return;
}

/** Cancellation releases waiter accounting, preserving writer preference.
The predicate also reenters the lock API to verify it runs outside state locks.
@param[in] exclusive cancel an X waiter, or an S waiter */
static void test_cancelled_table_wait(bool exclusive) {
  db_table_t table;
  if (exclusive) {
    table.m_mutex.lock_shared();
  } else {
    table.m_mutex.lock();
  }

  std::atomic<bool> allowed{true};
  std::atomic<size_t> checks{0};
  std::atomic<bool> done{false};
  bool acquired = true;
  std::thread waiter([&] {
    auto keep_waiting = [&] {
      const bool shared = table.m_mutex.try_lock_shared();
      if (shared) {
        table.m_mutex.unlock_shared();
      }

      ++checks;
      return (allowed.load());
    };
    acquired = exclusive ? table.m_mutex.lock_while(keep_waiting)
                         : table.m_mutex.lock_shared_while(keep_waiting);
    if (acquired) {
      if (exclusive) {
        table.m_mutex.unlock();
      } else {
        table.m_mutex.unlock_shared();
      }
    }

    done = true;
  });
  require_progress(wait_for_state([&] { return (checks.load() >= 2); }));
  if (exclusive) {
    const bool bypassed = table.m_mutex.try_lock_shared();
    CHECK(!bypassed);
    if (bypassed) {
      table.m_mutex.unlock_shared();
    }
  }

  allowed = false;
  require_progress(wait_for_worker(done));
  waiter.join();
  CHECK(!acquired);
  if (exclusive) {
    const bool released = table.m_mutex.try_lock_shared();
    CHECK(released);
    if (released) {
      table.m_mutex.unlock_shared();
    }

    table.m_mutex.unlock_shared();
  } else {
    table.m_mutex.unlock();
  }

  const bool reusable = table.m_mutex.try_lock();
  CHECK(reusable);
  if (reusable) {
    table.m_mutex.unlock();
  }

  return;
}

/** Cancellation releases a query waiting on a table, without releasing its X
 * holder. */
static void test_cancelled_query_open(bool snapshot_wait) {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, nullptr, &database).ok());
  if (database == nullptr) {
    return;
  }

  CHECK(write_table_value(*database, "m", "v").ok());
  db_table_t *table = db_test_access::tables_handle(*database).find("m");
  if (snapshot_wait) {
    db_test_access::snapshot_mutex(*database).lock();
  } else {
    table->m_mutex.lock();
  }

  query_cancel cancel;
  db_cursor *cursor = nullptr;
  db_status_t status;
  std::atomic<bool> started{false};
  std::atomic<bool> done{false};
  std::thread reader([&] {
    started = true;
    status = database->cursor_open("SELECT v FROM m", false, &cursor, &cancel);
    done = true;
  });
  CHECK(wait_for_worker(started));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  CHECK(!done.load());
  cancel.request();
  const bool cancelled = wait_for_worker(done);
  CHECK(cancelled);
  if (snapshot_wait) {
    db_test_access::snapshot_mutex(*database).unlock();
  } else {
    table->m_mutex.unlock();
  }
  reader.join();
  CHECK(status.code() == DB_INTERRUPTED);
  CHECK(cursor == nullptr);
  db_cursor::destroy(cursor);

  CHECK(database->cursor_open("SELECT v FROM m", false, &cursor).ok());
  db_cursor::destroy(cursor);
  db_close(database);
}

/** Exercise cache lifetime and S/X behavior. */
int main() {
  test_table_lock_guard();
  test_cancelled_query_open(false);
  test_cancelled_query_open(true);
  test_concurrent_creation();
  test_shared_lookup();
  test_slot_lifetime();
  test_table_lock_modes();
  test_writer_progress();
  test_write_lock_modes();
  test_creation_lock();
  test_failed_schema_write(true);
  test_failed_schema_write(false);
  test_query_lock_order();
  test_cancelled_table_wait(true);
  test_cancelled_table_wait(false);

  return (test_result("db_table-t"));
}
