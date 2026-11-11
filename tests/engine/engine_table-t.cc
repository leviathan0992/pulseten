/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine_table-t.cc
 *    Concurrent creation converges on stable entries; lookups share the lock.
 *
 *-------------------------------------------------------------------------
 */

#include "engine_table.h"

#include <atomic>
#include <chrono>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "test_util.h"

/** Racing first users get the same object, retained through later inserts. */
static void test_concurrent_creation() {
  engine_tables_t tables;
  constexpr size_t workers = 16;
  std::vector<engine_table_ref_t> results(workers);
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
      engine_table_ref_t table;
      for (size_t n = 0; n < 100; ++n) {
        if (!tables.find_or_create("table_" + std::to_string(i * 100 + n),
                                    table).ok()) {
          results[i].reset();
          return;
        }
      }
    });
  }
  start = true;
  for (std::thread &thread : threads) {
    thread.join();
  }
  CHECK(results[0].get() != nullptr);
  for (const engine_table_ref_t &table : results) {
    CHECK(table.get() == results[0].get());
  }
  engine_table_ref_t found;
  CHECK(tables.find_or_create("cpu", found).ok());
  CHECK(found.get() == results[0].get());
  CHECK(tables.size() == 1);
}

/** Finding an existing table does not wait for other registry readers. */
static void test_shared_lookup() {
  engine_tables_t tables;
  engine_table_ref_t original;
  CHECK(tables.find_or_create("cpu", original).ok());
  std::atomic<bool> done{false};
  engine_table_ref_t found;
  std::shared_lock<std::shared_mutex> reader(tables.m_mutex);
  std::thread lookup([&] {
    if (tables.find_or_create("cpu", found).ok()) {
      done = true;
    }
  });
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(5);
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  CHECK(done.load());
  reader.unlock();
  lookup.join();
  CHECK(found.get() == original.get());
}

/** Failed operations and dropped ordinary names do not accumulate entries. */
static void test_reclaim_names() {
  engine_tables_t tables;
  engine_table_ref_t entry;
  for (size_t i = 0; i < 1000; ++i) {
    CHECK(tables.find_or_create("rejected_" + std::to_string(i), entry).ok());
    entry.reset();
    CHECK(tables.size() == 0);
  }

  CHECK(tables.find_or_create("ordinary", entry).ok());
  entry->m_is_ordinary = true;
  entry.reset();
  CHECK(tables.size() == 1);

  engine_table_ref_t other_user;
  CHECK(tables.find_or_create("ordinary", entry).ok());
  CHECK(tables.find_or_create("ordinary", other_user).ok());
  entry->m_is_ordinary = false;
  entry.reset();
  CHECK(tables.size() == 1);
  CHECK(other_user->m_name == "ordinary");
  other_user.reset();
  CHECK(tables.size() == 0);
}

/** Lookup and final release may race without invalidating another user's pin. */
static void test_concurrent_reclaim() {
  engine_tables_t tables;
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (size_t worker = 0; worker < 8; ++worker) {
    threads.emplace_back([&, worker] {
      for (size_t i = 0; i < 1000; ++i) {
        const std::string name = "temporary_" + std::to_string((i + worker) % 8);
        engine_table_ref_t entry;
        if (!tables.find_or_create(name, entry).ok()) {
          failed = true;
          return;
        }
        std::this_thread::yield();
        if (entry->m_name != name) {
          failed = true;
        }
      }
    });
  }
  for (std::thread &thread : threads) {
    thread.join();
  }
  CHECK(!failed.load());
  CHECK(tables.size() == 0);
}

/** Published time-series entries stay stable without a per-lookup pin. */
static void test_permanent_table() {
  engine_tables_t tables;
  schema_t schema;
  schema.m_name = "cpu";
  engine_table_ref_t entry;
  CHECK(tables.find_or_create("cpu", entry).ok());
  engine_table_t *original = entry.get();
  entry->m_schema.store(&schema);
  entry.reset();
  CHECK(tables.size() == 1);
  CHECK(tables.find_or_create("cpu", entry).ok());
  CHECK(entry.get() == original);
  CHECK(entry->m_references.load() == 0);
  entry.reset();
  CHECK(tables.size() == 1);
}

/** Moving a lease preserves its pin; replacing it releases the old name. */
static void test_move_reference() {
  engine_tables_t tables;
  engine_table_ref_t first;
  CHECK(tables.find_or_create("first", first).ok());
  engine_table_ref_t moved(std::move(first));
  CHECK(first.get() == nullptr && moved->m_name == "first");
  CHECK(tables.find_or_create("second", first).ok());
  first = std::move(moved);
  CHECK(moved.get() == nullptr && first->m_name == "first");
  CHECK(tables.size() == 1);
  CHECK(tables.find_or_create(first->m_name, first).ok());
  CHECK(first->m_name == "first");
  first.reset();
  CHECK(tables.size() == 0);
}

/** Run registry concurrency tests. */
int main() {
  test_concurrent_creation();
  test_shared_lookup();
  test_reclaim_names();
  test_concurrent_reclaim();
  test_permanent_table();
  test_move_reference();
  return (test_result("engine_table-t"));
}
