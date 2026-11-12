/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable-t.cc
 *    Tests for the memtable: column directories, ordered publication,
 *    failure, and concurrent writers.
 *
 *-------------------------------------------------------------------------
 */

#include "memtable.h"

#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "ingest.h"
#include "schema.h"
#include "test_memtable.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"

/** Plan a one-point write of the given fields against the memtable's current
schema. */
static void plan_write(memtable &mt, const char *table_name,
                       const ingest_field_t *fields, uint32_t n_fields,
                       ingest_plan_t &plan) {
  const schema_ref_t schema = mt.find_schema(table_name);
  ingest_point_t p = point(T0, nullptr, 0, fields, n_fields);
  CHECK(plan_for(schema.get(), table_name, &p, 1, plan).ok());

  return;
}

/** Capacity counts reserved writes and excludes retired batches even while
snapshots retain them. Zero disables the threshold. */
static void test_capacity() {
  ingest_field_t fields[] = {field_int("n", 1)};
  ingest_point_t p = point(T0, nullptr, 0, fields, 1);
  ingest_plan_t initial;
  CHECK(plan_for(nullptr, "m", &p, 1, initial).ok());
  const uint64_t bytes = initial.m_batch.data_bytes();
  CHECK(bytes > 1);

  for (const uint64_t limit : {uint64_t{0}, bytes - 1, bytes, bytes + 1}) {
    memtable *mt = nullptr;
    CHECK(memtable::create(0, limit, &mt).ok());
    CHECK(mt->has_space());

    ingest_plan_t plan;
    plan_write(*mt, "m", fields, 1, plan);
    CHECK(mt->reserve(1, plan).ok());
    const bool expected = limit == 0 || limit > bytes;
    CHECK(mt->has_space() == expected);
    mt->publish(1);
    CHECK(mt->has_space() == expected);

    memtable_snapshot_t snapshot;
    snapshot_all(*mt, snapshot);
    mt->trim(1);
    memtable_stats_t stats;
    mt->read_stats(stats);
    CHECK(stats.m_retired_writes == 1);
    CHECK(mt->has_space());
    mt->release_snapshot(snapshot);
    memtable::destroy(mt);
  }

  return;
}

/** A reserved write is seen by planning at once and by readers only once
published. */
static void test_reserve_then_publish() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());

  ingest_field_t f[] = {field_int("n", 1)};
  ingest_plan_t plan;
  plan_write(*mt, "m", f, 1, plan);
  CHECK(mt->reserve(1, plan).ok());

  /* Reserved: planning sees the new table, readers do not. */
  const schema_ref_t schema = mt->find_schema("m");
  CHECK(schema != nullptr && schema->m_cols.size() == 2);

  memtable_snapshot_t snapshot;
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_visible_seq == 0 && snapshot.m_tables.empty());
  mt->release_snapshot(snapshot);

  /* Published: readers see it. */
  mt->publish(1);
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_visible_seq == 1);
  CHECK(snapshot.m_tables.size() == 1);

  const memtable_table_view_t &view = snapshot.m_tables["m"];
  CHECK(view.m_schema == schema);
  CHECK(view.m_batches.size() == 1 &&
        view.m_batches[0]->m_cols[1].m_ints[0] == 1);

  mt->release_snapshot(snapshot);
  memtable::destroy(mt);

  return;
}

/** A batch keeps the columns it was planned against; views use the newest
visible directory. */
static void test_column_prefixes() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_plan_t first;
  plan_write(*mt, "m", f1, 1, first);
  CHECK(mt->reserve(1, first).ok());

  /* Planned before another write appended a column, but reserved after it. */
  ingest_plan_t stale;
  plan_write(*mt, "m", f1, 1, stale);

  ingest_field_t f2[] = {field_int("n", 2), field_double("extra", 0.5)};
  ingest_plan_t grow;
  plan_write(*mt, "m", f2, 2, grow);
  CHECK(mt->reserve(2, grow).ok());
  CHECK(mt->reserve(3, stale).ok());

  mt->publish(1);
  mt->publish(2);
  mt->publish(3);

  memtable_snapshot_t snapshot;
  snapshot_all(*mt, snapshot);

  const memtable_table_view_t &view = snapshot.m_tables["m"];
  CHECK(view.m_batches.size() == 3);

  /* The view includes the appended column despite the last batch being shorter.
   */
  CHECK(view.m_schema->m_cols.size() == 3);
  CHECK(view.m_batches[0]->m_cols.size() == 2);
  CHECK(view.m_batches[1]->m_cols.size() == 3);
  CHECK(view.m_batches[2]->m_cols.size() == 2);

  mt->release_snapshot(snapshot);
  memtable::destroy(mt);

  return;
}

/** Directories remain pinned by active snapshots and are reclaimed once
old batches and their readers retire. Reserved future columns stay invisible. */
static void test_schema_prefix_lifetime() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());
  ingest_field_t a[] = {field_int("a", 1)};
  ingest_plan_t plan;
  plan_write(*mt, "m", a, 1, plan);
  CHECK(mt->reserve(1, plan).ok());
  mt->publish(1);
  std::weak_ptr<const schema_t> old = mt->find_schema("m");

  memtable_snapshot_t snapshot;
  mt->pin_snapshot(snapshot);
  ingest_field_t b[] = {field_int("a", 2), field_int("b", 3)};
  plan_write(*mt, "m", b, 2, plan);
  CHECK(mt->reserve(2, plan).ok());
  CHECK(mt->complete_snapshot(0, snapshot).ok());
  CHECK(snapshot.m_tables["m"].m_schema->m_cols.size() == 2);
  CHECK(snapshot.m_visible_seq == 1);
  mt->publish(2);
  mt->trim(2);
  CHECK(!old.expired());
  CHECK(snapshot.m_tables["m"].m_schema->m_cols.size() == 2);
  mt->release_snapshot(snapshot);
  CHECK(old.expired());
  CHECK(mt->find_schema("m")->m_cols.size() == 3);
  memtable::destroy(mt);

  return;
}

/** Plans that no longer fit the current schema are refused. */
static void test_plan_must_fit() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_plan_t create;
  plan_write(*mt, "m", f1, 1, create);

  /* A second "create" of the same table, planned before the first was
  reserved. */
  ingest_plan_t create_again;
  plan_write(*mt, "m", f1, 1, create_again);

  ingest_field_t fa[] = {field_int("a", 1)};
  ingest_field_t fb[] = {field_int("b", 1)};

  CHECK(mt->reserve(1, create).ok());
  CHECK(mt->reserve(2, create_again).code() == DB_CORRUPTION);

  /* Two column additions planned against the same directory: only the first
  fits. */
  ingest_plan_t add_a;
  ingest_plan_t add_b;
  plan_write(*mt, "m", fa, 1, add_a);
  plan_write(*mt, "m", fb, 1, add_b);
  CHECK(mt->reserve(2, add_a).ok());
  CHECK(mt->reserve(3, add_b).code() == DB_CORRUPTION);

  /* A write to a table the memtable does not have. */
  const schema_ref_t m = mt->find_schema("m");
  schema_t other = *m;
  other.m_name = "other";
  ingest_point_t p = point(T0, nullptr, 0, f1, 1);
  ingest_plan_t missing;
  CHECK(plan_for(&other, "other", &p, 1, missing).ok());
  CHECK(mt->reserve(3, missing).code() == DB_CORRUPTION);

  /* Rejected reserves changed nothing: 3 is still the next number. */
  CHECK(mt->find_schema("m")->m_cols.size() == 3);
  ingest_plan_t next;
  plan_write(*mt, "m", fb, 1, next);
  ingest_plan_t incomplete = next;
  incomplete.m_batch.m_cols.pop_back();
  CHECK(mt->reserve(3, incomplete).code() == DB_CORRUPTION);
  CHECK(mt->reserve(3, next).ok());

  memtable::destroy(mt);

  return;
}

/** Reserve n one-point writes to table "m". */
static void reserve_n(memtable &mt, int n) {
  ingest_field_t f[] = {field_int("n", 1)};

  for (int i = 1; i <= n; ++i) {
    ingest_plan_t plan;
    plan_write(mt, "m", f, 1, plan);
    CHECK(mt.reserve(static_cast<batch_seq_t>(i), plan).ok());
  }

  return;
}

/** Take a snapshot and read its visible sequence boundary.
@return the visible_seq of a new snapshot */
static batch_seq_t visible(memtable &mt) {
  memtable_snapshot_t snapshot;
  snapshot_all(mt, snapshot);

  const batch_seq_t seq = snapshot.m_visible_seq;
  mt.release_snapshot(snapshot);

  return (seq);
}

/** The owner publishes a completed prefix without waiting inside memtable. */
static void test_publish_prefix() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());
  reserve_n(*mt, 3);
  CHECK(visible(*mt) == 0);
  mt->publish(2);
  CHECK(visible(*mt) == 2);
  mt->publish(3);
  CHECK(visible(*mt) == 3);
  memtable::destroy(mt);

  return;
}

/** Shared state of the concurrent test: the lock that hands out sequence
numbers, as the write path will. */
struct writers_t {
  /** Memtable under test. */
  memtable *m_mt{nullptr};

  /** Hands out sequence numbers and orders reservations. */
  std::mutex m_mutex;

  /** Sequence number of the next write. */
  batch_seq_t m_next_seq{1};
};

struct writer_arg_t {
  /** State shared by every writer. */
  writers_t *m_shared{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Writes to make. */
  int m_count{0};
};

/** Write count times: plan, reserve and publish under the owner's shared lock.
Every 50th write adds a column.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  writers_t *w = arg->m_shared;
  char name[32];

  for (int i = 0; i < arg->m_count; ++i) {
    /* Every 50th write adds a column. */
    snprintf(name, sizeof(name), "c%d_%d", arg->m_thread_no, i);
    ingest_field_t f[] = {field_int("n", i), field_int(name, i)};
    const uint32_t n_fields = i % 50 == 0 ? 2 : 1;

    batch_seq_t seq;
    {
      std::lock_guard<std::mutex> lock(w->m_mutex);

      ingest_plan_t plan;
      plan_write(*w->m_mt, "m", f, n_fields, plan);

      seq = w->m_next_seq++;
      CHECK(w->m_mt->reserve(seq, plan).ok());
      w->m_mt->publish(seq);
    }
  }

  return;
}

/** Owner-ordered reservations and publication leave every write visible
exactly once. */
static void test_concurrent_writers() {
  writers_t shared;
  CHECK(memtable::create(0, 0, &shared.m_mt).ok());

  const int threads = 8;
  const int count = 200;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_shared = &shared;
    args[t].m_thread_no = t;
    args[t].m_count = count;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
  }

  memtable_snapshot_t snapshot;
  snapshot_all(*shared.m_mt, snapshot);
  CHECK(snapshot.m_visible_seq == static_cast<batch_seq_t>(threads * count));

  /* 2 base columns plus one new column per thread every 50 writes. */
  const memtable_table_view_t &view = snapshot.m_tables["m"];
  CHECK(view.m_batches.size() == static_cast<size_t>(threads * count));
  CHECK(view.m_schema->m_cols.size() ==
        static_cast<size_t>(2 + threads * (count / 50)));

  shared.m_mt->release_snapshot(snapshot);
  memtable::destroy(shared.m_mt);

  return;
}

/** Reserve and publish one write of value v to a table. */
static void write_value(memtable &mt, batch_seq_t seq, const char *table_name,
                        int64_t v) {
  ingest_field_t f[] = {field_int("n", v)};
  ingest_plan_t plan;
  plan_write(mt, table_name, f, 1, plan);
  CHECK(mt.reserve(seq, plan).ok());
  mt.publish(seq);

  return;
}

/** Collect hands out only visible writes; trim takes them out but keeps the
schemas. */
static void test_collect_and_trim() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "a", 10);
  write_value(*mt, 2, "b", 20);

  /* Write 3 is reserved but not visible, so it is not collected. */
  ingest_field_t f[] = {field_int("n", 30)};
  ingest_plan_t plan;
  plan_write(*mt, "a", f, 1, plan);
  CHECK(mt->reserve(3, plan).ok());

  std::vector<memtable_flush_item_t> items;
  mt->collect_unflushed(items);
  CHECK(items.size() == 2);
  CHECK(items[0].m_seq == 1 && items[0].m_batch->m_table_name == "a");
  CHECK(items[1].m_seq == 2 && items[1].m_schema->m_name == "b");

  mt->trim(2);

  memtable_stats_t stats;
  mt->read_stats(stats);
  CHECK(stats.m_flushed_seq == 2 && stats.m_live_writes == 1);
  CHECK(stats.m_live_rows == 1 && stats.m_retired_writes == 0);

  /* Retired writes leave the snapshot; schemas stay for planning. */
  memtable_snapshot_t snapshot;
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_base_seq == 2 && snapshot.m_visible_seq == 2 &&
        snapshot.m_tables.empty());
  mt->release_snapshot(snapshot);
  CHECK(mt->find_schema("b") != nullptr);

  mt->publish(3);
  mt->collect_unflushed(items);
  CHECK(items.size() == 1 && items[0].m_seq == 3);

  memtable::destroy(mt);

  return;
}

/** A snapshot keeps retired batches until it is released. */
static void test_snapshot_pins_retired() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "m", 1);
  write_value(*mt, 2, "m", 2);

  memtable_snapshot_t old_snap;
  snapshot_all(*mt, old_snap);

  mt->trim(2);

  /* The old snapshot still reads its batches. */
  memtable_stats_t stats;
  mt->read_stats(stats);
  CHECK(stats.m_retired_writes == 2);

  const memtable_table_view_t &view = old_snap.m_tables["m"];
  CHECK(view.m_batches.size() == 2);
  CHECK(view.m_batches[0]->m_cols[1].m_ints[0] == 1);
  CHECK(view.m_batches[1]->m_cols[1].m_ints[0] == 2);

  /* A newer snapshot does not see them and does not hold them. */
  memtable_snapshot_t new_snap;
  snapshot_all(*mt, new_snap);
  CHECK(new_snap.m_tables.empty());
  mt->release_snapshot(new_snap);

  mt->read_stats(stats);
  CHECK(stats.m_retired_writes == 2);

  /* Releasing the old one frees them. */
  mt->release_snapshot(old_snap);
  CHECK(old_snap.m_entries.empty());

  mt->read_stats(stats);
  CHECK(stats.m_retired_writes == 0);

  memtable::destroy(mt);

  return;
}

/** Stored tables restore their current directories, and writes continue
after F. */
static void test_restore_schema() {
  /* Writes up to 10 are in storage; the directory now has three columns. */
  memtable *mt = nullptr;
  CHECK(memtable::create(10, 0, &mt).ok());

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_point_t p1 = point(T0, nullptr, 0, f1, 1);
  ingest_plan_t first;
  CHECK(plan_for(nullptr, "m", &p1, 1, first).ok());
  const schema_t initial_schema = schema_after(nullptr, first);

  ingest_field_t f2[] = {field_int("n", 1), field_double("d", 1.5)};
  ingest_point_t p2 = point(T0, nullptr, 0, f2, 2);
  ingest_plan_t second;
  CHECK(plan_for(&initial_schema, "m", &p2, 1, second).ok());
  const schema_t expanded_schema = schema_after(&initial_schema, second);

  CHECK(mt->restore_schema(expanded_schema).ok());
  CHECK(mt->restore_schema(expanded_schema).code() == DB_CORRUPTION);
  CHECK(mt->find_schema("m")->m_cols.size() == 3);

  /* A replayed write planned before the column append still fits. */
  ingest_plan_t short_write;
  CHECK(plan_for(&initial_schema, "m", &p1, 1, short_write).ok());
  ingest_plan_t bad = short_write;
  bad.m_batch.m_cols[1].m_type = col_type_t::DOUBLE;
  CHECK(mt->reserve(11, bad).code() == DB_CORRUPTION);
  bad = short_write;
  bad.m_batch.m_cols[1].m_col_id = 2;
  CHECK(mt->reserve(11, bad).code() == DB_CORRUPTION);
  CHECK(mt->reserve(11, short_write).ok());
  mt->publish(11);

  memtable_snapshot_t snapshot;
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_base_seq == 10 && snapshot.m_visible_seq == 11);
  CHECK(snapshot.m_tables["m"].m_batches.size() == 1);
  CHECK(snapshot.m_tables["m"].m_schema->m_cols.size() == 3);
  CHECK(snapshot.m_tables["m"].m_batches[0]->m_cols.size() == 2);
  mt->release_snapshot(snapshot);

  std::vector<memtable_flush_item_t> items;
  mt->collect_unflushed(items);
  CHECK(items.size() == 1 && items[0].m_schema->m_cols.size() == 3);
  memtable::destroy(mt);

  return;
}

struct flusher_arg_t {
  /** Memtable under test. */
  memtable *m_mt{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *m_stop{nullptr};

  /** Protects *stop. */
  std::mutex *m_stop_mutex{nullptr};

  /** Rounds that retired something. */
  int m_rounds{0};
};

/** Retire every visible write, over and over, as a flush would.
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

    std::vector<memtable_flush_item_t> items;
    arg->m_mt->collect_unflushed(items);

    if (!items.empty()) {
      arg->m_mt->trim(items.back().m_seq);
      arg->m_rounds++;
    }
  }

  return;
}

struct reader_arg_t {
  /** Memtable under test. */
  memtable *m_mt{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *m_stop{nullptr};

  /** Protects *stop. */
  std::mutex *m_stop_mutex{nullptr};

  /** Calls that failed. */
  int m_errors{0};
};

/** Read every value of every snapshot while batches are retired.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void reader_main(reader_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->m_stop_mutex);
      if (*arg->m_stop) {
        break;
      }
    }

    memtable_snapshot_t snapshot;
    snapshot_all(*arg->m_mt, snapshot);

    size_t batches = 0;
    std::map<std::string, memtable_table_view_t>::iterator it;

    for (it = snapshot.m_tables.begin(); it != snapshot.m_tables.end(); ++it) {
      for (size_t i = 0; i < it->second.m_batches.size(); ++i) {
        const batch_t *b = it->second.m_batches[i];
        if (b->m_cols[1].m_ints[0] < 0 || b->m_n_rows != 1) {
          arg->m_errors++;
        }

        batches++;
      }
    }

    if (batches != snapshot.m_visible_seq - snapshot.m_base_seq) {
      arg->m_errors++;
    }

    arg->m_mt->release_snapshot(snapshot);
  }

  return;
}

/** Writers, a retiring flusher and readers run together; readers always see
consistent snapshots. */
static void test_concurrent_trim() {
  writers_t shared;
  CHECK(memtable::create(0, 0, &shared.m_mt).ok());

  bool stop = false;
  std::mutex stop_mutex;

  flusher_arg_t flusher;
  flusher.m_mt = shared.m_mt;
  flusher.m_stop = &stop;
  flusher.m_stop_mutex = &stop_mutex;
  std::thread flusher_thread(flusher_main, &flusher);

  reader_arg_t reader;
  reader.m_mt = shared.m_mt;
  reader.m_stop = &stop;
  reader.m_stop_mutex = &stop_mutex;
  std::thread reader_thread(reader_main, &reader);

  const int threads = 4;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_shared = &shared;
    args[t].m_thread_no = t;
    args[t].m_count = 300;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
  }

  {
    std::lock_guard<std::mutex> lock(stop_mutex);
    stop = true;
  }

  flusher_thread.join();
  reader_thread.join();

  CHECK(reader.m_errors == 0);
  printf("concurrent trim: %d flush rounds\n", flusher.m_rounds);

  /* Retire the rest; with no snapshot open nothing stays behind. */
  std::vector<memtable_flush_item_t> items;
  shared.m_mt->collect_unflushed(items);
  if (!items.empty()) {
    shared.m_mt->trim(items.back().m_seq);
  }

  memtable_stats_t stats;
  shared.m_mt->read_stats(stats);
  CHECK(stats.m_flushed_seq == static_cast<batch_seq_t>(threads * 300));
  CHECK(stats.m_live_writes == 0 && stats.m_live_rows == 0);
  CHECK(stats.m_retired_writes == 0);

  memtable::destroy(shared.m_mt);

  return;
}

/** Copied directories are independent; retained directories survive later
 * writes. */
static void test_schema_ownership() {
  memtable *mt = nullptr;
  CHECK(memtable::create(0, 0, &mt).ok());
  ingest_field_t first[] = {field_int("n", 1)};
  ingest_plan_t plan;
  plan_write(*mt, "m", first, 1, plan);
  CHECK(mt->reserve(1, plan).ok());

  std::vector<schema_ref_t> borrowed;
  mt->reference_schemas(borrowed);
  CHECK(borrowed.size() == 1);
  if (borrowed.size() == 1) {
    CHECK(borrowed[0] == mt->find_schema("m"));
    CHECK(borrowed[0]->m_name == "m");

    ingest_field_t second[] = {field_int("n", 2), field_double("d", 1.5)};
    plan_write(*mt, "m", second, 2, plan);
    CHECK(mt->reserve(2, plan).ok());
    CHECK(borrowed[0]->m_cols.size() == 2);
    CHECK(mt->find_schema("m")->m_cols.size() == 3);
    mt->reference_schemas(borrowed);
    CHECK(borrowed.size() == 1 && borrowed[0]->m_cols.size() == 3);
  }

  memtable::destroy(mt);
  CHECK(borrowed.size() == 1 && borrowed[0]->m_name == "m");

  return;
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_capacity();
  test_reserve_then_publish();
  test_column_prefixes();
  test_schema_prefix_lifetime();
  test_schema_ownership();
  test_plan_must_fit();
  test_publish_prefix();
  test_concurrent_writers();
  test_collect_and_trim();
  test_snapshot_pins_retired();
  test_restore_schema();
  test_concurrent_trim();

  return (test_result("memtable-t"));
}
