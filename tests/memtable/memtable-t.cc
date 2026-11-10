/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable-t.cc
 *    Tests for the memtable: schema versions, ordered publication,
 *    failure, and concurrent writers.
 *
 *-------------------------------------------------------------------------
 */

#include "memtable.h"

#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ingest.h"
#include "schema.h"
#include "test_memtable.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"

/** Plan a one-point write of the given fields against the memtable's current
schema. */
static void plan_write(memtable_t &mt, const char *table,
                       const ingest_field_t *fields, uint32_t n_fields,
                       ingest_plan_t &plan) {
  const schema_t *schema = memtable_get_schema(mt, table);
  ingest_point_t p = point(T0, nullptr, 0, fields, n_fields);
  CHECK(plan_for(schema, table, &p, 1, plan).ok());
}

/** A reserved write is seen by planning at once and by readers only once
published. */
static void test_reserve_then_publish() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt).ok());

  ingest_field_t f[] = {field_int("n", 1)};
  ingest_plan_t plan;
  plan_write(*mt, "m", f, 1, plan);
  CHECK(memtable_reserve(*mt, 1, plan).ok());

  /* Reserved: planning sees the new table, readers do not. */
  const schema_t *schema = memtable_get_schema(*mt, "m");
  CHECK(schema != nullptr && schema->m_version == 1 &&
        schema->m_cols.size() == 2);

  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);
  CHECK(snap.m_visible_seq == 0 && snap.m_tables.empty());
  memtable_snapshot_release(*mt, snap);

  /* Published: readers see it. */
  CHECK(memtable_publish(*mt, 1).ok());
  snapshot_all(*mt, snap);
  CHECK(snap.m_visible_seq == 1);
  CHECK(snap.m_tables.size() == 1);

  const memtable_table_view_t &view = snap.m_tables["m"];
  CHECK(view.m_schema == schema);
  CHECK(view.m_batches.size() == 1 &&
        view.m_batches[0]->m_cols[1].m_ints[0] == 1);

  memtable_snapshot_release(*mt, snap);
  memtable_free(mt);
}

/** A batch keeps the version it was planned against; views use the newest
version. */
static void test_schema_versions() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt).ok());

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_plan_t first;
  plan_write(*mt, "m", f1, 1, first);
  CHECK(memtable_reserve(*mt, 1, first).ok());

  /* Planned against version 1 without new columns, but reserved only
  after another write moved the table to version 2. */
  ingest_plan_t stale;
  plan_write(*mt, "m", f1, 1, stale);

  ingest_field_t f2[] = {field_int("n", 2), field_double("extra", 0.5)};
  ingest_plan_t grow;
  plan_write(*mt, "m", f2, 2, grow);
  CHECK(memtable_reserve(*mt, 2, grow).ok());
  CHECK(memtable_reserve(*mt, 3, stale).ok());

  CHECK(memtable_publish(*mt, 1).ok());
  CHECK(memtable_publish(*mt, 2).ok());
  CHECK(memtable_publish(*mt, 3).ok());

  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);

  const memtable_table_view_t &view = snap.m_tables["m"];
  CHECK(view.m_batches.size() == 3);

  /* The view uses the newest version, although the last batch is older. */
  CHECK(view.m_schema->m_version == 2 && view.m_schema->m_cols.size() == 3);
  CHECK(view.m_batches[0]->m_cols.size() == 2);
  CHECK(view.m_batches[1]->m_cols.size() == 3);
  CHECK(view.m_batches[2]->m_cols.size() == 2);
  CHECK(view.m_batches[2]->m_schema_version == 1);

  memtable_snapshot_release(*mt, snap);
  memtable_free(mt);
}

/** Plans that no longer fit the current schema are refused. */
static void test_plan_must_fit() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt).ok());

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_plan_t create;
  plan_write(*mt, "m", f1, 1, create);

  /* A second "create" of the same table, planned before the first was
  reserved. */
  ingest_plan_t create_again;
  plan_write(*mt, "m", f1, 1, create_again);

  ingest_field_t fa[] = {field_int("a", 1)};
  ingest_field_t fb[] = {field_int("b", 1)};

  CHECK(memtable_reserve(*mt, 1, create).ok());
  CHECK(memtable_reserve(*mt, 2, create_again).code() == DB_CORRUPTION);

  /* Two schema changes planned against the same version: only the first
  fits. */
  ingest_plan_t add_a;
  ingest_plan_t add_b;
  plan_write(*mt, "m", fa, 1, add_a);
  plan_write(*mt, "m", fb, 1, add_b);
  CHECK(memtable_reserve(*mt, 2, add_a).ok());
  CHECK(memtable_reserve(*mt, 3, add_b).code() == DB_CORRUPTION);

  /* A write to a table the memtable does not have. */
  const schema_t *m = memtable_get_schema(*mt, "m");
  schema_t other = *m;
  other.m_name = "other";
  ingest_point_t p = point(T0, nullptr, 0, f1, 1);
  ingest_plan_t missing;
  CHECK(plan_for(&other, "other", &p, 1, missing).ok());
  CHECK(memtable_reserve(*mt, 3, missing).code() == DB_CORRUPTION);

  /* Rejected reserves changed nothing: 3 is still the next number. */
  CHECK(memtable_get_schema(*mt, "m")->m_version == 2);
  ingest_plan_t next;
  plan_write(*mt, "m", fb, 1, next);
  CHECK(memtable_reserve(*mt, 3, next).ok());

  memtable_free(mt);
}

struct publisher_t {
  /** Memtable under test. */
  memtable_t *m_mt{nullptr};

  /** Write to publish. */
  batch_seq_t m_seq{0};

  /** What memtable_publish() returned. */
  db_status_t m_err{DB_ERROR};

  /** Set when memtable_publish() returned. Read only after join. */
  bool m_returned{false};
};

/** Publish one write; record that the call returned. */
static void publisher_main(publisher_t *p) {
  p->m_err = memtable_publish(*p->m_mt, p->m_seq);
  p->m_returned = true;
}

/** Reserve n one-point writes to table "m". */
static void reserve_n(memtable_t &mt, int n) {
  ingest_field_t f[] = {field_int("n", 1)};

  for (int i = 1; i <= n; ++i) {
    ingest_plan_t plan;
    plan_write(mt, "m", f, 1, plan);
    CHECK(memtable_reserve(mt, static_cast<batch_seq_t>(i), plan).ok());
  }
}

/** Take a snapshot and read its visible sequence boundary.
@return the visible_seq of a new snapshot */
static batch_seq_t visible(memtable_t &mt) {
  memtable_snapshot_t snap;
  snapshot_all(mt, snap);

  const batch_seq_t seq = snap.m_visible_seq;
  memtable_snapshot_release(mt, snap);

  return (seq);
}

/** Writes become visible in sequence order, whatever order they are published
in. */
static void test_publish_in_order() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt).ok());
  reserve_n(*mt, 3);

  /* Write 3 finishes first; it must wait for 1 and 2. */
  publisher_t third;
  third.m_mt = mt;
  third.m_seq = 3;
  std::thread waiter(publisher_main, &third);

  usleep(50 * 1000);
  CHECK(visible(*mt) == 0);

  CHECK(memtable_publish(*mt, 1).ok());
  CHECK(visible(*mt) == 1);

  CHECK(memtable_publish(*mt, 2).ok());
  waiter.join();

  CHECK(third.m_returned && third.m_err.ok());
  CHECK(visible(*mt) == 3);

  memtable_free(mt);
}

/** After a failure, waiting writers and later calls are rejected and old
snapshots stay valid. */
static void test_fail() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt).ok());
  reserve_n(*mt, 2);

  CHECK(memtable_publish(*mt, 1).ok());

  /* Reserve 3 as well... */
  ingest_field_t f[] = {field_int("n", 1)};
  ingest_plan_t plan;
  plan_write(*mt, "m", f, 1, plan);
  CHECK(memtable_reserve(*mt, 3, plan).ok());

  /* ...but publish 3 while 2 is still in flight, then fail. */
  publisher_t third;
  third.m_mt = mt;
  third.m_seq = 3;
  std::thread waiter(publisher_main, &third);
  usleep(50 * 1000);

  memtable_fail(*mt);
  waiter.join();
  CHECK(third.m_err.code() == DB_FENCED);

  /* Everything after the failure is rejected; the snapshot keeps what
  was visible before. */
  CHECK(memtable_publish(*mt, 2).code() == DB_FENCED);
  ingest_plan_t later;
  plan_write(*mt, "m", f, 1, later);
  CHECK(memtable_reserve(*mt, 4, later).code() == DB_FENCED);
  CHECK(visible(*mt) == 1);

  memtable_free(mt);
}

/** Shared state of the concurrent test: the lock that hands out sequence
numbers, as the write path will. */
struct writers_t {
  /** Memtable under test. */
  memtable_t *m_mt{nullptr};

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

/** Write count times: plan and reserve under the shared lock, publish outside
it. Every 50th write adds a column.
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
      CHECK(memtable_reserve(*w->m_mt, seq, plan).ok());
    }

    /* Publish outside the lock, so writes finish in any order. */
    CHECK(memtable_publish(*w->m_mt, seq).ok());
  }
}

/** Writers reserving and publishing concurrently leave every write visible
exactly once. */
static void test_concurrent_writers() {
  writers_t shared;
  CHECK(memtable_create(0, &shared.m_mt).ok());

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

  memtable_snapshot_t snap;
  snapshot_all(*shared.m_mt, snap);
  CHECK(snap.m_visible_seq == static_cast<batch_seq_t>(threads * count));

  /* 2 base columns plus one new column per thread every 50 writes. */
  const memtable_table_view_t &view = snap.m_tables["m"];
  CHECK(view.m_batches.size() == static_cast<size_t>(threads * count));
  CHECK(view.m_schema->m_cols.size() ==
        static_cast<size_t>(2 + threads * (count / 50)));

  memtable_snapshot_release(*shared.m_mt, snap);
  memtable_free(shared.m_mt);
}

/** Reserve and publish one write of value v to a table. */
static void write_value(memtable_t &mt, batch_seq_t seq, const char *table,
                        int64_t v) {
  ingest_field_t f[] = {field_int("n", v)};
  ingest_plan_t plan;
  plan_write(mt, table, f, 1, plan);
  CHECK(memtable_reserve(mt, seq, plan).ok());
  CHECK(memtable_publish(mt, seq).ok());
}

/** Collect hands out only visible writes; retire takes them out but keeps the
schemas. */
static void test_collect_and_retire() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt).ok());

  write_value(*mt, 1, "a", 10);
  write_value(*mt, 2, "b", 20);

  /* Write 3 is reserved but not visible, so it is not collected. */
  ingest_field_t f[] = {field_int("n", 30)};
  ingest_plan_t plan;
  plan_write(*mt, "a", f, 1, plan);
  CHECK(memtable_reserve(*mt, 3, plan).ok());

  std::vector<memtable_flush_item_t> items;
  memtable_collect_unflushed(*mt, items);
  CHECK(items.size() == 2);
  CHECK(items[0].m_seq == 1 && items[0].m_batch->m_table == "a");
  CHECK(items[1].m_seq == 2 && items[1].m_schema->m_name == "b");

  memtable_trim(*mt, 2);

  memtable_stats_t stats;
  memtable_get_stats(*mt, stats);
  CHECK(stats.m_flushed_seq == 2 && stats.m_live_writes == 1);
  CHECK(stats.m_live_rows == 1 && stats.m_garbage_writes == 0);

  /* Retired writes leave the snapshot; schemas stay for planning. */
  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);
  CHECK(snap.m_base_seq == 2 && snap.m_visible_seq == 2 &&
        snap.m_tables.empty());
  memtable_snapshot_release(*mt, snap);
  CHECK(memtable_get_schema(*mt, "b") != nullptr);

  CHECK(memtable_publish(*mt, 3).ok());
  memtable_collect_unflushed(*mt, items);
  CHECK(items.size() == 1 && items[0].m_seq == 3);

  memtable_free(mt);
}

/** A snapshot keeps retired batches until it is released. */
static void test_snapshot_pins_retired() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt).ok());

  write_value(*mt, 1, "m", 1);
  write_value(*mt, 2, "m", 2);

  memtable_snapshot_t old_snap;
  snapshot_all(*mt, old_snap);

  memtable_trim(*mt, 2);

  /* The old snapshot still reads its batches. */
  memtable_stats_t stats;
  memtable_get_stats(*mt, stats);
  CHECK(stats.m_garbage_writes == 2);

  const memtable_table_view_t &view = old_snap.m_tables["m"];
  CHECK(view.m_batches.size() == 2);
  CHECK(view.m_batches[0]->m_cols[1].m_ints[0] == 1);
  CHECK(view.m_batches[1]->m_cols[1].m_ints[0] == 2);

  /* A newer snapshot does not see them and does not hold them. */
  memtable_snapshot_t new_snap;
  snapshot_all(*mt, new_snap);
  CHECK(new_snap.m_tables.empty());
  memtable_snapshot_release(*mt, new_snap);

  memtable_get_stats(*mt, stats);
  CHECK(stats.m_garbage_writes == 2);

  /* Releasing the old one frees them. */
  memtable_snapshot_release(*mt, old_snap);
  CHECK(old_snap.m_entries.empty());

  memtable_get_stats(*mt, stats);
  CHECK(stats.m_garbage_writes == 0);

  memtable_free(mt);
}

/** Stored tables are loaded with all their versions, and writes continue
after F. */
static void test_add_table() {
  /* Writes up to 10 are in storage; the table has versions 1 and 2. */
  memtable_t *mt = nullptr;
  CHECK(memtable_create(10, &mt).ok());

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_point_t p1 = point(T0, nullptr, 0, f1, 1);
  ingest_plan_t first;
  CHECK(plan_for(nullptr, "m", &p1, 1, first).ok());
  const schema_t v1 = schema_after(nullptr, first);

  ingest_field_t f2[] = {field_int("n", 1), field_double("d", 1.5)};
  ingest_point_t p2 = point(T0, nullptr, 0, f2, 2);
  ingest_plan_t second;
  CHECK(plan_for(&v1, "m", &p2, 1, second).ok());
  const schema_t v2 = schema_after(&v1, second);

  std::vector<schema_t> versions;
  versions.push_back(v1);
  versions.push_back(v2);

  /* Versions out of order are refused. */
  std::vector<schema_t> backwards;
  backwards.push_back(v2);
  CHECK(memtable_add_table(*mt, backwards).code() == DB_CORRUPTION);

  CHECK(memtable_add_table(*mt, versions).ok());
  CHECK(memtable_add_table(*mt, versions).code() == DB_CORRUPTION);
  CHECK(memtable_get_schema(*mt, "m")->m_version == 2);

  /* A replayed write planned against version 1 still fits. */
  ingest_plan_t old_write;
  CHECK(plan_for(&v1, "m", &p1, 1, old_write).ok());
  CHECK(memtable_reserve(*mt, 11, old_write).ok());
  CHECK(memtable_publish(*mt, 11).ok());

  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);
  CHECK(snap.m_base_seq == 10 && snap.m_visible_seq == 11);
  CHECK(snap.m_tables["m"].m_batches.size() == 1);
  memtable_snapshot_release(*mt, snap);

  memtable_free(mt);
}

struct flusher_arg_t {
  /** Memtable under test. */
  memtable_t *m_mt{nullptr};

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
    memtable_collect_unflushed(*arg->m_mt, items);

    if (!items.empty()) {
      memtable_trim(*arg->m_mt, items.back().m_seq);
      arg->m_rounds++;
    }
  }
}

struct reader_arg_t {
  /** Memtable under test. */
  memtable_t *m_mt{nullptr};

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

    memtable_snapshot_t snap;
    snapshot_all(*arg->m_mt, snap);

    size_t batches = 0;
    std::map<std::string, memtable_table_view_t>::iterator it;

    for (it = snap.m_tables.begin(); it != snap.m_tables.end(); ++it) {
      for (size_t i = 0; i < it->second.m_batches.size(); ++i) {
        const batch_t *b = it->second.m_batches[i];
        if (b->m_cols[1].m_ints[0] < 0 || b->m_n_rows != 1) {
          arg->m_errors++;
        }
        batches++;
      }
    }

    if (batches != snap.m_visible_seq - snap.m_base_seq) {
      arg->m_errors++;
    }

    memtable_snapshot_release(*arg->m_mt, snap);
  }
}

/** Writers, a retiring flusher and readers run together; readers always see
consistent snapshots. */
static void test_concurrent_retire() {
  writers_t shared;
  CHECK(memtable_create(0, &shared.m_mt).ok());

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
  printf("concurrent retire: %d flush rounds\n", flusher.m_rounds);

  /* Retire the rest; with no snapshot open nothing stays behind. */
  std::vector<memtable_flush_item_t> items;
  memtable_collect_unflushed(*shared.m_mt, items);
  if (!items.empty()) {
    memtable_trim(*shared.m_mt, items.back().m_seq);
  }

  memtable_stats_t stats;
  memtable_get_stats(*shared.m_mt, stats);
  CHECK(stats.m_flushed_seq == static_cast<batch_seq_t>(threads * 300));
  CHECK(stats.m_live_writes == 0 && stats.m_live_rows == 0);
  CHECK(stats.m_garbage_writes == 0);

  memtable_free(shared.m_mt);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_reserve_then_publish();
  test_schema_versions();
  test_plan_must_fit();
  test_publish_in_order();
  test_fail();
  test_concurrent_writers();
  test_collect_and_retire();
  test_snapshot_pins_retired();
  test_add_table();
  test_concurrent_retire();

  return (test_result("memtable-t"));
}
