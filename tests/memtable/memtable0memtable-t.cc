/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable0memtable-t.cc
 *    Tests for the memtable: schema versions, ordered publication,
 *    failure, and concurrent writers.
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "memtable0memtable.h"
#include "test0memtable.h"
#include "test0point.h"
#include "test0util.h"

/** Plan a one-point write of the given fields against the memtable's current
schema.
@param[in,out]  mt        memtable
@param[in]      table     logical table name
@param[in]      fields    input fields
@param[in]      n_fields  number of fields
@param[out]     plan      plan for the input fields against the current schema
*/
static void plan_write(memtable_t *mt, const char *table,
                       const ingest_field_t *fields, uint32_t n_fields,
                       ingest_plan_t *plan) {
  const schema_t *schema = memtable_get_schema(*mt, table);
  ingest_point_t p = point(T0, nullptr, 0, fields, n_fields);
  CHECK(plan_for(schema, table, &p, 1, plan) == DB_SUCCESS);
}

/** A reserved write is seen by planning at once and by readers only once
published. */
static void test_reserve_then_publish() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);

  ingest_field_t f[] = {field_int("n", 1)};
  ingest_plan_t plan;
  plan_write(mt, "m", f, 1, &plan);
  CHECK(memtable_reserve(*mt, 1, plan) == DB_SUCCESS);

  /* Reserved: planning sees the new table, readers do not. */
  const schema_t *schema = memtable_get_schema(*mt, "m");
  CHECK(schema != nullptr && schema->version == 1 && schema->cols.size() == 2);

  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);
  CHECK(snap.visible_seq == 0 && snap.tables.empty());
  memtable_snapshot_release(*mt, snap);

  /* Published: readers see it. */
  CHECK(memtable_publish(*mt, 1) == DB_SUCCESS);
  snapshot_all(*mt, snap);
  CHECK(snap.visible_seq == 1);
  CHECK(snap.tables.size() == 1);

  const memtable_table_view_t &view = snap.tables["m"];
  CHECK(view.schema == schema);
  CHECK(view.batches.size() == 1 && view.batches[0]->cols[1].ints[0] == 1);

  memtable_snapshot_release(*mt, snap);
  memtable_free(mt);
}

/** A batch keeps the version it was planned against; views use the newest
version. */
static void test_schema_versions() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_plan_t first;
  plan_write(mt, "m", f1, 1, &first);
  CHECK(memtable_reserve(*mt, 1, first) == DB_SUCCESS);

  /* Planned against version 1 without new columns, but reserved only
  after another write moved the table to version 2. */
  ingest_plan_t stale;
  plan_write(mt, "m", f1, 1, &stale);

  ingest_field_t f2[] = {field_int("n", 2), field_double("extra", 0.5)};
  ingest_plan_t grow;
  plan_write(mt, "m", f2, 2, &grow);
  CHECK(memtable_reserve(*mt, 2, grow) == DB_SUCCESS);
  CHECK(memtable_reserve(*mt, 3, stale) == DB_SUCCESS);

  CHECK(memtable_publish(*mt, 1) == DB_SUCCESS);
  CHECK(memtable_publish(*mt, 2) == DB_SUCCESS);
  CHECK(memtable_publish(*mt, 3) == DB_SUCCESS);

  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);

  const memtable_table_view_t &view = snap.tables["m"];
  CHECK(view.batches.size() == 3);

  /* The view uses the newest version, although the last batch is older. */
  CHECK(view.schema->version == 2 && view.schema->cols.size() == 3);
  CHECK(view.batches[0]->cols.size() == 2);
  CHECK(view.batches[1]->cols.size() == 3);
  CHECK(view.batches[2]->cols.size() == 2);
  CHECK(view.batches[2]->schema_version == 1);

  memtable_snapshot_release(*mt, snap);
  memtable_free(mt);
}

/** Plans that no longer fit the current schema are refused. */
static void test_plan_must_fit() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_plan_t create;
  plan_write(mt, "m", f1, 1, &create);

  /* A second "create" of the same table, planned before the first was
  reserved. */
  ingest_plan_t create_again;
  plan_write(mt, "m", f1, 1, &create_again);

  ingest_field_t fa[] = {field_int("a", 1)};
  ingest_field_t fb[] = {field_int("b", 1)};

  CHECK(memtable_reserve(*mt, 1, create) == DB_SUCCESS);
  CHECK(memtable_reserve(*mt, 2, create_again) == DB_CORRUPTION);

  /* Two schema changes planned against the same version: only the first
  fits. */
  ingest_plan_t add_a;
  ingest_plan_t add_b;
  plan_write(mt, "m", fa, 1, &add_a);
  plan_write(mt, "m", fb, 1, &add_b);
  CHECK(memtable_reserve(*mt, 2, add_a) == DB_SUCCESS);
  CHECK(memtable_reserve(*mt, 3, add_b) == DB_CORRUPTION);

  /* A write to a table the memtable does not have. */
  const schema_t *m = memtable_get_schema(*mt, "m");
  schema_t other = *m;
  other.name = "other";
  ingest_point_t p = point(T0, nullptr, 0, f1, 1);
  ingest_plan_t missing;
  CHECK(plan_for(&other, "other", &p, 1, &missing) == DB_SUCCESS);
  CHECK(memtable_reserve(*mt, 3, missing) == DB_CORRUPTION);

  /* Rejected reserves changed nothing: 3 is still the next number. */
  CHECK(memtable_get_schema(*mt, "m")->version == 2);
  ingest_plan_t next;
  plan_write(mt, "m", fb, 1, &next);
  CHECK(memtable_reserve(*mt, 3, next) == DB_SUCCESS);

  memtable_free(mt);
}

struct publisher_t {
  /** Memtable under test. */
  memtable_t *mt{nullptr};

  /** Write to publish. */
  batch_seq_t seq{0};

  /** What memtable_publish() returned. */
  dberr_t err{DB_ERROR};

  /** Set when memtable_publish() returned. Read only after join. */
  bool returned{false};
};

/** Publish one write; record that the call returned.
@param[in,out]  p  publication arguments and returned status */
static void publisher_main(publisher_t *p) {
  p->err = memtable_publish(*p->mt, p->seq);
  p->returned = true;
}

/** Reserve n one-point writes to table "m".
@param[in,out]  mt  memtable
@param[in]      n   number of consecutive one-point writes to reserve */
static void reserve_n(memtable_t *mt, int n) {
  ingest_field_t f[] = {field_int("n", 1)};

  for (int i = 1; i <= n; ++i) {
    ingest_plan_t plan;
    plan_write(mt, "m", f, 1, &plan);
    CHECK(memtable_reserve(*mt, (batch_seq_t)i, plan) == DB_SUCCESS);
  }
}

/** Take a snapshot and read its visible sequence boundary.
@param[in,out]  mt  memtable
@return the visible_seq of a new snapshot */
static batch_seq_t visible(memtable_t *mt) {
  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);

  const batch_seq_t seq = snap.visible_seq;
  memtable_snapshot_release(*mt, snap);

  return (seq);
}

/** Writes become visible in sequence order, whatever order they are published
in. */
static void test_publish_in_order() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);
  reserve_n(mt, 3);

  /* Write 3 finishes first; it must wait for 1 and 2. */
  publisher_t third;
  third.mt = mt;
  third.seq = 3;
  std::thread waiter(publisher_main, &third);

  usleep(50 * 1000);
  CHECK(visible(mt) == 0);

  CHECK(memtable_publish(*mt, 1) == DB_SUCCESS);
  CHECK(visible(mt) == 1);

  CHECK(memtable_publish(*mt, 2) == DB_SUCCESS);
  waiter.join();

  CHECK(third.returned && third.err == DB_SUCCESS);
  CHECK(visible(mt) == 3);

  memtable_free(mt);
}

/** After a failure, waiting writers and later calls are rejected and old
snapshots stay valid. */
static void test_fail() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);
  reserve_n(mt, 2);

  CHECK(memtable_publish(*mt, 1) == DB_SUCCESS);

  /* Reserve 3 as well... */
  ingest_field_t f[] = {field_int("n", 1)};
  ingest_plan_t plan;
  plan_write(mt, "m", f, 1, &plan);
  CHECK(memtable_reserve(*mt, 3, plan) == DB_SUCCESS);

  /* ...but publish 3 while 2 is still in flight, then fail. */
  publisher_t third;
  third.mt = mt;
  third.seq = 3;
  std::thread waiter(publisher_main, &third);
  usleep(50 * 1000);

  memtable_fail(*mt);
  waiter.join();
  CHECK(third.err == DB_FENCED);

  /* Everything after the failure is rejected; the snapshot keeps what
  was visible before. */
  CHECK(memtable_publish(*mt, 2) == DB_FENCED);
  ingest_plan_t later;
  plan_write(mt, "m", f, 1, &later);
  CHECK(memtable_reserve(*mt, 4, later) == DB_FENCED);
  CHECK(visible(mt) == 1);

  memtable_free(mt);
}

/** Shared state of the concurrent test: the lock that hands out sequence
numbers, as the write path will. */
struct writers_t {
  /** Memtable under test. */
  memtable_t *mt{nullptr};

  /** Hands out sequence numbers and orders reservations. */
  std::mutex mutex;

  /** Sequence number of the next write. */
  batch_seq_t next_seq{1};
};

struct writer_arg_t {
  /** State shared by every writer. */
  writers_t *shared{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int thread_no{0};

  /** Writes to make. */
  int count{0};
};

/** Write count times: plan and reserve under the shared lock, publish outside
it. Every 50th write adds a column.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  writers_t *w = arg->shared;
  char name[32];

  for (int i = 0; i < arg->count; ++i) {
    /* Every 50th write adds a column. */
    snprintf(name, sizeof(name), "c%d_%d", arg->thread_no, i);
    ingest_field_t f[] = {field_int("n", i), field_int(name, i)};
    const uint32_t n_fields = i % 50 == 0 ? 2 : 1;

    batch_seq_t seq;
    {
      std::lock_guard<std::mutex> lock(w->mutex);

      ingest_plan_t plan;
      plan_write(w->mt, "m", f, n_fields, &plan);

      seq = w->next_seq++;
      CHECK(memtable_reserve(*w->mt, seq, plan) == DB_SUCCESS);
    }

    /* Publish outside the lock, so writes finish in any order. */
    CHECK(memtable_publish(*w->mt, seq) == DB_SUCCESS);
  }
}

/** Writers reserving and publishing concurrently leave every write visible
exactly once. */
static void test_concurrent_writers() {
  writers_t shared;
  CHECK(memtable_create(0, &shared.mt) == DB_SUCCESS);

  const int threads = 8;
  const int count = 200;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].shared = &shared;
    args[t].thread_no = t;
    args[t].count = count;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
  }

  memtable_snapshot_t snap;
  snapshot_all(*shared.mt, snap);
  CHECK(snap.visible_seq == (batch_seq_t)(threads * count));

  /* 2 base columns plus one new column per thread every 50 writes. */
  const memtable_table_view_t &view = snap.tables["m"];
  CHECK(view.batches.size() == (size_t)(threads * count));
  CHECK(view.schema->cols.size() == (size_t)(2 + threads * (count / 50)));

  memtable_snapshot_release(*shared.mt, snap);
  memtable_free(shared.mt);
}

/** Reserve and publish one write of value v to a table.
@param[in,out]  mt     memtable
@param[in]      seq    write or record sequence number
@param[in]      table  logical table name
@param[in]      v      value of field n */
static void write_value(memtable_t *mt, batch_seq_t seq, const char *table,
                        int64_t v) {
  ingest_field_t f[] = {field_int("n", v)};
  ingest_plan_t plan;
  plan_write(mt, table, f, 1, &plan);
  CHECK(memtable_reserve(*mt, seq, plan) == DB_SUCCESS);
  CHECK(memtable_publish(*mt, seq) == DB_SUCCESS);
}

/** Collect hands out only visible writes; retire takes them out but keeps the
schemas. */
static void test_collect_and_retire() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);

  write_value(mt, 1, "a", 10);
  write_value(mt, 2, "b", 20);

  /* Write 3 is reserved but not visible, so it is not collected. */
  ingest_field_t f[] = {field_int("n", 30)};
  ingest_plan_t plan;
  plan_write(mt, "a", f, 1, &plan);
  CHECK(memtable_reserve(*mt, 3, plan) == DB_SUCCESS);

  std::vector<memtable_flush_item_t> items;
  memtable_collect(*mt, items);
  CHECK(items.size() == 2);
  CHECK(items[0].seq == 1 && items[0].batch->table == "a");
  CHECK(items[1].seq == 2 && items[1].schema->name == "b");

  memtable_retire(*mt, 2);

  memtable_stats_t stats;
  memtable_get_stats(*mt, stats);
  CHECK(stats.flushed_seq == 2 && stats.live_writes == 1);
  CHECK(stats.live_rows == 1 && stats.garbage_writes == 0);

  /* Retired writes leave the snapshot; schemas stay for planning. */
  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);
  CHECK(snap.base_seq == 2 && snap.visible_seq == 2 && snap.tables.empty());
  memtable_snapshot_release(*mt, snap);
  CHECK(memtable_get_schema(*mt, "b") != nullptr);

  CHECK(memtable_publish(*mt, 3) == DB_SUCCESS);
  memtable_collect(*mt, items);
  CHECK(items.size() == 1 && items[0].seq == 3);

  memtable_free(mt);
}

/** A snapshot keeps retired batches until it is released. */
static void test_snapshot_pins_retired() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);

  write_value(mt, 1, "m", 1);
  write_value(mt, 2, "m", 2);

  memtable_snapshot_t old_snap;
  snapshot_all(*mt, old_snap);

  memtable_retire(*mt, 2);

  /* The old snapshot still reads its batches. */
  memtable_stats_t stats;
  memtable_get_stats(*mt, stats);
  CHECK(stats.garbage_writes == 2);

  const memtable_table_view_t &view = old_snap.tables["m"];
  CHECK(view.batches.size() == 2);
  CHECK(view.batches[0]->cols[1].ints[0] == 1);
  CHECK(view.batches[1]->cols[1].ints[0] == 2);

  /* A newer snapshot does not see them and does not hold them. */
  memtable_snapshot_t new_snap;
  snapshot_all(*mt, new_snap);
  CHECK(new_snap.tables.empty());
  memtable_snapshot_release(*mt, new_snap);

  memtable_get_stats(*mt, stats);
  CHECK(stats.garbage_writes == 2);

  /* Releasing the old one frees them. */
  memtable_snapshot_release(*mt, old_snap);
  CHECK(old_snap.id == 0);

  memtable_get_stats(*mt, stats);
  CHECK(stats.garbage_writes == 0);

  memtable_free(mt);
}

/** Stored tables are loaded with all their versions, and writes continue
after F. */
static void test_add_table() {
  /* Writes up to 10 are in storage; the table has versions 1 and 2. */
  memtable_t *mt = nullptr;
  CHECK(memtable_create(10, &mt) == DB_SUCCESS);

  ingest_field_t f1[] = {field_int("n", 1)};
  ingest_point_t p1 = point(T0, nullptr, 0, f1, 1);
  ingest_plan_t first;
  CHECK(plan_for(nullptr, "m", &p1, 1, &first) == DB_SUCCESS);
  const schema_t v1 = schema_after(nullptr, first);

  ingest_field_t f2[] = {field_int("n", 1), field_double("d", 1.5)};
  ingest_point_t p2 = point(T0, nullptr, 0, f2, 2);
  ingest_plan_t second;
  CHECK(plan_for(&v1, "m", &p2, 1, &second) == DB_SUCCESS);
  const schema_t v2 = schema_after(&v1, second);

  std::vector<schema_t> versions;
  versions.push_back(v1);
  versions.push_back(v2);

  /* Versions out of order are refused. */
  std::vector<schema_t> backwards;
  backwards.push_back(v2);
  CHECK(memtable_add_table(*mt, backwards) == DB_CORRUPTION);

  CHECK(memtable_add_table(*mt, versions) == DB_SUCCESS);
  CHECK(memtable_add_table(*mt, versions) == DB_CORRUPTION);
  CHECK(memtable_get_schema(*mt, "m")->version == 2);

  /* A replayed write planned against version 1 still fits. */
  ingest_plan_t old_write;
  CHECK(plan_for(&v1, "m", &p1, 1, &old_write) == DB_SUCCESS);
  CHECK(memtable_reserve(*mt, 11, old_write) == DB_SUCCESS);
  CHECK(memtable_publish(*mt, 11) == DB_SUCCESS);

  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);
  CHECK(snap.base_seq == 10 && snap.visible_seq == 11);
  CHECK(snap.tables["m"].batches.size() == 1);
  memtable_snapshot_release(*mt, snap);

  memtable_free(mt);
}

struct flusher_arg_t {
  /** Memtable under test. */
  memtable_t *mt{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *stop{nullptr};

  /** Protects *stop. */
  std::mutex *stop_mutex{nullptr};

  /** Rounds that retired something. */
  int rounds{0};
};

/** Retire every visible write, over and over, as a flush would.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void flusher_main(flusher_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->stop_mutex);
      if (*arg->stop) {
        break;
      }
    }

    std::vector<memtable_flush_item_t> items;
    memtable_collect(*arg->mt, items);

    if (!items.empty()) {
      memtable_retire(*arg->mt, items.back().seq);
      arg->rounds++;
    }
  }
}

struct reader_arg_t {
  /** Memtable under test. */
  memtable_t *mt{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *stop{nullptr};

  /** Protects *stop. */
  std::mutex *stop_mutex{nullptr};

  /** Calls that failed. */
  int errors{0};
};

/** Read every value of every snapshot while batches are retired.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void reader_main(reader_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->stop_mutex);
      if (*arg->stop) {
        break;
      }
    }

    memtable_snapshot_t snap;
    snapshot_all(*arg->mt, snap);

    size_t batches = 0;
    std::map<std::string, memtable_table_view_t>::iterator it;

    for (it = snap.tables.begin(); it != snap.tables.end(); ++it) {
      for (size_t i = 0; i < it->second.batches.size(); ++i) {
        const batch_t *b = it->second.batches[i];
        if (b->cols[1].ints[0] < 0 || b->n_rows != 1) {
          arg->errors++;
        }
        batches++;
      }
    }

    if (batches != snap.visible_seq - snap.base_seq) {
      arg->errors++;
    }

    memtable_snapshot_release(*arg->mt, snap);
  }
}

/** Writers, a retiring flusher and readers run together; readers always see
consistent snapshots. */
static void test_concurrent_retire() {
  writers_t shared;
  CHECK(memtable_create(0, &shared.mt) == DB_SUCCESS);

  bool stop = false;
  std::mutex stop_mutex;

  flusher_arg_t flusher;
  flusher.mt = shared.mt;
  flusher.stop = &stop;
  flusher.stop_mutex = &stop_mutex;
  std::thread flusher_thread(flusher_main, &flusher);

  reader_arg_t reader;
  reader.mt = shared.mt;
  reader.stop = &stop;
  reader.stop_mutex = &stop_mutex;
  std::thread reader_thread(reader_main, &reader);

  const int threads = 4;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].shared = &shared;
    args[t].thread_no = t;
    args[t].count = 300;
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

  CHECK(reader.errors == 0);
  printf("concurrent retire: %d flush rounds\n", flusher.rounds);

  /* Retire the rest; with no snapshot open nothing stays behind. */
  std::vector<memtable_flush_item_t> items;
  memtable_collect(*shared.mt, items);
  if (!items.empty()) {
    memtable_retire(*shared.mt, items.back().seq);
  }

  memtable_stats_t stats;
  memtable_get_stats(*shared.mt, stats);
  CHECK(stats.flushed_seq == (batch_seq_t)(threads * 300));
  CHECK(stats.live_writes == 0 && stats.live_rows == 0);
  CHECK(stats.garbage_writes == 0);

  memtable_free(shared.mt);
}

/** A dropped table survives only its exact batch and planner owners. */
static void test_drop_owners() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);
  write_value(mt, 1, "m", 7);

  memtable_schema_ref_t planner;
  memtable_schema_acquire(*mt, "m", planner);
  CHECK(planner.schema != nullptr);
  memtable_snapshot_t snap;
  snapshot_all(*mt, snap);
  memtable_retire(*mt, 1);
  memtable_drop_table(*mt, "m");
  CHECK(memtable_get_schema(*mt, "m") == nullptr);

  /* The same name may now have an unrelated schema. */
  ingest_field_t text = field_str("text", "new");
  ingest_plan_t create;
  plan_write(mt, "m", &text, 1, &create);
  CHECK(memtable_reserve(*mt, 2, create) == DB_SUCCESS);
  CHECK(memtable_publish(*mt, 2) == DB_SUCCESS);
  CHECK(memtable_get_schema(*mt, "m")->cols[1].type == COL_VARCHAR);
  CHECK(planner.schema->cols[1].type == COL_BIGINT);
  CHECK(snap.tables["m"].schema->cols[1].type == COL_BIGINT);
  CHECK(snap.tables["m"].batches[0]->cols[1].ints[0] == 7);

  /* Releasing the reader alone must not invalidate a write planner. */
  memtable_snapshot_release(*mt, snap);
  memtable_stats_t stats;
  memtable_get_stats(*mt, stats);
  CHECK(stats.garbage_writes == 0 && stats.dropped_tables == 1);
  CHECK(planner.schema->cols[1].name == "n");
  memtable_schema_release(*mt, planner);
  CHECK(planner.schema == nullptr && planner.table == nullptr);
  memtable_schema_release(*mt, planner);
  memtable_get_stats(*mt, stats);
  CHECK(stats.dropped_tables == 0);

  /* With only a reader left, its batch holds the table until release. */
  memtable_schema_acquire(*mt, "m", planner);
  snapshot_all(*mt, snap);
  memtable_retire(*mt, 2);
  memtable_drop_table(*mt, "m");
  memtable_schema_release(*mt, planner);
  CHECK(snap.tables["m"].schema->cols[1].type == COL_VARCHAR);
  memtable_get_stats(*mt, stats);
  CHECK(stats.garbage_writes == 1 && stats.dropped_tables == 1);
  memtable_snapshot_release(*mt, snap);
  memtable_get_stats(*mt, stats);
  CHECK(stats.garbage_writes == 0 && stats.dropped_tables == 0);
  memtable_free(mt);
}

/** A reader still fixing its cold snapshot keeps dropped schemas alive. */
static void test_drop_with_pending_pin() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);
  write_value(mt, 1, "m", 9);
  memtable_snapshot_t snap;
  memtable_snapshot_pin(*mt, snap);
  memtable_retire(*mt, 1);
  memtable_drop_table(*mt, "m");
  memtable_stats_t stats;
  memtable_get_stats(*mt, stats);
  CHECK(stats.dropped_tables == 1);
  CHECK(memtable_snapshot_at(*mt, 0, snap) == DB_SUCCESS);
  CHECK(snap.tables["m"].schema->cols[1].name == "n");
  CHECK(snap.tables["m"].batches[0]->cols[1].ints[0] == 9);
  memtable_snapshot_release(*mt, snap);
  memtable_get_stats(*mt, stats);
  CHECK(stats.dropped_tables == 0 && stats.garbage_writes == 0);
  memtable_free(mt);
}

/** Repeated loading and dropping must not accumulate schema history. */
static void test_repeated_drop_reclaims_schemas() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);
  schema_t schema;
  schema.name = "m";
  col_def_t time;
  time.name = "time";
  time.role = COL_ROLE_TIME;
  time.type = COL_TIMESTAMP;
  schema.cols.push_back(time);
  std::vector<schema_t> versions;
  for (uint32_t c = 1; c < SCHEMA_MAX_COLUMNS; ++c) {
    col_def_t col;
    col.id = c;
    col.name = "f" + std::to_string(c) + std::string(120, 'x');
    col.role = COL_ROLE_FIELD;
    col.type = COL_BIGINT;
    schema.cols.push_back(col);
    schema.version = c;
    versions.push_back(schema);
  }
  for (int round = 0; round < 200; ++round) {
    CHECK(memtable_add_table(*mt, versions) == DB_SUCCESS);
    memtable_drop_table(*mt, "m");
    memtable_stats_t stats;
    memtable_get_stats(*mt, stats);
    CHECK(stats.dropped_tables == 0);
  }
  memtable_free(mt);
}

/** A planner repeatedly holds a schema while another thread drops it. */
struct drop_planner_t {
  /** Memtable being dropped and recreated. */
  memtable_t *mt{nullptr};

  /** Stop requested by the main thread. */
  std::atomic<bool> stop{false};

  /** Completed plans, also used to wait until the reader has started. */
  std::atomic<uint64_t> plans{0};

  /** Only written by the worker, read after joining it. */
  int errors{0};
};

/** Acquire, plan against and release the schema of m until told to stop,
counting plans and failures.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void drop_planner_main(drop_planner_t *arg) {
  ingest_field_t field = field_int("n", 1);
  ingest_point_t p = point(T0, nullptr, 0, &field, 1);
  while (!arg->stop.load()) {
    memtable_schema_ref_t ref;
    memtable_schema_acquire(*arg->mt, "m", ref);
    if (ref.schema != nullptr) {
      ingest_plan_t plan;
      if (plan_for(ref.schema, "m", &p, 1, &plan) != DB_SUCCESS) {
        ++arg->errors;
      }
      ++arg->plans;
    }
    memtable_schema_release(*arg->mt, ref);
  }
}

/** A planner holding a schema reference stays safe while the table is dropped
and created again. */
static void test_planning_during_drops() {
  memtable_t *mt = nullptr;
  CHECK(memtable_create(0, &mt) == DB_SUCCESS);
  ingest_field_t field = field_int("n", 1);
  ingest_point_t p = point(T0, nullptr, 0, &field, 1);
  ingest_plan_t plan;
  CHECK(plan_for(nullptr, "m", &p, 1, &plan) == DB_SUCCESS);
  const std::vector<schema_t> versions = {schema_after(nullptr, plan)};
  CHECK(memtable_add_table(*mt, versions) == DB_SUCCESS);
  drop_planner_t arg;
  arg.mt = mt;
  std::thread worker(drop_planner_main, &arg);
  while (arg.plans.load() == 0) {
    std::this_thread::yield();
  }
  for (int round = 0; round < 1000; ++round) {
    memtable_drop_table(*mt, "m");
    CHECK(memtable_add_table(*mt, versions) == DB_SUCCESS);
  }
  arg.stop.store(true);
  worker.join();
  CHECK(arg.errors == 0);
  memtable_drop_table(*mt, "m");
  memtable_stats_t stats;
  memtable_get_stats(*mt, stats);
  CHECK(stats.dropped_tables == 0);
  memtable_free(mt);
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
  test_drop_owners();
  test_drop_with_pending_pin();
  test_repeated_drop_reclaims_schemas();
  test_planning_during_drops();
  test_concurrent_retire();

  return (test_result("memtable0memtable-t"));
}
