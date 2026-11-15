/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable-t.cc
 *    Tests for the memtable: column directories, ordered publication, failure,
 *    and concurrent writers.
 *
 * Covers: src/memtable/memtable.cc, src/db/db_batch.cc
 *
 *-------------------------------------------------------------------------
 */

#include "memtable.h"

#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "db_schema.h"
#include "ingest.h"
#include "test_memtable.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"

/** Plan a one-point write of the given fields against the memtable's current
schema. */
static void plan_write(test_memtable &mt, const char *table_name,
                       const ingest_field_t *fields, uint32_t n_fields,
                       ingest_plan_t &plan) {
  const schema_ref_t schema = mt.find_schema(table_name);
  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, fields, n_fields);
  CHECK(plan_for(schema.get(), table_name, &p, 1, plan).ok());

  return;
}

/** Capacity counts reserved writes and excludes retired batches even while
snapshots retain them. Zero disables the threshold. */
static void test_capacity() {
  ingest_field_t fields[] = {field_int("n", 1)};

  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, fields, 1);
  ingest_plan_t initial;
  CHECK(plan_for(nullptr, "m", &p, 1, initial).ok());

  const uint64_t bytes = initial.m_batch.calculate_data_bytes();
  CHECK(bytes > 1);

  for (const uint64_t limit : {uint64_t{0}, bytes - 1, bytes, bytes + 1}) {
    test_memtable *mt = nullptr;
    CHECK(test_memtable::create(0, limit, &mt).ok());
    CHECK(mt->has_space());

    ingest_plan_t plan;
    plan_write(*mt, "m", fields, 1, plan);
    CHECK(mt->reserve(1, plan).ok());

    const bool expected = limit == 0 || limit > bytes;
    CHECK(mt->has_space() == expected);

    mt->publish(1);
    CHECK(mt->has_space() == expected);

    test_memtable::snapshot_t snapshot;
    snapshot_all(*mt, snapshot);
    mt->retire(1);

    test_memtable::stats_t stats;
    mt->stats(stats);
    CHECK(stats.m_retired_write_count == 1);
    CHECK(mt->has_space());

    mt->release_snapshot(snapshot);
    test_memtable::destroy(mt);
  }

  return;
}

/** A reserved write is seen by planning at once and by readers only once
published. */
static void test_reserve_then_publish() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  ingest_field_t f[] = {field_int("n", 1)};

  ingest_plan_t plan;
  plan_write(*mt, "m", f, 1, plan);
  CHECK(mt->reserve(1, plan).ok());

  /* Reserved: planning sees the new table, readers do not. */
  const schema_ref_t schema = mt->find_schema("m");
  CHECK(schema != nullptr && schema->m_columns.size() == 2);

  test_memtable::snapshot_t snapshot;
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_visible_seq == 0 && snapshot_schemas(*mt, snapshot).empty());

  mt->release_snapshot(snapshot);

  /* Published: readers see it. */
  mt->publish(1);
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_visible_seq == 1);
  CHECK(snapshot_schemas(*mt, snapshot).size() == 1);

  const schema_ref_t visible_schema = snapshot_schema(*mt, snapshot, "m");
  const std::vector<const batch_t *> batches = snapshot_batches(snapshot, "m");
  CHECK(visible_schema == schema);
  CHECK(batches.size() == 1 && batches[0]->m_columns[1].m_integers[0] == 1);

  mt->release_snapshot(snapshot);
  test_memtable::destroy(mt);

  return;
}

/** A batch keeps the columns it was planned against; views use the newest
visible directory. */
static void test_column_prefixes() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

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

  test_memtable::snapshot_t snapshot;
  snapshot_all(*mt, snapshot);

  const schema_ref_t visible_schema = snapshot_schema(*mt, snapshot, "m");
  const std::vector<const batch_t *> batches = snapshot_batches(snapshot, "m");
  CHECK(batches.size() == 3);

  /* The view includes the appended column though the last batch is shorter. */
  CHECK(visible_schema->m_columns.size() == 3);
  CHECK(batches[0]->m_columns.size() == 2);
  CHECK(batches[1]->m_columns.size() == 3);
  CHECK(batches[2]->m_columns.size() == 2);

  mt->release_snapshot(snapshot);
  test_memtable::destroy(mt);

  return;
}

/** Directories remain pinned by active snapshots and are reclaimed once
old batches and their readers retire. Reserved future columns stay invisible. */
static void test_schema_prefix_lifetime() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  ingest_field_t a[] = {field_int("a", 1)};

  ingest_plan_t plan;
  plan_write(*mt, "m", a, 1, plan);
  CHECK(mt->reserve(1, plan).ok());

  mt->publish(1);

  std::weak_ptr<const schema_t> old = mt->find_schema("m");

  test_memtable::snapshot_t snapshot;
  mt->pin_snapshot(snapshot);

  ingest_field_t b[] = {field_int("a", 2), field_int("b", 3)};
  plan_write(*mt, "m", b, 2, plan);
  CHECK(mt->reserve(2, plan).ok());
  CHECK(mt->align_snapshot(0, snapshot).ok());
  CHECK(snapshot_schema(*mt, snapshot, "m")->m_columns.size() == 2);
  CHECK(snapshot.m_visible_seq == 1);

  mt->publish(2);
  mt->retire(2);
  CHECK(!old.expired());
  CHECK(snapshot_schema(*mt, snapshot, "m")->m_columns.size() == 2);

  mt->release_snapshot(snapshot);
  CHECK(!old.expired());

  /* Version definitions remain in the table directory until it closes. */
  CHECK(mt->find_schema("m")->m_columns.size() == 3);

  test_memtable::destroy(mt);

  return;
}

/** Plans that no longer fit the current schema are refused. */
static void test_plan_must_fit() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

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

  ingest_point_t p = point(TEST_BASE_TIME_US, nullptr, 0, f1, 1);
  ingest_plan_t missing;
  CHECK(plan_for(&other, "other", &p, 1, missing).ok());
  CHECK(mt->reserve(3, missing).code() == DB_CORRUPTION);

  /* Rejected reserves changed nothing: 3 is still the next number. */
  CHECK(mt->find_schema("m")->m_columns.size() == 3);

  ingest_plan_t next;
  plan_write(*mt, "m", fb, 1, next);

  ingest_plan_t incomplete = next;
  incomplete.m_batch.m_columns.pop_back();
  CHECK(mt->reserve(3, incomplete).code() == DB_CORRUPTION);
  CHECK(mt->reserve(3, next).ok());

  test_memtable::destroy(mt);

  return;
}

/** Reserve n one-point writes to table "m". */
static void reserve_writes(test_memtable &mt, int n) {
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
static batch_seq_t visible_seq(test_memtable &mt) {
  test_memtable::snapshot_t snapshot;
  snapshot_all(mt, snapshot);

  const batch_seq_t seq = snapshot.m_visible_seq;
  mt.release_snapshot(snapshot);

  return (seq);
}

/** The owner publishes a completed prefix without waiting inside memtable. */
static void test_publish_prefix() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  reserve_writes(*mt, 3);
  CHECK(visible_seq(*mt) == 0);

  mt->publish(2);
  CHECK(visible_seq(*mt) == 2);

  mt->publish(3);
  CHECK(visible_seq(*mt) == 3);

  test_memtable::destroy(mt);

  return;
}

/** Shared state of the concurrent test: the lock that hands out sequence
numbers, as the write path will. */
struct writers_t {
  /* ====================== Immutable test input ====================== */

  /** Memtable under test. */
  test_memtable *m_memtable{nullptr};

  /* ==================== Write reservation state ===================== */

  /** Protects this group and orders memtable reservations. */
  std::mutex m_mutex;

  /** Sequence number of the next write. */
  batch_seq_t m_next_seq{1};
};

struct writer_arg_t {
  /** State shared by every writer. */
  writers_t *m_shared{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_index{0};

  /** Writes to make. */
  int m_write_count{0};
};

/** Write count times: plan, reserve and publish under the owner's shared lock.
Every 50th write adds a column.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  writers_t *w = arg->m_shared;
  char name[32];

  for (int i = 0; i < arg->m_write_count; ++i) {
    /* Every 50th write adds a column. */
    snprintf(name, sizeof(name), "c%d_%d", arg->m_thread_index, i);

    ingest_field_t f[] = {field_int("n", i), field_int(name, i)};

    const uint32_t n_fields = i % 50 == 0 ? 2 : 1;

    batch_seq_t seq;

    {
      std::lock_guard<std::mutex> lock(w->m_mutex);

      ingest_plan_t plan;
      plan_write(*w->m_memtable, "m", f, n_fields, plan);

      seq = w->m_next_seq++;
      CHECK(w->m_memtable->reserve(seq, plan).ok());

      w->m_memtable->publish(seq);
    }
  }

  return;
}

/** Owner-ordered reservations and publication leave every write visible
exactly once. */
static void test_concurrent_writers() {
  writers_t shared;
  CHECK(test_memtable::create(0, 0, &shared.m_memtable).ok());

  const int threads = 8;
  const int count = 200;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_shared = &shared;
    args[t].m_thread_index = t;
    args[t].m_write_count = count;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
  }

  test_memtable::snapshot_t snapshot;
  snapshot_all(*shared.m_memtable, snapshot);
  CHECK(snapshot.m_visible_seq == static_cast<batch_seq_t>(threads * count));

  /* 2 base columns plus one new column per thread every 50 writes. */
  const schema_ref_t visible_schema =
      snapshot_schema(*shared.m_memtable, snapshot, "m");

  const std::vector<const batch_t *> batches = snapshot_batches(snapshot, "m");
  CHECK(batches.size() == static_cast<size_t>(threads * count));
  CHECK(visible_schema->m_columns.size() ==
        static_cast<size_t>(2 + threads * (count / 50)));

  shared.m_memtable->release_snapshot(snapshot);
  test_memtable::destroy(shared.m_memtable);

  return;
}

/** Reserve and publish one write of value v to a table. */
static void write_value(test_memtable &mt, batch_seq_t seq,
                        const char *table_name, int64_t v) {
  ingest_field_t f[] = {field_int("n", v)};

  ingest_plan_t plan;
  plan_write(mt, table_name, f, 1, plan);
  CHECK(mt.reserve(seq, plan).ok());

  mt.publish(seq);

  return;
}

/** Byte accounting stays exact across reserved writes, schema growth, short
column prefixes, partial flushes and independently released snapshots. */
static void test_heterogeneous_byte_accounting() {
  const uint64_t small_bytes = 18;
  const uint64_t large_bytes = 4169;
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, small_bytes + large_bytes + 1, &mt).ok());

  write_value(*mt, 1, "m", 1);

  test_memtable::snapshot_t old;
  snapshot_all(*mt, old);

  ingest_field_t narrow_field = field_int("n", 4);
  ingest_plan_t narrow;
  plan_write(*mt, "m", &narrow_field, 1, narrow);

  const std::string payload(4096, 'p');
  ingest_field_t fields[2][4] = {
      {field_int("n", 2), field_str("pad", "x"), field_bool("enabled", true),
       field_double("score", .5)},
      {field_int("n", 3), field_str("pad", payload.c_str()),
       field_bool("enabled", false), field_double("score", -1.)}};

  ingest_point_t points[] = {
      point(TEST_BASE_TIME_US, nullptr, 0, fields[0], 4),
      point(TEST_BASE_TIME_US, nullptr, 0, fields[1], 4)};

  ingest_plan_t plan;
  const schema_ref_t schema = mt->find_schema("m");
  CHECK(plan_for(schema.get(), "m", points, 2, plan).ok());
  CHECK(plan.m_batch.calculate_data_bytes() == large_bytes);
  CHECK(mt->reserve(2, plan).ok());

  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_visible_seq == 1 && stats.m_live_writes == 2);
  CHECK(stats.m_live_rows == 3);
  CHECK(stats.m_live_bytes == small_bytes + large_bytes && mt->has_space());

  mt->publish(2);
  CHECK(mt->reserve(3, narrow).ok());

  mt->publish(3);
  mt->stats(stats);
  CHECK(stats.m_live_bytes == 2 * small_bytes + large_bytes);
  CHECK(!mt->has_space());

  mt->retire(1);
  mt->stats(stats);
  CHECK(stats.m_live_bytes == small_bytes + large_bytes && mt->has_space());
  CHECK(stats.m_retired_write_count == 1 &&
        stats.m_retired_bytes == small_bytes);

  test_memtable::snapshot_t newer;
  snapshot_all(*mt, newer);
  mt->retire(3);
  mt->stats(stats);
  CHECK(stats.m_live_rows == 0 && stats.m_live_bytes == 0);
  CHECK(stats.m_retired_write_count == 3);
  CHECK(stats.m_retired_bytes == 2 * small_bytes + large_bytes);

  mt->release_snapshot(old);
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 2);
  CHECK(stats.m_retired_bytes == small_bytes + large_bytes);

  const auto batches = snapshot_batches(newer, "m");
  CHECK(batches.size() == 2);
  CHECK(batches[0]->m_row_count == 2 && batches[1]->m_columns.size() == 2);

  mt->release_snapshot(newer);
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0 && stats.m_retired_bytes == 0);

  test_memtable::destroy(mt);
}

/** Collect only visible writes; retirement preserves their schema versions. */
static void test_collect_and_retire() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "a", 10);
  write_value(*mt, 2, "b", 20);

  /* Write 3 is reserved but not visible, so it is not collected. */
  ingest_field_t f[] = {field_int("n", 30)};

  ingest_plan_t plan;
  plan_write(*mt, "a", f, 1, plan);
  CHECK(mt->reserve(3, plan).ok());

  std::vector<const test_memtable::entry_t *> entries;
  mt->collect_unflushed(entries);
  CHECK(entries.size() == 2);
  CHECK(entries[0]->m_seq == 1 && entries[0]->m_batch.m_table_name == "a");
  CHECK(entries[1]->m_seq == 2 && entries[1]->m_batch.m_table_name == "b");

  mt->retire(2);

  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_flushed_seq == 2 && stats.m_live_writes == 1);
  CHECK(stats.m_live_rows == 1 && stats.m_retired_write_count == 0);

  /* Retired writes leave the snapshot; schemas stay for planning. */
  test_memtable::snapshot_t snapshot;
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_base_seq == 2 && snapshot.m_visible_seq == 2 &&
        snapshot_schemas(*mt, snapshot).empty());

  mt->release_snapshot(snapshot);
  CHECK(mt->find_schema("b") != nullptr);

  mt->publish(3);
  mt->collect_unflushed(entries);
  CHECK(entries.size() == 1 && entries[0]->m_seq == 3);

  test_memtable::destroy(mt);

  return;
}

/** A snapshot keeps retired batches until it is released. */
static void test_snapshot_pins_retired() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "m", 1);
  write_value(*mt, 2, "m", 2);

  test_memtable::snapshot_t old_snap;
  snapshot_all(*mt, old_snap);

  mt->retire(2);

  /* The old snapshot still reads its batches. */
  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 2);

  const std::vector<const batch_t *> batches = snapshot_batches(old_snap, "m");
  CHECK(batches.size() == 2);
  CHECK(batches[0]->m_columns[1].m_integers[0] == 1);
  CHECK(batches[1]->m_columns[1].m_integers[0] == 2);

  /* A newer snapshot does not see them and does not hold them. */
  test_memtable::snapshot_t new_snap;
  snapshot_all(*mt, new_snap);
  CHECK(snapshot_schemas(*mt, new_snap).empty());

  mt->release_snapshot(new_snap);

  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 2);

  /* Releasing the old one frees them. */
  mt->release_snapshot(old_snap);
  CHECK(old_snap.m_owner == nullptr);

  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0);

  test_memtable::destroy(mt);

  return;
}

/** Schema lookup selects requested tables once and excludes future columns,
including when a flush has retired the protected interval. */
static void test_snapshot_schema_lookup() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "a", 1);

  for (batch_seq_t seq = 2; seq <= 140; ++seq) {
    write_value(*mt, seq, "b", 2);
  }

  test_memtable::snapshot_t snapshot;
  snapshot_all(*mt, snapshot);

  ingest_field_t fields[] = {field_int("future", 3)};

  ingest_plan_t plan;
  plan_write(*mt, "a", fields, 1, plan);
  CHECK(mt->reserve(141, plan).ok());

  mt->publish(141);
  mt->retire(141);

  std::vector<schema_ref_t> schemas;
  mt->snapshot_schemas(snapshot, {"a", "a", "missing"}, schemas);
  CHECK(schemas.size() == 1);

  if (schemas.size() == 1) {
    CHECK(schemas[0]->m_name == "a");
    CHECK(schemas[0]->m_columns.size() == 2);
  }

  CHECK(mt->find_schema("a")->m_columns.size() == 3);

  mt->snapshot_schemas(snapshot, {}, schemas);
  CHECK(schemas.empty());

  mt->release_snapshot(snapshot);
  test_memtable::destroy(mt);
}

/** Reconcile across a partial flush, then keep only the query's original
upper boundary while later writes and flushes advance. */
static void test_interval_reconciliation() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "a", 1);
  write_value(*mt, 2, "a", 2);
  write_value(*mt, 3, "b", 3);
  write_value(*mt, 4, "b", 4);

  test_memtable::snapshot_t snapshot;
  mt->pin_snapshot(snapshot);
  mt->retire(2);
  CHECK(mt->align_snapshot(2, snapshot).ok());
  CHECK(snapshot.m_base_seq == 2 && snapshot.m_visible_seq == 4);
  CHECK(snapshot_schema(*mt, snapshot, "a") == nullptr);
  CHECK(snapshot_schemas(*mt, snapshot).size() == 1);
  CHECK(snapshot_schema(*mt, snapshot, "b") != nullptr);

  write_value(*mt, 5, "b", 5);
  mt->retire(5);

  const std::vector<const batch_t *> batches = snapshot_batches(snapshot, "b");
  CHECK(batches.size() == 2);
  CHECK(batches[0]->m_columns[1].m_integers[0] == 3);
  CHECK(batches[1]->m_columns[1].m_integers[0] == 4);

  /* Future write 5 is flushed but is outside the protected interval. */
  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 2);
  CHECK(stats.m_retired_bytes > 0);
  CHECK(stats.m_live_bytes == 0 && stats.m_live_writes == 0);

  mt->release_snapshot(snapshot);
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0 && stats.m_retired_bytes == 0);

  mt->release_snapshot(snapshot);
  test_memtable::destroy(mt);
}

/** Equal lower boundaries are independently registered; an empty interval
does not prevent reclamation after storage overtakes its upper boundary. */
static void test_shared_and_empty_intervals() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "m", 1);
  test_memtable::snapshot_t first, second, overtaken;
  snapshot_all(*mt, first);
  snapshot_all(*mt, second);
  mt->pin_snapshot(overtaken);
  write_value(*mt, 2, "m", 2);
  mt->retire(2);
  CHECK(mt->align_snapshot(2, overtaken).ok());
  CHECK(overtaken.m_base_seq == 2 && overtaken.m_visible_seq == 2);
  CHECK(snapshot_schemas(*mt, overtaken).empty());

  write_value(*mt, 3, "m", 3);

  test_memtable::snapshot_t later;
  snapshot_all(*mt, later);
  mt->retire(3);

  mt->release_snapshot(first);

  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 2);
  CHECK(snapshot_batches(second, "m")[0]->m_columns[1].m_integers[0] == 1);

  mt->release_snapshot(second);
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 1);
  CHECK(snapshot_batches(later, "m")[0]->m_columns[1].m_integers[0] == 3);

  mt->release_snapshot(later);

  write_value(*mt, 4, "m", 4);
  mt->retire(4);
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0);

  /* Its EOF remains valid after the deque has reclaimed past this interval. */
  batch_seq_t seq = overtaken.m_base_seq;
  const batch_t *batch = nullptr;
  CHECK(mt->scan_batches(overtaken, "m", seq, &batch, 1) == 0);
  CHECK(seq == overtaken.m_visible_seq);

  mt->release_snapshot(overtaken);
  test_memtable::destroy(mt);
}

/** Equal lower bounds with different upper bounds remain independent. Later
flushes release the suffix while a short old snapshot remains readable. */
static void test_snapshot_upper_bounds() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  write_value(*mt, 1, "m", 1);
  test_memtable::snapshot_t short_snap, long_snap;
  snapshot_all(*mt, short_snap);

  const batch_t *borrowed = snapshot_batches(short_snap, "m")[0];
  const uint64_t bytes = borrowed->calculate_data_bytes();
  write_value(*mt, 2, "m", 2);
  snapshot_all(*mt, long_snap);
  mt->retire(2);
  mt->release_snapshot(long_snap);

  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 1 && stats.m_retired_bytes == bytes);

  for (batch_seq_t seq = 3; seq <= 200; ++seq) {
    write_value(*mt, seq, "m", static_cast<int64_t>(seq));
    mt->retire(seq);
    mt->stats(stats);
    CHECK(stats.m_retired_write_count == 1 && stats.m_retired_bytes == bytes);
    CHECK(borrowed->m_columns[1].m_integers[0] == 1);
  }

  CHECK(snapshot_batches(short_snap, "m").size() == 1);

  mt->release_snapshot(short_snap);
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0 && stats.m_retired_bytes == 0);

  /* Appending after removing a deque containing holes preserves seq offsets. */
  write_value(*mt, 201, "m", 201);
  snapshot_all(*mt, long_snap);
  CHECK(snapshot_batches(long_snap, "m")[0]->m_columns[1].m_integers[0] == 201);

  mt->release_snapshot(long_snap);
  mt->retire(201);
  test_memtable::destroy(mt);
}

/** Snapshots retain disjoint spans and overlap across a flush. Check retained
bytes against a per-sequence oracle rather than the reclamation range index. */
static void test_interval_reclamation_model() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  std::vector<std::unique_ptr<test_memtable::snapshot_t>> snapshots(8);
  batch_seq_t flushed = 0;
  uint64_t bytes = 0;
  uint64_t random = 20261113;

  for (batch_seq_t seq = 1; seq <= 600; ++seq) {
    write_value(*mt, seq, "m", static_cast<int64_t>(seq));
    random = random * 6364136223846793005ULL + 1;

    const size_t slot = (random >> 32) % snapshots.size();

    if (seq % 5 == 0) {
      if (snapshots[slot] != nullptr) {
        mt->release_snapshot(*snapshots[slot]);
      }

      snapshots[slot] = std::make_unique<test_memtable::snapshot_t>();
      mt->pin_snapshot(*snapshots[slot]);

      if (seq % 10 == 0) {
        flushed = seq - 1;
        mt->retire(flushed);
      }

      CHECK(mt->align_snapshot(flushed, *snapshots[slot]).ok());
    } else if (seq % 7 == 0 && snapshots[slot] != nullptr) {
      mt->release_snapshot(*snapshots[slot]);
      snapshots[slot].reset();
    }

    if (seq % 3 == 0) {
      flushed = seq;
      mt->retire(flushed);
    }

    uint64_t expected = 0;

    for (batch_seq_t candidate = 1; candidate <= flushed; ++candidate) {
      for (const auto &snapshot : snapshots) {
        if (snapshot != nullptr && candidate > snapshot->m_base_seq &&
            candidate <= snapshot->m_visible_seq) {
          ++expected;
          break;
        }
      }
    }

    for (const auto &snapshot : snapshots) {
      if (snapshot == nullptr) {
        continue;
      }

      const auto batches = snapshot_batches(*snapshot, "m");
      CHECK(batches.size() == snapshot->m_visible_seq - snapshot->m_base_seq);

      batch_seq_t candidate = snapshot->m_base_seq;

      for (const batch_t *batch : batches) {
        ++candidate;
        CHECK(batch->m_columns[1].m_integers[0] ==
              static_cast<int64_t>(candidate));

        bytes = batch->calculate_data_bytes();
      }
    }

    test_memtable::stats_t stats;
    mt->stats(stats);
    CHECK(stats.m_retired_write_count == expected);
    CHECK(stats.m_retired_bytes == expected * bytes);
    CHECK(stats.m_live_writes == seq - flushed);

    std::vector<const test_memtable::entry_t *> entries;
    mt->collect_unflushed(entries);
    CHECK(entries.size() == seq - flushed);

    for (size_t i = 0; i < entries.size(); ++i) {
      CHECK(entries[i]->m_seq == flushed + i + 1);
    }
  }

  for (auto &snapshot : snapshots) {
    if (snapshot != nullptr) {
      mt->release_snapshot(*snapshot);
    }
  }

  mt->retire(600);

  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0 && stats.m_retired_bytes == 0);

  test_memtable::destroy(mt);
}

/** A failed reconciliation retains the original protection until cleanup. */
static void test_failed_interval_completion() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(10, 0, &mt).ok());

  write_value(*mt, 11, "m", 11);

  test_memtable::snapshot_t snapshot;
  mt->pin_snapshot(snapshot);
  mt->retire(11);
  CHECK(mt->align_snapshot(9, snapshot).code() == DB_CORRUPTION);
  CHECK(mt->align_snapshot(12, snapshot).code() == DB_CORRUPTION);
  CHECK(snapshot.m_base_seq == 10 && snapshot.m_visible_seq == 11);
  CHECK(snapshot_batches(snapshot, "m")[0]->m_columns[1].m_integers[0] == 11);

  mt->release_snapshot(snapshot);

  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0);

  test_memtable::destroy(mt);
}

/** Table filtering may return an empty group before EOF, and pointers taken
before a flush remain valid until interval release. */
static void test_bounded_interval_reads() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  for (batch_seq_t seq = 1; seq <= 150; ++seq) {
    write_value(*mt, seq, seq <= 70 ? "other" : "m", seq);
  }

  test_memtable::snapshot_t snapshot;
  snapshot_all(*mt, snapshot);

  batch_seq_t seq = snapshot.m_base_seq;
  const batch_t *batches[64];
  CHECK(mt->scan_batches(snapshot, "m", seq, batches, 64) == 0);
  CHECK(seq == 64);
  CHECK(mt->scan_batches(snapshot, "m", seq, batches, 64) == 58);
  CHECK(seq == 128);

  mt->retire(150);
  CHECK(batches[0]->m_columns[1].m_integers[0] == 71);
  CHECK(batches[57]->m_columns[1].m_integers[0] == 128);
  CHECK(mt->scan_batches(snapshot, "m", seq, batches, 64) == 22);
  CHECK(seq == 150);
  CHECK(mt->scan_batches(snapshot, "m", seq, batches, 64) == 0);

  mt->release_snapshot(snapshot);
  test_memtable::destroy(mt);
}

/** Stored tables restore their current directories, and writes continue
after F. */
static void test_restore_schema() {
  /* Writes up to 10 are in storage; the directory now has three columns. */
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(10, 0, &mt).ok());

  ingest_field_t f1[] = {field_int("n", 1)};

  ingest_point_t p1 = point(TEST_BASE_TIME_US, nullptr, 0, f1, 1);
  ingest_plan_t first;
  CHECK(plan_for(nullptr, "m", &p1, 1, first).ok());

  const schema_t initial_schema = schema_after(nullptr, first);

  ingest_field_t f2[] = {field_int("n", 1), field_double("d", 1.5)};

  ingest_point_t p2 = point(TEST_BASE_TIME_US, nullptr, 0, f2, 2);
  ingest_plan_t second;
  CHECK(plan_for(&initial_schema, "m", &p2, 1, second).ok());

  const schema_t expanded_schema = schema_after(&initial_schema, second);

  CHECK(mt->restore_schema(expanded_schema).ok());
  CHECK(mt->restore_schema(expanded_schema).code() == DB_CORRUPTION);
  CHECK(mt->find_schema("m")->m_columns.size() == 3);

  /* A replayed write planned before the column append still fits. */
  ingest_plan_t short_write;
  CHECK(plan_for(&initial_schema, "m", &p1, 1, short_write).ok());

  ingest_plan_t bad = short_write;
  bad.m_batch.m_columns[1].m_data_type = data_type_t::DOUBLE;
  CHECK(mt->reserve(11, bad).code() == DB_CORRUPTION);

  bad = short_write;
  bad.m_batch.m_columns[1].m_column_id = 2;
  CHECK(mt->reserve(11, bad).code() == DB_CORRUPTION);
  CHECK(mt->reserve(11, short_write).ok());

  mt->publish(11);

  test_memtable::snapshot_t snapshot;
  snapshot_all(*mt, snapshot);
  CHECK(snapshot.m_base_seq == 10 && snapshot.m_visible_seq == 11);
  CHECK(snapshot_batches(snapshot, "m").size() == 1);
  CHECK(snapshot_schema(*mt, snapshot, "m")->m_columns.size() == 3);
  CHECK(snapshot_batches(snapshot, "m")[0]->m_columns.size() == 2);

  mt->release_snapshot(snapshot);

  std::vector<const test_memtable::entry_t *> entries;
  mt->collect_unflushed(entries);
  CHECK(entries.size() == 1 && entries[0]->m_batch.m_schema_version == 3);

  test_memtable::destroy(mt);

  return;
}

struct flusher_arg_t {
  /* ====================== Immutable test input ====================== */

  /** Memtable under test. */
  test_memtable *m_memtable{nullptr};

  /* ======================= Shared stop state ======================== */

  /** Shared stop flag; reads and writes hold *m_stop_mutex. */
  bool *m_stop{nullptr};

  /** Protects *m_stop. */
  std::mutex *m_stop_mutex{nullptr};

  /* ===================== Worker-owned counters ====================== */

  /** Rounds that retired something. Read by the caller after join. */
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

    std::vector<const test_memtable::entry_t *> entries;
    arg->m_memtable->collect_unflushed(entries);

    if (!entries.empty()) {
      arg->m_memtable->retire(entries.back()->m_seq);
      arg->m_rounds++;
    }
  }

  return;
}

struct reader_arg_t {
  /* ====================== Immutable test input ====================== */

  /** Memtable under test. */
  test_memtable *m_memtable{nullptr};

  /* ======================= Shared stop state ======================== */

  /** Shared stop flag; reads and writes hold *m_stop_mutex. */
  bool *m_stop{nullptr};

  /** Protects *m_stop. */
  std::mutex *m_stop_mutex{nullptr};

  /* ===================== Worker-owned counters ====================== */

  /** Calls that failed. Read by the caller after join. */
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

    test_memtable::snapshot_t snapshot;
    snapshot_all(*arg->m_memtable, snapshot);

    size_t batches = 0;

    for (const schema_ref_t &schema :
         snapshot_schemas(*arg->m_memtable, snapshot)) {
      for (const batch_t *b : snapshot_batches(snapshot, schema->m_name)) {
        if (b->m_columns[1].m_integers[0] < 0 || b->m_row_count != 1) {
          arg->m_errors++;
        }

        batches++;
      }
    }

    if (batches != snapshot.m_visible_seq - snapshot.m_base_seq) {
      arg->m_errors++;
    }

    arg->m_memtable->release_snapshot(snapshot);
  }

  return;
}

/** Writers, a retiring flusher and readers run together; readers always see
consistent snapshots. */
static void test_concurrent_retirement() {
  writers_t shared;
  CHECK(test_memtable::create(0, 0, &shared.m_memtable).ok());

  bool stop = false;
  std::mutex stop_mutex;

  flusher_arg_t flusher;
  flusher.m_memtable = shared.m_memtable;
  flusher.m_stop = &stop;
  flusher.m_stop_mutex = &stop_mutex;

  std::thread flusher_thread(flusher_main, &flusher);

  reader_arg_t reader;
  reader.m_memtable = shared.m_memtable;
  reader.m_stop = &stop;
  reader.m_stop_mutex = &stop_mutex;

  std::thread reader_thread(reader_main, &reader);

  const int threads = 4;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_shared = &shared;
    args[t].m_thread_index = t;
    args[t].m_write_count = 300;
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

  printf("concurrent retirement: %d flush rounds\n", flusher.m_rounds);

  /* Retire the rest; with no snapshot open nothing stays behind. */
  std::vector<const test_memtable::entry_t *> entries;
  shared.m_memtable->collect_unflushed(entries);

  if (!entries.empty()) {
    shared.m_memtable->retire(entries.back()->m_seq);
  }

  test_memtable::stats_t stats;
  shared.m_memtable->stats(stats);
  CHECK(stats.m_flushed_seq == static_cast<batch_seq_t>(threads * 300));
  CHECK(stats.m_live_writes == 0 && stats.m_live_rows == 0);
  CHECK(stats.m_retired_write_count == 0);

  test_memtable::destroy(shared.m_memtable);

  return;
}

/** A long-lived reader keeps its payload while writers, readers and a flusher
reclaim later string batches concurrently. */
static void test_concurrent_reclamation() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  const std::string payload(4096, 'p');
  ingest_field_t fields[] = {field_int("n", 0),
                             field_str("pad", payload.c_str())};

  ingest_plan_t plan;
  plan_write(*mt, "m", fields, 2, plan);
  CHECK(mt->reserve(1, plan).ok());

  mt->publish(1);

  test_memtable::snapshot_t old;
  snapshot_all(*mt, old);

  const batch_t *borrowed = snapshot_batches(old, "m")[0];
  const uint64_t bytes = borrowed->calculate_data_bytes();

  bool stop = false;
  std::mutex stop_mutex;
  flusher_arg_t flusher;
  flusher.m_memtable = mt;
  flusher.m_stop = &stop;
  flusher.m_stop_mutex = &stop_mutex;

  reader_arg_t reader;
  reader.m_memtable = mt;
  reader.m_stop = &stop;
  reader.m_stop_mutex = &stop_mutex;

  std::thread flush_thread(flusher_main, &flusher);
  std::thread read_thread(reader_main, &reader);

  for (batch_seq_t seq = 2; seq <= 3000; ++seq) {
    fields[0] = field_int("n", static_cast<int64_t>(seq));
    plan_write(*mt, "m", fields, 2, plan);
    CHECK(mt->reserve(seq, plan).ok());

    mt->publish(seq);
    CHECK(borrowed->m_columns[2].m_string_data == payload);
  }

  {
    std::lock_guard<std::mutex> lock(stop_mutex);
    stop = true;
  }

  flush_thread.join();
  read_thread.join();
  CHECK(reader.m_errors == 0);

  mt->retire(3000);

  test_memtable::stats_t stats;
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 1 && stats.m_retired_bytes == bytes);
  CHECK(borrowed->m_columns[1].m_integers[0] == 0);
  CHECK(borrowed->m_columns[2].m_string_data == payload);

  mt->release_snapshot(old);
  mt->stats(stats);
  CHECK(stats.m_retired_write_count == 0 && stats.m_retired_bytes == 0);

  test_memtable::destroy(mt);
}

/** Copied directories are independent; retained directories survive later
writes. */
static void test_schema_ownership() {
  test_memtable *mt = nullptr;
  CHECK(test_memtable::create(0, 0, &mt).ok());

  ingest_field_t first[] = {field_int("n", 1)};

  ingest_plan_t plan;
  plan_write(*mt, "m", first, 1, plan);
  CHECK(mt->reserve(1, plan).ok());

  std::vector<schema_ref_t> borrowed;
  mt->collect_schemas(borrowed);
  CHECK(borrowed.size() == 1);

  if (borrowed.size() == 1) {
    CHECK(borrowed[0] == mt->find_schema("m"));
    CHECK(borrowed[0]->m_name == "m");

    ingest_field_t second[] = {field_int("n", 2), field_double("d", 1.5)};
    plan_write(*mt, "m", second, 2, plan);
    CHECK(mt->reserve(2, plan).ok());
    CHECK(borrowed[0]->m_columns.size() == 2);
    CHECK(mt->find_schema("m")->m_columns.size() == 3);

    mt->collect_schemas(borrowed);
    CHECK(borrowed.size() == 1 && borrowed[0]->m_columns.size() == 3);
  }

  test_memtable::destroy(mt);
  CHECK(borrowed.size() == 1 && borrowed[0]->m_name == "m");

  return;
}

/** Run visibility, snapshot, reclamation and concurrency tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"capacity", test_capacity},
      {"heterogeneous_byte_accounting", test_heterogeneous_byte_accounting},

      {"reserve_then_publish", test_reserve_then_publish},
      {"column_prefixes", test_column_prefixes},
      {"schema_prefix_lifetime", test_schema_prefix_lifetime},
      {"schema_ownership", test_schema_ownership},
      {"plan_must_fit", test_plan_must_fit},
      {"publish_prefix", test_publish_prefix},
      {"concurrent_writers", test_concurrent_writers},

      {"collect_and_retire", test_collect_and_retire},
      {"snapshot_pins_retired", test_snapshot_pins_retired},
      {"snapshot_schema_lookup", test_snapshot_schema_lookup},

      {"interval_reconciliation", test_interval_reconciliation},
      {"shared_and_empty_intervals", test_shared_and_empty_intervals},
      {"snapshot_upper_bounds", test_snapshot_upper_bounds},
      {"interval_reclamation_model", test_interval_reclamation_model},
      {"failed_interval_completion", test_failed_interval_completion},
      {"bounded_interval_reads", test_bounded_interval_reads},

      {"restore_schema", test_restore_schema},

      {"concurrent_retirement", test_concurrent_retirement},
      {"concurrent_reclamation", test_concurrent_reclamation},
  };

  return (test_main("memtable-t", argc, argv, cases));
}
