/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_memtable.h
 *    Helpers for tests that read the memtable directly.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_memtable_h
#define test_memtable_h

#include <map>
#include <string>
#include <vector>

#include "db_schema.h"
#include "db_table.h"
#include "memtable.h"
#include "test_util.h"
#include "ut.h"

/** Take a snapshot of every visible, unflushed write, as a query does: pin,
then complete the pin at the current flushed boundary. */
static inline void snapshot_all(memtable &mt, memtable::snapshot_t &snapshot) {
  mt.pin_snapshot(snapshot);

  memtable::stats_t stats;
  mt.stats(stats);

  CHECK(mt.align_snapshot(stats.m_flushed_seq, snapshot).ok());

  return;
}

/** Resolve snapshot versions through the database-owned table directory. */
static inline std::vector<schema_ref_t> snapshot_schemas(
    const memtable::snapshot_t &snapshot, const db_tables &tables) {
  std::vector<schema_ref_t> current;
  tables.collect_schemas(current);

  std::vector<std::string> names;

  for (const schema_ref_t &schema : current) {
    names.push_back(schema->m_name);
  }

  std::map<std::string, schema_version_t> versions;
  snapshot.m_owner->schema_versions(snapshot, names, versions);

  std::vector<schema_ref_t> schemas;

  for (const auto &[name, version] : versions) {
    schemas.push_back(tables.find(name)->schema(version));
  }

  return (schemas);
}

/** Read one table's schema within the protected interval. */
static inline schema_ref_t snapshot_schema(const memtable::snapshot_t &snapshot,
                                           const db_tables &tables,
                                           const std::string &name) {
  std::map<std::string, schema_version_t> versions;
  snapshot.m_owner->schema_versions(snapshot, {name}, versions);

  return (versions.empty() ? nullptr
                           : tables.find(name)->schema(versions.at(name)));
}

/** A standalone test owns its table directory separately from its memtable.
Production uses the same db_tables reservation and version-resolution paths. */
class test_memtable final : private ut_noncopyable {
 public:
  typedef memtable::snapshot_t snapshot_t;
  typedef memtable::stats_t stats_t;
  typedef memtable::entry_t entry_t;

  /** Create independent table metadata and batch storage for a test. */
  static db_status_t create(batch_seq_t seq, uint64_t limit,
                            test_memtable **out) {
    *out = nullptr;
    test_memtable *test = ut_new<test_memtable>();
    if (test == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    const db_status_t err = memtable::create(seq, limit, &test->m_memtable);
    if (!err.ok()) {
      ut_delete(test);

      return (err);
    }

    *out = test;

    return (DB_SUCCESS);
  }

  /** Destroy batches before the fixture's schema directory is released. */
  ~test_memtable() { memtable::destroy(m_memtable); }

  /** Delete a memtable fixture; NULL is accepted. */
  static void destroy(test_memtable *mt) { ut_delete(mt); }

  /** Return the underlying memtable. */
  operator memtable &() { return (*m_memtable); }

  /** Return the schema directory associated with this memtable. */
  const db_tables &tables() const { return (m_tables); }

  /** Retain a table's current schema, or return NULL if absent. */
  schema_ref_t find_schema(const std::string &name) const {
    const db_table *table = m_tables.find(name);

    return (table == nullptr ? nullptr : table->schema());
  }

  /** Restore a schema in the fixture's table directory. */
  db_status_t restore_schema(const schema_t &schema) {
    return (m_tables.restore_schema(schema));
  }

  /** Retain the current schema of every initialized table. */
  void collect_schemas(std::vector<schema_ref_t> &schemas) const {
    m_tables.collect_schemas(schemas);
  }

  /** Reserve a batch together with its table-owned schema version. */
  db_status_t reserve(batch_seq_t seq, ingest_plan_t &plan) {
    return (m_tables.reserve_write(*m_memtable, seq, plan));
  }

  /** Make the reserved prefix visible through the given sequence. */
  void publish(batch_seq_t seq) { m_memtable->publish(seq); }

  /** Mark the prefix as flushed and reclaim batches not pinned by snapshots. */
  void retire(batch_seq_t seq) { m_memtable->retire(seq); }

  /** Read the underlying memtable's sequence and memory counters. */
  void stats(stats_t &out) { m_memtable->stats(out); }

  /** Check whether the memtable is below its capacity threshold. */
  bool has_space() { return (m_memtable->has_space()); }

  /** Collect visible batches that have not yet been flushed. */
  void collect_unflushed(std::vector<const entry_t *> &entries) {
    m_memtable->collect_unflushed(entries);
  }

  /** Pin the memtable interval before fixing the stored snapshot. */
  void pin_snapshot(snapshot_t &snapshot) {
    m_memtable->pin_snapshot(snapshot);
  }

  /** Align a pinned interval with the stored transaction's boundary. */
  db_status_t align_snapshot(batch_seq_t seq, snapshot_t &snapshot) {
    return (m_memtable->align_snapshot(seq, snapshot));
  }

  /** Release a pinned interval and allow its batches to be reclaimed. */
  void release_snapshot(snapshot_t &snapshot) {
    m_memtable->release_snapshot(snapshot);
  }

  /** Resolve schema versions for the requested tables in a snapshot. */
  void snapshot_schemas(const snapshot_t &snapshot,
                        const std::vector<std::string> &names,
                        std::vector<schema_ref_t> &schemas) {
    std::map<std::string, schema_version_t> versions;
    m_memtable->schema_versions(snapshot, names, versions);
    schemas.clear();

    for (const auto &[name, version] : versions) {
      schemas.push_back(m_tables.find(name)->schema(version));
    }
  }

  /** Read a bounded set of a table's batches from a pinned interval. */
  size_t scan_batches(const snapshot_t &snapshot, const std::string &name,
                      batch_seq_t &seq, const batch_t **out, size_t capacity) {
    return (m_memtable->scan_batches(snapshot, name, seq, out, capacity));
  }

 private:
  /** Own schema versions before and after individual batches are retired. */
  db_tables m_tables;

  /** Batch storage, destroyed before the table directory. */
  memtable *m_memtable{nullptr};
};

/** Resolve a snapshot's schemas through the fixture's table directory. */
static inline std::vector<schema_ref_t> snapshot_schemas(
    const test_memtable &mt, const memtable::snapshot_t &snapshot) {
  return (snapshot_schemas(snapshot, mt.tables()));
}

/** Resolve one table's schema at the snapshot boundary. */
static inline schema_ref_t snapshot_schema(const test_memtable &mt,
                                           const memtable::snapshot_t &snapshot,
                                           const std::string &name) {
  return (snapshot_schema(snapshot, mt.tables(), name));
}

/** Read a table through the same bounded interval access as the scanner.
@return batch pointers valid until the snapshot is released */
static inline std::vector<const batch_t *> snapshot_batches(
    const memtable::snapshot_t &snapshot, const std::string &table_name) {
  std::vector<const batch_t *> result;
  batch_seq_t seq = snapshot.m_base_seq;

  while (seq < snapshot.m_visible_seq) {
    const batch_t *batches[7];
    const size_t count =
        snapshot.m_owner->scan_batches(snapshot, table_name, seq, batches,
                                       sizeof(batches) / sizeof(batches[0]));
    result.insert(result.end(), batches, batches + count);
  }

  return (result);
}

#endif /* test_memtable_h */
