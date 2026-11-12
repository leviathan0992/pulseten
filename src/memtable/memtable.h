/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable.h
 *    In-memory store of accepted writes and the table schemas.
 *
 * A write enters in two steps. memtable::reserve() takes its schema change
 * and batch; later writes plan against that schema at once, but readers do
 * not see the batch. The db group leader calls memtable::publish() after WAL
 * success to expose a complete prefix. This layer does not wait for writes
 * or retain a permanent failure state.
 *
 * memtable::trim() takes writes out after a flush stored them in
 * DuckDB. A snapshot may still read a retired batch, which is freed with the
 * last snapshot that references it. Schemas and batches never change once
 * added. Tables live until the memtable is freed; schemas live while
 * referenced.
 *
 * All functions are thread-safe unless noted.
 *
 *-------------------------------------------------------------------------
 */

#ifndef memtable_h
#define memtable_h

#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <vector>

#include "batch.h"
#include "ingest.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_noncopyable.h"

class memtable;
struct memtable_entry_t;

/** One table in a snapshot. */
struct memtable_table_view_t {
  /** Latest directory visible to these batches. */
  schema_ref_t m_schema;

  /** In sequence order. An older batch lacks columns added after it; they
  read as NULL. */
  std::vector<const batch_t *> m_batches;
};

/** Every batch with m_base_seq < seq <= m_visible_seq. Writes up to m_base_seq
are in DuckDB instead. */
struct memtable_snapshot_t {
  /** Entries pinned by this snapshot; released by
  memtable::release_snapshot(). */
  std::vector<memtable_entry_t *> m_entries;

  /** Writes up to this sequence are read from DuckDB instead of this
  snapshot. */
  batch_seq_t m_base_seq{0};

  /** Highest sequence included in this snapshot, inclusive. */
  batch_seq_t m_visible_seq{0};

  /** Tables with at least one batch, by normalized name. */
  std::map<std::string, memtable_table_view_t> m_tables;
};

/** A visible, unflushed write. Its pointers stay valid until it is retired. */
struct memtable_flush_item_t {
  /** Sequence number of the write to flush. */
  batch_seq_t m_seq{0};

  /** Borrowed column batch to append to storage. */
  const batch_t *m_batch{nullptr};

  /** Pinned column directory; the batch may use a shorter prefix. */
  schema_ref_t m_schema;
};

struct memtable_stats_t {
  /** Every write up to this sequence has been flushed and retired. */
  batch_seq_t m_flushed_seq{0};

  /** Every write up to this sequence is visible to readers. */
  batch_seq_t m_visible_seq{0};

  /** Writes not yet retired, visible or not; the next two count these. */
  uint64_t m_live_writes{0};

  /** Rows in writes not yet retired, including writes not yet visible. */
  uint64_t m_live_rows{0};

  /** Column data bytes in writes not yet retired, as measured by
  batch_t::data_bytes(). */
  uint64_t m_live_bytes{0};

  /** Retired writes still held by snapshots. */
  uint64_t m_retired_writes{0};
};

/** Owns schemas, batches and snapshot references. */
class memtable final : private ut_noncopyable {
 public:
  /** Construct an empty object with a fixed capacity threshold. */
  explicit memtable(uint64_t limit_bytes) : m_limit_bytes(limit_bytes) {}

  /** Release every resource still owned by the object. */
  ~memtable();

  /** Create an empty memtable whose first write will be start_seq + 1.
  limit_bytes is the capacity threshold; 0 disables capacity waiting.
  @return DB_SUCCESS or DB_OUT_OF_MEMORY */
  static db_status_t create(batch_seq_t start_seq, uint64_t limit_bytes,
                            memtable **out);

  /** Restore a stored schema without loading persisted rows.
  Call before reserving any writes; schema must already be validated.
  @return DB_SUCCESS, DB_OUT_OF_MEMORY or DB_CORRUPTION if the table already
    exists */
  db_status_t restore_schema(const schema_t &schema);

  /** Retain the current immutable directory for write planning, including
  schema changes of reserved writes. NULL if absent. */
  schema_ref_t find_schema(const std::string &table_name);

  /** Take a write and move the batch out of plan. It is not visible yet.
  REQUIRES: seq is one more than the previous reserved write.
  @retval DB_SUCCESS reserved
  @retval DB_CORRUPTION the plan does not fit the current schema; nothing
  changed
  @retval DB_OUT_OF_MEMORY allocation failed; nothing changed */
  db_status_t reserve(batch_seq_t seq, ingest_plan_t &plan);

  /** Publish the complete prefix through seq after db commits its WAL group.
  REQUIRES: seq advances the visible boundary without skipping uncommitted I/O.
  No waiting or allocation is performed here. */
  void publish(batch_seq_t seq);

  /** List the writes flushed_seq + 1 .. visible_seq, in sequence order. */
  void collect_unflushed(std::vector<memtable_flush_item_t> &items);

  /** Remove stored writes up to flushed_seq from the live batches. Batches
  still referenced by snapshots are retained until those references are
  released. REQUIRES: flushed_seq <= visible_seq. */
  void trim(batch_seq_t flushed_seq);

  /** Retain each table's current immutable schema, including reserved writes.
  Replaces schemas; each reference remains valid until the caller releases it.
  */
  void reference_schemas(std::vector<schema_ref_t> &schemas);

  /** Reference every visible, unflushed write. Call before starting the DuckDB
  transaction: writes retired later stay alive, so the stored snapshot taken
  next never misses one. snapshot must be released even if
  memtable::complete_snapshot() fails. */
  void pin_snapshot(memtable_snapshot_t &snapshot);

  /** Complete a pinned snapshot with the F read by its DuckDB transaction,
  keeping only the writes after F. If F passed the pinned visible_seq, snapshot
  holds no batches and its visible_seq becomes F.
  @return DB_SUCCESS, or DB_CORRUPTION if F precedes the pin or a batch is
    missing */
  db_status_t complete_snapshot(batch_seq_t flushed_seq,
                                memtable_snapshot_t &snapshot);

  /** Release a snapshot and reset it. Does nothing if it was never taken. */
  void release_snapshot(memtable_snapshot_t &snapshot);

  /** Whether unretired column bytes are below the threshold, or it is disabled.
  Does not reserve capacity or wait for space. */
  bool has_space();

  /** Read the counters. */
  void read_stats(memtable_stats_t &stats);

  /** Destroy a memtable; mt may be NULL.
  REQUIRES: no call in progress and no snapshot in use. */
  static void destroy(memtable *mt);

 private:
  /** Assert sequence ordering and live-entry count consistency. */
  void assert_consistent() const;

  /** Check whether a plan is consistent with the table and schema state.
  @return true if the consistency checks pass; allocation may still fail when
    applying the plan */
  bool can_apply_plan(const ingest_plan_t &plan);

  /** Apply a plan validated by memtable::can_apply_plan().
  @param[in,out]  plan  validated write plan; its batch is moved into the
                        memtable
  @return DB_SUCCESS after publishing the schema and reserved batch under the
    memtable lock */
  db_status_t apply_plan(batch_seq_t seq, ingest_plan_t &plan);

  /** Locate a reserved, unretired write by sequence number.
  @return the live entry with a sequence number */
  memtable_entry_t &get_live_entry(batch_seq_t seq);

  /** Free retired entries that no open snapshot can use. */
  void reclaim();

  /** Append one selected immutable batch. */
  void snapshot_append(const memtable_entry_t &entry,
                       memtable_snapshot_t &snapshot) const;

  /** Capacity threshold fixed at construction; 0 disables capacity waiting. */
  const uint64_t m_limit_bytes{0};

  /* Protected by m_mutex. */

  /** Held only briefly; nothing else is locked while it is held. */
  ut_mutex_t m_mutex;

  /** Tables by normalized name. */
  std::map<std::string, schema_ref_t> m_tables;

  /** Every write after m_flushed_seq; m_live[i] has sequence number
  m_flushed_seq + 1 + i. */
  std::deque<memtable_entry_t *> m_live;

  /* Inclusive sequence boundaries: F <= V <= R.

  +------------------+---------------------+-----------------------+
  | seq <= F         | F < seq <= V        | V < seq <= R          |
  | flushed, retired | visible, unflushed  | reserved, not visible |
  +------------------+---------------------+-----------------------+ --> seq
                     |<---------------- m_live ----------------->|

  Either live range may be empty. V advances over consecutive completed WAL
  writes; F advances only after storage commit and retirement. */

  /** Last flushed and retired sequence F. */
  batch_seq_t m_flushed_seq{0};

  /** Last visible sequence V. */
  batch_seq_t m_visible_seq{0};

  /** Last reserved sequence R. */
  batch_seq_t m_reserved_seq{0};

  /** Rows in m_live. */
  uint64_t m_live_rows{0};

  /** Column bytes in m_live. */
  uint64_t m_live_bytes{0};

  /** First of the retired entries not freed yet, oldest first. */
  memtable_entry_t *m_retired_head{nullptr};

  /** Last retired entry, where memtable::trim() appends. */
  memtable_entry_t *m_retired_tail{nullptr};

  /** Number of retired entries awaiting release. */
  uint64_t m_retired_count{0};
};

#endif /* memtable_h */
