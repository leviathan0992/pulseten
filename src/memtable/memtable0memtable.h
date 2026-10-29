/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable0memtable.h
 *    In-memory store of accepted writes and the table schemas.
 *
 * A write goes through two steps. memtable_reserve() takes the write's
 * schema change and batch under its sequence number; the schema change is
 * at once seen by later write planning, but readers do not see the batch
 * yet. memtable_publish(), called once the write is durable, makes it
 * visible. Batches become visible strictly in sequence order: a snapshot
 * always holds every batch up to its visible_seq and nothing after, even
 * when writes finish out of order.
 *
 * After a flush has stored writes up to some sequence number in DuckDB,
 * memtable_retire() takes them out. Schemas and batches are never changed
 * once added. Fixed snapshots hold exact references to their batches;
 * readers still establishing a snapshot temporarily pin later retirements.
 * A retired batch is freed once no reference or pending pin needs it, so
 * unrelated later batches do not wait for old queries. A dropped table's
 * schemas live until its last batch or write planner releases them.
 *
 *-------------------------------------------------------------------------
 */

#ifndef memtable0memtable_h
#define memtable0memtable_h

#include <map>
#include <string>
#include <vector>

#include "batch0batch.h"
#include "ingest0ingest.h"
#include "schema0schema.h"
#include "ut0err.h"
#include "ut0ut.h"

/** An in-memory store. */
struct memtable_t;

/** A table whose schema versions are owned by the memtable. */
struct memtable_table_t;

/** A planner's reference to one immutable schema. Do not copy it; release
it before freeing the memtable. */
struct memtable_schema_ref_t {
  /** The schema; NULL if the table does not exist or the memtable failed. */
  const schema_t *schema{nullptr};

  /** Table kept alive until the reference is released. */
  memtable_table_t *table{nullptr};
};

/** One table in a snapshot. */
struct memtable_table_view_t {
  /** Schema of the newest visible batch of the table. */
  const schema_t *schema{nullptr};

  /** Visible batches in sequence order. A batch follows the schema version
  it was written with, so it lacks columns added after it; those read as
  NULL. */
  std::vector<const batch_t *> batches;
};

/** A consistent view: every batch with base_seq < seq <= visible_seq.
Writes up to base_seq are flushed and no longer in the memtable. Must be
released with memtable_snapshot_release(). */
struct memtable_snapshot_t {
  /** Identifies the snapshot to the memtable; 0 when not taken. */
  uint64_t id{0};

  /** Writes up to here are flushed; the snapshot holds none of them. */
  batch_seq_t base_seq{0};

  /** The snapshot holds every write after base_seq up to here. */
  batch_seq_t visible_seq{0};

  /** Tables with at least one visible batch, by canonical name. */
  std::map<std::string, memtable_table_view_t> tables;
};

/** A visible, unflushed write handed to a flush. */
struct memtable_flush_item_t {
  /** Sequence number of the write. */
  batch_seq_t seq{0};

  /** Its rows; valid until the write is retired. */
  const batch_t *batch{nullptr};

  /** Schema version the batch follows. */
  const schema_t *schema{nullptr};
};

/** Counters of a memtable. */
struct memtable_stats_t {
  /** Every write up to here is flushed and retired. */
  batch_seq_t flushed_seq{0};

  /** Every write up to here is visible. */
  batch_seq_t visible_seq{0};

  /** Writes not yet retired, visible or not. */
  uint64_t live_writes{0};

  /** Rows of those writes. */
  uint64_t live_rows{0};

  /** Column bytes of those writes. */
  uint64_t live_bytes{0};

  /** Retired writes held by exact snapshot refs or pending reader pins. */
  uint64_t garbage_writes{0};

  /** Dropped tables still held by retired batches or write planners. */
  uint64_t dropped_tables{0};
};

/** Create an empty memtable.
@param[in]   start_seq  sequence number of the last write that is not in this
                        memtable; the first reserved write is start_seq + 1
@param[out]  mt         new memtable
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
dberr_t memtable_create(batch_seq_t start_seq, memtable_t **mt);

/** Free a memtable and every schema and batch in it. No call may be in
progress and no snapshot may be in use.
@param[in]  mt  memtable; may be NULL */
void memtable_free(memtable_t *mt);

/** Add a table that already exists in storage, with all its schema versions.
Only before the first write is reserved.
@param[in]  mt        memtable
@param[in]  versions  versions 1, 2, ... of one table
@return DB_SUCCESS, DB_CORRUPTION if the versions are not consecutive or the
  table exists, or DB_OUT_OF_MEMORY */
dberr_t memtable_add_table(memtable_t &mt,
                           const std::vector<schema_t> &versions);

/** Newest schema of a table, including schema changes of writes that are
reserved but not yet visible. Used to plan writes. The caller must exclude
concurrent drops while using the pointer; use memtable_schema_acquire() when
that is not possible.
@param[in]  mt     memtable
@param[in]  table  canonical table name
@return the schema, valid until the table is dropped, or NULL if the table
  does not exist */
const schema_t *memtable_get_schema(memtable_t &mt, const std::string &table);

/** Hold the newest schema for planning outside the engine's write lock. The
reference survives table drops and schema changes. Does not allocate.
@param[in]   mt     memtable
@param[in]   table  canonical table name
@param[out]  ref    new reference; must not already hold one */
void memtable_schema_acquire(memtable_t &mt, const std::string &table,
                             memtable_schema_ref_t &ref);

/** Release a planner's schema reference; a dropped table may be freed.
@param[in]      mt   memtable
@param[in,out]  ref  reference; reset, also safe when empty */
void memtable_schema_release(memtable_t &mt, memtable_schema_ref_t &ref);

/** Take a write: apply its schema change and hold its batch, not visible yet.
The batch is moved out of plan.
@param[in]      mt    memtable
@param[in]      seq   sequence number of the write; must be one more than the
                      previous reserved write
@param[in,out]  plan  write plan made against the current schema
@retval DB_SUCCESS reserved
@retval DB_CORRUPTION the plan does not fit the current schema; nothing
  changed
@retval DB_FENCED the memtable has failed; nothing changed
@retval DB_OUT_OF_MEMORY the memtable has failed */
dberr_t memtable_reserve(memtable_t &mt, batch_seq_t seq, ingest_plan_t &plan);

/** Mark a reserved write durable and wait until it is visible, which happens
once every earlier write is visible too.
@param[in]  mt   memtable
@param[in]  seq  sequence number of a reserved write
@return DB_SUCCESS, or DB_FENCED if the memtable failed first */
dberr_t memtable_publish(memtable_t &mt, batch_seq_t seq);

/** Wait until every reserved write is visible.
@param[in]  mt  memtable
@return DB_SUCCESS, or DB_FENCED if the memtable failed first */
dberr_t memtable_wait_visible(memtable_t &mt);

/** Fail the memtable after a write could not be made durable. Reserved writes
never become visible; waiting and later calls return DB_FENCED. Snapshots
taken before stay valid.
@param[in]  mt  memtable */
void memtable_fail(memtable_t &mt);

/** List the visible writes that are not flushed yet, in sequence order. Their
batches stay valid until they are retired.
@param[in]   mt     memtable
@param[out]  items  writes flushed_seq + 1 .. visible_seq */
void memtable_collect(memtable_t &mt,
                      std::vector<memtable_flush_item_t> &items);

/** Take writes up to a sequence number out after they were stored.
@param[in]  mt           memtable
@param[in]  flushed_seq  last write now flushed; at most visible_seq */
void memtable_retire(memtable_t &mt, batch_seq_t flushed_seq);

/** Take a table out once every write to it is retired. A later write of its
name creates a new table. Snapshots and retired batches that refer to it stay
valid. The table must exist and have no write left that is not retired.
@param[in]  mt     memtable
@param[in]  table  canonical table name */
void memtable_drop_table(memtable_t &mt, const std::string &table);

/** Copy the newest schema of every table, by name.
@param[in]   mt       memtable
@param[out]  schemas  the schemas */
void memtable_list_schemas(memtable_t &mt, std::vector<schema_t> &schemas);

/** Register a reader before starting its DuckDB transaction. Retirements
following this registration retain their batches until snapshot_at() replaces
the broad pin with exact references, or until release. No batch pointers are
copied yet. Release even if snapshot_at() later fails.
@param[in,out]  mt    memtable
@param[out]     snap  new pending snapshot pin; must be released even if
                      completion fails */
void memtable_snapshot_pin(memtable_t &mt, memtable_snapshot_t &snap);

/** Complete a pinned reader using the F read by its DuckDB transaction.
Includes retained and live batches with F < seq <= current visible_seq. F must
not precede the retirement waterline at pin time.
@param[in,out]  mt           memtable
@param[in]      flushed_seq  last sequence number already stored in analytical
                             storage
@param[in,out]  snap         pending pin to replace with exact references to
                             selected batches
@return DB_SUCCESS, or DB_CORRUPTION for an invalid pin/boundary or missing
  batch */
dberr_t memtable_snapshot_at(memtable_t &mt, batch_seq_t flushed_seq,
                             memtable_snapshot_t &snap);

/** Release a snapshot; its batches may be freed afterwards. Releasing a
snapshot that was not taken does nothing.
@param[in]      mt    memtable
@param[in,out]  snap  snapshot; reset */
void memtable_snapshot_release(memtable_t &mt, memtable_snapshot_t &snap);

/** Read the counters.
@param[in]   mt     memtable
@param[out]  stats  counters */
void memtable_get_stats(memtable_t &mt, memtable_stats_t &stats);

#endif
