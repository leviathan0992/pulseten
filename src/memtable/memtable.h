/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable.h
 *    In-memory store of accepted writes and the table schemas.
 *
 * A write enters in two steps. memtable_reserve() takes its schema change
 * and batch; later writes plan against that schema at once, but readers do
 * not see the batch. memtable_publish(), called once the WAL write completes,
 * makes it visible. Batches become visible strictly in sequence order, even
 * when writes finish out of order.
 *
 * memtable_retire_flushed() takes writes out after a flush stored them in
 * DuckDB. A snapshot may still read a retired batch, which is freed with the
 * last snapshot that references it. Schemas and batches never change once added.
 * Tables and schemas live until the memtable is freed.
 *
 * All functions are thread-safe unless noted.
 *
 *-------------------------------------------------------------------------
 */

#ifndef memtable_h
#define memtable_h

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "batch.h"
#include "ingest.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"

struct memtable_t;
struct memtable_entry_t;

/** One table in a snapshot. */
struct memtable_table_view_t {
  /** Schema of the newest batch in m_batches. */
  const schema_t *m_schema{nullptr};

  /** In sequence order. An older batch lacks columns added after it; they
  read as NULL. */
  std::vector<const batch_t *> m_batches;
};

/** Every batch with m_base_seq < seq <= m_visible_seq. Writes up to m_base_seq
are in DuckDB instead. */
struct memtable_snapshot_t {
  /** Entries pinned by this snapshot; released by
  memtable_snapshot_release(). */
  std::vector<memtable_entry_t *> m_entries;

  /** Writes up to this sequence are read from DuckDB instead of this
  snapshot. */
  batch_seq_t m_base_seq{0};

  /** Highest sequence included in this snapshot, inclusive. */
  batch_seq_t m_visible_seq{0};

  /** Tables with at least one batch, by canonical name. */
  std::map<std::string, memtable_table_view_t> m_tables;
};

/** A visible, unflushed write. Its pointers stay valid until it is retired. */
struct memtable_flush_item_t {
  /** Sequence number of the write to flush. */
  batch_seq_t m_seq{0};

  /** Borrowed column batch to append to storage. */
  const batch_t *m_batch{nullptr};

  /** The version the batch was written with. */
  const schema_t *m_schema{nullptr};
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
  batch_calc_data_bytes(). */
  uint64_t m_live_bytes{0};

  /** Retired writes still held by snapshots. */
  uint64_t m_garbage_writes{0};
};

/** Create an empty memtable whose first write will be start_seq + 1.
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
db_status_t memtable_create(batch_seq_t start_seq, memtable_t **out);

/** Destroy a memtable; mt may be NULL.
REQUIRES: no call in progress and no snapshot in use. */
void memtable_destroy(memtable_t *mt);

/** Restore a stored table; versions holds its versions 1, 2, ...
REQUIRES: no write reserved yet.
@return DB_SUCCESS, DB_OUT_OF_MEMORY, or DB_CORRUPTION if the versions are not
  consecutive or the table exists */
db_status_t memtable_restore_table(memtable_t &mt,
                                   const std::vector<schema_t> &versions);

/** Newest schema of a table, by canonical name, for write planning. Includes
schema changes of writes not yet visible.
@return the schema, valid until the memtable is freed, or NULL if the table
  does not exist or the memtable failed */
const schema_t *memtable_find_schema(memtable_t &mt,
                                     const std::string &table_name);

/** Take a write and move the batch out of plan. It is not visible yet.
REQUIRES: seq is one more than the previous reserved write.
@retval DB_SUCCESS reserved
@retval DB_CORRUPTION the plan does not fit the current schema; nothing changed
@retval DB_FENCED the memtable has failed; nothing changed
@retval DB_OUT_OF_MEMORY the memtable has now failed */
db_status_t memtable_reserve(memtable_t &mt, batch_seq_t seq,
                             ingest_plan_t &plan);

/** Mark a reserved WAL write complete and wait until every write up to it is
visible.
@return DB_SUCCESS, or DB_FENCED if the memtable failed first */
db_status_t memtable_publish(memtable_t &mt, batch_seq_t seq);

/** Stop the memtable after a WAL write failed. Reserved writes
never become visible; waiters and later calls get DB_FENCED. Existing snapshots
stay valid. */
void memtable_fail(memtable_t &mt);

/** List the writes flushed_seq + 1 .. visible_seq, in sequence order. */
void memtable_collect_unflushed(memtable_t &mt,
                                std::vector<memtable_flush_item_t> &items);

/** Remove stored writes up to flushed_seq from the live queue. Batches still
referenced by snapshots are retained until those references are released.
REQUIRES: flushed_seq <= visible_seq. */
void memtable_retire_flushed(memtable_t &mt, batch_seq_t flushed_seq);

/** Copy each table's newest schema in name order, including reserved writes.
The caller owns the copies. Replaces the contents of schemas. */
void memtable_copy_schemas(memtable_t &mt,
                           std::vector<schema_t> &schemas);

/** Borrow each table's newest schema in name order, including reserved writes.
The immutable schemas remain valid until memtable_destroy(), even if later
writes add newer versions. Replaces the contents of schemas. */
void memtable_reference_schemas(memtable_t &mt,
                                std::vector<const schema_t *> &schemas);

/** Reference every visible, unflushed write. Call before starting the DuckDB
transaction: writes retired later stay alive, so the stored snapshot taken next
never misses one. snap must be released even if memtable_snapshot_complete()
fails. */
void memtable_snapshot_pin(memtable_t &mt, memtable_snapshot_t &snap);

/** Complete a pinned snapshot with the F read by its DuckDB transaction,
keeping only the writes after F. If F passed the pinned visible_seq, snap holds
no batches and its visible_seq becomes F.
@return DB_SUCCESS, or DB_CORRUPTION if F precedes the pin or a batch is
  missing */
db_status_t memtable_snapshot_complete(memtable_t &mt, batch_seq_t flushed_seq,
                                       memtable_snapshot_t &snap);

/** Release a snapshot and reset it. Does nothing if it was never taken. */
void memtable_snapshot_release(memtable_t &mt, memtable_snapshot_t &snap);

/** Read the counters. */
void memtable_read_stats(memtable_t &mt, memtable_stats_t &stats);

#endif /* memtable_h */
