/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable0memtable.cc
 *    In-memory store with ordered publication and deferred freeing.
 *
 * Every write after flushed_seq is in the queue live, in sequence order:
 *
 *   flushed_seq < live[0] ... visible_seq ... reserved_seq
 *                 |-- visible, unflushed --|-- reserved, not visible --|
 *
 * Retiring takes entries off the front of live. A retired entry may still
 * be in use by a snapshot taken before it was retired, so it goes on the
 * garbage list. Fixed snapshots reference only their selected entries.
 * Readers still establishing a cold snapshot temporarily protect later
 * retirements by registration id; completion replaces that pin with exact
 * references, so unrelated later writes can be freed immediately.
 *
 *-------------------------------------------------------------------------
 */

#include "memtable0memtable.h"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <new>
#include <set>
#include <utility>

#include "ut0new.h"

struct memtable_table_t;

/** One accepted write. */
struct memtable_entry_t {
  /** Sequence number of the write. */
  batch_seq_t seq{0};

  /** Set by memtable_publish(); the entry is visible once it and every
  earlier entry are durable. */
  bool durable{false};

  /** Table written to; this entry holds a reference until it is freed. */
  memtable_table_t *table{nullptr};

  /** The rows; never changed once the entry is reserved. */
  batch_t batch;

  /** Fixed snapshots that hold this immutable batch. Protected by the
  memtable mutex, like the garbage list. */
  uint64_t readers{0};

  /** Newest snapshot id when the entry was retired; readers pinned
  before then may still need it. */
  uint64_t retire_tag{0};

  /** Next retired entry on the garbage list. */
  memtable_entry_t *next_garbage{nullptr};
};

/** A fixed snapshot's hold on the memtable. */
struct memtable_reader_t {
  /** Exact entries the snapshot holds; empty while a pin is pending. */
  std::vector<memtable_entry_t *> entries;
};

/** One table. */
struct memtable_table_t {
  /** One reference from the table map, plus one per batch and schema
  reference.
  Protected by the memtable mutex. */
  uint64_t refs{1};

  /** Removed from the table map; counted in dropped_tables until freed. */
  bool dropped{false};

  /** Every schema version; schemas[i] has version i + 1. */
  std::vector<schema_t *> schemas;

  /** Live writes to the table in sequence order, visible or not. */
  std::deque<memtable_entry_t *> entries;
};

/** An in-memory store. */
struct memtable_t {
  /** Protects every field below. Held only briefly; nothing else is
  locked while it is held. */
  std::mutex mutex;

  /** Signalled whenever visible_seq advances or the memtable fails. */
  std::condition_variable published;

  /** Tables by canonical name. */
  std::map<std::string, memtable_table_t *> tables;

  /** Dropped tables held by batches or planners; freed at zero refs. */
  uint64_t dropped_tables{0};

  /** Every write after flushed_seq; live[i] has sequence number
  flushed_seq + 1 + i. */
  std::deque<memtable_entry_t *> live;

  /** Every write up to here is flushed and retired. */
  batch_seq_t flushed_seq{0};

  /** Every write up to here is visible. */
  batch_seq_t visible_seq{0};

  /** Sequence number of the last reserved write. */
  batch_seq_t reserved_seq{0};

  /** memtable_fail() was called: reserved writes never become visible,
  and later calls return DB_FENCED. */
  bool failed{false};

  /** Rows in live. */
  uint64_t live_rows{0};

  /** Column bytes in live. */
  uint64_t live_bytes{0};

  /** First of the retired entries not freed yet, oldest first. */
  memtable_entry_t *garbage_head{nullptr};

  /** Last of them, where retire() appends. */
  memtable_entry_t *garbage_tail{nullptr};

  /** Number of them. */
  uint64_t garbage_count{0};

  /** Id of the newest snapshot taken. */
  uint64_t last_snapshot_id{0};

  /** Each fixed reader owns references to exactly its selected entries. */
  std::map<uint64_t, memtable_reader_t> readers;

  /** Readers registered before BEGIN but not yet fixed at (F, S). Only
  these readers protect all retirements after their registration. */
  std::set<uint64_t> pending_pins;
};

/** Create an empty memtable.
@param[in]   start_seq  sequence number of the last write that is not in this
                        memtable; the first reserved write is start_seq + 1
@param[out]  out        new memtable
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
dberr_t memtable_create(batch_seq_t start_seq, memtable_t **out) {
  *out = nullptr;

  memtable_t *mt = ut_new<memtable_t>();
  if (mt == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  mt->flushed_seq = start_seq;
  mt->visible_seq = start_seq;
  mt->reserved_seq = start_seq;

  *out = mt;

  return (DB_SUCCESS);
}

/** Free a table and its schemas.
@param[in]  table  unreferenced table whose schemas are destroyed */
static void memtable_free_table(memtable_table_t *table) {
  for (size_t i = 0; i < table->schemas.size(); ++i) {
    ut_delete(table->schemas[i]);
  }

  ut_delete(table);
}

/** Release a table owner. Caller holds the mutex, or is closing mt.
@param[in,out]  mt     memtable
@param[in,out]  table  table whose owner count is decremented; destroyed at
                       zero */
static void memtable_release_table(memtable_t &mt, memtable_table_t &table) {
  ut_a(table.refs > 0);
  --table.refs;
  if (table.refs == 0) {
    if (table.dropped) {
      ut_a(mt.dropped_tables > 0);
      --mt.dropped_tables;
    }
    memtable_free_table(&table);
  }
}

/** Release a batch and its hold on the table.
@param[in,out]  mt     memtable
@param[in]      entry  entry to destroy, releasing its hold on the table */
static void memtable_free_entry(memtable_t &mt, memtable_entry_t *entry) {
  memtable_table_t *table = entry->table;
  ut_delete(entry);
  memtable_release_table(mt, *table);
}

/** Free a memtable and every schema and batch in it.
@param[in]  mt  memtable; may be NULL */
void memtable_free(memtable_t *mt) {
  if (mt == nullptr) {
    return;
  }

  std::map<std::string, memtable_table_t *>::iterator it;

  for (it = mt->tables.begin(); it != mt->tables.end(); ++it) {
    memtable_release_table(*mt, *it->second);
  }

  for (size_t i = 0; i < mt->live.size(); ++i) {
    memtable_free_entry(*mt, mt->live[i]);
  }

  while (mt->garbage_head != nullptr) {
    memtable_entry_t *entry = mt->garbage_head;
    mt->garbage_head = entry->next_garbage;
    memtable_free_entry(*mt, entry);
  }

  ut_a(mt->dropped_tables == 0);
  ut_delete(mt);
}

/** Find an active table while holding the memtable mutex.
@param[in,out]  mt    memtable
@param[in]      name  canonical table name
@return the table, or NULL. Caller holds mt.mutex. */
static memtable_table_t *memtable_find(memtable_t &mt,
                                       const std::string &name) {
  std::map<std::string, memtable_table_t *>::iterator it = mt.tables.find(name);

  return (it != mt.tables.end() ? it->second : nullptr);
}

/** Add a table that already exists in storage, with all its schema versions.
@param[in]  mt        memtable
@param[in]  versions  versions 1, 2, ... of one table
@return DB_SUCCESS, DB_CORRUPTION if the versions are not consecutive or the
  table exists, or DB_OUT_OF_MEMORY */
dberr_t memtable_add_table(memtable_t &mt,
                           const std::vector<schema_t> &versions) {
  std::lock_guard<std::mutex> lock(mt.mutex);

  /* Only before the first write, and with versions 1, 2, ... */
  ut_a(mt.live.empty() && mt.reserved_seq == mt.flushed_seq);

  if (versions.empty() || memtable_find(mt, versions[0].name) != nullptr) {
    return (DB_CORRUPTION);
  }

  for (size_t i = 0; i < versions.size(); ++i) {
    if (versions[i].version != i + 1 || versions[i].name != versions[0].name) {
      return (DB_CORRUPTION);
    }
  }

  memtable_table_t *table = ut_new<memtable_table_t>();
  if (table == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  for (size_t i = 0; i < versions.size(); ++i) {
    schema_t *schema = ut_new<schema_t>(versions[i]);
    if (schema == nullptr) {
      memtable_free_table(table);

      return (DB_OUT_OF_MEMORY);
    }
    table->schemas.push_back(schema);
  }

  mt.tables[versions[0].name] = table;

  return (DB_SUCCESS);
}

/** Newest schema of a table, including schema changes of writes that are
reserved but not yet visible.
@param[in]  mt     memtable
@param[in]  table  canonical table name
@return the schema, valid until the table is dropped, or NULL if the table
  does not exist */
const schema_t *memtable_get_schema(memtable_t &mt, const std::string &table) {
  std::lock_guard<std::mutex> lock(mt.mutex);

  memtable_table_t *t = memtable_find(mt, table);

  if (mt.failed || t == nullptr || t->schemas.empty()) {
    return (nullptr);
  }

  return (t->schemas.back());
}

/** Hold the newest schema for planning outside the engine's write lock.
@param[in]   mt    memtable
@param[in]   name  canonical table name
@param[out]  ref   new reference; must not already hold one */
void memtable_schema_acquire(memtable_t &mt, const std::string &name,
                             memtable_schema_ref_t &ref) {
  ref = memtable_schema_ref_t();
  std::lock_guard<std::mutex> lock(mt.mutex);
  memtable_table_t *table = memtable_find(mt, name);
  if (mt.failed || table == nullptr || table->schemas.empty()) {
    return;
  }

  ++table->refs;
  ref.table = table;
  ref.schema = table->schemas.back();
}

/** Release a planner's schema reference; a dropped table may be freed.
@param[in]      mt   memtable
@param[in,out]  ref  reference; reset, also safe when empty */
void memtable_schema_release(memtable_t &mt, memtable_schema_ref_t &ref) {
  if (ref.table == nullptr) {
    return;
  }

  std::lock_guard<std::mutex> lock(mt.mutex);
  memtable_release_table(mt, *ref.table);
  ref = memtable_schema_ref_t();
}

/** Check that a plan fits the current schema of its table.
@param[in]  table  the table, or NULL if it does not exist
@param[in]  plan   the plan
@return true if the plan can be applied */
static bool memtable_plan_fits(const memtable_table_t *table,
                               const ingest_plan_t &plan) {
  /* The table exists exactly when the plan does not create it. */
  if ((table == nullptr) != plan.creates_table) {
    return (false);
  }

  const schema_t *current = table != nullptr ? table->schemas.back() : nullptr;
  const uint64_t version = current != nullptr ? current->version : 0;
  const size_t n_cols = current != nullptr ? current->cols.size() : 0;

  /* A schema change must be based on the newest version, and its columns
  must continue the column ids with new names. */
  if (!plan.new_cols.empty()) {
    if (plan.base_version != version) {
      return (false);
    }

    if (n_cols + plan.new_cols.size() > SCHEMA_MAX_COLUMNS) {
      return (false);
    }

    for (size_t i = 0; i < plan.new_cols.size(); ++i) {
      const col_def_t &def = plan.new_cols[i];

      if (def.id != n_cols + i) {
        return (false);
      }

      if (current != nullptr && schema_find_col(current->cols, def.name)) {
        return (false);
      }

      for (size_t k = 0; k < i; ++k) {
        if (plan.new_cols[k].name == def.name) {
          return (false);
        }
      }
    }
  }

  /* The batch follows an existing version, or the one this plan adds. */
  const uint64_t batch_version = plan.batch.schema_version;
  const uint64_t newest = version + (plan.new_cols.empty() ? 0 : 1);

  if (batch_version == 0 || batch_version > newest) {
    return (false);
  }

  /* Its columns match that version one by one. */
  std::vector<col_def_t> cols;

  if (batch_version <= version) {
    cols = table->schemas[batch_version - 1]->cols;
  } else {
    if (current != nullptr) {
      cols = current->cols;
    }
    cols.insert(cols.end(), plan.new_cols.begin(), plan.new_cols.end());
  }

  if (plan.batch.cols.size() != cols.size()) {
    return (false);
  }

  for (size_t i = 0; i < cols.size(); ++i) {
    if (plan.batch.cols[i].type != cols[i].type) {
      return (false);
    }
  }

  return (true);
}

/** Apply a plan that fits. Caller holds mt.mutex.
@param[in,out]  mt    memtable
@param[in]      seq   next reserved sequence number
@param[in,out]  plan  validated write plan; its batch is moved into the
                      memtable
@return DB_SUCCESS after publishing the schema and queued batch under the
  memtable lock */
static dberr_t memtable_apply(memtable_t &mt, batch_seq_t seq,
                              ingest_plan_t &plan) {
  memtable_table_t *table = memtable_find(mt, plan.table);

  /* Allocate every new object first: a failure here changes nothing. */
  memtable_table_t *new_table = nullptr;
  if (table == nullptr) {
    new_table = ut_new<memtable_table_t>();
    if (new_table == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }
    table = new_table;
  }

  schema_t *schema = nullptr;
  if (!plan.new_cols.empty()) {
    schema = ut_new<schema_t>();
    if (schema == nullptr) {
      ut_delete(new_table);

      return (DB_OUT_OF_MEMORY);
    }
  }

  memtable_entry_t *entry = ut_new<memtable_entry_t>();
  if (entry == nullptr) {
    ut_delete(schema);
    ut_delete(new_table);

    return (DB_OUT_OF_MEMORY);
  }

  /* The new schema version: the newest one plus the new columns. */
  if (schema != nullptr) {
    if (!table->schemas.empty()) {
      *schema = *table->schemas.back();
    }
    schema->name = plan.table;
    schema->version = plan.base_version + 1;
    schema->cols.insert(schema->cols.end(), plan.new_cols.begin(),
                        plan.new_cols.end());
    table->schemas.push_back(schema);
  }

  entry->seq = seq;
  entry->table = table;
  table->entries.push_back(entry);
  mt.live.push_back(entry);

  if (new_table != nullptr) {
    const bool inserted = mt.tables.emplace(plan.table, new_table).second;
    ut_a(inserted);
  }
  ++table->refs;
  std::swap(entry->batch, plan.batch);
  mt.live_rows += entry->batch.n_rows;
  mt.live_bytes += batch_bytes(entry->batch);

  return (DB_SUCCESS);
}

/** Take a write: apply its schema change and hold its batch, not visible yet.
@param[in]      mt    memtable
@param[in]      seq   sequence number of the write; must be one more than the
                      previous reserved write
@param[in,out]  plan  write plan made against the current schema
@retval DB_SUCCESS reserved
@retval DB_CORRUPTION the plan does not fit the current schema; nothing
  changed
@retval DB_FENCED the memtable has failed; nothing changed
@retval DB_OUT_OF_MEMORY the memtable has failed */
dberr_t memtable_reserve(memtable_t &mt, batch_seq_t seq, ingest_plan_t &plan) {
  std::lock_guard<std::mutex> lock(mt.mutex);

  if (mt.failed) {
    return (DB_FENCED);
  }

  /* The caller hands out sequence numbers; they arrive in order. */
  ut_a(seq == mt.reserved_seq + 1);

  const memtable_table_t *table = memtable_find(mt, plan.table);

  if (!memtable_plan_fits(table, plan)) {
    return (DB_CORRUPTION);
  }

  const dberr_t err = memtable_apply(mt, seq, plan);

  if (err != DB_SUCCESS) {
    mt.failed = true;
    mt.published.notify_all();

    return (err);
  }

  mt.reserved_seq = seq;

  return (DB_SUCCESS);
}

/** Locate a reserved, unretired write by sequence number.
@param[in]  mt   memtable whose mutex the caller holds
@param[in]  seq  sequence number in the live, unretired range
@return the live entry with a sequence number. Caller holds the mutex. */
static memtable_entry_t *memtable_live_entry(memtable_t &mt, batch_seq_t seq) {
  memtable_entry_t *entry = mt.live[seq - mt.flushed_seq - 1];
  ut_a(entry->seq == seq);

  return (entry);
}

/** Mark a reserved write durable and wait until it is visible, which happens
once every earlier write is visible too.
@param[in]  mt   memtable
@param[in]  seq  sequence number of a reserved write
@return DB_SUCCESS, or DB_FENCED if the memtable failed first */
dberr_t memtable_publish(memtable_t &mt, batch_seq_t seq) {
  std::unique_lock<std::mutex> lock(mt.mutex);

  if (mt.failed) {
    return (DB_FENCED);
  }

  ut_a(seq > mt.visible_seq && seq <= mt.reserved_seq);
  memtable_live_entry(mt, seq)->durable = true;

  /* Advance over every durable entry after visible_seq. */
  bool advanced = false;

  while (mt.visible_seq < mt.reserved_seq &&
         memtable_live_entry(mt, mt.visible_seq + 1)->durable) {
    mt.visible_seq++;
    advanced = true;
  }

  if (advanced) {
    mt.published.notify_all();
  }

  /* Wait for earlier writes that are still in flight. */
  while (mt.visible_seq < seq && !mt.failed) {
    mt.published.wait(lock);
  }

  return (mt.visible_seq >= seq ? DB_SUCCESS : DB_FENCED);
}

/** Wait until every reserved write is visible.
@param[in]  mt  memtable
@return DB_SUCCESS, or DB_FENCED if the memtable failed first */
dberr_t memtable_wait_visible(memtable_t &mt) {
  std::unique_lock<std::mutex> lock(mt.mutex);

  while (mt.visible_seq < mt.reserved_seq && !mt.failed) {
    mt.published.wait(lock);
  }

  return (mt.failed ? DB_FENCED : DB_SUCCESS);
}

/** Fail the memtable after a write could not be made durable.
@param[in]  mt  memtable */
void memtable_fail(memtable_t &mt) {
  std::lock_guard<std::mutex> lock(mt.mutex);

  mt.failed = true;
  mt.published.notify_all();
}

/** List the visible writes that are not flushed yet, in sequence order.
@param[in]   mt     memtable
@param[out]  items  writes flushed_seq + 1 .. visible_seq */
void memtable_collect(memtable_t &mt,
                      std::vector<memtable_flush_item_t> &items) {
  items.clear();

  std::lock_guard<std::mutex> lock(mt.mutex);

  for (batch_seq_t seq = mt.flushed_seq + 1; seq <= mt.visible_seq; ++seq) {
    const memtable_entry_t *entry = memtable_live_entry(mt, seq);

    memtable_flush_item_t item;
    item.seq = seq;
    item.batch = &entry->batch;
    item.schema = entry->table->schemas[entry->batch.schema_version - 1];

    items.push_back(item);
  }
}

/** Free retired entries that no open snapshot can use. Caller holds the
mutex.
@param[in,out]  mt  memtable */
static void memtable_free_garbage(memtable_t &mt) {
  const uint64_t oldest_pin =
      mt.pending_pins.empty() ? UINT64_MAX : *mt.pending_pins.begin();
  memtable_entry_t *previous = nullptr;
  memtable_entry_t *entry = mt.garbage_head;
  while (entry != nullptr) {
    memtable_entry_t *next = entry->next_garbage;
    if (entry->readers == 0 && entry->retire_tag < oldest_pin) {
      if (previous != nullptr) {
        previous->next_garbage = next;
      } else {
        mt.garbage_head = next;
      }
      if (mt.garbage_tail == entry) {
        mt.garbage_tail = previous;
      }
      --mt.garbage_count;
      memtable_free_entry(mt, entry);
    } else {
      previous = entry;
    }
    entry = next;
  }
}

/** Take writes up to a sequence number out after they were stored.
@param[in]  mt           memtable
@param[in]  flushed_seq  last write now flushed; at most visible_seq */
void memtable_retire(memtable_t &mt, batch_seq_t flushed_seq) {
  std::lock_guard<std::mutex> lock(mt.mutex);

  /* Only visible writes can have been flushed. */
  ut_a(flushed_seq >= mt.flushed_seq && flushed_seq <= mt.visible_seq);

  while (mt.flushed_seq < flushed_seq) {
    memtable_entry_t *entry = mt.live.front();
    mt.live.pop_front();

    /* A table's oldest live entry is the oldest live entry overall among
    the entries of that table. */
    ut_a(entry->table->entries.front() == entry);
    entry->table->entries.pop_front();

    mt.flushed_seq = entry->seq;
    mt.live_rows -= entry->batch.n_rows;
    mt.live_bytes -= batch_bytes(entry->batch);

    entry->retire_tag = mt.last_snapshot_id;
    entry->next_garbage = nullptr;

    if (mt.garbage_tail == nullptr) {
      mt.garbage_head = entry;
    } else {
      mt.garbage_tail->next_garbage = entry;
    }
    mt.garbage_tail = entry;
    mt.garbage_count++;
  }

  memtable_free_garbage(mt);
}

/** Take a table out once every write to it is retired.
@param[in]  mt    memtable
@param[in]  name  canonical table name
@retval DB_SUCCESS dropped
@retval DB_NOT_FOUND no such table
@retval DB_ERROR the table has writes that are not retired */
void memtable_drop_table(memtable_t &mt, const std::string &name) {
  std::lock_guard<std::mutex> lock(mt.mutex);

  std::map<std::string, memtable_table_t *>::iterator it = mt.tables.find(name);
  ut_a(it != mt.tables.end());
  ut_a(it->second->entries.empty());

  memtable_table_t *table = it->second;
  table->dropped = true;
  ++mt.dropped_tables;
  mt.tables.erase(it);
  memtable_release_table(mt, *table);
}

/** Copy the newest schema of every table, by name.
@param[in]   mt       memtable
@param[out]  schemas  the schemas */
void memtable_list_schemas(memtable_t &mt, std::vector<schema_t> &schemas) {
  schemas.clear();

  std::lock_guard<std::mutex> lock(mt.mutex);

  std::map<std::string, memtable_table_t *>::const_iterator it;

  for (it = mt.tables.begin(); it != mt.tables.end(); ++it) {
    schemas.push_back(*it->second->schemas.back());
  }
}

/** Register a reader before starting its DuckDB transaction.
@param[in,out]  mt    memtable
@param[out]     snap  new pending snapshot pin; must be released even if
                      completion fails */
void memtable_snapshot_pin(memtable_t &mt, memtable_snapshot_t &snap) {
  snap = memtable_snapshot_t();
  std::lock_guard<std::mutex> lock(mt.mutex);
  const uint64_t id = mt.last_snapshot_id + 1;
  mt.readers.emplace(id, memtable_reader_t());
  mt.pending_pins.insert(id);
  mt.last_snapshot_id = id;
  snap.id = id;
  snap.base_seq = mt.flushed_seq;
}

/** Append one selected immutable batch. Caller holds the memtable mutex.
@param[in]      entry  immutable entry selected for the snapshot
@param[in,out]  snap   table views receiving this batch and its schema */
static void memtable_snapshot_append(const memtable_entry_t &entry,
                                     memtable_snapshot_t &snap) {
  memtable_table_view_t &view = snap.tables[entry.batch.table];
  view.batches.push_back(&entry.batch);
  const schema_t *schema = entry.table->schemas[entry.batch.schema_version - 1];
  if (view.schema == nullptr || schema->version > view.schema->version) {
    view.schema = schema;
  }
}

/** Complete a pinned reader using the F read by its DuckDB transaction.
@param[in,out]  mt           memtable
@param[in]      flushed_seq  last sequence number already stored in analytical
                             storage
@param[in,out]  snap         pending pin to replace with exact references to
                             selected batches
@return DB_SUCCESS, or DB_CORRUPTION for an invalid pin/boundary or missing
  batch */
dberr_t memtable_snapshot_at(memtable_t &mt, batch_seq_t flushed_seq,
                             memtable_snapshot_t &snap) {
  std::lock_guard<std::mutex> lock(mt.mutex);
  if (snap.id == 0 || mt.pending_pins.count(snap.id) == 0 ||
      flushed_seq < snap.base_seq || flushed_seq > mt.visible_seq) {
    return (DB_CORRUPTION);
  }
  snap.tables.clear();
  snap.base_seq = flushed_seq;
  snap.visible_seq = mt.visible_seq;
  batch_seq_t selected = 0;
  std::vector<memtable_entry_t *> references;
  /* A pin taken before BEGIN protects every retirement that BEGIN may
  miss. Entries retired earlier are already in the cold snapshot. */
  for (memtable_entry_t *entry = mt.garbage_head; entry != nullptr;
       entry = entry->next_garbage) {
    if (entry->seq > flushed_seq && entry->seq <= snap.visible_seq) {
      memtable_snapshot_append(*entry, snap);
      references.push_back(entry);
      ++selected;
    }
  }
  for (memtable_entry_t *entry : mt.live) {
    if (entry->seq > snap.visible_seq) {
      break;
    }
    if (entry->seq > flushed_seq) {
      memtable_snapshot_append(*entry, snap);
      references.push_back(entry);
      ++selected;
    }
  }
  if (selected != snap.visible_seq - flushed_seq) {
    snap.tables.clear();

    return (DB_CORRUPTION);
  }
  /* Under the same mutex, take exact references before removing the
  broad registration pin. */
  memtable_reader_t &reader = mt.readers.at(snap.id);
  reader.entries.swap(references);
  for (memtable_entry_t *entry : reader.entries) {
    ++entry->readers;
  }
  mt.pending_pins.erase(snap.id);
  memtable_free_garbage(mt);

  return (DB_SUCCESS);
}

/** Release a snapshot; its batches may be freed afterwards.
@param[in]      mt    memtable
@param[in,out]  snap  snapshot; reset */
void memtable_snapshot_release(memtable_t &mt, memtable_snapshot_t &snap) {
  if (snap.id == 0) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(mt.mutex);

    std::map<uint64_t, memtable_reader_t>::iterator reader =
        mt.readers.find(snap.id);
    ut_a(reader != mt.readers.end());
    for (memtable_entry_t *entry : reader->second.entries) {
      ut_a(entry->readers > 0);
      --entry->readers;
    }
    mt.readers.erase(reader);
    mt.pending_pins.erase(snap.id);
    memtable_free_garbage(mt);
  }

  snap = memtable_snapshot_t();
}

/** Read the counters.
@param[in]   mt     memtable
@param[out]  stats  counters */
void memtable_get_stats(memtable_t &mt, memtable_stats_t &stats) {
  std::lock_guard<std::mutex> lock(mt.mutex);

  stats.flushed_seq = mt.flushed_seq;
  stats.visible_seq = mt.visible_seq;
  stats.live_writes = mt.live.size();
  stats.live_rows = mt.live_rows;
  stats.live_bytes = mt.live_bytes;
  stats.garbage_writes = mt.garbage_count;
  stats.dropped_tables = mt.dropped_tables;
}
