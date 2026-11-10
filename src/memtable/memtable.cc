/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable.cc
 *    In-memory store with ordered publication and deferred freeing.
 *
 * Every write after m_flushed_seq is in m_live, in sequence order:
 *
 *   m_flushed_seq < m_live[0] ... m_visible_seq ... m_reserved_seq
 *                    |-- visible, unflushed --|-- reserved, not visible --|
 *
 * Retiring takes entries off the front of m_live. A retired entry may still
 * be in use by a snapshot taken before it was retired, so it goes on the
 * garbage list until no snapshot references it. A snapshot references every
 * visible live entry when it is pinned, and drops those its stored snapshot
 * already stores once it learns F.
 *
 *-------------------------------------------------------------------------
 */

#include "memtable.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "batch.h"
#include "ingest.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_new.h"

struct memtable_table_t;

/** One accepted write. */
struct memtable_entry_t {
  /** Sequence number of the write. */
  batch_seq_t m_seq{0};

  /** Set by memtable_publish(); the entry is visible once it and every
  earlier entry have completed their WAL writes. */
  bool m_committed{false};

  /** Table written to. */
  memtable_table_t *m_table{nullptr};

  /** The rows; never changed once the entry is reserved. */
  batch_t m_batch{};

  /** Snapshots that hold this immutable batch. Protected by the memtable
  mutex, like the garbage list. */
  uint64_t m_readers{0};

  /** Next retired entry on the garbage list. */
  memtable_entry_t *m_next_garbage{nullptr};
};

/** One table. */
struct memtable_table_t {
  /** Every schema version; m_schemas[i] has version i + 1. */
  std::vector<schema_t *> m_schemas;
};

/** An in-memory store. */
struct memtable_t {
  /* Protected by m_mutex. */

  /** Held only briefly; nothing else is locked while it is held. */
  ut_mutex_t m_mutex;

  /** Signalled whenever m_visible_seq advances or the memtable fails. */
  ut_condition_variable_t m_published;

  /** Tables by canonical name. */
  std::map<std::string, memtable_table_t *> m_tables;

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

  /** Last flushed and retired sequence (F). */
  batch_seq_t m_flushed_seq{0};

  /** Last visible sequence (V). */
  batch_seq_t m_visible_seq{0};

  /** Last reserved sequence (R). */
  batch_seq_t m_reserved_seq{0};

  /** memtable_fail() was called: reserved writes never become visible,
  and later calls return DB_FENCED. */
  bool m_failed{false};

  /** Rows in m_live. */
  uint64_t m_live_rows{0};

  /** Column bytes in m_live. */
  uint64_t m_live_bytes{0};

  /** First of the retired entries not freed yet, oldest first. */
  memtable_entry_t *m_garbage_head{nullptr};

  /** Last of them, where memtable_trim() appends. */
  memtable_entry_t *m_garbage_tail{nullptr};

  /** Number of them. */
  uint64_t m_garbage_count{0};
};

/** Create an empty memtable. */
db_status_t memtable_create(batch_seq_t start_seq, memtable_t **out) {
  *out = nullptr;

  memtable_t *mt = ut_new<memtable_t>();
  if (mt == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  mt->m_flushed_seq = start_seq;
  mt->m_visible_seq = start_seq;
  mt->m_reserved_seq = start_seq;

  *out = mt;

  return (DB_SUCCESS);
}

/** Check the sequence boundaries: F <= V <= R, and m_live holds exactly the
writes after F.
REQUIRES: mt.m_mutex is held. */
static void memtable_check_boundaries(const memtable_t &mt) {
  ut_ad(mt.m_mutex.is_owned());

  ut_a(mt.m_flushed_seq <= mt.m_visible_seq);
  ut_a(mt.m_visible_seq <= mt.m_reserved_seq);
  ut_a(mt.m_live.size() == mt.m_reserved_seq - mt.m_flushed_seq);
}

/** Free a table and its schemas. */
static void memtable_free_table(memtable_table_t *table) {
  for (size_t i = 0; i < table->m_schemas.size(); ++i) {
    ut_delete(table->m_schemas[i]);
  }

  ut_delete(table);
}

/** Free a memtable and every schema and batch in it. */
void memtable_free(memtable_t *mt) {
  if (mt == nullptr) {
    return;
  }

  for (memtable_entry_t *entry : mt->m_live) {
    ut_delete(entry);
  }

  while (mt->m_garbage_head != nullptr) {
    memtable_entry_t *entry = mt->m_garbage_head;
    mt->m_garbage_head = entry->m_next_garbage;
    ut_delete(entry);
  }

  std::map<std::string, memtable_table_t *>::iterator it;

  for (it = mt->m_tables.begin(); it != mt->m_tables.end(); ++it) {
    memtable_free_table(it->second);
  }

  ut_delete(mt);
}

/** Find a table.
@param[in]  name  canonical table name
@return the table, or NULL */
static memtable_table_t *memtable_find_table(memtable_t &mt,
                                             const std::string &name) {
  ut_ad(mt.m_mutex.is_owned());

  std::map<std::string, memtable_table_t *>::iterator it =
      mt.m_tables.find(name);

  return (it != mt.m_tables.end() ? it->second : nullptr);
}

/** Add a table that already exists in storage, with all its schema versions. */
db_status_t memtable_add_table(memtable_t &mt,
                               const std::vector<schema_t> &versions) {
  ut_lock_guard_t lock(mt.m_mutex);

  /* Only before the first write, and with versions 1, 2, ... */
  ut_a(mt.m_live.empty() && mt.m_reserved_seq == mt.m_flushed_seq);

  if (versions.empty() ||
      memtable_find_table(mt, versions[0].m_name) != nullptr) {
    return (DB_CORRUPTION);
  }

  for (size_t i = 0; i < versions.size(); ++i) {
    if (versions[i].m_version != i + 1 ||
        versions[i].m_name != versions[0].m_name) {
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
    table->m_schemas.push_back(schema);
  }

  mt.m_tables[versions[0].m_name] = table;

  return (DB_SUCCESS);
}

/** Newest schema of a table, including schema changes of writes that are
reserved but not yet visible. */
const schema_t *memtable_get_schema(memtable_t &mt, const std::string &table) {
  ut_lock_guard_t lock(mt.m_mutex);

  memtable_table_t *t = memtable_find_table(mt, table);

  if (mt.m_failed || t == nullptr || t->m_schemas.empty()) {
    return (nullptr);
  }

  return (t->m_schemas.back());
}

/** Check whether a plan is consistent with the table and schema state.
@return true if the consistency checks pass; allocation may still fail when
  applying the plan */
static bool memtable_can_apply_plan(memtable_t &mt, const ingest_plan_t &plan) {
  ut_ad(mt.m_mutex.is_owned());

  const memtable_table_t *table = memtable_find_table(mt, plan.m_table);

  /* The table exists exactly when the plan does not create it. */
  if ((table == nullptr) != plan.m_creates_table) {
    return (false);
  }

  const schema_t *current =
      table != nullptr ? table->m_schemas.back() : nullptr;
  const uint64_t version = current != nullptr ? current->m_version : 0;
  const size_t n_cols = current != nullptr ? current->m_cols.size() : 0;

  /* A schema change must be based on the newest version, and its columns
  must continue the column ids with new names. */
  if (!plan.m_new_cols.empty()) {
    if (plan.m_base_version != version) {
      return (false);
    }

    if (n_cols + plan.m_new_cols.size() > SCHEMA_MAX_COLUMNS) {
      return (false);
    }

    for (size_t i = 0; i < plan.m_new_cols.size(); ++i) {
      const col_def_t &def = plan.m_new_cols[i];

      if (def.m_id != n_cols + i) {
        return (false);
      }

      if (current != nullptr && schema_find_col(current->m_cols, def.m_name)) {
        return (false);
      }

      for (size_t k = 0; k < i; ++k) {
        if (plan.m_new_cols[k].m_name == def.m_name) {
          return (false);
        }
      }
    }
  }

  /* The batch follows an existing version, or the one this plan adds. */
  const uint64_t batch_version = plan.m_batch.m_schema_version;
  const uint64_t newest = version + (plan.m_new_cols.empty() ? 0 : 1);

  if (batch_version == 0 || batch_version > newest) {
    return (false);
  }

  /* Its columns match that version one by one. */
  std::vector<col_def_t> cols;

  /* An existing version, or else the one this plan adds. */
  if (batch_version <= version) {
    cols = table->m_schemas[batch_version - 1]->m_cols;
  } else {
    if (current != nullptr) {
      cols = current->m_cols;
    }
    cols.insert(cols.end(), plan.m_new_cols.begin(), plan.m_new_cols.end());
  }

  if (plan.m_batch.m_cols.size() != cols.size()) {
    return (false);
  }

  for (size_t i = 0; i < cols.size(); ++i) {
    if (plan.m_batch.m_cols[i].m_type != cols[i].m_type) {
      return (false);
    }
  }

  return (true);
}

/** Apply a plan validated by memtable_can_apply_plan().
@param[in,out]  plan  validated write plan; its batch is moved into the
                      memtable
@return DB_SUCCESS after publishing the schema and queued batch under the
  memtable lock */
static db_status_t memtable_apply_plan(memtable_t &mt, batch_seq_t seq,
                                       ingest_plan_t &plan) {
  ut_ad(mt.m_mutex.is_owned());

  memtable_table_t *table = memtable_find_table(mt, plan.m_table);

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
  if (!plan.m_new_cols.empty()) {
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
    if (!table->m_schemas.empty()) {
      *schema = *table->m_schemas.back();
    }
    schema->m_name = plan.m_table;
    schema->m_version = plan.m_base_version + 1;
    schema->m_cols.insert(schema->m_cols.end(), plan.m_new_cols.begin(),
                          plan.m_new_cols.end());
    table->m_schemas.push_back(schema);
    ut_a(schema->m_version == table->m_schemas.size());
  }

  /* A batch must find its schema version by index. */
  const uint64_t batch_version = plan.m_batch.m_schema_version;
  ut_a(batch_version >= 1 && batch_version <= table->m_schemas.size());

  entry->m_seq = seq;
  entry->m_table = table;
  mt.m_live.push_back(entry);

  /* Only now, when nothing can fail any more. */
  if (new_table != nullptr) {
    const bool inserted = mt.m_tables.emplace(plan.m_table, new_table).second;
    ut_a(inserted);
  }
  std::swap(entry->m_batch, plan.m_batch);
  mt.m_live_rows += entry->m_batch.m_n_rows;
  mt.m_live_bytes += batch_bytes(entry->m_batch);

  return (DB_SUCCESS);
}

/** Take a write: apply its schema change and hold its batch, not visible
yet. */
db_status_t memtable_reserve(memtable_t &mt, batch_seq_t seq,
                             ingest_plan_t &plan) {
  ut_lock_guard_t lock(mt.m_mutex);

  if (mt.m_failed) {
    return (DB_FENCED);
  }

  /* The caller hands out sequence numbers; they arrive in order. */
  ut_a(seq == mt.m_reserved_seq + 1);

  if (!memtable_can_apply_plan(mt, plan)) {
    return (DB_CORRUPTION);
  }

  const db_status_t err = memtable_apply_plan(mt, seq, plan);
  /* The WAL already holds this write, so memory can no longer match it. */
  if (!err.ok()) {
    mt.m_failed = true;
    mt.m_published.notify_all();

    return (err);
  }

  mt.m_reserved_seq = seq;
  memtable_check_boundaries(mt);

  return (DB_SUCCESS);
}

/** Locate a reserved, unretired write by sequence number.
@return the live entry with a sequence number */
static memtable_entry_t &memtable_get_live_entry(memtable_t &mt,
                                                 batch_seq_t seq) {
  ut_ad(mt.m_mutex.is_owned());
  ut_a(seq > mt.m_flushed_seq && seq <= mt.m_reserved_seq);

  memtable_entry_t &entry = *mt.m_live[seq - mt.m_flushed_seq - 1];
  ut_a(entry.m_seq == seq);

  return (entry);
}

/** Mark a reserved WAL write complete and wait until it is visible, which
happens once every earlier write is visible too. */
db_status_t memtable_publish(memtable_t &mt, batch_seq_t seq) {
  ut_unique_lock_t lock(mt.m_mutex);

  if (mt.m_failed) {
    return (DB_FENCED);
  }

  ut_a(seq > mt.m_visible_seq && seq <= mt.m_reserved_seq);

  memtable_get_live_entry(mt, seq).m_committed = true;

  /* Advance over every committed entry after visible_seq. */
  bool advanced = false;

  while (mt.m_visible_seq < mt.m_reserved_seq &&
         memtable_get_live_entry(mt, mt.m_visible_seq + 1).m_committed) {
    mt.m_visible_seq++;
    advanced = true;
  }

  memtable_check_boundaries(mt);

  if (advanced) {
    mt.m_published.notify_all();
  }

  /* Wait for earlier writes that are still in flight. */
  while (mt.m_visible_seq < seq && !mt.m_failed) {
    mt.m_published.wait(lock);
  }

  return (mt.m_visible_seq >= seq ? DB_SUCCESS : DB_FENCED);
}

/** Fail the memtable after a WAL write failed. */
void memtable_fail(memtable_t &mt) {
  ut_lock_guard_t lock(mt.m_mutex);

  mt.m_failed = true;
  mt.m_published.notify_all();
}

/** List the visible writes that are not flushed yet, in sequence order. */
void memtable_collect_unflushed(memtable_t &mt,
                                std::vector<memtable_flush_item_t> &items) {
  items.clear();

  ut_lock_guard_t lock(mt.m_mutex);

  for (batch_seq_t seq = mt.m_flushed_seq + 1; seq <= mt.m_visible_seq; ++seq) {
    const memtable_entry_t &entry = memtable_get_live_entry(mt, seq);

    /* Storage must never get ahead of the WAL. */
    ut_a(entry.m_committed);

    memtable_flush_item_t item;
    item.m_seq = seq;
    item.m_batch = &entry.m_batch;
    item.m_schema =
        entry.m_table->m_schemas[entry.m_batch.m_schema_version - 1];
    ut_a(item.m_schema->m_version == entry.m_batch.m_schema_version);

    items.push_back(item);
  }
}

/** Free retired entries that no open snapshot can use. */
static void memtable_free_garbage(memtable_t &mt) {
  ut_ad(mt.m_mutex.is_owned());

  memtable_entry_t *previous = nullptr;
  memtable_entry_t *entry = mt.m_garbage_head;
  while (entry != nullptr) {
    memtable_entry_t *next = entry->m_next_garbage;
    if (entry->m_readers == 0) {
      if (previous != nullptr) {
        previous->m_next_garbage = next;
      } else {
        mt.m_garbage_head = next;
      }
      if (mt.m_garbage_tail == entry) {
        mt.m_garbage_tail = previous;
      }
      --mt.m_garbage_count;
      ut_delete(entry);
    } else {
      previous = entry;
    }
    entry = next;
  }
}

/** Remove stored writes from the live queue and free unreferenced batches. */
void memtable_trim(memtable_t &mt, batch_seq_t flushed_seq) {
  ut_lock_guard_t lock(mt.m_mutex);

  /* Only visible writes can have been flushed. */
  ut_a(flushed_seq >= mt.m_flushed_seq && flushed_seq <= mt.m_visible_seq);

  while (mt.m_flushed_seq < flushed_seq) {
    memtable_entry_t *entry = mt.m_live.front();
    mt.m_live.pop_front();

    ut_a(entry->m_seq == mt.m_flushed_seq + 1);
    mt.m_flushed_seq = entry->m_seq;
    mt.m_live_rows -= entry->m_batch.m_n_rows;
    mt.m_live_bytes -= batch_bytes(entry->m_batch);

    entry->m_next_garbage = nullptr;

    if (mt.m_garbage_tail == nullptr) {
      mt.m_garbage_head = entry;
    } else {
      mt.m_garbage_tail->m_next_garbage = entry;
    }
    mt.m_garbage_tail = entry;
    mt.m_garbage_count++;
  }

  memtable_check_boundaries(mt);
  memtable_free_garbage(mt);
}

/** Copy the newest schema of every table, by name. */
void memtable_list_schemas(memtable_t &mt, std::vector<schema_t> &schemas) {
  schemas.clear();

  ut_lock_guard_t lock(mt.m_mutex);

  std::map<std::string, memtable_table_t *>::const_iterator it;

  for (it = mt.m_tables.begin(); it != mt.m_tables.end(); ++it) {
    schemas.push_back(*it->second->m_schemas.back());
  }
}

/** Reference every visible, unflushed write before starting a DuckDB
transaction. */
void memtable_snapshot_pin(memtable_t &mt, memtable_snapshot_t &snap) {
  /* Overwriting a pinned snapshot would keep its batches forever. */
  ut_a(snap.m_entries.empty());

  snap = memtable_snapshot_t();

  ut_lock_guard_t lock(mt.m_mutex);

  snap.m_base_seq = mt.m_flushed_seq;
  snap.m_visible_seq = mt.m_visible_seq;

  for (memtable_entry_t *entry : mt.m_live) {
    /* Reserved, but not visible yet. */
    if (entry->m_seq > snap.m_visible_seq) {
      break;
    }

    ++entry->m_readers;
    snap.m_entries.push_back(entry);
  }
}

/** Append one selected immutable batch. */
static void memtable_snapshot_append([[maybe_unused]] const memtable_t &mt,
                                     const memtable_entry_t &entry,
                                     memtable_snapshot_t &snap) {
  ut_ad(mt.m_mutex.is_owned());

  memtable_table_view_t &view = snap.m_tables[entry.m_batch.m_table];
  view.m_batches.push_back(&entry.m_batch);
  const schema_t *schema =
      entry.m_table->m_schemas[entry.m_batch.m_schema_version - 1];
  ut_a(schema->m_version == entry.m_batch.m_schema_version);

  if (view.m_schema == nullptr ||
      schema->m_version > view.m_schema->m_version) {
    view.m_schema = schema;
  }
}

/** Complete a pinned snapshot using the F read by its DuckDB transaction: drop
the writes the stored snapshot already stores and keep the rest. */
db_status_t memtable_snapshot_complete(memtable_t &mt, batch_seq_t flushed_seq,
                                       memtable_snapshot_t &snap) {
  ut_lock_guard_t lock(mt.m_mutex);

  /* F never moves back, and only visible writes are flushed. */
  if (flushed_seq < snap.m_base_seq || flushed_seq > mt.m_visible_seq) {
    return (DB_CORRUPTION);
  }

  /* A flush that committed after the pin may have stored writes after
  it; the stored snapshot then holds every one of them. */
  if (flushed_seq > snap.m_visible_seq) {
    snap.m_visible_seq = flushed_seq;
  }

  snap.m_base_seq = flushed_seq;
  snap.m_tables.clear();

  std::vector<memtable_entry_t *> kept;

  for (memtable_entry_t *entry : snap.m_entries) {
    /* Already in the stored snapshot, so this pin is not needed. */
    if (entry->m_seq <= flushed_seq) {
      ut_a(entry->m_readers > 0);
      --entry->m_readers;
    } else {
      memtable_snapshot_append(mt, *entry, snap);
      kept.push_back(entry);
    }
  }

  snap.m_entries.swap(kept);
  memtable_free_garbage(mt);

  /* Every write after F must have been pinned. */
  if (snap.m_entries.size() != snap.m_visible_seq - flushed_seq) {
    snap.m_tables.clear();

    return (DB_CORRUPTION);
  }

  return (DB_SUCCESS);
}

/** Release a snapshot; its batches may be freed afterwards. */
void memtable_snapshot_release(memtable_t &mt, memtable_snapshot_t &snap) {
  if (!snap.m_entries.empty()) {
    ut_lock_guard_t lock(mt.m_mutex);

    for (memtable_entry_t *entry : snap.m_entries) {
      ut_a(entry->m_readers > 0);
      --entry->m_readers;
    }

    memtable_free_garbage(mt);
  }

  snap = memtable_snapshot_t();
}

/** Read the counters. */
void memtable_get_stats(memtable_t &mt, memtable_stats_t &stats) {
  ut_lock_guard_t lock(mt.m_mutex);

  stats.m_flushed_seq = mt.m_flushed_seq;
  stats.m_visible_seq = mt.m_visible_seq;
  stats.m_live_writes = mt.m_live.size();
  stats.m_live_rows = mt.m_live_rows;
  stats.m_live_bytes = mt.m_live_bytes;
  stats.m_garbage_writes = mt.m_garbage_count;
}
