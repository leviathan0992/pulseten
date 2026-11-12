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
 * retired list until no snapshot references it. A snapshot references every
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

/** One accepted write. */
struct memtable_entry_t {
  /** Sequence number of the write. */
  batch_seq_t m_seq{0};

  /** Column directory visible when this write was reserved. */
  schema_ref_t m_schema;

  /** The rows; never changed once the entry is reserved. */
  batch_t m_batch{};

  /** Snapshots that hold this immutable batch. Protected by the memtable
  mutex, like the retired list. */
  uint64_t m_readers{0};

  /** Next entry awaiting release. */
  memtable_entry_t *m_next{nullptr};
};

/** Create an empty memtable. */
db_status_t memtable::create(batch_seq_t start_seq, uint64_t limit_bytes,
                             memtable **out) {
  *out = nullptr;

  memtable *mt = ut_new<memtable>(limit_bytes);
  if (mt == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  mt->m_flushed_seq = start_seq;
  mt->m_visible_seq = start_seq;
  mt->m_reserved_seq = start_seq;

  *out = mt;

  return (DB_SUCCESS);
}

/** Assert sequence ordering and live-entry count consistency. */
void memtable::assert_consistent() const {
  ut_ad(m_mutex.is_owned());

  ut_a(m_flushed_seq <= m_visible_seq);
  ut_a(m_visible_seq <= m_reserved_seq);
  ut_a(m_live.size() == m_reserved_seq - m_flushed_seq);

  return;
}

/** Free a memtable and every schema and batch in it. */
memtable::~memtable() {
  for (memtable_entry_t *entry : m_live) {
    ut_delete(entry);
  }

  while (m_retired_head != nullptr) {
    memtable_entry_t *entry = m_retired_head;
    m_retired_head = entry->m_next;
    ut_delete(entry);
  }
}

void memtable::destroy(memtable *mt) {
  ut_delete(mt);

  return;
}

/** Restore a validated stored column directory before WAL replay. */
db_status_t memtable::restore_schema(const schema_t &schema) {
  ut_lock_guard_t lock(m_mutex);

  /* Seed stored schemas before WAL replay reserves any writes. */
  ut_a(m_live.empty() && m_reserved_seq == m_flushed_seq);

  /* Storage has validated the schema; check the memtable's own state here. */
  if (m_tables.count(schema.m_name) != 0) {
    return (DB_CORRUPTION);
  }

  /* Keep an immutable copy for write planning and replay; rows stay in storage.
   */
  schema_t *copy = ut_new<schema_t>(schema);
  if (copy == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  m_tables.emplace(schema.m_name, schema_ref_t(copy));

  return (DB_SUCCESS);
}

/** Newest schema of a table, including schema changes of writes that are
reserved but not yet visible. */
schema_ref_t memtable::find_schema(const std::string &table_name) {
  ut_lock_guard_t lock(m_mutex);

  const auto table = m_tables.find(table_name);

  return (table == m_tables.end() ? nullptr : table->second);
}

/** Check whether a plan is consistent with the table and schema state.
@return true if the consistency checks pass; allocation may still fail when
  applying the plan */
bool memtable::can_apply_plan(const ingest_plan_t &plan) {
  ut_ad(m_mutex.is_owned());

  const auto table = m_tables.find(plan.m_table_name);
  const schema_t *current =
      table == m_tables.end() ? nullptr : table->second.get();

  /* The table exists exactly when the plan does not create it. */
  if ((current == nullptr) != plan.m_creates_table) {
    return (false);
  }

  const size_t n_cols = current != nullptr ? current->m_cols.size() : 0;

  /* A schema change must be based on the current directory, and its columns
  must continue the column ids with new names. */
  if (!plan.m_new_cols.empty()) {
    if (plan.m_base_columns != n_cols) {
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

  /* A batch identifies its columns by their prefix length. Missing trailing
  columns of the current directory are NULL. */
  const size_t total_cols = n_cols + plan.m_new_cols.size();
  if (plan.m_batch.m_cols.empty() || plan.m_batch.m_cols.size() > total_cols ||
      plan.m_batch.m_cols.size() !=
          plan.m_base_columns + plan.m_new_cols.size() ||
      (!plan.m_new_cols.empty() && plan.m_batch.m_cols.size() != total_cols)) {
    return (false);
  }

  for (size_t i = 0; i < plan.m_batch.m_cols.size(); ++i) {
    const col_def_t &column =
        i < n_cols ? current->m_cols[i] : plan.m_new_cols[i - n_cols];
    if (plan.m_batch.m_cols[i].m_col_id != i ||
        plan.m_batch.m_cols[i].m_type != column.m_type) {
      return (false);
    }
  }

  return (true);
}

/** Apply a plan validated by memtable::can_apply_plan().
@param[in,out]  plan  validated write plan; its batch is moved into the
                      memtable
@return DB_SUCCESS after reserving the schema and batch, or DB_OUT_OF_MEMORY
  without changing memtable state */
db_status_t memtable::apply_plan(batch_seq_t seq, ingest_plan_t &plan) {
  ut_ad(m_mutex.is_owned());

  const auto table = m_tables.find(plan.m_table_name);
  schema_ref_t schema = table == m_tables.end() ? nullptr : table->second;

  /* Allocate the new schema and entry before publishing either. */
  if (!plan.m_new_cols.empty()) {
    schema_t *next = ut_new<schema_t>();
    if (next == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    if (schema != nullptr) {
      *next = *schema;
    }

    next->m_name = plan.m_table_name;
    next->m_cols.insert(next->m_cols.end(), plan.m_new_cols.begin(),
                        plan.m_new_cols.end());
    schema.reset(next);
  }

  memtable_entry_t *entry = ut_new<memtable_entry_t>();
  if (entry == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  /* Pin the directory at this sequence; future columns must not appear in
  an older query snapshot. The batch itself may use a shorter prefix. */
  m_tables[plan.m_table_name] = schema;
  entry->m_schema = schema;
  entry->m_seq = seq;
  m_live.push_back(entry);

  std::swap(entry->m_batch, plan.m_batch);
  m_live_rows += entry->m_batch.m_n_rows;
  m_live_bytes += entry->m_batch.data_bytes();

  return (DB_SUCCESS);
}

/** Take a write: apply its schema change and hold its batch, not visible
yet. */
db_status_t memtable::reserve(batch_seq_t seq, ingest_plan_t &plan) {
  ut_lock_guard_t lock(m_mutex);

  /* The caller hands out sequence numbers; they arrive in order. */
  ut_a(seq == m_reserved_seq + 1);

  if (!can_apply_plan(plan)) {
    return (DB_CORRUPTION);
  }

  if (const db_status_t err = apply_plan(seq, plan); !err.ok()) {
    return (err);
  }

  m_reserved_seq = seq;
  assert_consistent();

  return (DB_SUCCESS);
}

/** Locate a reserved, unretired write by sequence number.
@return the live entry with a sequence number */
memtable_entry_t &memtable::get_live_entry(batch_seq_t seq) {
  ut_ad(m_mutex.is_owned());
  ut_a(seq > m_flushed_seq && seq <= m_reserved_seq);

  memtable_entry_t &entry = *m_live[seq - m_flushed_seq - 1];
  ut_a(entry.m_seq == seq);

  return (entry);
}

/** Publish the complete prefix whose WAL I/O db has finished successfully. */
void memtable::publish(batch_seq_t seq) {
  ut_lock_guard_t lock(m_mutex);

  ut_a(seq > m_visible_seq && seq <= m_reserved_seq);

  m_visible_seq = seq;

  assert_consistent();

  return;
}

/** List the visible writes that are not flushed yet, in sequence order. */
void memtable::collect_unflushed(std::vector<memtable_flush_item_t> &items) {
  items.clear();

  ut_lock_guard_t lock(m_mutex);

  for (batch_seq_t seq = m_flushed_seq + 1; seq <= m_visible_seq; ++seq) {
    const memtable_entry_t &entry = get_live_entry(seq);

    /* Storage must never get ahead of the WAL. */

    memtable_flush_item_t item;
    item.m_seq = seq;
    item.m_batch = &entry.m_batch;
    item.m_schema = entry.m_schema;
    ut_a(item.m_schema->m_cols.size() >= entry.m_batch.m_cols.size());

    items.push_back(item);
  }

  return;
}

/** Free retired entries that no open snapshot can use. */
void memtable::reclaim() {
  ut_ad(m_mutex.is_owned());

  memtable_entry_t *previous = nullptr;
  memtable_entry_t *entry = m_retired_head;
  while (entry != nullptr) {
    memtable_entry_t *next = entry->m_next;
    if (entry->m_readers == 0) {
      if (previous != nullptr) {
        previous->m_next = next;
      } else {
        m_retired_head = next;
      }

      if (m_retired_tail == entry) {
        m_retired_tail = previous;
      }

      --m_retired_count;
      ut_delete(entry);
    } else {
      previous = entry;
    }

    entry = next;
  }

  return;
}

/** Remove stored writes from the live queue and free unreferenced batches. */
void memtable::trim(batch_seq_t flushed_seq) {
  ut_lock_guard_t lock(m_mutex);

  /* Only visible writes can have been flushed. */
  ut_a(flushed_seq >= m_flushed_seq && flushed_seq <= m_visible_seq);

  while (m_flushed_seq < flushed_seq) {
    memtable_entry_t *entry = m_live.front();
    m_live.pop_front();

    ut_a(entry->m_seq == m_flushed_seq + 1);
    m_flushed_seq = entry->m_seq;
    m_live_rows -= entry->m_batch.m_n_rows;
    m_live_bytes -= entry->m_batch.data_bytes();

    entry->m_next = nullptr;

    if (m_retired_tail == nullptr) {
      m_retired_head = entry;
    } else {
      m_retired_tail->m_next = entry;
    }

    m_retired_tail = entry;
    m_retired_count++;
  }

  assert_consistent();
  reclaim();

  return;
}

/** Retain the current immutable directories without copying their columns. */
void memtable::reference_schemas(std::vector<schema_ref_t> &schemas) {
  schemas.clear();

  ut_lock_guard_t lock(m_mutex);

  for (const auto &table : m_tables) {
    schemas.push_back(table.second);
  }

  return;
}

/** Reference every visible, unflushed write before starting a DuckDB
transaction. */
void memtable::pin_snapshot(memtable_snapshot_t &snapshot) {
  /* Overwriting a pinned snapshot would keep its batches forever. */
  ut_a(snapshot.m_entries.empty());

  snapshot = memtable_snapshot_t();

  ut_lock_guard_t lock(m_mutex);

  snapshot.m_base_seq = m_flushed_seq;
  snapshot.m_visible_seq = m_visible_seq;

  for (memtable_entry_t *entry : m_live) {
    /* Reserved, but not visible yet. */
    if (entry->m_seq > snapshot.m_visible_seq) {
      break;
    }

    ++entry->m_readers;
    snapshot.m_entries.push_back(entry);
  }

  return;
}

/** Append one selected immutable batch. */
void memtable::snapshot_append(const memtable_entry_t &entry,
                               memtable_snapshot_t &snapshot) const {
  ut_ad(m_mutex.is_owned());

  memtable_table_view_t &view = snapshot.m_tables[entry.m_batch.m_table_name];
  view.m_batches.push_back(&entry.m_batch);
  const schema_ref_t &schema = entry.m_schema;
  ut_a(schema->m_cols.size() >= entry.m_batch.m_cols.size());

  if (view.m_schema == nullptr ||
      schema->m_cols.size() > view.m_schema->m_cols.size()) {
    view.m_schema = schema;
  }

  return;
}

/** Complete a pinned snapshot using the F read by its DuckDB transaction: drop
the writes the stored snapshot already stores and keep the rest. */
db_status_t memtable::complete_snapshot(batch_seq_t flushed_seq,
                                        memtable_snapshot_t &snapshot) {
  ut_lock_guard_t lock(m_mutex);

  /* F never moves back, and only visible writes are flushed. */
  if (flushed_seq < snapshot.m_base_seq || flushed_seq > m_visible_seq) {
    return (DB_CORRUPTION);
  }

  /* A flush that committed after the pin may have stored writes after
  it; the stored snapshot then holds every one of them. */
  if (flushed_seq > snapshot.m_visible_seq) {
    snapshot.m_visible_seq = flushed_seq;
  }

  snapshot.m_base_seq = flushed_seq;
  snapshot.m_tables.clear();

  std::vector<memtable_entry_t *> kept;

  for (memtable_entry_t *entry : snapshot.m_entries) {
    /* Already in the stored snapshot, so this pin is not needed. */
    if (entry->m_seq <= flushed_seq) {
      ut_a(entry->m_readers > 0);
      --entry->m_readers;
    } else {
      snapshot_append(*entry, snapshot);
      kept.push_back(entry);
    }
  }

  snapshot.m_entries.swap(kept);
  reclaim();

  /* Every write after F must have been pinned. */
  if (snapshot.m_entries.size() != snapshot.m_visible_seq - flushed_seq) {
    snapshot.m_tables.clear();

    return (DB_CORRUPTION);
  }

  return (DB_SUCCESS);
}

/** Release a snapshot; its batches may be freed afterwards. */
void memtable::release_snapshot(memtable_snapshot_t &snapshot) {
  if (!snapshot.m_entries.empty()) {
    ut_lock_guard_t lock(m_mutex);

    for (memtable_entry_t *entry : snapshot.m_entries) {
      ut_a(entry->m_readers > 0);
      --entry->m_readers;
    }

    reclaim();
  }

  snapshot = memtable_snapshot_t();

  return;
}

/** Check the capacity threshold without reserving space. */
bool memtable::has_space() {
  if (m_limit_bytes == 0) {
    return (true);
  }

  ut_lock_guard_t lock(m_mutex);

  return (m_live_bytes < m_limit_bytes);
}

/** Read the counters. */
void memtable::read_stats(memtable_stats_t &stats) {
  ut_lock_guard_t lock(m_mutex);

  stats.m_flushed_seq = m_flushed_seq;
  stats.m_visible_seq = m_visible_seq;
  stats.m_live_writes = m_live.size();
  stats.m_live_rows = m_live_rows;
  stats.m_live_bytes = m_live_bytes;
  stats.m_retired_writes = m_retired_count;

  return;
}
