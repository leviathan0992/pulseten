/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable.cc
 *    In-memory store with ordered publication and deferred freeing.
 *
 * One deque holds (reclaimed, reserved]. Publication and storage commit
 * advance visible and flushed. Registered query intervals retain only the
 * entries they cover, without taking a reference to each batch.
 *
 *-------------------------------------------------------------------------
 */

#include "memtable.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "db_batch.h"
#include "db_schema.h"
#include "ut.h"

/** Own detached entries until the memtable mutex has been released.
Declare this before the lock guard so destruction runs after unlocking. */
class memtable::reclaimer final : private ut_noncopyable {
 public:
  /** Destroy entries that are no longer reachable through the memtable. */
  ~reclaimer() {
    for (batch_t *entry : m_entries) {
      ut_delete(entry);
    }
  }

  /** Take ownership of an entry being detached. */
  void add(batch_t *entry) { m_entries.push_back(entry); }

 private:
  /** Detached entries, released in removal order. */
  std::vector<batch_t *> m_entries;
};

/** Create an empty memtable. */
db_status_t memtable::create(batch_seq_t start_seq, uint64_t limit_bytes,
                             memtable **out) {
  *out = nullptr;

  memtable *mt = ut_new<memtable>(limit_bytes);
  if (mt == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  mt->m_seq.m_flushed_seq = start_seq;
  mt->m_seq.m_visible_seq = start_seq;
  mt->m_seq.m_reserved_seq = start_seq;
  mt->m_seq.m_reclaimed_seq = start_seq;

  *out = mt;

  return (DB_SUCCESS);
}

/** Check sequence boundaries and live-entry accounting under the mutex. */
void memtable::validate() const {
  ut_ad(m_mutex.is_owned());

  ut_a(m_seq.m_flushed_seq <= m_seq.m_visible_seq);
  ut_a(m_seq.m_visible_seq <= m_seq.m_reserved_seq);
  ut_a(m_seq.m_reclaimed_seq <= m_seq.m_flushed_seq);
  ut_a(m_entries.size() == m_seq.m_reserved_seq - m_seq.m_reclaimed_seq);
#ifdef PT_TESTING
  ut_a(m_retired_write_count <= m_seq.m_flushed_seq - m_seq.m_reclaimed_seq);
  ut_a(m_retired_ranges.empty() == (m_retired_write_count == 0));
#endif

  return;
}

/** Free every batch still held by the memtable. */
memtable::~memtable() {
  ut_a(m_snapshot_pins.empty());

  for (batch_t *entry : m_entries) {
    ut_delete(entry);
  }
}

/** Delete the memtable after callers have released their snapshots. */
void memtable::destroy(memtable *mt) {
  ut_delete(mt);

  return;
}

/** Accept a validated batch and its table-owned schema version. */
db_status_t memtable::reserve(batch_seq_t seq, batch_t &batch) {
  ut_lock_guard_t lock(m_mutex);

  ut_a(seq == m_seq.m_reserved_seq + 1);
  ut_a(batch.m_schema_version > 0 &&
       batch.m_schema_version <= SCHEMA_MAX_COLUMNS);
  ut_a(!batch.m_columns.empty() &&
       batch.m_columns.size() <= batch.m_schema_version);

  batch_t *entry = ut_new<batch_t>();
  if (entry == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  m_entries.push_back(entry);

  std::swap(*entry, batch);

#ifdef PT_TESTING
  m_live_rows += entry->m_row_count;
#endif

  entry->m_data_bytes = entry->calculate_data_bytes();

  m_live_bytes += entry->m_data_bytes;

  m_seq.m_reserved_seq = seq;

  validate();

  return (DB_SUCCESS);
}

/** Locate an unreclaimed sequence in the deque while holding m_mutex. */
batch_t &memtable::entry(batch_seq_t seq) {
  ut_ad(m_mutex.is_owned());
  ut_a(seq > m_seq.m_reclaimed_seq && seq <= m_seq.m_reserved_seq);

  batch_t *ptr = m_entries[seq - m_seq.m_reclaimed_seq - 1];
  ut_a(ptr != nullptr);

  return (*ptr);
}

/** Publish the complete prefix whose WAL I/O db has finished successfully. */
void memtable::publish(batch_seq_t seq) {
  ut_lock_guard_t lock(m_mutex);

  ut_a(seq > m_seq.m_visible_seq && seq <= m_seq.m_reserved_seq);

  m_seq.m_visible_seq = seq;

  validate();

  return;
}

/** List the visible writes that are not flushed yet, in sequence order. */
batch_seq_t memtable::collect_unflushed(std::vector<const batch_t *> &entries) {
  entries.clear();

  ut_lock_guard_t lock(m_mutex);

  for (batch_seq_t seq = m_seq.m_flushed_seq; seq < m_seq.m_visible_seq;) {
    entries.push_back(&entry(++seq));
  }

  return (m_seq.m_flushed_seq);
}

/** Detach an inclusive range of flushed entries for later destruction. */
void memtable::detach_retired(batch_seq_t first, batch_seq_t last,
                              reclaimer &removed) {
  ut_ad(m_mutex.is_owned());
  ut_a(first > m_seq.m_reclaimed_seq && last <= m_seq.m_flushed_seq);
  ut_ad(first <= last);

  for (batch_seq_t seq = first;; ++seq) {
    batch_t *&entry = m_entries[seq - m_seq.m_reclaimed_seq - 1];

    ut_a(entry != nullptr);

#ifdef PT_TESTING
    ut_ad(m_retired_write_count > 0);
    ut_ad(m_retired_bytes >= entry->m_data_bytes);

    m_retired_bytes -= entry->m_data_bytes;
    --m_retired_write_count;
#endif

    removed.add(entry);
    entry = nullptr;

    if (seq == last) {
      break;
    }
  }

  return;
}

/** Detach flushed entries outside the union of registered query intervals. */
void memtable::reclaim(reclaimer &removed) {
  ut_ad(m_mutex.is_owned());

  if (m_retired_ranges.empty()) {
    return;
  }

  /* Merge bounds, not batches. Protected spans are skipped in constant time. */
  std::vector<std::pair<batch_seq_t, batch_seq_t>> covered;

  /* Merge snapshot-protected flushed ranges so reclamation skips them. */
  for (const auto &[interval, count] : m_snapshot_pins) {
    ut_a(count > 0);

    if (interval.first >= m_seq.m_flushed_seq) {
      break;
    }

    const batch_seq_t first = interval.first + 1;
    const batch_seq_t last = std::min(interval.second, m_seq.m_flushed_seq);

    if (!covered.empty() && first - 1 <= covered.back().second) {
      covered.back().second = std::max(covered.back().second, last);
    } else {
      covered.emplace_back(first, last);
    }
  }

  std::map<batch_seq_t, batch_seq_t> retained;
  size_t index = 0;

  /* Retain snapshot-protected spans and detach the rest for reclamation. */
  for (const auto &[first, last] : m_retired_ranges) {
    while (index < covered.size() && covered[index].second < first) {
      ++index;
    }

    batch_seq_t cursor = first;
    bool complete = false;

    for (size_t i = index; i < covered.size() && covered[i].first <= last;
         ++i) {
      /* Keep the protected overlap and detach any preceding gap. */
      const batch_seq_t begin = std::max(cursor, covered[i].first);
      const batch_seq_t end = std::min(last, covered[i].second);

      if (cursor < begin) {
        /* No snapshot needs the gap before this protected span. */
        detach_retired(cursor, begin - 1, removed);
      }

      retained.emplace(begin, end);

      if (end == last) {
        complete = true;
        break;
      }

      cursor = end + 1;
    }

    if (!complete) {
      /* No snapshot protects the remaining tail, so reclaim it too. */
      detach_retired(cursor, last, removed);
    }
  }

  m_retired_ranges.swap(retained);

  /* Keep sequence-to-slot offsets stable until empty slots reach the front. */
  while (!m_entries.empty() && m_entries.front() == nullptr) {
    m_entries.pop_front();
    ++m_seq.m_reclaimed_seq;
  }

  validate();

  return;
}

/** Mark stored writes as flushed, then retain only reader-protected ranges. */
void memtable::retire(batch_seq_t flushed_seq) {
  reclaimer removed;

  ut_lock_guard_t lock(m_mutex);

  ut_a(flushed_seq >= m_seq.m_flushed_seq);
  ut_a(flushed_seq <= m_seq.m_visible_seq);

  if (flushed_seq > m_seq.m_flushed_seq) {
    /* Track newly flushed batches until snapshots allow reclamation. */
    if (!m_retired_ranges.empty() &&
        m_retired_ranges.rbegin()->second == m_seq.m_flushed_seq) {
      /* Extend the last range when the new interval is adjacent. */
      m_retired_ranges.rbegin()->second = flushed_seq;
    } else {
      m_retired_ranges.emplace(m_seq.m_flushed_seq + 1, flushed_seq);
    }

#ifdef PT_TESTING
    m_retired_write_count += flushed_seq - m_seq.m_flushed_seq;
#endif
  }

  while (m_seq.m_flushed_seq < flushed_seq) {
    /* Reclassify each flushed batch as retired; reclaim() frees it later. */
    const batch_t &batch = entry(++m_seq.m_flushed_seq);
    const uint64_t bytes = batch.m_data_bytes;

#ifdef PT_TESTING
    ut_ad(m_live_rows >= batch.m_row_count);
    m_live_rows -= batch.m_row_count;
#endif
    ut_ad(m_live_bytes >= bytes);

    m_live_bytes -= bytes;
#ifdef PT_TESTING
    m_retired_bytes += bytes;
#endif
  }

  /* Detach unpinned batches; removed frees them after unlocking. */
  reclaim(removed);

  return;
}

/** Register a boundary before fixing the stored transaction snapshot. */
void memtable::pin_snapshot(memtable::snapshot_t &snapshot) {
  ut_a(snapshot.m_owner == nullptr);

  ut_lock_guard_t lock(m_mutex);

  snapshot.m_owner = this;
  snapshot.m_base_seq = m_seq.m_flushed_seq;
  snapshot.m_visible_seq = m_seq.m_visible_seq;

  if (snapshot.m_base_seq == snapshot.m_visible_seq) {
    return;
  }

  ++m_snapshot_pins[{snapshot.m_base_seq, snapshot.m_visible_seq}];

  return;
}

/** Remove one protection of an interval. */
void memtable::unregister_snapshot(batch_seq_t base_seq,
                                   batch_seq_t visible_seq) {
  ut_ad(m_mutex.is_owned());

  const auto pin = m_snapshot_pins.find({base_seq, visible_seq});
  ut_a(pin != m_snapshot_pins.end());
  ut_a(pin->second > 0);

  if (--pin->second == 0) {
    m_snapshot_pins.erase(pin);
  }

  return;
}

/** Reconcile the interval with the stored transaction, without visiting
batches. */
db_status_t memtable::align_snapshot(batch_seq_t flushed_seq,
                                     memtable::snapshot_t &snapshot) {
  reclaimer removed;
  ut_lock_guard_t lock(m_mutex);
  ut_a(snapshot.m_owner == this);

  if (flushed_seq < snapshot.m_base_seq || flushed_seq > m_seq.m_visible_seq) {
    return (DB_CORRUPTION);
  }

  if (snapshot.m_base_seq < snapshot.m_visible_seq &&
      flushed_seq != snapshot.m_base_seq) {
    unregister_snapshot(snapshot.m_base_seq, snapshot.m_visible_seq);
  }

  if (flushed_seq >= snapshot.m_visible_seq) {
    snapshot.m_visible_seq = flushed_seq;
  } else if (flushed_seq != snapshot.m_base_seq) {
    ++m_snapshot_pins[{flushed_seq, snapshot.m_visible_seq}];
  }

  snapshot.m_base_seq = flushed_seq;
  reclaim(removed);

  return (DB_SUCCESS);
}

/** Release the interval before allowing prefix reclamation. */
void memtable::release_snapshot(memtable::snapshot_t &snapshot) {
  if (snapshot.m_owner == nullptr) {
    return;
  }

  reclaimer removed;
  ut_lock_guard_t lock(m_mutex);
  ut_a(snapshot.m_owner == this);

  if (snapshot.m_base_seq < snapshot.m_visible_seq) {
    unregister_snapshot(snapshot.m_base_seq, snapshot.m_visible_seq);
  }

  snapshot.m_owner = nullptr;

  snapshot.m_base_seq = 0;
  snapshot.m_visible_seq = 0;
  reclaim(removed);

  return;
}

/** Find each requested table's schema at the snapshot's upper boundary. */
void memtable::schema_versions(
    const memtable::snapshot_t &snapshot, const std::vector<std::string> &names,
    std::map<std::string, schema_version_t> &versions) {
  versions.clear();

  ut_a(snapshot.m_owner == this);
  if (snapshot.m_visible_seq == snapshot.m_base_seq) {
    return;
  }

  std::set<std::string_view> pending(names.begin(), names.end());

  /* Limit work per mutex acquisition while walking schema versions. */
  static const size_t scan_batches = 64;
  batch_seq_t seq = snapshot.m_visible_seq;

  while (seq > snapshot.m_base_seq && !pending.empty()) {
    const batch_t *entries[scan_batches];
    const size_t count =
        std::min<batch_seq_t>(scan_batches, seq - snapshot.m_base_seq);

    {
      ut_lock_guard_t lock(m_mutex);

      for (size_t i = 0; i < count; ++i) {
        entries[i] = &entry(seq--);
      }
    }

    /* The snapshot keeps these entries alive after unlocking. */
    for (size_t i = 0; i < count; ++i) {
      const batch_t &batch = *entries[i];
      if (pending.erase(batch.m_table_name) != 0) {
        versions.emplace(batch.m_table_name, batch.m_schema_version);
      }
    }
  }

  return;
}

/** Read a bounded number of deque slots; the interval keeps them alive. */
size_t memtable::scan_batches(const memtable::snapshot_t &snapshot,
                              const std::string &table_name, batch_seq_t &seq,
                              const batch_t **batches, size_t capacity) {
  size_t n = 0;

  {
    ut_lock_guard_t lock(m_mutex);

    ut_a(snapshot.m_owner == this);
    ut_a(seq >= snapshot.m_base_seq && seq <= snapshot.m_visible_seq);
    ut_a(capacity > 0);

    if (seq == snapshot.m_visible_seq) {
      return (0);
    }

    n = std::min<batch_seq_t>(capacity, snapshot.m_visible_seq - seq);
    ut_a(seq >= m_seq.m_reclaimed_seq);

    const size_t offset = seq - m_seq.m_reclaimed_seq;

    ut_a(offset <= m_entries.size());
    ut_a(n <= m_entries.size() - offset);

    auto entry = m_entries.begin() + static_cast<std::ptrdiff_t>(offset);

    for (size_t i = 0; i < n; ++i, ++entry) {
      ut_a(*entry != nullptr);
      batches[i] = *entry;
    }

    seq += n;
  }

  /* The interval protects every copied batch, including other tables. */
  size_t count = 0;

  for (size_t i = 0; i < n; ++i) {
    if (batches[i]->m_table_name == table_name) {
      batches[count++] = batches[i];
    }
  }

  return (count);
}

/** Check the capacity threshold without reserving space. */
bool memtable::has_space() {
  if (m_limit_bytes == 0) {
    return (true);
  }

  ut_lock_guard_t lock(m_mutex);

  return (m_live_bytes < m_limit_bytes);
}

/** Copy sequence boundaries and memory counters under the mutex. */
void memtable::stats(memtable::stats_t &stats) {
  ut_lock_guard_t lock(m_mutex);

  stats.m_flushed_seq = m_seq.m_flushed_seq;
  stats.m_visible_seq = m_seq.m_visible_seq;
  stats.m_live_bytes = m_live_bytes;
#ifdef PT_TESTING
  stats.m_live_writes = m_seq.m_reserved_seq - m_seq.m_flushed_seq;
  stats.m_live_rows = m_live_rows;
  stats.m_retired_write_count = m_retired_write_count;
  stats.m_retired_bytes = m_retired_bytes;
#endif

  return;
}
