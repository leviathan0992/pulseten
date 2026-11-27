/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * memtable.h
 *    In-memory store of accepted writes.
 *
 * reserve() accepts a validated batch and its schema version; publish()
 * exposes a complete prefix after WAL success. The database table directory
 * owns schema definitions; this layer only retains version numbers.
 *
 * retire() advances the stored boundary after a flush commits in DuckDB.
 * Flushed batches covered by a registered query interval stay in the deque;
 * the others are freed, leaving empty slots until the prefix can be
 * removed. Batches never change once added.
 *
 * All functions are thread-safe unless noted.
 *
 *-------------------------------------------------------------------------
 */

#ifndef memtable_h
#define memtable_h

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "db_batch.h"
#include "db_schema.h"
#include "ut.h"

/** Owns batches and query interval registrations. */
class memtable final : private ut_noncopyable {
 public:
  /** Every batch with m_base_seq < seq <= m_visible_seq. Writes up to
  m_base_seq are in DuckDB instead. Registration must be released before its
  owner dies. */
  struct snapshot_t : private ut_noncopyable {
    /** Owner of the registered interval, including an empty snapshot. */
    memtable *m_owner{nullptr};

    /** Writes up to this sequence are read from DuckDB instead of this
    snapshot. */
    batch_seq_t m_base_seq{0};

    /** Highest sequence included in this snapshot, inclusive. */
    batch_seq_t m_visible_seq{0};
  };

  /** Statistics copied under the memtable mutex. */
  struct stats_t {
    /** Every write up to this sequence has been flushed and retired. */
    batch_seq_t m_flushed_seq{0};

    /** Every write up to this sequence is visible to readers. */
    batch_seq_t m_visible_seq{0};

#ifdef PT_TESTING
    /** Writes not yet retired, including writes not yet visible. */
    uint64_t m_live_writes{0};

    /** Rows in writes not yet retired, including writes not yet visible. */
    uint64_t m_live_rows{0};
#endif

    /** Column data bytes in writes not yet retired, as measured by
    batch_t::calculate_data_bytes(). */
    uint64_t m_live_bytes{0};

#ifdef PT_TESTING
    /** Flushed writes covered by active query intervals. */
    uint64_t m_retired_write_count{0};

    /** Flushed column bytes covered by active query intervals. */
    uint64_t m_retired_bytes{0};
#endif
  };

  /** Construct an empty object with a fixed capacity threshold. */
  explicit memtable(uint64_t limit_bytes) : m_limit_bytes(limit_bytes) {}

  ~memtable();

  /** Create an empty memtable whose first write will be start_seq + 1.
  limit_bytes is the capacity threshold; 0 disables capacity waiting.
  @return DB_SUCCESS or DB_OUT_OF_MEMORY */
  static db_status_t create(batch_seq_t start_seq, uint64_t limit_bytes,
                            memtable **out);

  /** Accept a validated batch and its schema version, moving its data.
  The database must retain that version while the batch can be read.
  REQUIRES: seq is one more than the previous reserved write.
  @return DB_SUCCESS or DB_OUT_OF_MEMORY without accepting the batch */
  db_status_t reserve(batch_seq_t seq, batch_t &batch);

  /** Publish the complete prefix through seq after db commits its WAL group.
  REQUIRES: seq advances the visible boundary without skipping uncommitted I/O.
  No waiting or allocation is performed here. */
  void publish(batch_seq_t seq);

  /** List unflushed entries in sequence order. The caller serializes
  retirement; returned pointers stay valid until those entries are retired.
  @return the flushed sequence immediately before the collected range */
  batch_seq_t collect_unflushed(std::vector<const batch_t *> &entries);

  /** Advance the stored boundary and free writes outside query intervals.
  REQUIRES: flushed_seq <= visible_seq. */
  void retire(batch_seq_t flushed_seq);

  /** Protect the visible sequence interval before starting the DuckDB
  transaction. Release the snapshot even if align_snapshot() fails. */
  void pin_snapshot(snapshot_t &snapshot);

  /** Align a registered snapshot with the F read by its DuckDB transaction,
  keeping only the writes after F. If F passed the registered visible_seq,
  snapshot holds no batches and its visible_seq becomes F.
  @return DB_SUCCESS, or DB_CORRUPTION if F precedes registration or exceeds
    the current visible boundary */
  db_status_t align_snapshot(batch_seq_t flushed_seq, snapshot_t &snapshot);

  /** Release a snapshot and reset it. Does nothing if it was never taken. */
  void release_snapshot(snapshot_t &snapshot);

  /** Read schema versions from the latest writes to requested tables.
  Replaces versions; tables without writes in the interval are omitted. */
  void schema_versions(const snapshot_t &snapshot,
                       const std::vector<std::string> &names,
                       std::map<std::string, schema_version_t> &versions);

  /** Inspect at most capacity sequence positions after seq, returning only
  batches of table_name. Advance seq past every inspected position, including
  other tables. The registered snapshot keeps returned pointers valid until
  release, allowing table-name filtering after unlocking. No deque iterator
  escapes the lock.
  @return number of batch pointers written, possibly zero before EOF */
  size_t scan_batches(const snapshot_t &snapshot, const std::string &table_name,
                      batch_seq_t &seq, const batch_t **batches,
                      size_t capacity);

  /** Check whether unretired column bytes are below the threshold, or the
  threshold is disabled. Does not reserve capacity or wait for space. */
  bool has_space();

  /** Read the counters. */
  void stats(stats_t &stats);

  /** Destroy a memtable; mt may be NULL.
  REQUIRES: no call in progress and no snapshot in use. */
  static void destroy(memtable *mt);

 private:
  /** Entries detached under m_mutex and destroyed after releasing it. */
  class reclaimer;

  /** Sequence numbers reached by each stage of a write. */
  struct seq_t {
    /** Last write reserved, including writes not yet published. */
    batch_seq_t m_reserved_seq{0};

    /** Last write visible to queries. */
    batch_seq_t m_visible_seq{0};

    /** Last write stored in DuckDB. */
    batch_seq_t m_flushed_seq{0};

    /** Last sequence whose deque slot has been removed from the prefix. */
    batch_seq_t m_reclaimed_seq{0};
  };

  /** Assert sequence ordering and live-entry count consistency. */
  void validate() const;

  /** Locate an unreclaimed write while holding m_mutex. */
  batch_t &entry(batch_seq_t seq);

  /** Detach retired entries that no open snapshot can use. The caller owns
  reclaimer until after releasing m_mutex. */
  void reclaim(reclaimer &removed);

  /** Detach every retained entry in an inclusive retired range. */
  void detach_retired(batch_seq_t first, batch_seq_t last, reclaimer &removed);

  /** Remove one registration of a nonempty interval. */
  void unregister_snapshot(batch_seq_t base_seq, batch_seq_t visible_seq);

  /* ======================= Immutable settings ======================= */

  /** Capacity threshold fixed at construction; 0 disables capacity waiting. */
  const uint64_t m_limit_bytes{0};

  /* ================= Batches and snapshot intervals ================= */

  /** Protects this group. Held briefly; no other lock is acquired under it. */
  ut_mutex_t m_mutex;

  /** Slots for (reclaimed, reserved]. Index i has sequence reclaimed + 1 + i.
  Freed flushed entries leave NULL slots; unflushed entries are never NULL. */
  std::deque<batch_t *> m_entries;

  /** reclaimed <= flushed <= visible <= reserved. */
  seq_t m_seq;

  /** Active nonempty interval counts, keyed by (exclusive lower, inclusive
  upper). No individual batch references are registered. */
  std::map<std::pair<batch_seq_t, batch_seq_t>, uint64_t> m_snapshot_pins;

  /** Disjoint inclusive ranges of flushed entries still present. Empty slots
  are excluded, so reclamation skips freed gaps and protected spans. */
  std::map<batch_seq_t, batch_seq_t> m_retired_ranges;

#ifdef PT_TESTING
  /** Unflushed rows, including reserved writes. */
  uint64_t m_live_rows{0};
#endif

  /** Unflushed column bytes, including reserved writes. */
  uint64_t m_live_bytes{0};

#ifdef PT_TESTING
  /** Number of flushed entries still present, excluding empty slots. */
  uint64_t m_retired_write_count{0};

  /** Flushed column bytes still in m_entries; not counted for capacity. */
  uint64_t m_retired_bytes{0};
#endif
};

#endif /* memtable_h */
