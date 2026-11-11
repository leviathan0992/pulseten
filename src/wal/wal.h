/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal.h
 *    Write-ahead log with group commit.
 *
 * A record is queued with wal_append_enqueue() and is written when
 * wal_append_commit() returns DB_SUCCESS. With sync enabled it is also
 * durable. Concurrent callers queue up; the caller at the head of the queue
 * becomes the leader, writes every queued
 * record (up to a size limit) with one write and an optional sync, then wakes
 * the others. Under load one sync covers many records; with a single caller no
 * time is spent waiting for a batch to fill.
 *
 * The WAL takes no lock of its own: the engine's directory lock keeps a
 * second opener out.
 *
 * The on-disk format is described in wal_format.h; recovery of an
 * existing WAL in wal_recv.h.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal_h
#define wal_h

#include <cstddef>
#include <cstdint>
#include <string>

#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "wal_recv.h"

/** Default upper bound on the bytes written by one group. */
#define WAL_DEFAULT_GROUP_BYTES (1024 * 1024)

/** Default size at which a new segment is started. */
#define WAL_DEFAULT_SEGMENT_BYTES (64 * 1024 * 1024)

/** An open WAL. */
struct wal_t;

/** A record queued by wal_append_enqueue(). Owned by the caller, who must
keep it and the payload alive until wal_append_commit() returns. The fields
belong to the WAL. */
struct wal_writer_t {
  /** The record's payload. */
  const unsigned char *m_payload{nullptr};

  /** Length of payload in bytes. */
  size_t m_len{0};

  /** Sequence number given to the record when it was queued. */
  batch_seq_t m_seq{0};

  /** Set by the leader that wrote this record; m_err is valid only then. */
  bool m_done{false};

  /** Outcome of committing the record's group. */
  db_status_t m_err{DB_SUCCESS};

  /** Signalled when m_done is set, or when this writer becomes leader. */
  ut_condition_variable_t m_cv;

  /** Next writer in the queue. Only the tail's m_next changes while
  writers are queued, and only under the WAL mutex. */
  wal_writer_t *m_next{nullptr};
};

/** Per-handle write counters and current segment count. m_records, m_groups
and m_bytes start at zero after wal_create() or wal_open(); recovered records
are reported through wal_recv_t instead of adding to these write counters. */
struct wal_stats_t {
  /** Records successfully written. */
  uint64_t m_records{0};

  /** Successful group writes; each is one write and an optional sync. */
  uint64_t m_groups{0};

  /** Record bytes successfully written, headers included. */
  uint64_t m_bytes{0};

  /** Current number of segment files, the active segment included. */
  uint64_t m_segments{0};
};

/** Create a new, empty WAL file in a directory. The file appears under its
final name only after its header is durable.
@param[in]   dir              existing directory; must not contain a WAL
@param[in]   group_max_bytes  upper bound on bytes per group write, 0 for the
                              default; a single record may exceed it
@param[out]  out              new WAL, or NULL on failure
@param[in]   wal_id           stored database identity; 0 generates one
@param[in]   sync             sync every group before acknowledging writes
@return DB_SUCCESS, DB_IO_ERROR, DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
db_status_t wal_create(const char *dir, size_t group_max_bytes, wal_t **out,
                       uint64_t wal_id = 0, bool sync = true);

/** Check whether a directory holds a WAL.
@param[out]  exists  true if the WAL file exists
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_IO_ERROR */
db_status_t wal_exists(const char *dir, bool &exists);

/** Open an existing WAL and recover it: replay every valid record through
apply, cut off an unfinished last group, and leave the WAL ready to append
after the last recovered record. If the WAL scan finds corruption, published
segments are not truncated or rewritten. Opening may remove unpublished .init
leftovers even on failure.
@param[in]   apply        callback per recovered record, or NULL
@param[out]  out          open WAL, or NULL on failure
@param[in]   expected_id  database identity checked before replay; 0 for a
                          standalone WAL
@param[in]   sync         sync every group before acknowledging writes
@return DB_SUCCESS, DB_CORRUPTION, DB_IO_ERROR, DB_INVALID_ARGUMENT,
  DB_OUT_OF_MEMORY or the error returned by apply */
db_status_t wal_open(const char *dir, size_t group_max_bytes, wal_apply_t apply,
                     void *ctx, wal_t **out, wal_recv_t &recv,
                     uint64_t expected_id = 0, bool sync = true);

/** Close a WAL. No append may be in progress.
@param[in]  wal  WAL from wal_create() or wal_open(); may be NULL
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal_close(wal_t *wal);

/** Give a record its sequence number and queue it, without waiting. Records
are written in the order they are queued. Every successful call must be
followed by wal_append_commit() on the same writer; until then, records queued
later may not be written.
@param[in]   payload  record payload; must stay valid until wal_append_commit()
                      returns
@param[in]   len      payload length, 1 to WAL_MAX_PAYLOAD
@param[out]  w        caller-owned queue entry
@param[out]  seq      sequence number of the record, 0 on failure
@retval DB_SUCCESS queued
@retval DB_INVALID_ARGUMENT rejected; nothing queued
@retval DB_FENCED rejected after an earlier failure; nothing queued */
db_status_t wal_append_enqueue(wal_t &wal, const void *payload, size_t len,
                               wal_writer_t &w, batch_seq_t &seq);

/** Wait until a queued record is written, writing a group as leader if this
writer reaches the head of the queue.
@retval DB_SUCCESS written, and durable when sync is enabled
@retval DB_FENCED not written; an earlier record failed
@retval DB_IO_ERROR outcome unknown; the WAL is now fenced
@retval DB_OUT_OF_MEMORY not written; the WAL is now fenced */
db_status_t wal_append_commit(wal_t &wal, wal_writer_t &w);

/** Set the size at which the next group starts a new segment. */
void wal_set_segment_bytes(wal_t &wal, uint64_t bytes);

/** Sync groups completed before this call so storage can advance durably.
Thread-safe with appends and purging; closed segments are already synced.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal_sync(wal_t &wal);

/** Delete the closed segments that hold only stored records. Thread-safe;
appends may continue meanwhile.
@param[in]  flushed_seq  every record up to here is flushed to storage
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal_purge(wal_t &wal, batch_seq_t flushed_seq, uint64_t &removed);

/** Read the counters. */
void wal_get_stats(wal_t &wal, wal_stats_t &stats);

/** Read why the WAL failed: the operation, the segment and the cause.
@return the retained failure, or success before a group fails */
db_status_t wal_status(wal_t &wal);

#ifdef PT_DEBUG
/** Make the next group sync fail with DB_IO_ERROR after its data has been
written. Debug builds only; used by tests. */
void wal_dbg_fail_next_sync(wal_t &wal);
#endif /* PT_DEBUG */

#endif /* wal_h */
