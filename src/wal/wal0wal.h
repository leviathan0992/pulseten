/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal0wal.h
 *    Write-ahead log with group commit.
 *
 * A record is queued with wal_append_enqueue() and is durable when
 * wal_append_wait() returns DB_SUCCESS. Concurrent callers queue up; the
 * caller at the head of the queue becomes the leader, writes every queued
 * record (up to a size limit) with one write and one flush, then wakes the
 * others. Under load one flush covers many records; with a
 * single caller no time is spent waiting for a batch to fill.
 *
 * The WAL takes no lock of its own: the engine's directory lock keeps a
 * second opener out.
 *
 * The on-disk format is described in wal0format.h; recovery of an
 * existing WAL in wal0recv.h.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal0wal_h
#define wal0wal_h

#include <condition_variable>
#include <cstddef>
#include <cstdint>

#include "ut0err.h"
#include "ut0ut.h"
#include "wal0format.h"
#include "wal0recv.h"

/** Default upper bound on the bytes written by one group. */
#define WAL_DEFAULT_GROUP_BYTES (1024 * 1024)

/** Default size at which a new segment is started. */
#define WAL_DEFAULT_SEGMENT_BYTES (64 * 1024 * 1024)

/** An open WAL. */
struct wal_t;

/** A record queued by wal_append_enqueue(). Owned by the caller, who must
keep it and the payload alive until wal_append_wait() returns. The fields
belong to the WAL. */
struct wal_writer_t {
  /** The record's payload. */
  const unsigned char *payload{nullptr};

  /** Length of payload in bytes. */
  size_t len{0};

  /** Sequence number given to the record when it was queued. */
  batch_seq_t seq{0};

  /** Set by the leader that wrote this record; err is valid only then. */
  bool done{false};

  /** Outcome of writing and flushing the record's group. */
  dberr_t err{DB_SUCCESS};

  /** Signalled when done is set, or when this writer becomes leader. */
  std::condition_variable cv;

  /** Next writer in the queue. Only the tail's next changes while
  writers are queued, and only under the WAL mutex. */
  wal_writer_t *next{nullptr};
};

/** Per-handle write counters and current segment count. records, groups and
bytes start at zero after wal_create() or wal_open(); recovered records are
reported through wal_recv_t instead of adding to these write counters. */
struct wal_stats_t {
  /** Records made durable. */
  uint64_t records{0};

  /** Successful group writes; each is one write and one flush. */
  uint64_t groups{0};

  /** Record bytes made durable, headers included. */
  uint64_t bytes{0};

  /** Current number of segment files, the active segment included. */
  uint64_t segments{0};
};

/** Create a new, empty WAL file in a directory. The file appears under its
final name only after its header is durable.
@param[in]   dir              existing directory; must not contain a WAL
@param[in]   group_max_bytes  upper bound on bytes per group write, 0 for the
                              default; a single record may exceed it
@param[out]  wal              new WAL, or NULL on failure
@param[in]   wal_id           stored database identity; 0 generates one
@return DB_SUCCESS, DB_IO_ERROR, DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
dberr_t wal_create(const char *dir, size_t group_max_bytes, wal_t **wal,
                   uint64_t wal_id = 0);

/** Check whether a directory holds a WAL.
@param[in]   dir     directory
@param[out]  exists  true if the WAL file exists
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_IO_ERROR */
dberr_t wal_exists(const char *dir, bool &exists);

/** Open an existing WAL and recover it: replay every valid record through
apply, cut off an unfinished last group, and leave the WAL ready to append
after the last recovered record. If the WAL scan finds corruption, published
segments are not truncated or rewritten. Opening may remove unpublished .init
leftovers even on failure.
@param[in]   dir              directory holding the WAL
@param[in]   group_max_bytes  as for wal_create()
@param[in]   apply            callback per recovered record, or NULL
@param[in]   ctx              passed to apply
@param[out]  wal              open WAL, or NULL on failure
@param[out]  recv             what recovery found
@param[in]   expected_id      database identity checked before replay; 0 for a
                              standalone WAL
@return DB_SUCCESS, DB_CORRUPTION, DB_IO_ERROR, DB_INVALID_ARGUMENT,
  DB_OUT_OF_MEMORY or the error returned by apply */
dberr_t wal_open(const char *dir, size_t group_max_bytes, wal_apply_t apply,
                 void *ctx, wal_t **wal, wal_recv_t &recv,
                 uint64_t expected_id = 0);

/** Close a WAL. No append may be in progress.
@param[in]  wal  WAL from wal_create() or wal_open(); may be NULL
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t wal_close(wal_t *wal);

/** Give a record its sequence number and queue it, without waiting. Records
are written in the order they are queued. Every successful call must be
followed by wal_append_wait() on the same writer; until then, records queued
later may not be written.
@param[in]   wal      WAL
@param[in]   payload  record payload; must stay valid until wal_append_wait()
                      returns
@param[in]   len      payload length, 1 to WAL_MAX_PAYLOAD
@param[out]  w        caller-owned queue entry
@param[out]  seq      sequence number of the record, 0 on failure
@retval DB_SUCCESS queued
@retval DB_INVALID_ARGUMENT rejected; nothing queued
@retval DB_FENCED rejected after an earlier failure; nothing queued */
dberr_t wal_append_enqueue(wal_t &wal, const void *payload, size_t len,
                           wal_writer_t &w, batch_seq_t &seq);

/** Wait until a queued record is durable, writing a group as leader if this
writer reaches the head of the queue.
@param[in]      wal  WAL
@param[in,out]  w    entry from wal_append_enqueue()
@retval DB_SUCCESS durable
@retval DB_FENCED not written; an earlier record failed
@retval DB_IO_ERROR outcome unknown; the WAL is now fenced
@retval DB_OUT_OF_MEMORY not written; the WAL is now fenced */
dberr_t wal_append_wait(wal_t &wal, wal_writer_t &w);

/** Set the size at which the next group starts a new segment.
@param[in]  wal    WAL
@param[in]  bytes  segment size */
void wal_set_segment_bytes(wal_t &wal, uint64_t bytes);

/** Delete the closed segments that hold only stored records. Thread-safe;
appends may continue meanwhile.
@param[in]   wal          WAL
@param[in]   flushed_seq  every record up to here is flushed to storage
@param[out]  removed      segments deleted
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t wal_recycle(wal_t &wal, batch_seq_t flushed_seq, uint64_t &removed);

/** Read the counters.
@param[in]   wal    WAL
@param[out]  stats  counters */
void wal_get_stats(wal_t &wal, wal_stats_t &stats);

#ifdef PT_DEBUG
/** Make the next group flush fail with DB_IO_ERROR after its data has been
written. Debug builds only; used by tests.
@param[in]  wal  WAL */
void wal_dbg_fail_next_flush(wal_t &wal);
#endif

#endif
