/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_recv.h
 *    WAL recovery: replay the records of a segment and find where the
 *    valid log ends.
 *
 * Records are read in order. Only a physically incomplete final header,
 * or a header with valid independent CRC whose payload is incomplete,
 * may be discarded in the newest segment. Complete bad headers and bad
 * record CRCs are corruption, even without a later group: the damaged
 * record may already have been acknowledged. Recovery never truncates on
 * a corruption result.
 *
 * Later valid groups can still prove that an apparent incomplete tail
 * was inside a completed group. Earlier segments must be complete.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal_recv_h
#define wal_recv_h

#include <cstddef>
#include <cstdint>

#include "os_file.h"
#include "ut.h"
#include "ut_err.h"

/** Called once per recovered record, in sequence order.
@param[in]  ctx      caller context
@param[in]  payload  record payload, valid only during the call
@return DB_SUCCESS to continue; any other code stops recovery with it */
typedef db_status_t (*wal_apply_t)(void *ctx, batch_seq_t seq,
                                   const unsigned char *payload, size_t len);

/** Result of recovering a whole WAL, or one segment of it. */
struct wal_recv_t {
  /** WAL id from the file headers. */
  uint64_t m_wal_id{0};

  /** Sequence number of the first record the WAL still has; records
  before it were in segments that were removed. */
  batch_seq_t m_first_seq{0};

  /** Records found and applied. */
  uint64_t m_records{0};

  /** Sequence number of the last valid record; m_first_seq - 1 if there is
  none. */
  batch_seq_t m_last_seq{0};

  /** End of the last valid record in the last segment; new records are
  appended here. */
  uint64_t m_end_offset{0};

  /** Bytes after m_end_offset belonging to an unfinished last group. */
  uint64_t m_tail_bytes{0};

  /** Segment files found. */
  uint64_t m_segments{0};
};

/** Scan one segment, call apply for each valid record and locate the end of
the valid log. The file is not modified. Records are applied while the scan
proceeds, so after a failure the caller must discard what it applied.
@param[in]   first_seq  sequence number the segment must start with
@param[in]   is_last    true for the newest segment, the only one that may end
                        in an unfinished group
@param[in]   apply      callback per record, or NULL to only validate
@param[out]  recv       scan result for this segment, valid on DB_SUCCESS
@return DB_SUCCESS, DB_CORRUPTION, DB_IO_ERROR, DB_OUT_OF_MEMORY or the error
  returned by apply */
db_status_t wal_recv_replay_segment(os_file_t file, batch_seq_t first_seq,
                                    bool is_last, wal_apply_t apply, void *ctx,
                                    wal_recv_t &recv);

#endif
