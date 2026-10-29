/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal0recv.h
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
 * was inside previously durable data. Earlier segments must be complete.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal0recv_h
#define wal0recv_h

#include <cstddef>
#include <cstdint>

#include "os0file.h"
#include "ut0err.h"
#include "ut0ut.h"

/** Called once per recovered record, in sequence order.
@param[in]  ctx      caller context
@param[in]  seq      batch sequence number
@param[in]  payload  record payload, valid only during the call
@param[in]  len      payload length
@return DB_SUCCESS to continue; any other code stops recovery with it */
typedef dberr_t (*wal_apply_t)(void *ctx, batch_seq_t seq,
                               const unsigned char *payload, size_t len);

/** Result of recovering a whole WAL, or one segment of it. */
struct wal_recv_t {
  /** WAL id from the file headers. */
  uint64_t wal_id{0};

  /** Sequence number of the first record the WAL still has; records
  before it were in segments that were removed. */
  batch_seq_t first_seq{0};

  /** Records found and applied. */
  uint64_t records{0};

  /** Sequence number of the last valid record; first_seq - 1 if there is
  none. */
  batch_seq_t last_seq{0};

  /** End of the last valid record in the last segment; new records are
  appended here. */
  uint64_t end_offset{0};

  /** Bytes after end_offset belonging to an unfinished last group. */
  uint64_t tail_bytes{0};

  /** Segment files found. */
  uint64_t segments{0};
};

/** Scan one segment, call apply for each valid record and locate the end of
the valid log. The file is not modified. Records are applied while the scan
proceeds, so after a failure the caller must discard what it applied.
@param[in]   file       open segment file
@param[in]   first_seq  sequence number the segment must start with
@param[in]   is_last    true for the newest segment, the only one that may end
                        in an unfinished group
@param[in]   apply      callback per record, or NULL to only validate
@param[in]   ctx        passed to apply
@param[out]  recv       scan result for this segment, valid on DB_SUCCESS
@return DB_SUCCESS, DB_CORRUPTION, DB_IO_ERROR, DB_OUT_OF_MEMORY or the error
  returned by apply */
dberr_t wal_recv_scan(os_file_t file, batch_seq_t first_seq, bool is_last,
                      wal_apply_t apply, void *ctx, wal_recv_t &recv);

#endif
