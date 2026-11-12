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
#include "ut_noncopyable.h"

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

/** Scan a borrowed WAL segment, owning only the record buffer.
Records already applied must be discarded by the caller if replay fails. */
class wal_segment_reader final : private ut_noncopyable {
 public:
  /** Borrow a segment descriptor; never close or modify the file. */
  explicit wal_segment_reader(os_file_t file) : m_file(file) {}

  /** Release the reusable record buffer. */
  ~wal_segment_reader();

  /** Validate and replay one segment; only is_last may have an incomplete tail.
   */
  db_status_t replay(batch_seq_t first_seq, bool is_last, wal_apply_t apply,
                     void *ctx, wal_recv_t &recv);

 private:
  /** Grow the record buffer to at least size bytes. */
  db_status_t reserve_buffer(size_t size);

  /** Reject damage followed by a later completed group. */
  db_status_t check_tail(uint64_t start, uint64_t size, batch_seq_t expected);

  /** Read and validate a complete record, or identify an incomplete tail. */
  db_status_t read_record(uint64_t at, uint64_t avail, batch_seq_t expected,
                          bool &valid, batch_seq_t &seq, size_t &len);

  /** Borrowed descriptor retained only for the reader's lifetime. */
  os_file_t m_file{OS_FILE_CLOSED};

  /** Reusable record allocation. */
  unsigned char *m_buf{nullptr};

  /** Bytes allocated in m_buf. */
  size_t m_buf_size{0};
};

#endif /* wal_recv_h */
