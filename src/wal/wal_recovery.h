/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_recovery.h
 *    WAL recovery: replay the records of a file and find where the
 *    valid log ends.
 *
 * Records are read in order. The newest file stops at its first invalid
 * or incomplete record; that record and every later byte are discarded.
 * Earlier files and every file header must be valid. I/O, allocation
 * and replay callback failures stop recovery without repairing files.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal_recovery_h
#define wal_recovery_h

#include <cstddef>
#include <cstdint>

#include "os_file.h"
#include "ut.h"

/** Called once per recovered record that needs replay, in sequence order.
@param[in]  ctx      caller context
@param[in]  payload  record payload, valid only during the call
@return DB_SUCCESS to continue; any other code stops recovery with it */
typedef db_status_t (*wal_replay_callback_t)(void *ctx, batch_seq_t seq,
                                             const unsigned char *payload,
                                             size_t len);

/** Result of recovering a whole WAL, or one file of it. */
struct wal_recovery_t {
  /** WAL id from the file headers. */
  uint64_t m_wal_id{0};

  /** Sequence number of the first record the WAL still has; records
  before it were in files that were removed. */
  batch_seq_t m_first_seq{0};

  /** Valid records recovered. */
  uint64_t m_records{0};

  /** Sequence number of the last valid record; m_first_seq - 1 if there is
  none. */
  batch_seq_t m_last_seq{0};

  /** End of the last valid record in the last file; new records are
  appended here. */
  uint64_t m_end_offset{0};

  /** Bytes to discard after m_end_offset in the newest file. */
  uint64_t m_tail_bytes{0};

  /** Files found. */
  uint64_t m_file_count{0};
};

/** Read a WAL file without changing or closing the file.
Records already applied must be discarded by the caller if replay fails. */
class wal_file_reader final : private ut_noncopyable {
 public:
  /** Read from the given descriptor; the caller keeps it open. */
  explicit wal_file_reader(os_file_t file) : m_file(file) {}

  /** Release the reusable record buffer. */
  ~wal_file_reader();

  /** Replay the valid prefix; only is_last may have an invalid suffix.
  Records through stored_seq are already durable in the store, so check only
  their framing and skip the replay callback for them. */
  db_status_t replay(batch_seq_t first_seq, bool is_last,
                     wal_replay_callback_t apply, void *ctx,
                     wal_recovery_t &recovery, batch_seq_t stored_seq = 0);

 private:
  /** Grow the record buffer to at least size bytes. */
  db_status_t reserve_buffer(size_t size);

  /** Read a record or just its header if already stored. */
  db_status_t read_record(uint64_t at, uint64_t avail, batch_seq_t expected,
                          bool header_only, bool &valid, batch_seq_t &seq,
                          size_t &len);

  /** File descriptor; opened and closed by the caller. */
  os_file_t m_file{OS_FILE_CLOSED};

  /** Reusable record allocation. */
  unsigned char *m_buffer{nullptr};

  /** Bytes allocated in m_buffer. */
  size_t m_buffer_capacity_bytes{0};
};

#endif /* wal_recovery_h */
