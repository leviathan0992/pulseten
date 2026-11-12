/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal.h
 *    Write-ahead log I/O over segment files.
 *
 * The database chooses ordered record groups and owns their completion.
 * This layer appends their bytes, optionally syncs, rotates and recovers files.
 * The caller stops using a handle after an append error and reopens it.
 *
 * The on-disk format is described in wal_format.h.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal_h
#define wal_h

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

#include "os_file.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_noncopyable.h"
#include "wal_format.h"
#include "wal_recv.h"

/** Default size at which a new segment is started. */
#define WAL_DEFAULT_SEGMENT_BYTES (64 * 1024 * 1024)

/** A borrowed record whose sequence is assigned by the caller. */
struct wal_record_t {
  /** Sequence number in the database's ordered write stream. */
  batch_seq_t m_seq{0};

  /** Payload, alive until wal::append() returns. */
  const unsigned char *m_payload{nullptr};

  /** Payload bytes, between 1 and WAL_MAX_PAYLOAD. */
  size_t m_len{0};
};

/** Per-handle write counters and current segment count. m_records, m_groups
and m_bytes start at zero after wal::create() or wal::open(); recovered
records are reported through wal_recv_t instead of adding to these write
counters. */
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

/** Scan WAL segments in sequence order and remove unfinished .init files.
@param[in]   dir     existing WAL directory
@param[out]  firsts  replaced with sorted segment start sequences; empty if
                      no segments exist; discard on failure
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal_scan_segments(const std::string &dir,
                              std::vector<batch_seq_t> &firsts);

class wal_file_guard;

/** Owns WAL files and their I/O; writer coordination belongs to db. */
class wal final : private ut_noncopyable {
 public:
  /** Construct an empty handle. */
  explicit wal(uint64_t segment_bytes = WAL_DEFAULT_SEGMENT_BYTES)
      : m_segment_bytes(segment_bytes) {}

  /** Close every owned resource. */
  ~wal();

  /** Create an empty WAL with a durable header.
  @param[in] dir non-NULL existing directory with no WAL
  @param[out] out new handle, or NULL on failure
  @param[in] wal_id identity; 0 generates one
  @param[in] sync sync every appended group before returning
  @param[in] segment_bytes rotation threshold for this open
  @return DB_SUCCESS, DB_IO_ERROR or DB_OUT_OF_MEMORY */
  static db_status_t create(const char *dir, wal **out, uint64_t wal_id = 0,
                            bool sync = true,
                            uint64_t segment_bytes = WAL_DEFAULT_SEGMENT_BYTES);

  /** Recover records, truncate only an unfinished last tail, and open for
  append. Corrupt published segments remain unchanged.
  @param[in] dir non-NULL existing WAL directory
  @param[in] apply callback for each recovered record; may be NULL
  @param[out] out new handle, or NULL on failure
  @param[out] recv recovered boundaries and identity
  @param[in] expected_id expected identity; 0 disables the identity check
  @param[in] sync sync every appended group
  @param[in] segment_bytes rotation threshold for this open
  @return DB_SUCCESS, an I/O/corruption/allocation error, or callback failure */
  static db_status_t open(const char *dir, wal_apply_t apply, void *ctx,
                          wal **out, wal_recv_t &recv, uint64_t expected_id = 0,
                          bool sync = true,
                          uint64_t segment_bytes = WAL_DEFAULT_SEGMENT_BYTES);

  /** Append one complete group with consecutive, caller-assigned sequences.
  Only one caller appends at a time; sync and purge may run concurrently.
  An attempted data write or sync that fails returns DB_OUTCOME_UNKNOWN.
  Other failures occur before this group is written. The caller owns recovery
  policy, including failures after reserving a sequence or rotating a segment.
  No queue or permanent failure state is retained here.
  @return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_OUT_OF_MEMORY, DB_IO_ERROR
    before writing the group, or DB_OUTCOME_UNKNOWN */
  db_status_t append(const wal_record_t *records, size_t count);

  /** Sync earlier appended groups, excluding append and rotation. */
  db_status_t sync();

  /** Remove closed segments whose records are all stored. */
  db_status_t purge(batch_seq_t flushed_seq, uint64_t &removed);

  /** Copy per-open I/O counters and segment count. */
  void read_stats(wal_stats_t &stats);

  /** Close and delete a handle; no operation may still be running. */
  static db_status_t destroy(wal *log);

 private:
#ifdef PT_TESTING
  /** Failure injection is implemented in the separate test support library. */
  friend class wal_test_access;
#endif /* PT_TESTING */

  /** Release owned resources after successful or partial initialization. */
  db_status_t close();

  /** Validate payloads and consecutive sequences, and calculate encoded size.
   */
  static db_status_t check_records(const wal_record_t *records, size_t count,
                                   size_t &bytes);

  /** Encode a validated group; bytes is the size computed by check_records().
  The I/O mutex must be held. */
  db_status_t encode_group(const wal_record_t *records, size_t count,
                           size_t bytes);

  /** Sync and close the previous segment, then durably create the next one. */
  db_status_t rotate(batch_seq_t first_seq);

  /** Durably publish a new segment before returning its descriptor. */
  static db_status_t create_segment(const std::string &dir, uint64_t wal_id,
                                    batch_seq_t first_seq, os_file_t &file);

  /** Validate a recovered segment's identity before replay. */
  static db_status_t check_identity(os_file_t file, uint64_t expected_id);

  /** Replay ordered segments and retain the newest descriptor. */
  static db_status_t recover_segments(const std::string &dir,
                                      const std::vector<batch_seq_t> &firsts,
                                      wal_apply_t apply, void *ctx,
                                      os_file_t &file, wal_recv_t &recv,
                                      uint64_t expected_id);

  /** Allocate a handle and transfer the descriptor only on success. */
  static db_status_t allocate(const std::string &dir, wal_file_guard &file,
                              wal **out, uint64_t segment_bytes);

  /** WAL directory, fixed before publication. */
  std::string m_dir;

  /** Database identity written in segment headers. */
  uint64_t m_wal_id{0};

  /** Every append must sync before return. */
  bool m_sync{true};

  /** Rotation threshold, fixed for this open. */
  const uint64_t m_segment_bytes;

  /** Serializes append and sync, including I/O; never acquired under
   * m_state_mutex.
   */
  ut_mutex_t m_io_mutex;

  /** Active descriptor, protected by m_io_mutex. */
  os_file_t m_file{OS_FILE_CLOSED};

  /** Active segment byte offset, protected by m_io_mutex. */
  uint64_t m_offset{WAL_FILE_HEADER_SIZE};

  /** Reusable encoding buffer, protected by m_io_mutex. */
  unsigned char *m_buf{nullptr};

  /** Allocated encoding buffer bytes. */
  size_t m_buf_size{0};

  /** Serializes purging; acquired before m_state_mutex, never by appenders. */
  std::mutex m_purge_mutex;

  /** Protects segment metadata, counters and test injection; no I/O under it.
   */
  ut_mutex_t m_state_mutex;

  /** Closed segments, oldest first. */
  std::deque<batch_seq_t> m_closed;

  /** Current segment's first sequence. */
  batch_seq_t m_current_first{1};

  /** Expected first sequence for the next successful append. */
  batch_seq_t m_next_seq{1};

#ifdef PT_TESTING
  /** Fail the next requested sync after writing its group. */
  bool m_test_fail_next_sync{false};

  /** Write only a prefix of the next group, then report an I/O error. */
  bool m_test_partial_write{false};
#endif /* PT_TESTING */

  /** Counters for successful group I/O. */
  wal_stats_t m_stats;
};

#endif /* wal_h */
