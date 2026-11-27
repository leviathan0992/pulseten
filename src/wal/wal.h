/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal.h
 *    Write-ahead log I/O over files.
 *
 * The database chooses ordered record groups and owns their completion.
 * This layer appends their bytes, optionally syncs, rotates and recovers files.
 * The caller stops using a handle after an append error and reopens it.
 *
 *-------------------------------------------------------------------------
 */

#ifndef wal_h
#define wal_h

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "os_file.h"
#include "ut.h"
#include "ut_debug.h"
#include "wal_recovery.h"

/* ========================== On-disk format ========================== */

/* The WAL is a run of files in one directory, named
 * wal-<first sequence number, 20 digits>.pt. Each file starts with a
 * file header followed by records; the records of consecutive files
 * continue the sequence numbers without gaps. All integers are
 * little-endian.
 *
 *   File header (40 bytes)
 *     0  "PTWAL001"
 *     8  u32 format version (1)
 *    12  u32 header size (40)
 *    16  u64 WAL id, the same for every file of one WAL
 *    24  u64 sequence number of the file's first record
 *    32  u32 reserved (0)
 *    36  u32 CRC-32C of bytes [0, 36)
 *
 *   Record (24-byte header followed by the payload)
 *     0  u32 magic "PTRC"
 *     4  u32 payload length
 *     8  u64 batch sequence number, 1, 2, 3, ... without gaps
 *    16  u32 CRC-32C of bytes [0, 16) followed by the payload
 *    20  u32 CRC-32C of header bytes [0, 20), including the data CRC
 *
 * The separate header CRC is checked before trusting sequence and payload
 * length. Recovery stops at the first invalid record in the newest file.
 *
 * A group is the records written by one append, followed by an optional sync;
 * group commit packs many writes into one group. With sync enabled,
 * completed groups are durable. Unsynced groups may be lost or corrupted on a
 * system crash or power loss. A group never spans two files.
 */

/** First 8 bytes of every file, without a terminating NUL. */
#define WAL_FILE_MAGIC "PTWAL001"

/** Current file format; only this value is accepted. */
#define WAL_FORMAT_VERSION 1

/** Bytes of a file header. */
#define WAL_FILE_HEADER_BYTES 40

/** Bytes of a record header. */
#define WAL_RECORD_HEADER_BYTES 24

/** "PTRC" read as a little-endian u32. */
#define WAL_RECORD_MAGIC 0x43525450

/** Largest record, header included. */
#define WAL_MAX_RECORD_BYTES (8 * 1024 * 1024)

/** Largest record payload. */
#define WAL_MAX_PAYLOAD_BYTES (WAL_MAX_RECORD_BYTES - WAL_RECORD_HEADER_BYTES)

/** Build a WAL file name from its first sequence number. */
std::string wal_file_name(batch_seq_t first_seq);

/** Parse a WAL file name and return its first record sequence.
@return true if name matches the WAL file naming rules */
bool wal_parse_file_name(const char *name, batch_seq_t &first_seq);

/** Recognize an unfinished WAL file name ending in .init. */
bool wal_parse_incomplete_file_name(const std::string &name,
                                    batch_seq_t &first_seq);

/** Fill a file header. */
void wal_encode_file_header(unsigned char *b, uint64_t id,
                            batch_seq_t first_seq);

/** Decode a file header.
@return true if b holds a valid file header of this format version */
bool wal_decode_file_header(const unsigned char *b, uint64_t &id,
                            batch_seq_t &first_seq);

/** Encode one record. */
void wal_encode_record(unsigned char *b, batch_seq_t seq,
                       const unsigned char *payload, size_t len);

/** Decode a validated record header without accessing its payload.
@param[in] b at least WAL_RECORD_HEADER_BYTES bytes
@return true if magic, CRC, length and sequence are valid */
bool wal_decode_record_header(const unsigned char *b, batch_seq_t &seq,
                              size_t &len);

/** Validate the data CRC of a complete record with a checked header. */
bool wal_record_crc_valid(const unsigned char *b, size_t len);

/* ============================ WAL files ============================ */

/** Default size at which a new file is started. */
#define WAL_DEFAULT_FILE_SIZE_BYTES (64 * 1024 * 1024)

/** One write to append to the WAL. */
struct wal_record_t {
  /** Record bytes; the caller keeps them valid until append() returns. */
  const unsigned char *m_payload{nullptr};

  /** Payload bytes, between 1 and WAL_MAX_PAYLOAD_BYTES. */
  size_t m_payload_bytes{0};
};

/** List published WAL files in sequence order.
@param[in]   dir              existing WAL directory
@param[out]  file_first_seqs  sorted first record sequences; empty if no files
                             exist; discard on failure
@param[in]   cleanup          remove unpublished .init files while scanning;
                             requires exclusive directory use
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal_scan_files(const std::string &dir,
                           std::vector<batch_seq_t> &file_first_seqs,
                           bool cleanup = false);

/** Owns WAL files and their I/O; writer coordination belongs to db. */
class wal final : private ut_noncopyable {
 public:
  /** Current file count, plus per-handle write counters in test builds. Write
  counters start at zero after wal::create() or wal::open(); recovered records
  are reported through wal_recovery_t. */
  struct stats_t {
#ifdef PT_TESTING
    /** Records successfully written. */
    uint64_t m_records{0};

    /** Successful group writes; each is one write and an optional sync. */
    uint64_t m_groups{0};
#endif

    /** Current number of files, the active file included. */
    uint64_t m_file_count{0};
  };

  /** Construct an empty handle. */
  explicit wal(uint64_t file_size_bytes = WAL_DEFAULT_FILE_SIZE_BYTES)
      : m_wal_file_size_bytes(file_size_bytes) {}

  /** Close every owned resource. */
  ~wal();

  /** Create a WAL and durably publish its first file.
  @param[in] dir non-NULL existing directory with no WAL
  @param[out] out new handle, or NULL on failure
  @param[in] wal_id identity; 0 generates one
  @param[in] sync sync every appended group before returning
  @param[in] file_size_bytes rotation threshold for this open
  @return DB_SUCCESS, DB_INVALID_ARGUMENT if files already exist,
    DB_IO_ERROR or DB_OUT_OF_MEMORY */
  static db_status_t create(
      const char *dir, wal **out, uint64_t wal_id = 0, bool sync = true,
      uint64_t file_size_bytes = WAL_DEFAULT_FILE_SIZE_BYTES);

  /** Recover the valid prefix and discard the newest file's invalid suffix.
  Invalid file headers and damage in earlier files fail recovery.
  Validate required_seq before truncating; failures during replay leave files
  unchanged.
  @param[in] dir non-NULL existing WAL directory
  @param[in] apply callback for each recovered record; may be NULL
  @param[out] out new handle, or NULL on failure
  @param[out] recovery recovered boundaries and identity; valid on success
  @param[in] expected_id expected identity; 0 disables the identity check
  @param[in] sync sync every appended group
  @param[in] file_size_bytes rotation threshold for this open
  @param[in] required_seq minimum recovered sequence, such as the stored F
  @return DB_SUCCESS, an I/O/corruption/allocation error, or callback failure */
  static db_status_t open(
      const char *dir, wal_replay_callback_t apply, void *ctx, wal **out,
      wal_recovery_t &recovery, uint64_t expected_id = 0, bool sync = true,
      uint64_t file_size_bytes = WAL_DEFAULT_FILE_SIZE_BYTES,
      batch_seq_t required_seq = 0);

  /** Append one complete group with consecutive, caller-assigned sequences.
  Only one caller appends at a time; sync and purge may run concurrently.
  first_seq is the caller-assigned sequence of the first record; each record
  thereafter takes the next sequence.
  An attempted data write or sync that fails returns DB_OUTCOME_UNKNOWN.
  Other failures occur before this group is written. The caller owns recovery
  policy, including failures after reserving a sequence or rotating a file.
  No queue or permanent failure state is retained here.
  @return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_OUT_OF_MEMORY, DB_IO_ERROR
    before writing the group, or DB_OUTCOME_UNKNOWN */
  db_status_t append(batch_seq_t first_seq, const wal_record_t *records,
                     size_t count);

  /** Sync earlier appended groups, excluding append and rotation. */
  db_status_t sync();

  /** Remove closed files whose records are all stored, after syncing the
  active file. REQUIRES: no other purge runs concurrently. */
  db_status_t purge(batch_seq_t flushed_seq);

  /** Copy the file count and, in test builds, the write counters. */
  void stats(stats_t &stats);

  /** Close and delete a handle; no operation may still be running. */
  static db_status_t destroy(wal *log);

  /** Recover into an allocated handle; the caller deletes it on failure.
  A supplied stored_seq also checks the retained prefix and allows header-only
  checks through that sequence. For a database with no stored writes, create
  the first file if none exists. required_seq is the minimum recovered seq. */
  db_status_t open_low(const char *dir, wal_replay_callback_t apply, void *ctx,
                       wal_recovery_t &recovery, uint64_t expected_id,
                       bool sync, batch_seq_t required_seq,
                       std::optional<batch_seq_t> stored_seq = std::nullopt);

 private:
  /** Resolve the directory and list published files while cleaning .init files.
   */
  db_status_t scan_directory(const char *dir,
                             std::vector<batch_seq_t> &file_first_seqs);

  /** Create the WAL in an existing directory without published files. */
  db_status_t create_low(const char *dir, uint64_t wal_id, bool sync);

  /** Publish the first file after scanning an empty WAL directory. */
  db_status_t create_first_file(uint64_t wal_id, bool sync);

  /** Give an initialized handle to the caller, or delete it after a failure.
  @param[out] out log on success; unchanged on failure */
  static db_status_t finish_init(wal *log, const db_status_t &status,
                                 wal **out);

  /** Release owned resources after successful or partial initialization. */
  db_status_t close();

  /** Validate payloads and the sequence range; compute encoded size. */
  static db_status_t validate_records(batch_seq_t first_seq,
                                      const wal_record_t *records, size_t count,
                                      size_t &bytes);

  /** Encode a validated group; bytes is the size computed by
  validate_records(). The I/O mutex must be held. */
  db_status_t encode_group(batch_seq_t first_seq, const wal_record_t *records,
                           size_t count, size_t bytes);

  /** Sync the active file and durably create its replacement before closing
  the old descriptor. */
  db_status_t rotate(batch_seq_t first_seq);

  /** Durably publish a new WAL file before returning its descriptor. */
  db_status_t create_file(batch_seq_t first_seq, os_file_t &file);

  /** Replay ordered files and retain the newest descriptor. */
  db_status_t recover_files(const std::vector<batch_seq_t> &file_first_seqs,
                            wal_replay_callback_t apply, void *ctx,
                            wal_recovery_t &recovery, uint64_t expected_id,
                            batch_seq_t required_seq, batch_seq_t stored_seq);

  /* ======================= Immutable settings ======================= */

  /** WAL directory, fixed before publication. */
  std::string m_dir;

  /** WAL identity shared by every file header. */
  uint64_t m_wal_id{0};

  /** Every append must sync before return. */
  bool m_sync{true};

  /** Configured WAL file size for rotation; a single group may exceed it. */
  const uint64_t m_wal_file_size_bytes;

  /* ========================= Active WAL I/O ========================= */

  /** Protects this group and serializes append, rotation and sync.
  Held during I/O; acquired before m_state_mutex. */
  ut_mutex_t m_io_mutex;

  /** Active file descriptor; closed during teardown. */
  os_file_t m_file{OS_FILE_CLOSED};

  /** Next byte offset in the active file. */
  uint64_t m_write_offset{WAL_FILE_HEADER_BYTES};

  /** Owned encoding buffer, reused between appends. */
  unsigned char *m_buffer{nullptr};

  /** Allocated encoding buffer bytes. */
  size_t m_buffer_capacity_bytes{0};

  /* ================= File metadata and counters ================== */

  /** Protects this group; sequence fields also permit I/O-lock reads as noted.
  Never held during I/O. */
  ut_mutex_t m_state_mutex;

  /** First sequences of closed WAL files, oldest first. */
  std::deque<batch_seq_t> m_closed_files;

  /** First sequence in the active file, or zero before initialization.
  After open, read under either mutex; update while holding both. */
  batch_seq_t m_file_first_seq{0};

  /** Expected first sequence of the next append. After open, read under either
  m_io_mutex or m_state_mutex; update while holding both. */
  batch_seq_t m_next_seq{1};

#ifdef PT_TESTING
  /** Records and groups written successfully by this handle. */
  uint64_t m_records_written{0};
  uint64_t m_groups_written{0};
#endif
};

#endif /* wal_h */
