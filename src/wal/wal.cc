/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal.cc
 *    Write-ahead log I/O, file rotation and recovery.
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "os_file.h"
#include "ut.h"
#include "wal_recovery.h"

/* ========================== On-disk format ========================== */

/** Build a file name from its first sequence number. */
std::string wal_file_name(batch_seq_t first_seq) {
  char buf[64];
  snprintf(buf, sizeof(buf), "wal-%020llu.pt",
           static_cast<unsigned long long>(first_seq));

  return (buf);
}

/** Parse a file name and return its first sequence number. */
bool wal_parse_file_name(const char *name, batch_seq_t &first_seq) {
  /* "wal-" + 20 digits + ".pt" */
  if (strlen(name) != 27 || strncmp(name, "wal-", 4) != 0 ||
      strcmp(name + 24, ".pt") != 0) {
    return (false);
  }

  batch_seq_t v = 0;

  for (int i = 4; i < 24; ++i) {
    if (name[i] < '0' || name[i] > '9') {
      return (false);
    }

    const batch_seq_t digit = static_cast<batch_seq_t>(name[i] - '0');

    if (v > (UINT64_MAX - digit) / 10) {
      return (false);
    }

    v = v * 10 + digit;
  }

  first_seq = v;

  return (v >= 1);
}

/** Remove the initialization suffix before applying the file name rules. */
bool wal_parse_incomplete_file_name(const std::string &name,
                                    batch_seq_t &first_seq) {
  static const std::string suffix = ".init";
  if (name.size() <= suffix.size()) {
    return (false);
  }

  const size_t end = name.size() - suffix.size();
  if (name.compare(end, suffix.size(), suffix) != 0) {
    return (false);
  }

  return (wal_parse_file_name(name.substr(0, end).c_str(), first_seq));
}

/** Encode the WAL identity and first record sequence into a file header. */
void wal_encode_file_header(unsigned char *b, uint64_t id,
                            batch_seq_t first_seq) {
  memcpy(b, "PTWAL001", 8);

  ut_write_u32(b + 8, WAL_FORMAT_VERSION);
  ut_write_u32(b + 12, WAL_FILE_HEADER_BYTES);
  ut_write_u64(b + 16, id);
  ut_write_u64(b + 24, first_seq);
  ut_write_u32(b + 32, 0);
  ut_write_u32(b + 36, ut_crc32c(b, 36, 0));

  return;
}

/** Validate and decode a WAL file header. */
bool wal_decode_file_header(const unsigned char *b, uint64_t &id,
                            batch_seq_t &first_seq) {
  if (memcmp(b, "PTWAL001", 8) != 0) {
    return (false);
  }

  if (ut_read_u32(b + 8) != WAL_FORMAT_VERSION ||
      ut_read_u32(b + 12) != WAL_FILE_HEADER_BYTES ||
      ut_read_u32(b + 32) != 0) {
    return (false);
  }

  if (ut_read_u32(b + 36) != ut_crc32c(b, 36, 0)) {
    return (false);
  }

  id = ut_read_u64(b + 16);
  first_seq = ut_read_u64(b + 24);

  return (first_seq >= 1);
}

/** Compute the CRC-32C of the record header prefix and payload.
@return CRC of a record: header bytes [0, 20) followed by the payload */
static uint32_t wal_record_crc(const unsigned char *b, size_t len) {
  const uint32_t crc = ut_crc32c(b, 20, 0);

  return (ut_crc32c(b + WAL_RECORD_HEADER_BYTES, len, crc));
}

/** Encode a record header, payload and checksum. */
void wal_encode_record(unsigned char *b, batch_seq_t seq, uint32_t group_index,
                       const unsigned char *payload, size_t len) {
  ut_write_u32(b, WAL_RECORD_MAGIC);
  ut_write_u32(b + 4, static_cast<uint32_t>(len));
  ut_write_u64(b + 8, seq);
  ut_write_u32(b + 16, group_index);
  memcpy(b + WAL_RECORD_HEADER_BYTES, payload, len);

  ut_write_u32(b + 20, wal_record_crc(b, len));
  ut_write_u32(b + 24, ut_crc32c(b, 24, 0));

  return;
}

/** Validate a record header and read its declared payload length. */
bool wal_decode_record_header(const unsigned char *b, batch_seq_t &seq,
                              uint32_t &group_index, size_t &len) {
  if (ut_read_u32(b) != WAL_RECORD_MAGIC) {
    return (false);
  }

  if (ut_read_u32(b + 24) != ut_crc32c(b, 24, 0)) {
    return (false);
  }

  const uint32_t n = ut_read_u32(b + 4);

  if (n == 0 || n > WAL_MAX_PAYLOAD_BYTES) {
    return (false);
  }

  const batch_seq_t header_seq = ut_read_u64(b + 8);
  const uint32_t index = ut_read_u32(b + 16);
  if (header_seq == 0 || index >= header_seq) {
    return (false);
  }

  seq = header_seq;
  group_index = index;
  len = n;

  return (true);
}

/** Validate a complete record's framing, sequence and checksum. */
bool wal_decode_record(const unsigned char *b, size_t avail, batch_seq_t &seq,
                       uint32_t &group_index, size_t &len) {
  if (avail < WAL_RECORD_HEADER_BYTES) {
    return (false);
  }

  size_t n = 0;
  batch_seq_t s = 0;
  uint32_t index = 0;
  if (!wal_decode_record_header(b, s, index, n)) {
    return (false);
  }

  if (n > avail - WAL_RECORD_HEADER_BYTES) {
    return (false);
  }

  if (ut_read_u32(b + 20) != wal_record_crc(b, n)) {
    return (false);
  }

  seq = s;
  group_index = index;
  len = n;

  return (true);
}

/* ============================ WAL files ============================ */

/** Join the WAL directory and a file name. */
static std::string wal_path(const std::string &dir, const std::string &name) {
  return (dir + "/" + name);
}

/** Create a WAL file whose first record will be first_seq. The file
appears under its final name only after its header is durable.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal::create_file(batch_seq_t first_seq, os_file_t &file) {
  file = OS_FILE_CLOSED;

  const std::string path = wal_path(m_dir, wal_file_name(first_seq));
  const std::string init_path = path + ".init";

  /* A leftover init file is from a create that never finished; it was
  never published under the final name. */
  if (const db_status_t err = os_file_delete_if_exists(init_path.c_str());
      !err.ok()) {
    return (err);
  }

  os_file_t created_file = OS_FILE_CLOSED;
  if (const db_status_t err =
          os_file_open(init_path.c_str(), OS_FILE_CREATE, created_file);
      !err.ok()) {
    return (err);
  }

  unsigned char header[WAL_FILE_HEADER_BYTES];
  wal_encode_file_header(header, m_wal_id, first_seq);

  db_status_t written = UT_DEBUG_FAULT("wal.write_header");
  if (written.ok()) {
    written = os_file_write(created_file, header, sizeof(header), 0);
  }

  if (!written.ok()) {
    static_cast<void>(os_file_close(created_file));

    return (
        written.with_context("cannot write WAL header '" + init_path + "'"));
  }

  if (const db_status_t err = os_file_sync(created_file); !err.ok()) {
    static_cast<void>(os_file_close(created_file));

    return (err.with_context("cannot sync WAL header '" + init_path + "'"));
  }

  if (const db_status_t err =
          os_file_rename_noreplace(init_path.c_str(), path.c_str());
      !err.ok()) {
    static_cast<void>(os_file_close(created_file));

    return (err);
  }

  if (const db_status_t err = os_dir_sync(m_dir.c_str()); !err.ok()) {
    static_cast<void>(os_file_close(created_file));

    return (err);
  }

  file = created_file;

  return (DB_SUCCESS);
}

/** Start a new file for a group that would overflow the current one.
The old file is synced before the new one is published, so only the
newest file can have an unsynced tail. Called only by the group leader.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal::rotate(batch_seq_t first_seq) {
  ut_ad(m_io_mutex.is_owned());
  ut_ad(first_seq == m_next_seq && first_seq > m_file_first_seq);

  os_file_t file;

  /* A reopened file may contain writes from a previous unsynced open. */
  if (const db_status_t sync_err = os_file_sync(m_file); !sync_err.ok()) {
    return (sync_err);
  }

  if (const db_status_t err = create_file(first_seq, file); !err.ok()) {
    return (err);
  }

  /* Everything in the old file is durable; a close error cannot lose
  any of it. */
  static_cast<void>(os_file_close(m_file));

  m_file = file;
  m_write_offset = WAL_FILE_HEADER_BYTES;

  ut_lock_guard_t lock(m_state_mutex);
  m_closed_files.push_back(m_file_first_seq);
  m_file_first_seq = first_seq;
  m_stats.m_file_count++;

  return (DB_SUCCESS);
}

/** Remove unfinished WAL files that were never published. */
db_status_t wal_cleanup_incomplete_files(const std::string &dir) {
  std::vector<std::string> names;
  if (const db_status_t err = os_dir_list(dir.c_str(), names); !err.ok()) {
    return (err);
  }

  for (const std::string &name : names) {
    batch_seq_t first = 0;
    if (!wal_parse_incomplete_file_name(name, first)) {
      continue;
    }

    if (const db_status_t err =
            os_file_delete_if_exists(wal_path(dir, name).c_str());
        !err.ok()) {
      return (err);
    }
  }

  return (DB_SUCCESS);
}

/** List published files in sequence order; the directory is unchanged. */
db_status_t wal_scan_files(const std::string &dir,
                           std::vector<batch_seq_t> &file_first_seqs) {
  file_first_seqs.clear();

  std::vector<std::string> names;
  if (const db_status_t err = os_dir_list(dir.c_str(), names); !err.ok()) {
    return (err);
  }

  for (const std::string &name : names) {
    batch_seq_t first = 0;
    if (wal_parse_file_name(name.c_str(), first)) {
      file_first_seqs.push_back(first);
    }
  }

  std::sort(file_first_seqs.begin(), file_first_seqs.end());

  return (DB_SUCCESS);
}

/** Give an initialized handle to the caller, or delete it after a failure. */
db_status_t wal::finish_init(wal *log, const db_status_t &status, wal **out) {
  if (!status.ok()) {
    ut_delete(log);

    return (status);
  }

  *out = log;

  return (DB_SUCCESS);
}

/** Create a WAL and durably publish its first file in the directory. */
db_status_t wal::create(const char *dir, wal **out, uint64_t wal_id, bool sync,
                        uint64_t file_size_bytes) {
  *out = nullptr;

  wal *log = ut_new<wal>(file_size_bytes);
  if (log == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  return (finish_init(log, log->create_low(dir, wal_id, sync), out));
}

/** Initialize this handle before it is published to callers. */
db_status_t wal::create_low(const char *dir, uint64_t wal_id, bool sync) {
  ut_a(dir != nullptr);

  std::string directory;
  if (const db_status_t err = os_dir_resolve(dir, directory); !err.ok()) {
    return (err);
  }

  m_dir = std::move(directory);
  dir = m_dir.c_str();

  if (const db_status_t err = wal_cleanup_incomplete_files(dir); !err.ok()) {
    return (err);
  }

  std::vector<batch_seq_t> file_first_seqs;
  if (const db_status_t err = wal_scan_files(dir, file_first_seqs); !err.ok()) {
    return (err);
  }

  /* Creating a WAL must not replace any published file. */
  if (!file_first_seqs.empty()) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "cannot create WAL: published files already exist"));
  }

  /* A standalone WAL has no stored identity to reuse. */
  if (wal_id == 0) {
    if (const db_status_t err = os_generate_uid(wal_id); !err.ok()) {
      return (err);
    }
  }

  m_wal_id = wal_id;

  if (const db_status_t err = create_file(1, m_file); !err.ok()) {
    return (err);
  }

  m_sync = sync;
  m_stats.m_file_count = 1;

  return (DB_SUCCESS);
}

/** Check the database identity before callbacks or tail truncation.
@param[in]  expected_id  nonzero expected WAL identity; the caller skips this
                        check when no identity is required
@return DB_SUCCESS, DB_IO_ERROR, or DB_CORRUPTION for a missing/invalid header
  or identity mismatch */
db_status_t wal::validate_identity(uint64_t expected_id) {
  uint64_t size = 0;
  if (const db_status_t err = os_file_size(m_file, size); !err.ok()) {
    return (err);
  }

  if (size < WAL_FILE_HEADER_BYTES) {
    return (DB_CORRUPTION);
  }

  unsigned char header[WAL_FILE_HEADER_BYTES];
  if (const db_status_t err = os_file_read(m_file, header, sizeof(header), 0);
      !err.ok()) {
    return (err);
  }

  uint64_t id = 0;
  batch_seq_t first = 0;
  if (!wal_decode_file_header(header, id, first) || id != expected_id) {
    return (DB_CORRUPTION);
  }

  return (DB_SUCCESS);
}

/** Recover the files in order. On success the newest one is left open in
the current descriptor with its invalid suffix cut off.
@param[in]  file_first_seqs  sorted first record sequences of the WAL files
@param[in]  apply            record replay callback; may be NULL
@param[in]  expected_id      expected WAL identity; 0 skips the check
@return DB_SUCCESS, a file I/O/corruption error, or the replay callback
  error */
db_status_t wal::recover_files(const std::vector<batch_seq_t> &file_first_seqs,
                               wal_replay_callback_t apply, void *ctx,
                               wal_recovery_t &recovery, uint64_t expected_id,
                               batch_seq_t required_seq,
                               batch_seq_t stored_seq) {
  ut_a(!file_first_seqs.empty());

  recovery.m_first_seq = file_first_seqs[0];
  recovery.m_last_seq = file_first_seqs[0] - 1;

  for (size_t i = 0; i < file_first_seqs.size(); ++i) {
    const bool is_last = i + 1 == file_first_seqs.size();

    /* Each file continues where the previous one ended. */
    if (file_first_seqs[i] != recovery.m_last_seq + 1) {
      return (DB_CORRUPTION);
    }

    const std::string path = wal_path(m_dir, wal_file_name(file_first_seqs[i]));

    if (const db_status_t err =
            os_file_open(path.c_str(), OS_FILE_OPEN, m_file);
        !err.ok()) {
      return (err);
    }

    /* Before replay, so that another database's WAL never reaches apply. */
    if (expected_id != 0) {
      if (const db_status_t err = validate_identity(expected_id); !err.ok()) {
        return (err.with_context("cannot recover WAL file '" + path + "'"));
      }
    }

    wal_recovery_t file_recovery;
    if (const db_status_t err = wal_file_reader(m_file).replay(
            file_first_seqs[i], is_last, apply, ctx, file_recovery, stored_seq);
        !err.ok()) {
      return (err.with_context("cannot recover WAL file '" + path + "'"));
    }

    /* Every file must belong to the same WAL. */
    if (i > 0 && file_recovery.m_wal_id != recovery.m_wal_id) {
      return (DB_CORRUPTION);
    }

    /* Only the newest file stays open for appending. */
    if (!is_last) {
      static_cast<void>(os_file_close(m_file));
    }

    recovery.m_wal_id = file_recovery.m_wal_id;
    recovery.m_records += file_recovery.m_records;
    recovery.m_last_seq = file_recovery.m_last_seq;
    recovery.m_end_offset = file_recovery.m_end_offset;
    recovery.m_tail_bytes = file_recovery.m_tail_bytes;
    recovery.m_file_count++;
  }

  /* Each file continued the previous one, without a gap. */
  ut_a(recovery.m_file_count == file_first_seqs.size());
  ut_a(recovery.m_records == recovery.m_last_seq - recovery.m_first_seq + 1);

  /* A shorter prefix would contradict data already committed by the caller. */
  if (recovery.m_last_seq < required_seq) {
    return (db_status_t(DB_CORRUPTION, "WAL ends at sequence " +
                                           std::to_string(recovery.m_last_seq) +
                                           ", before required sequence " +
                                           std::to_string(required_seq)));
  }

  /* Discard the invalid suffix only after all recovery checks succeed. */
  if (recovery.m_tail_bytes > 0) {
    const std::string path =
        wal_path(m_dir, wal_file_name(file_first_seqs.back()));
    db_status_t truncated = UT_DEBUG_FAULT("wal.truncate");
    if (truncated.ok()) {
      truncated = os_file_truncate(m_file, recovery.m_end_offset);
    }

    if (!truncated.ok()) {
      return (truncated.with_context("cannot recover WAL file '" + path + "'"));
    }

    if (const db_status_t err = os_file_sync(m_file); !err.ok()) {
      return (err.with_context("cannot recover WAL file '" + path + "'"));
    }
  }

  return (DB_SUCCESS);
}

/** Open an existing WAL and recover it: replay every valid record through
apply, discard the invalid suffix, and leave the WAL ready to append after
the last recovered record. */
db_status_t wal::open(const char *dir, wal_replay_callback_t apply, void *ctx,
                      wal **out, wal_recovery_t &recovery, uint64_t expected_id,
                      bool sync, uint64_t file_size_bytes,
                      batch_seq_t required_seq) {
  *out = nullptr;

  wal *log = ut_new<wal>(file_size_bytes);
  if (log == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  const db_status_t err =
      log->open_low(dir, apply, ctx, recovery, expected_id, sync, required_seq);

  return (finish_init(log, err, out));
}

/** Recover into this handle; its owner closes any partial initialization. */
db_status_t wal::open_low(const char *dir, wal_replay_callback_t apply,
                          void *ctx, wal_recovery_t &recovery,
                          uint64_t expected_id, bool sync,
                          batch_seq_t required_seq, batch_seq_t stored_seq) {
  ut_a(dir != nullptr);
  ut_a(stored_seq <= required_seq);

  recovery = wal_recovery_t();

  std::string directory;
  if (const db_status_t err = os_dir_resolve(dir, directory); !err.ok()) {
    return (err);
  }

  m_dir = std::move(directory);
  dir = m_dir.c_str();

  if (const db_status_t err = wal_cleanup_incomplete_files(dir); !err.ok()) {
    return (err);
  }

  std::vector<batch_seq_t> file_first_seqs;
  if (const db_status_t err = wal_scan_files(dir, file_first_seqs); !err.ok()) {
    return (err);
  }

  /* An existing WAL has at least one file. */
  if (file_first_seqs.empty()) {
    return (db_status_t(DB_CORRUPTION,
                        "cannot open WAL: no published files found"));
  }

  if (const db_status_t err =
          recover_files(file_first_seqs, apply, ctx, recovery, expected_id,
                        required_seq, stored_seq);
      !err.ok()) {
    return (err);
  }

  ut_a(recovery.m_end_offset >= WAL_FILE_HEADER_BYTES);

  m_wal_id = recovery.m_wal_id;
  m_sync = sync;

  m_write_offset = recovery.m_end_offset;
  m_next_seq = recovery.m_last_seq + 1;
  m_file_first_seq = file_first_seqs.back();
  m_closed_files.assign(file_first_seqs.begin(), file_first_seqs.end() - 1);
  m_stats.m_file_count = file_first_seqs.size();

  return (DB_SUCCESS);
}

/** Close a WAL. */
db_status_t wal::close() {
  const db_status_t err = os_file_close(m_file);

  free(m_buffer);
  m_buffer = nullptr;
  m_buffer_capacity_bytes = 0;

  return (err);
}

/** Close the WAL descriptor and release its encoding buffer. */
wal::~wal() {
  static_cast<void>(close());
}

/** Close and delete a WAL handle, returning any close error. */
db_status_t wal::destroy(wal *log) {
  if (log == nullptr) {
    return (DB_SUCCESS);
  }

  const db_status_t err = log->close();
  ut_delete(log);

  return (err);
}

/** Delete the closed files that hold only stored records. */
db_status_t wal::purge(batch_seq_t flushed_seq, uint64_t &removed) {
  removed = 0;

  /* Only one purger may choose and delete the oldest file. Keep
  appenders free to run while deletion and directory sync are in progress. */
  std::lock_guard<std::mutex> purge_lock(m_purge_mutex);

  bool synced = false;

  for (;;) {
    batch_seq_t first;

    {
      ut_lock_guard_t lock(m_state_mutex);

      /* Storage holds only writes the WAL had. */
      ut_a(flushed_seq < m_next_seq);

      if (m_closed_files.empty()) {
        return (DB_SUCCESS);
      }

      const batch_seq_t next_first =
          m_closed_files.size() > 1 ? m_closed_files[1] : m_file_first_seq;

      /* A closed file holds only stored records exactly when the
      next file starts at or before the first not stored record. */
      if (next_first - 1 > flushed_seq) {
        return (DB_SUCCESS);
      }

      first = m_closed_files.front();
      ut_a(first < next_first);
    }

    /* Storage holds the only copy after deletion. On macOS, DuckDB's fsync
    may leave its commit in the drive's cache; F_FULLFSYNC on any file of the
    drive flushes that cache. */
    if (!synced) {
      if (const db_status_t err = sync(); !err.ok()) {
        return (err);
      }

      synced = true;
    }

    const std::string path = wal_path(m_dir, wal_file_name(first));

    db_status_t deleted = UT_DEBUG_FAULT("wal.purge");
    if (deleted.ok()) {
      /* Delete the file unless the test injected a failure. */
      deleted = os_file_delete_if_exists(path.c_str());
    }

    if (!deleted.ok()) {
      return (deleted);
    }

    /* Make each prefix deletion durable before deleting the next file.
    On failure keep this entry: retrying a missing file is harmless and
    retries its directory sync before advancing to a newer file. */
    if (const db_status_t err = os_dir_sync(m_dir.c_str()); !err.ok()) {
      return (err);
    }

    {
      ut_lock_guard_t lock(m_state_mutex);

      ut_a(!m_closed_files.empty() && m_closed_files.front() == first);

      /* Drop the in-memory record only after deletion is durable. */
      m_closed_files.pop_front();
      m_stats.m_file_count--;
    }

    removed++;
  }
}

/** Validate a complete group and calculate its encoded size without I/O. */
db_status_t wal::validate_records(const wal_record_t *records, size_t count,
                                  size_t &bytes) {
  if (records == nullptr || count == 0 || count > UINT32_MAX) {
    return (DB_INVALID_ARGUMENT);
  }

  /* Keep both the record range and the next sequence representable. */
  if (records[0].m_seq == 0 || count > UINT64_MAX - records[0].m_seq) {
    return (DB_INVALID_ARGUMENT);
  }

  /* Check payloads and sequence order before allocating or changing files. */
  bytes = 0;

  for (size_t i = 0; i < count; ++i) {
    const wal_record_t &record = records[i];
    if (record.m_payload == nullptr || record.m_payload_bytes == 0 ||
        record.m_payload_bytes > WAL_MAX_PAYLOAD_BYTES) {
      return (DB_INVALID_ARGUMENT);
    }

    /* Check the accumulated size before adding this record. */
    if (bytes > SIZE_MAX - WAL_RECORD_HEADER_BYTES ||
        record.m_payload_bytes > SIZE_MAX - WAL_RECORD_HEADER_BYTES - bytes) {
      return (DB_INVALID_ARGUMENT);
    }

    bytes += WAL_RECORD_HEADER_BYTES + record.m_payload_bytes;

    ut_a(record.m_seq == records[0].m_seq + i);
  }

  return (DB_SUCCESS);
}

/** Encode a validated group into the reusable buffer under the I/O mutex. */
db_status_t wal::encode_group(const wal_record_t *records, size_t count,
                              size_t bytes) {
  ut_ad(m_io_mutex.is_owned());

  /* Reuse the group buffer; grow it only when a group needs more space. */
  if (bytes > m_buffer_capacity_bytes) {
    unsigned char *buffer =
        static_cast<unsigned char *>(realloc(m_buffer, bytes));
    if (buffer == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    m_buffer = buffer;
    m_buffer_capacity_bytes = bytes;
  }

  /* Encode the whole group, including each record's index within the group. */
  unsigned char *position = m_buffer;

  for (size_t i = 0; i < count; ++i) {
    const wal_record_t &record = records[i];
    wal_encode_record(position, record.m_seq, static_cast<uint32_t>(i),
                      record.m_payload, record.m_payload_bytes);
    position += WAL_RECORD_HEADER_BYTES + record.m_payload_bytes;
  }

  return (DB_SUCCESS);
}

/** Encode and append one caller-ordered group. No writer queue is owned here.
The caller stops appending after an error and determines recovery policy. */
db_status_t wal::append(const wal_record_t *records, size_t count) {
  size_t bytes = 0;
  if (const db_status_t err = validate_records(records, count, bytes);
      !err.ok()) {
    return (err);
  }

  /* Serialize encoding, rotation and append with explicit WAL sync. */
  ut_lock_guard_t io(m_io_mutex);

  /* Each group must continue the existing WAL sequence. */
  ut_a(records[0].m_seq == m_next_seq);

  if (const db_status_t err = encode_group(records, count, bytes); !err.ok()) {
    return (err);
  }

  const uint64_t threshold = m_wal_file_size_bytes;
  if (m_write_offset > WAL_FILE_HEADER_BYTES &&
      (bytes > UINT64_MAX - m_write_offset ||
       m_write_offset + bytes > threshold)) {
    /* This group would exceed the file size; rotate before writing it. */
    if (const db_status_t err = rotate(records[0].m_seq); !err.ok()) {
      return (err.with_context("cannot rotate WAL before file " +
                               wal_file_name(records[0].m_seq)));
    }
  }

  /* A failed write may have reached the file partially; report an unknown
  outcome so the caller stops appending and recovers from the WAL. */
  const db_status_t partial = UT_DEBUG_FAULT("wal.partial_write");

  db_status_t err = os_file_write(
      m_file, m_buffer, partial.ok() ? bytes : bytes / 2, m_write_offset);
  if (err.ok()) {
    err = partial;
  }

  if (!err.ok()) {
    return (err.with_context("cannot write WAL file " +
                             wal_file_name(m_file_first_seq))
                .with_code(DB_OUTCOME_UNKNOWN));
  }

  /* Synchronous writes must reach durable storage before reporting success.
  A sync failure cannot establish whether the group became durable. */
  if (m_sync) {
    err = UT_DEBUG_FAULT("wal.sync");

    if (err.ok()) {
      err = os_file_sync(m_file);
    }

    if (!err.ok()) {
      return (err.with_context("cannot sync WAL file " +
                               wal_file_name(m_file_first_seq))
                  .with_code(DB_OUTCOME_UNKNOWN));
    }
  }

  /* Advance append state only after every required I/O step succeeds. */
  m_write_offset += bytes;

  {
    ut_lock_guard_t lock(m_state_mutex);

    m_next_seq += count;
    m_stats.m_records += count;
    m_stats.m_groups++;
    m_stats.m_bytes += bytes;
  }

  return (DB_SUCCESS);
}

/** Sync the published current file while excluding append and rotation. */
db_status_t wal::sync() {
  ut_lock_guard_t io(m_io_mutex);

  if (const db_status_t err = UT_DEBUG_FAULT("wal.sync"); !err.ok()) {
    return (err);
  }

  /* Reopen by name so a missing WAL cannot be hidden by an open descriptor. */
  const std::string path = wal_path(m_dir, wal_file_name(m_file_first_seq));
  os_file_t file = OS_FILE_CLOSED;
  if (const db_status_t err = os_file_open(path.c_str(), OS_FILE_OPEN, file);
      !err.ok()) {
    return (err);
  }

  const db_status_t err = os_file_sync(file);
  static_cast<void>(os_file_close(file));

  return (err.ok() ? err
                   : err.with_context("cannot sync WAL file '" + path + "'"));
}

/** Copy write counters and file count under the state mutex. */
void wal::stats(wal::stats_t &stats) {
  ut_lock_guard_t lock(m_state_mutex);

  stats = m_stats;

  return;
}
