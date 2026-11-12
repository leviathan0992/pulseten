/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal.cc
 *    Write-ahead log I/O, segment rotation and recovery.
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

#include "os_file.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_new.h"
#include "ut_noncopyable.h"
#include "wal_format.h"
#include "wal_recv.h"

/** Join the WAL directory and a file name.
@return the path of a file in the WAL directory */
static std::string wal_path(const std::string &dir, const std::string &name) {
  return (dir + "/" + name);
}

/** Own a descriptor until it is transferred to wal. */
class wal_file_guard final : private ut_noncopyable {
 public:
  /** Descriptor to close; OS_FILE_CLOSED once released. */
  os_file_t m_file{OS_FILE_CLOSED};

  /** Close a descriptor whose ownership was not transferred. */
  ~wal_file_guard() { static_cast<void>(os_file_close(m_file)); }

  /** Transfer the owned file descriptor without closing it.
  @return the descriptor previously owned by this guard; the guard is left
    closed */
  os_file_t release() {
    const os_file_t value = m_file;
    m_file = OS_FILE_CLOSED;

    return (value);
  }
};

/** Create a segment file whose first record will be first_seq. The file
appears under its final name only after its header is durable.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal::create_segment(const std::string &dir, uint64_t wal_id,
                                batch_seq_t first_seq, os_file_t &file) {
  file = OS_FILE_CLOSED;

  const std::string path = wal_path(dir, wal_segment_name(first_seq));
  const std::string init_path = path + ".init";

  /* A leftover init file is from a create that never finished; it was
  never published under the final name. */
  if (const db_status_t err = os_file_delete_if_exists(init_path.c_str());
      !err.ok()) {
    return (err);
  }

  wal_file_guard owner;
  if (const db_status_t err =
          os_file_open(init_path.c_str(), OS_FILE_CREATE, owner.m_file);
      !err.ok()) {
    return (err);
  }

  unsigned char header[WAL_FILE_HEADER_SIZE];
  wal_encode_file_header(header, wal_id, first_seq);

  if (const db_status_t err =
          os_file_write(owner.m_file, header, sizeof(header), 0);
      !err.ok()) {
    return (err.with_context("cannot write WAL header '" + init_path + "'"));
  }

  if (const db_status_t err = os_file_sync(owner.m_file); !err.ok()) {
    return (err.with_context("cannot sync WAL header '" + init_path + "'"));
  }

  if (const db_status_t err =
          os_file_rename_noreplace(init_path.c_str(), path.c_str());
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err = os_dir_sync(dir.c_str()); !err.ok()) {
    return (err);
  }

  file = owner.release();

  return (DB_SUCCESS);
}

/** Start a new segment for a group that would overflow the current one.
The old segment is synced before the new one is published, so only the
newest segment can have an unsynced tail. Called only by the group leader.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal::rotate(batch_seq_t first_seq) {
  os_file_t file;

  /* A reopened segment may contain writes from a previous unsynced open. */
  if (const db_status_t sync_err = os_file_sync(m_file); !sync_err.ok()) {
    return (sync_err);
  }

  if (const db_status_t err = create_segment(m_dir, m_wal_id, first_seq, file);
      !err.ok()) {
    return (err);
  }

  /* Everything in the old segment is durable; a close error cannot lose
  any of it. */
  static_cast<void>(os_file_close(m_file));

  m_file = file;
  m_offset = WAL_FILE_HEADER_SIZE;

  ut_lock_guard_t lock(m_state_mutex);
  m_closed.push_back(m_current_first);
  m_current_first = first_seq;
  m_stats.m_segments++;

  return (DB_SUCCESS);
}

/** Scan WAL segments in sequence order and remove unfinished .init files.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t wal_scan_segments(const std::string &dir,
                              std::vector<batch_seq_t> &firsts) {
  firsts.clear();

  std::vector<std::string> names;

  if (const db_status_t err = os_dir_list(dir.c_str(), names); !err.ok()) {
    return (err);
  }

  for (size_t i = 0; i < names.size(); ++i) {
    const std::string &name = names[i];
    batch_seq_t first = 0;

    if (wal_segment_parse_name(name.c_str(), first)) {
      firsts.push_back(first);
      continue;
    }

    /* "wal-<20 digits>.pt.init" was never published. */
    if (name.size() == 32 && name.compare(27, 5, ".init") == 0 &&
        wal_segment_parse_name(name.substr(0, 27).c_str(), first)) {
      if (const db_status_t err =
              os_file_delete_if_exists(wal_path(dir, name).c_str());
          !err.ok()) {
        return (err);
      }
    }
  }

  std::sort(firsts.begin(), firsts.end());

  return (DB_SUCCESS);
}

/** Make a wal. The caller retains the segment until attachment succeeds.
@param[out]  out              new WAL handle; unchanged on failure
@return DB_SUCCESS or DB_OUT_OF_MEMORY; descriptor ownership transfers only on
  success */
db_status_t wal::allocate(const std::string &dir, wal_file_guard &file,
                          wal **out, uint64_t segment_bytes) {
  wal *log = ut_new<wal>(segment_bytes);

  if (log == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  log->m_dir = dir;

  /* Other fields start from their member initializers. */
  log->m_file = file.release();

  *out = log;

  return (DB_SUCCESS);
}

/** Create a new, empty WAL file in a directory. */
db_status_t wal::create(const char *dir, wal **out, uint64_t wal_id, bool sync,
                        uint64_t segment_bytes) {
  ut_a(dir != nullptr);

  *out = nullptr;

  std::string directory;
  if (const db_status_t err = os_dir_resolve(dir, directory); !err.ok()) {
    return (err);
  }

  dir = directory.c_str();

  /* Only an empty directory gets a new WAL. */
  std::vector<batch_seq_t> firsts;
  if (const db_status_t err = wal_scan_segments(dir, firsts); !err.ok()) {
    return (err);
  }

  if (!firsts.empty()) {
    return (DB_IO_ERROR);
  }

  /* A standalone WAL has no stored identity to reuse. */
  if (wal_id == 0) {
    if (const db_status_t err = os_generate_uid(wal_id); !err.ok()) {
      return (err);
    }
  }

  wal_file_guard file;

  if (const db_status_t err = create_segment(dir, wal_id, 1, file.m_file);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err = wal::allocate(dir, file, out, segment_bytes);
      !err.ok()) {
    return (err);
  }

  (*out)->m_wal_id = wal_id;
  (*out)->m_sync = sync;
  (*out)->m_stats.m_segments = 1;

  return (DB_SUCCESS);
}

/** Check the database identity before callbacks or tail truncation.
@param[in]  expected_id  expected WAL identity; 0 disables the identity check
@return DB_SUCCESS, DB_IO_ERROR, or DB_CORRUPTION for a missing/invalid header
  or identity mismatch */
db_status_t wal::check_identity(os_file_t file, uint64_t expected_id) {
  uint64_t size = 0;
  if (const db_status_t err = os_file_get_size(file, size); !err.ok()) {
    return (err);
  }

  if (size < WAL_FILE_HEADER_SIZE) {
    return (DB_CORRUPTION);
  }

  unsigned char header[WAL_FILE_HEADER_SIZE];
  if (const db_status_t err = os_file_read(file, header, sizeof(header), 0);
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

/** Recover the segments in order. On success the newest one is left open in
*file with its unfinished tail cut off.
@param[in]  firsts       segment first sequence numbers, sorted from oldest
                         to newest
@param[in]  apply        record replay callback; may be NULL
@param[in]  expected_id  expected WAL identity; 0 disables the identity check
@return DB_SUCCESS, a segment I/O/corruption error, or the replay callback
  error */
db_status_t wal::recover_segments(const std::string &dir,
                                  const std::vector<batch_seq_t> &firsts,
                                  wal_apply_t apply, void *ctx, os_file_t &file,
                                  wal_recv_t &recv, uint64_t expected_id) {
  ut_a(!firsts.empty());

  recv.m_first_seq = firsts[0];
  recv.m_last_seq = firsts[0] - 1;

  for (size_t i = 0; i < firsts.size(); ++i) {
    const bool is_last = i + 1 == firsts.size();

    /* Each segment continues where the previous one ended. */
    if (firsts[i] != recv.m_last_seq + 1) {
      return (DB_CORRUPTION);
    }

    const std::string path = wal_path(dir, wal_segment_name(firsts[i]));

    if (const db_status_t err = os_file_open(path.c_str(), OS_FILE_OPEN, file);
        !err.ok()) {
      return (err);
    }

    /* Before replay, so that another database's WAL never reaches apply. */
    if (expected_id != 0) {
      if (const db_status_t err = check_identity(file, expected_id);
          !err.ok()) {
        return (err.with_context("cannot recover WAL segment '" + path + "'"));
      }
    }

    wal_recv_t seg;
    if (const db_status_t err = wal_segment_reader(file).replay(
            firsts[i], is_last, apply, ctx, seg);
        !err.ok()) {
      return (err.with_context("cannot recover WAL segment '" + path + "'"));
    }

    /* Every segment must belong to the same WAL. */
    if (i > 0 && seg.m_wal_id != recv.m_wal_id) {
      return (DB_CORRUPTION);
    }

    /* Only the newest segment stays open for appending. */
    if (!is_last) {
      static_cast<void>(os_file_close(file));
    }

    recv.m_wal_id = seg.m_wal_id;
    recv.m_records += seg.m_records;
    recv.m_last_seq = seg.m_last_seq;
    recv.m_end_offset = seg.m_end_offset;
    recv.m_tail_bytes = seg.m_tail_bytes;
    recv.m_segments++;
  }

  /* Each segment continued the previous one, without a gap. */
  ut_a(recv.m_segments == firsts.size());
  ut_a(recv.m_records == recv.m_last_seq - recv.m_first_seq + 1);

  /* Cut off an unfinished group so new records directly follow the last
  recovered one. */
  if (recv.m_tail_bytes > 0) {
    const std::string path = wal_path(dir, wal_segment_name(firsts.back()));
    if (const db_status_t err = os_file_truncate(file, recv.m_end_offset);
        !err.ok()) {
      return (err.with_context("cannot recover WAL segment '" + path + "'"));
    }

    if (const db_status_t err = os_file_sync(file); !err.ok()) {
      return (err.with_context("cannot recover WAL segment '" + path + "'"));
    }
  }

  return (DB_SUCCESS);
}

/** Open an existing WAL and recover it: replay every valid record through
apply, cut off an unfinished last group, and leave the WAL ready to append after
the last recovered record. */
db_status_t wal::open(const char *dir, wal_apply_t apply, void *ctx, wal **out,
                      wal_recv_t &recv, uint64_t expected_id, bool sync,
                      uint64_t segment_bytes) {
  ut_a(dir != nullptr);

  *out = nullptr;
  recv = wal_recv_t();

  std::string directory;
  if (const db_status_t err = os_dir_resolve(dir, directory); !err.ok()) {
    return (err);
  }

  dir = directory.c_str();

  std::vector<batch_seq_t> firsts;
  if (const db_status_t err = wal_scan_segments(dir, firsts); !err.ok()) {
    return (err);
  }

  /* An existing WAL has at least one segment. */
  if (firsts.empty()) {
    return (DB_IO_ERROR);
  }

  wal_file_guard file;

  if (const db_status_t err = recover_segments(dir, firsts, apply, ctx,
                                               file.m_file, recv, expected_id);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err = wal::allocate(dir, file, out, segment_bytes);
      !err.ok()) {
    return (err);
  }

  wal &log = **out;
  ut_a(recv.m_end_offset >= WAL_FILE_HEADER_SIZE);

  log.m_wal_id = recv.m_wal_id;
  log.m_sync = sync;
  log.m_offset = recv.m_end_offset;
  log.m_next_seq = recv.m_last_seq + 1;
  log.m_current_first = firsts.back();
  log.m_closed.assign(firsts.begin(), firsts.end() - 1);
  log.m_stats.m_segments = firsts.size();

  return (DB_SUCCESS);
}

/** Close a WAL. */
db_status_t wal::close() {
  const db_status_t err = os_file_close(m_file);

  free(m_buf);
  m_buf = nullptr;
  m_buf_size = 0;

  return (err);
}

wal::~wal() {
  static_cast<void>(close());
}

db_status_t wal::destroy(wal *log) {
  if (log == nullptr) {
    return (DB_SUCCESS);
  }

  const db_status_t err = log->close();
  ut_delete(log);

  return (err);
}

/** Delete the closed segments that hold only stored records. */
db_status_t wal::purge(batch_seq_t flushed_seq, uint64_t &removed) {
  removed = 0;

  /* Only one purger may choose and delete the oldest segment. Keep
  appenders free to run while deletion and directory sync are in progress. */
  std::lock_guard<std::mutex> purge_lock(m_purge_mutex);

  for (;;) {
    batch_seq_t first;

    {
      ut_lock_guard_t lock(m_state_mutex);

      /* Storage holds only writes the WAL had. */
      ut_a(flushed_seq < m_next_seq);

      if (m_closed.empty()) {
        return (DB_SUCCESS);
      }

      const batch_seq_t next_first =
          m_closed.size() > 1 ? m_closed[1] : m_current_first;

      /* A closed segment holds only stored records exactly when the
      next segment starts at or before the first not stored record. */
      if (next_first - 1 > flushed_seq) {
        return (DB_SUCCESS);
      }

      first = m_closed.front();
      ut_a(first < next_first);
    }

    const std::string path = wal_path(m_dir, wal_segment_name(first));

    if (const db_status_t err = os_file_delete_if_exists(path.c_str());
        !err.ok()) {
      return (err);
    }

    /* Make each prefix deletion durable before deleting the next file.
    On failure keep this entry: retrying a missing file is harmless and
    retries its directory sync before advancing to a newer segment. */
    if (const db_status_t err = os_dir_sync(m_dir.c_str()); !err.ok()) {
      return (err);
    }

    {
      ut_lock_guard_t lock(m_state_mutex);

      ut_a(!m_closed.empty() && m_closed.front() == first);

      m_closed.pop_front();
      m_stats.m_segments--;
    }

    removed++;
  }
}

/** Validate a complete group and calculate its encoded size without I/O. */
db_status_t wal::check_records(const wal_record_t *records, size_t count,
                               size_t &bytes) {
  if (records == nullptr || count == 0 || count > UINT32_MAX) {
    return (DB_INVALID_ARGUMENT);
  }

  /* Keep both the record range and the next sequence representable. */
  if (records[0].m_seq == 0 || count > UINT64_MAX - records[0].m_seq) {
    return (DB_INVALID_ARGUMENT);
  }

  /* Validate payloads and sequence order before allocating or changing files.
   */
  bytes = 0;
  for (size_t i = 0; i < count; ++i) {
    const wal_record_t &record = records[i];
    if (record.m_payload == nullptr || record.m_len == 0 ||
        record.m_len > WAL_MAX_PAYLOAD) {
      return (DB_INVALID_ARGUMENT);
    }

    /* Check the accumulated size before adding this record. */
    if (bytes > SIZE_MAX - WAL_RECORD_HEADER_SIZE ||
        record.m_len > SIZE_MAX - WAL_RECORD_HEADER_SIZE - bytes) {
      return (DB_INVALID_ARGUMENT);
    }

    bytes += WAL_RECORD_HEADER_SIZE + record.m_len;

    if (record.m_seq != records[0].m_seq + i) {
      return (DB_INVALID_ARGUMENT);
    }
  }

  return (DB_SUCCESS);
}

/** Encode a validated group into the reusable buffer under the I/O mutex. */
db_status_t wal::encode_group(const wal_record_t *records, size_t count,
                              size_t bytes) {
  ut_ad(m_io_mutex.is_owned());

  /* Reuse the group buffer; grow it only when the next group needs more space.
   */
  if (bytes > m_buf_size) {
    unsigned char *buffer = static_cast<unsigned char *>(realloc(m_buf, bytes));
    if (buffer == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    m_buf = buffer;
    m_buf_size = bytes;
  }

  /* Encode the whole group, including each record's index within the group. */
  unsigned char *position = m_buf;
  for (size_t i = 0; i < count; ++i) {
    const wal_record_t &record = records[i];
    wal_encode_record(position, record.m_seq, static_cast<uint32_t>(i),
                      record.m_payload, record.m_len);
    position += WAL_RECORD_HEADER_SIZE + record.m_len;
  }

  return (DB_SUCCESS);
}

/** Encode and append one caller-ordered group. No writer queue is owned here.
The caller stops appending after an error and determines recovery policy. */
db_status_t wal::append(const wal_record_t *records, size_t count) {
  size_t bytes = 0;
  if (const db_status_t err = check_records(records, count, bytes); !err.ok()) {
    return (err);
  }

  /* Serialize encoding, rotation and append with explicit WAL sync. */
  ut_lock_guard_t io(m_io_mutex);

  if (records[0].m_seq != m_next_seq) {
    return (DB_INVALID_ARGUMENT);
  }

#ifdef PT_TESTING
  /* Consume one-shot faults under their protecting mutex. */
  bool fail_sync = false;
  bool partial_write = false;
  {
    ut_lock_guard_t lock(m_state_mutex);
    fail_sync = m_test_fail_next_sync;
    m_test_fail_next_sync = false;
    partial_write = m_test_partial_write;
    m_test_partial_write = false;
  }
#endif /* PT_TESTING */

  if (const db_status_t err = encode_group(records, count, bytes); !err.ok()) {
    return (err);
  }

  /* Keep a group within one segment. An empty segment accepts an oversized
  group so the size threshold cannot prevent forward progress. */
  const uint64_t threshold = m_segment_bytes;
  if (m_offset > WAL_FILE_HEADER_SIZE &&
      (bytes > UINT64_MAX - m_offset || m_offset + bytes > threshold)) {
    if (const db_status_t err = rotate(records[0].m_seq); !err.ok()) {
      return (err.with_context("cannot rotate WAL before segment " +
                               wal_segment_name(records[0].m_seq)));
    }
  }

  /* A failed write may have reached the file partially; report an unknown
  outcome so the caller stops appending and recovers from the WAL. */
#ifdef PT_TESTING
  db_status_t err =
      os_file_write(m_file, m_buf, partial_write ? bytes / 2 : bytes, m_offset);
  if (partial_write && err.ok()) {
    err = db_status_t(DB_IO_ERROR, "injected partial WAL write");
  }
#else
  db_status_t err = os_file_write(m_file, m_buf, bytes, m_offset);
#endif /* PT_TESTING */
  if (!err.ok()) {
    return (err.with_context("cannot write WAL segment " +
                             wal_segment_name(m_current_first))
                .with_code(DB_OUTCOME_UNKNOWN));
  }

  /* Synchronous writes must reach durable storage before reporting success.
  A sync failure cannot establish whether the group became durable. */
  if (m_sync) {
#ifdef PT_TESTING
    err = fail_sync ? db_status_t(DB_IO_ERROR) : os_file_sync(m_file);
#else
    err = os_file_sync(m_file);
#endif /* PT_TESTING */
    if (!err.ok()) {
      return (err.with_context("cannot sync WAL segment " +
                               wal_segment_name(m_current_first))
                  .with_code(DB_OUTCOME_UNKNOWN));
    }
  }

  /* Advance append state only after every required I/O step succeeds. */
  m_offset += bytes;

  ut_lock_guard_t lock(m_state_mutex);

  m_next_seq += count;
  m_stats.m_records += count;
  m_stats.m_groups++;
  m_stats.m_bytes += bytes;

  return (DB_SUCCESS);
}

/** Sync the published current segment while excluding append and rotation. */
db_status_t wal::sync() {
  ut_lock_guard_t io(m_io_mutex);

  /* Reopen by name so a missing WAL cannot be hidden by an open descriptor. */
  const std::string path = wal_path(m_dir, wal_segment_name(m_current_first));
  wal_file_guard file;
  if (const db_status_t err =
          os_file_open(path.c_str(), OS_FILE_OPEN, file.m_file);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err = os_file_sync(file.m_file); !err.ok()) {
    return (err.with_context("cannot sync WAL segment '" + path + "'"));
  }

  return (DB_SUCCESS);
}

/** Read the counters. */
void wal::read_stats(wal_stats_t &stats) {
  ut_lock_guard_t lock(m_state_mutex);

  stats = m_stats;

  return;
}
