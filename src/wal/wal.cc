/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal.cc
 *    Write-ahead log with leader/follower group commit over segment files.
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
#include <vector>

#include "os_file.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_new.h"
#include "wal_format.h"
#include "wal_recv.h"

/** An open WAL. */
struct wal_t {
  /* Fixed before the first append. */

  /** The WAL directory. */
  std::string m_dir;

  /** Identity written in every segment header. */
  uint64_t m_wal_id{0};

  /** Upper bound on the bytes of one group write. */
  size_t m_group_max_bytes{WAL_DEFAULT_GROUP_BYTES};

  /** Size at which a new segment is started. */
  uint64_t m_segment_bytes{WAL_DEFAULT_SEGMENT_BYTES};

  /** Sync each group before completing its writes. */
  bool m_sync{true};

  /* Used only by the current leader, and by wal_close() when no append is
  in progress. */

  /** Current segment. */
  os_file_t m_file{OS_FILE_CLOSED};

  /** Bytes in the current segment; used like m_file. */
  uint64_t m_offset{WAL_FILE_HEADER_SIZE};

  /** Encoding buffer. */
  unsigned char *m_buf{nullptr};

  /** Size of m_buf in bytes. */
  size_t m_buf_size{0};

  /** Serializes purging, including file I/O. Acquired before m_mutex;
  appenders never acquire it. */
  std::mutex m_purge_mutex;

  /* Protected by m_mutex. */

  /** A leaf lock: nothing else is locked while it is held, and it is
  released during file I/O. */
  ut_mutex_t m_mutex;

  /** First sequence numbers of the closed segments, oldest first. */
  std::vector<batch_seq_t> m_closed;

  /** First sequence number of the current segment. */
  batch_seq_t m_current_first{1};

  /** First of the queued writers; it is the leader. */
  wal_writer_t *m_head{nullptr};

  /** Last of the queued writers, where new ones are appended. */
  wal_writer_t *m_tail{nullptr};

  /** Sequence number for the next accepted record. */
  batch_seq_t m_next_seq{1};

  /** Why a group failed. Success until then; after that every append returns
  DB_FENCED. */
  db_status_t m_failure;

  /** Make the next group sync fail; debug builds only. */
  bool m_dbg_fail_next_sync{false};

  /** Counters. */
  wal_stats_t m_stats;
};

/** Join the WAL directory and a file name.
@return the path of a file in the WAL directory */
static std::string wal_path(const std::string &dir, const std::string &name) {
  return (dir + "/" + name);
}

/** Own a descriptor until it is transferred to wal_t. */
struct wal_file_guard_t {
  /** Descriptor to close; OS_FILE_CLOSED once released. */
  os_file_t m_file{OS_FILE_CLOSED};

  /** Close a descriptor whose ownership was not transferred. */
  ~wal_file_guard_t() { static_cast<void>(os_file_close(m_file)); }

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
static db_status_t wal_segment_create(const std::string &dir, uint64_t wal_id,
                                      batch_seq_t first_seq, os_file_t &file) {
  file = OS_FILE_CLOSED;

  const std::string path = wal_path(dir, wal_segment_name(first_seq));
  const std::string init_path = path + ".init";

  /* A leftover init file is from a create that never finished; it was
  never published under the final name. */
  db_status_t err = os_file_delete_if_exists(init_path.c_str());
  if (!err.ok()) {
    return (err);
  }

  wal_file_guard_t owner;
  err = os_file_open(init_path.c_str(), OS_FILE_CREATE, owner.m_file);
  if (!err.ok()) {
    return (err);
  }

  unsigned char header[WAL_FILE_HEADER_SIZE];
  wal_encode_file_header(header, wal_id, first_seq);

  err = os_file_write(owner.m_file, header, sizeof(header), 0);
  if (!err.ok()) {
    return (err);
  }

  err = os_file_sync(owner.m_file);
  if (!err.ok()) {
    return (err);
  }

  err = os_file_rename_noreplace(init_path.c_str(), path.c_str());
  if (!err.ok()) {
    return (err);
  }

  err = os_dir_sync(dir.c_str());
  if (!err.ok()) {
    return (err);
  }

  file = owner.release();

  return (DB_SUCCESS);
}

/** Encode the records of writers first..last into wal.m_buf. Reads m_next
only of writers before last, which no other thread modifies. Called only by
the current group leader.
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
static db_status_t wal_encode_group(wal_t &wal, wal_writer_t &first,
                                    wal_writer_t &last, size_t bytes) {
  ut_ad(!wal.m_mutex.is_owned());

  if (bytes > wal.m_buf_size) {
    unsigned char *buf =
        static_cast<unsigned char *>(realloc(wal.m_buf, bytes));
    if (buf == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    wal.m_buf = buf;
    wal.m_buf_size = bytes;
  }

  unsigned char *p = wal.m_buf;
  uint32_t index = 0;
  wal_writer_t *w = &first;

  for (;;) {
    /* The queue holds consecutive sequence numbers. */
    ut_a(w->m_seq == first.m_seq + index);

    wal_encode_record(p, w->m_seq, index, w->m_payload, w->m_len);
    p += WAL_RECORD_HEADER_SIZE + w->m_len;
    ++index;

    if (w == &last) {
      break;
    }
    w = w->m_next;
  }

  ut_a(p == wal.m_buf + bytes);

  return (DB_SUCCESS);
}

/** Start a new segment for a group that would overflow the current one.
The old segment is synced before the new one is published, so only the
newest segment can have an unsynced tail. Called only by the group leader.
@return DB_SUCCESS or DB_IO_ERROR */
static db_status_t wal_rotate(wal_t &wal, batch_seq_t first_seq) {
  ut_ad(!wal.m_mutex.is_owned());

  os_file_t file;

  /* A reopened segment may contain writes from a previous unsynced open. */
  const db_status_t sync_err = os_file_sync(wal.m_file);
  if (!sync_err.ok()) {
    return (sync_err);
  }

  const db_status_t err =
      wal_segment_create(wal.m_dir, wal.m_wal_id, first_seq, file);
  if (!err.ok()) {
    return (err);
  }

  /* Everything in the old segment is durable; a close error cannot lose
  any of it. */
  static_cast<void>(os_file_close(wal.m_file));

  wal.m_file = file;
  wal.m_offset = WAL_FILE_HEADER_SIZE;

  return (DB_SUCCESS);
}

/** Fence the WAL and retain the group failure, including its owned cause. */
static void wal_set_error(wal_t &wal, const char *action, batch_seq_t segment,
                          const db_status_t &status) {
  ut_ad(wal.m_mutex.is_owned());
  ut_a(wal.m_failure.ok());

  wal.m_failure =
      status.with_context(std::string("cannot ") + action + " WAL segment " +
                          wal_segment_name(segment));
}

/** The writers one leader handles together. */
struct wal_group_t {
  /** First writer of the group, the leader. */
  wal_writer_t *m_first{nullptr};

  /** Last writer of the group. */
  wal_writer_t *m_last{nullptr};

  /** Encoded size of the group. */
  size_t m_bytes{0};

  /** The group must start a new segment. */
  bool m_rotate{false};

  /** A new segment was started. */
  bool m_rotated{false};

  /** First sequence number of the segment the group goes to. */
  batch_seq_t m_segment{0};

  /** What the leader was doing when the group failed. */
  const char *m_action{"create"};
};

/** Take queued writers into a group until the size limit, and decide whether
it starts a new segment.
REQUIRES: wal.m_mutex is held. */
static void wal_select_group(wal_t &wal, wal_group_t &group) {
  ut_ad(wal.m_mutex.is_owned());

  ut_a(wal.m_head != nullptr);

  /* The leader is always included, even if its record alone exceeds the
  limit. */
  group.m_first = wal.m_head;
  group.m_last = group.m_first;
  group.m_bytes = WAL_RECORD_HEADER_SIZE + group.m_first->m_len;

  while (group.m_last->m_next != nullptr) {
    const size_t rec = WAL_RECORD_HEADER_SIZE + group.m_last->m_next->m_len;

    if (group.m_bytes + rec > wal.m_group_max_bytes) {
      break;
    }

    group.m_bytes += rec;
    group.m_last = group.m_last->m_next;
  }

  /* A group that does not fit goes to a new segment, unless the current
  one is still empty. */
  group.m_rotate = wal.m_offset > WAL_FILE_HEADER_SIZE &&
                   wal.m_offset + group.m_bytes > wal.m_segment_bytes;
  group.m_segment = group.m_rotate ? group.m_first->m_seq : wal.m_current_first;
}

/** Write a group and optionally sync it. Called by the leader without the
mutex, so new writers can queue up behind the group.
@param[in]  fail_sync  pretend that the sync failed; debug builds only
@return DB_SUCCESS, DB_IO_ERROR or DB_OUT_OF_MEMORY */
static db_status_t wal_write_group(wal_t &wal, wal_group_t &group,
                                   bool fail_sync) {
  ut_ad(!wal.m_mutex.is_owned());

  db_status_t err = DB_SUCCESS;

  if (group.m_rotate) {
    err = wal_rotate(wal, group.m_first->m_seq);
    if (!err.ok()) {
      return (err);
    }

    group.m_rotated = true;
  }

  group.m_action = "encode a group for";
  err = wal_encode_group(wal, *group.m_first, *group.m_last, group.m_bytes);
  if (!err.ok()) {
    return (err);
  }

  group.m_action = "write";
  err = os_file_write(wal.m_file, wal.m_buf, group.m_bytes, wal.m_offset);
  if (!err.ok()) {
    return (err);
  }

  group.m_action = "sync";

  if (!wal.m_sync) {
    return (DB_SUCCESS);
  }

  return (fail_sync ? DB_IO_ERROR : os_file_sync(wal.m_file));
}

/** Give every writer of a group its result, and on success count the group.
REQUIRES: wal.m_mutex is held. */
static void wal_complete_group(wal_t &wal, const wal_group_t &group,
                               db_status_t err) {
  ut_ad(wal.m_mutex.is_owned());

  /* The queue only grows behind the leader while it writes. */
  ut_a(wal.m_head == group.m_first);

  /* The new segment becomes current; the old one is closed. */
  if (group.m_rotated) {
    ut_a(group.m_first->m_seq > wal.m_current_first);

    wal.m_closed.push_back(wal.m_current_first);
    wal.m_current_first = group.m_first->m_seq;
    wal.m_stats.m_segments++;
  }

  uint64_t records = 0;
  wal_writer_t *w = group.m_first;

  for (;;) {
    wal_writer_t *next = w->m_next;

    w->m_err = err;
    w->m_done = true;

    /* The first writer is this thread, which is not waiting. */
    if (w != group.m_first) {
      /* The waiter cannot return and free w before we release the mutex. */
      w->m_cv.notify_one();
    }

    ++records;

    if (w == group.m_last) {
      wal.m_head = next;
      break;
    }
    w = next;
  }

  if (err.ok()) {
    wal.m_offset += group.m_bytes;
    wal.m_stats.m_records += records;
    wal.m_stats.m_groups++;
    wal.m_stats.m_bytes += group.m_bytes;
  }
}

/** Reject every writer still queued. Sequence numbers of a failed group are
used up, so no later record may be written.
REQUIRES: wal.m_mutex is held. */
static void wal_reject_queue(wal_t &wal) {
  ut_ad(wal.m_mutex.is_owned());

  while (wal.m_head != nullptr) {
    wal_writer_t *w = wal.m_head;
    wal.m_head = w->m_next;

    w->m_err = wal.m_failure.with_code(DB_FENCED);
    w->m_done = true;
    w->m_cv.notify_one();
  }
}

/** Commit a group starting at the queue head, then complete every
writer in it. Called by the queue head's waiter. The mutex is released during
I/O and held again on return. */
static void wal_commit_group(wal_t &wal, wal_writer_t &first_writer,
                             ut_unique_lock_t &lock) {
  ut_ad(wal.m_mutex.is_owned());
  ut_ad(lock.mutex() == &wal.m_mutex && lock.owns_lock());
  ut_a(wal.m_head == &first_writer);

  wal_group_t group;
  wal_select_group(wal, group);

  const bool fail_sync = wal.m_dbg_fail_next_sync;
  wal.m_dbg_fail_next_sync = false;

  lock.unlock();

  const db_status_t err = wal_write_group(wal, group, fail_sync);

  lock.lock();

  if (!err.ok()) {
    wal_set_error(wal, group.m_action, group.m_segment, err);
  }

  wal_complete_group(wal, group, err.ok() ? err : wal.m_failure);

  if (!err.ok()) {
    wal_reject_queue(wal);
  }

  if (wal.m_head == nullptr) {
    wal.m_tail = nullptr;
  } else {
    /* Hand leadership to the new queue head. */
    wal.m_head->m_cv.notify_one();
  }
}

/** List the segments of a directory, oldest first, and remove leftovers of
segment creations that never finished.
@return DB_SUCCESS or DB_IO_ERROR */
static db_status_t wal_list_segments(const std::string &dir,
                                     std::vector<batch_seq_t> &firsts) {
  firsts.clear();

  std::vector<std::string> names;

  db_status_t err = os_dir_list(dir.c_str(), names);
  if (!err.ok()) {
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
      err = os_file_delete_if_exists(wal_path(dir, name).c_str());
      if (!err.ok()) {
        return (err);
      }
    }
  }

  std::sort(firsts.begin(), firsts.end());

  return (DB_SUCCESS);
}

/** Make a wal_t. The caller retains the segment until attachment succeeds.
@param[in]   group_max_bytes  maximum bytes in one WAL group; 0 selects the
                              default
@param[out]  out              new WAL handle; unchanged on failure
@return DB_SUCCESS or DB_OUT_OF_MEMORY; descriptor ownership transfers only on
  success */
static db_status_t wal_alloc(const std::string &dir, wal_file_guard_t &file,
                             size_t group_max_bytes, wal_t **out) {
  wal_t *wal = ut_new<wal_t>();

  if (wal == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  wal->m_dir = dir;

  /* Other fields start from their member initializers. */
  wal->m_file = file.release();

  if (group_max_bytes != 0) {
    wal->m_group_max_bytes = group_max_bytes;
  }

  *out = wal;

  return (DB_SUCCESS);
}

/** Create a new, empty WAL file in a directory. */
db_status_t wal_create(const char *dir, size_t group_max_bytes, wal_t **out,
                       uint64_t wal_id, bool sync) {
  *out = nullptr;

  if (dir == nullptr) {
    return (DB_INVALID_ARGUMENT);
  }

  std::string directory;
  db_status_t err = os_dir_resolve(dir, directory);
  if (!err.ok()) {
    return (err);
  }
  dir = directory.c_str();

  /* Only an empty directory gets a new WAL. */
  std::vector<batch_seq_t> firsts;
  err = wal_list_segments(dir, firsts);
  if (!err.ok()) {
    return (err);
  }

  if (!firsts.empty()) {
    return (DB_IO_ERROR);
  }

  /* A standalone WAL has no stored identity to reuse. */
  if (wal_id == 0) {
    err = os_random_id(wal_id);
    if (!err.ok()) {
      return (err);
    }
  }

  wal_file_guard_t file;

  err = wal_segment_create(dir, wal_id, 1, file.m_file);
  if (!err.ok()) {
    return (err);
  }

  err = wal_alloc(dir, file, group_max_bytes, out);
  if (!err.ok()) {
    return (err);
  }

  (*out)->m_wal_id = wal_id;
  (*out)->m_sync = sync;
  (*out)->m_stats.m_segments = 1;

  return (DB_SUCCESS);
}

/** Check whether a directory holds a WAL. */
db_status_t wal_exists(const char *dir, bool &exists) {
  exists = false;

  if (dir == nullptr) {
    return (DB_INVALID_ARGUMENT);
  }

  std::vector<batch_seq_t> firsts;

  const db_status_t err = wal_list_segments(dir, firsts);
  if (!err.ok()) {
    return (err);
  }

  exists = !firsts.empty();

  return (DB_SUCCESS);
}

/** Check the database identity before callbacks or tail truncation.
@param[in]  expected_id  expected WAL identity; 0 disables the identity check
@return DB_SUCCESS, DB_IO_ERROR, or DB_CORRUPTION for a missing/invalid header
  or identity mismatch */
static db_status_t wal_check_identity(os_file_t file, uint64_t expected_id) {
  uint64_t size = 0;
  db_status_t err = os_file_get_size(file, size);
  if (!err.ok()) {
    return (err);
  }
  if (size < WAL_FILE_HEADER_SIZE) {
    return (DB_CORRUPTION);
  }

  unsigned char header[WAL_FILE_HEADER_SIZE];
  err = os_file_read(file, header, sizeof(header), 0);
  if (!err.ok()) {
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
static db_status_t wal_recover_segments(const std::string &dir,
                                        const std::vector<batch_seq_t> &firsts,
                                        wal_apply_t apply, void *ctx,
                                        os_file_t &file, wal_recv_t &recv,
                                        uint64_t expected_id) {
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

    db_status_t err = os_file_open(path.c_str(), OS_FILE_OPEN, file);
    if (!err.ok()) {
      return (err);
    }

    /* Before replay, so that another database's WAL never reaches apply. */
    if (expected_id != 0) {
      err = wal_check_identity(file, expected_id);
      if (!err.ok()) {
        static_cast<void>(os_file_close(file));

        return (err);
      }
    }

    wal_recv_t seg;
    err = wal_recv_replay_segment(file, firsts[i], is_last, apply, ctx, seg);
    if (!err.ok()) {
      static_cast<void>(os_file_close(file));

      return (err);
    }

    /* Every segment must belong to the same WAL. */
    if (i > 0 && seg.m_wal_id != recv.m_wal_id) {
      static_cast<void>(os_file_close(file));

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
    db_status_t err = os_file_truncate(file, recv.m_end_offset);
    if (!err.ok()) {
      static_cast<void>(os_file_close(file));

      return (err);
    }

    err = os_file_sync(file);
    if (!err.ok()) {
      static_cast<void>(os_file_close(file));

      return (err);
    }
  }

  return (DB_SUCCESS);
}

/** Open an existing WAL and recover it: replay every valid record through
apply, cut off an unfinished last group, and leave the WAL ready to append after
the last recovered record. */
db_status_t wal_open(const char *dir, size_t group_max_bytes, wal_apply_t apply,
                     void *ctx, wal_t **out, wal_recv_t &recv,
                     uint64_t expected_id, bool sync) {
  *out = nullptr;
  recv = wal_recv_t();

  if (dir == nullptr) {
    return (DB_INVALID_ARGUMENT);
  }

  std::string directory;
  db_status_t err = os_dir_resolve(dir, directory);
  if (!err.ok()) {
    return (err);
  }
  dir = directory.c_str();

  std::vector<batch_seq_t> firsts;
  err = wal_list_segments(dir, firsts);
  if (!err.ok()) {
    return (err);
  }

  /* An existing WAL has at least one segment. */
  if (firsts.empty()) {
    return (DB_IO_ERROR);
  }

  wal_file_guard_t file;

  err = wal_recover_segments(dir, firsts, apply, ctx, file.m_file, recv,
                             expected_id);
  if (!err.ok()) {
    return (err);
  }

  err = wal_alloc(dir, file, group_max_bytes, out);
  if (!err.ok()) {
    return (err);
  }

  wal_t &wal = **out;
  ut_a(recv.m_end_offset >= WAL_FILE_HEADER_SIZE);

  wal.m_wal_id = recv.m_wal_id;
  wal.m_sync = sync;
  wal.m_offset = recv.m_end_offset;
  wal.m_next_seq = recv.m_last_seq + 1;
  wal.m_current_first = firsts.back();
  wal.m_closed.assign(firsts.begin(), firsts.end() - 1);
  wal.m_stats.m_segments = firsts.size();

  return (DB_SUCCESS);
}

/** Close a WAL. */
db_status_t wal_close(wal_t *wal) {
  if (wal == nullptr) {
    return (DB_SUCCESS);
  }

  wal->m_mutex.lock();
  ut_a(wal->m_head == nullptr && wal->m_tail == nullptr);
  wal->m_mutex.unlock();

  const db_status_t err = os_file_close(wal->m_file);

  free(wal->m_buf);
  ut_delete(wal);

  return (err);
}

/** Set the size at which the next group starts a new segment. */
void wal_set_segment_bytes(wal_t &wal, uint64_t bytes) {
  wal.m_mutex.lock();
  wal.m_segment_bytes = bytes;
  wal.m_mutex.unlock();
}

/** Delete the closed segments that hold only stored records. */
db_status_t wal_purge(wal_t &wal, batch_seq_t flushed_seq, uint64_t &removed) {
  removed = 0;

  /* Only one purger may choose and delete the oldest segment. Keep
  appenders free to run while deletion and directory sync are in progress. */
  std::lock_guard<std::mutex> purge_lock(wal.m_purge_mutex);

  for (;;) {
    batch_seq_t first;

    {
      ut_lock_guard_t lock(wal.m_mutex);

      /* Storage holds only writes the WAL had. */
      ut_a(flushed_seq < wal.m_next_seq);

      if (wal.m_closed.empty()) {
        return (DB_SUCCESS);
      }

      const batch_seq_t next_first =
          wal.m_closed.size() > 1 ? wal.m_closed[1] : wal.m_current_first;

      /* A closed segment holds only stored records exactly when the
      next segment starts at or before the first not stored record. */
      if (next_first - 1 > flushed_seq) {
        return (DB_SUCCESS);
      }

      first = wal.m_closed.front();
      ut_a(first < next_first);
    }

    const std::string path = wal_path(wal.m_dir, wal_segment_name(first));

    db_status_t err = os_file_delete_if_exists(path.c_str());
    if (!err.ok()) {
      return (err);
    }

    /* Make each prefix deletion durable before deleting the next file.
    On failure keep this entry: retrying a missing file is harmless and
    retries its directory sync before advancing to a newer segment. */
    err = os_dir_sync(wal.m_dir.c_str());
    if (!err.ok()) {
      return (err);
    }

    {
      ut_lock_guard_t lock(wal.m_mutex);

      ut_a(!wal.m_closed.empty() && wal.m_closed.front() == first);

      wal.m_closed.erase(wal.m_closed.begin());
      wal.m_stats.m_segments--;
    }

    removed++;
  }
}

/** Give a record its sequence number and queue it, without waiting. */
db_status_t wal_append_enqueue(wal_t &wal, const void *payload, size_t len,
                               wal_writer_t &w, batch_seq_t &seq) {
  seq = 0;

  if (payload == nullptr || len == 0 || len > WAL_MAX_PAYLOAD) {
    return (DB_INVALID_ARGUMENT);
  }

  /* The writer may be reused; reset everything but the condition
  variable, which cannot be copied. */
  w.m_payload = static_cast<const unsigned char *>(payload);
  w.m_len = len;
  w.m_seq = 0;
  w.m_done = false;
  w.m_err = DB_SUCCESS;
  w.m_next = nullptr;

  ut_lock_guard_t lock(wal.m_mutex);

  if (!wal.m_failure.ok()) {
    return (wal.m_failure.with_code(DB_FENCED));
  }

  /* Join the queue; the sequence number fixes the record's place in the
  file. */
  w.m_seq = wal.m_next_seq++;

  ut_a((wal.m_head == nullptr) == (wal.m_tail == nullptr));

  if (wal.m_tail == nullptr) {
    wal.m_head = &w;
  } else {
    ut_a(wal.m_tail->m_seq + 1 == w.m_seq);
    wal.m_tail->m_next = &w;
  }
  wal.m_tail = &w;

  seq = w.m_seq;

  return (DB_SUCCESS);
}

/** Wait until a queued record is written, writing a group as leader if this
writer reaches the head of the queue. */
db_status_t wal_append_commit(wal_t &wal, wal_writer_t &w) {
  ut_unique_lock_t lock(wal.m_mutex);

  /* Wait until a leader has written this record, or until this writer
  reaches the head of the queue and becomes the leader itself. The head
  is checked before waiting, so a hand-over made before this call is not
  missed. */
  while (!w.m_done && wal.m_head != &w) {
    w.m_cv.wait(lock);
  }

  /* Not written yet, so this writer is now the leader. */
  if (!w.m_done) {
    wal_commit_group(wal, w, lock);
  }

  ut_a(w.m_done);

  return (w.m_err);
}

/** Sync the current segment through a separate descriptor, which remains
valid if an appender rotates the WAL meanwhile. */
db_status_t wal_sync(wal_t &wal) {
  std::lock_guard<std::mutex> purge_lock(wal.m_purge_mutex);

  batch_seq_t first;
  {
    ut_lock_guard_t lock(wal.m_mutex);
    first = wal.m_current_first;
  }

  const std::string path = wal_path(wal.m_dir, wal_segment_name(first));
  wal_file_guard_t file;
  const db_status_t err = os_file_open(path.c_str(), OS_FILE_OPEN, file.m_file);
  if (!err.ok()) {
    return (err);
  }

  return (os_file_sync(file.m_file));
}

/** Read the counters. */
void wal_get_stats(wal_t &wal, wal_stats_t &stats) {
  wal.m_mutex.lock();
  stats = wal.m_stats;
  wal.m_mutex.unlock();
}

/** Read why the WAL failed. */
db_status_t wal_status(wal_t &wal) {
  ut_lock_guard_t lock(wal.m_mutex);

  return (wal.m_failure);
}

#ifdef PT_DEBUG
/** Make the next group sync fail with DB_IO_ERROR after its data has been
written. */
void wal_dbg_fail_next_sync(wal_t &wal) {
  wal.m_mutex.lock();
  wal.m_dbg_fail_next_sync = true;
  wal.m_mutex.unlock();
}
#endif
