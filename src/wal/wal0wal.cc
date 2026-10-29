/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal0wal.cc
 *    Write-ahead log with leader/follower group commit over segment files.
 *
 *-------------------------------------------------------------------------
 */

#include "wal0wal.h"

#include <algorithm>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <new>

#include "os0file.h"
#include "ut0new.h"
#include "wal0format.h"

/** An open WAL. */
struct wal_t {
  /** The WAL directory. */
  std::string dir;

  /** Identity written in every segment header. */
  uint64_t wal_id{0};

  /** Upper bound on the bytes of one group write. */
  size_t group_max_bytes{WAL_DEFAULT_GROUP_BYTES};

  /** Size at which a new segment is started. */
  uint64_t segment_bytes{WAL_DEFAULT_SEGMENT_BYTES};

  /** Current segment. Used only by the leader, and by wal_close() when no
  append is in progress. */
  os_file_t file{OS_FILE_CLOSED};

  /** Bytes in the current segment; used like file. */
  uint64_t offset{WAL_FILE_HEADER_SIZE};

  /** Protects every field below except buf and buf_size. A leaf lock:
  nothing else is locked while it is held, and it is released during file
  I/O. */
  std::mutex mutex;

  /** Serializes recycling, including file I/O. Acquired before mutex;
  appenders never acquire it. */
  std::mutex recycle_mutex;

  /** First sequence numbers of the closed segments, oldest first. */
  std::vector<batch_seq_t> closed;

  /** First sequence number of the current segment. */
  batch_seq_t current_first{1};

  /** First of the queued writers; it is the leader. */
  wal_writer_t *head{nullptr};

  /** Last of the queued writers, where new ones are appended. */
  wal_writer_t *tail{nullptr};

  /** Sequence number for the next accepted record. */
  batch_seq_t next_seq{1};

  /** Set after a failed group; every later append returns DB_FENCED. */
  bool fenced{false};

  /** Make the next group flush fail; debug builds only. */
  bool dbg_fail_next_flush{false};

  /** Counters. */
  wal_stats_t stats;

  /** Encoding buffer, used only by the current leader. */
  unsigned char *buf{nullptr};

  /** Size of buf in bytes. */
  size_t buf_size{0};
};

/** Join the WAL directory and a file name.
@param[in]  dir   WAL directory
@param[in]  name  segment or lock file name
@return the path of a file in the WAL directory */
static std::string wal_path(const std::string &dir, const std::string &name) {
  return (dir + "/" + name);
}

/** Create a segment file whose first record will be first_seq. The file
appears under its final name only after its header is durable.
@param[in]   dir        WAL directory
@param[in]   wal_id     WAL id
@param[in]   first_seq  sequence number of the segment's first record
@param[out]  file       the open segment
@return DB_SUCCESS or DB_IO_ERROR */
static dberr_t wal_segment_create(const std::string &dir, uint64_t wal_id,
                                  batch_seq_t first_seq, os_file_t &file) {
  file = OS_FILE_CLOSED;

  const std::string path = wal_path(dir, wal_segment_name(first_seq));
  const std::string init_path = path + ".init";

  /* A leftover init file is from a create that never finished; it was
  never published under the final name. */
  dberr_t err = os_file_delete_if_exists(init_path.c_str());
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = os_file_open(init_path.c_str(), OS_FILE_CREATE, file);
  if (err != DB_SUCCESS) {
    return (err);
  }

  unsigned char header[WAL_FILE_HEADER_SIZE];
  wal_encode_file_header(header, wal_id, first_seq);

  err = os_file_write(file, header, sizeof(header), 0);

  if (err == DB_SUCCESS) {
    err = os_file_flush(file);
  }

  if (err == DB_SUCCESS) {
    err = os_file_rename_noreplace(init_path.c_str(), path.c_str());
  }

  if (err == DB_SUCCESS) {
    err = os_dir_flush(dir.c_str());
  }

  if (err != DB_SUCCESS) {
    (void)os_file_close(file);
  }

  return (err);
}

/** Encode the records of writers first..last into wal.buf. Called by the
leader without the mutex; reads next only of writers before last, which no
other thread modifies.
@param[in,out]  wal    WAL
@param[in]      first  first writer of the group
@param[in]      last   last writer of the group
@param[in]      bytes  total encoded size of the group
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
static dberr_t wal_encode_group(wal_t &wal, wal_writer_t &first,
                                wal_writer_t &last, size_t bytes) {
  if (bytes > wal.buf_size) {
    unsigned char *buf = (unsigned char *)realloc(wal.buf, bytes);
    if (buf == nullptr) {
      return (DB_OUT_OF_MEMORY);
    }

    wal.buf = buf;
    wal.buf_size = bytes;
  }

  unsigned char *p = wal.buf;
  uint32_t index = 0;
  wal_writer_t *w = &first;

  for (;;) {
    wal_encode_record(p, w->seq, index, w->payload, w->len);
    p += WAL_RECORD_HEADER_SIZE + w->len;
    ++index;

    if (w == &last) {
      break;
    }
    w = w->next;
  }

  ut_a(p == wal.buf + bytes);

  return (DB_SUCCESS);
}

/** Start a new segment for a group that would overflow the current one.
Called by the leader without the mutex. The old segment is complete: its last
group was flushed before this group was formed.
@param[in,out]  wal        WAL
@param[in]      first_seq  sequence number of the group's first record
@return DB_SUCCESS or DB_IO_ERROR */
static dberr_t wal_rotate(wal_t &wal, batch_seq_t first_seq) {
  os_file_t file;

  const dberr_t err = wal_segment_create(wal.dir, wal.wal_id, first_seq, file);
  if (err != DB_SUCCESS) {
    return (err);
  }

  /* Everything in the old segment is durable; a close error cannot lose
  any of it. */
  (void)os_file_close(wal.file);

  wal.file = file;
  wal.offset = WAL_FILE_HEADER_SIZE;

  return (DB_SUCCESS);
}

/** Write and flush a group starting at the queue head, then complete every
writer in it. Called by the leader, which is the queue head, with the mutex
held; the mutex is released during I/O and held again on return.
@param[in,out]  wal   WAL
@param[in,out]  lock  holds wal.mutex */
static void wal_write_group(wal_t &wal, std::unique_lock<std::mutex> &lock) {
  /* Take queued writers into the group until the size limit. The leader
  is always included, even if its record alone exceeds the limit. */
  wal_writer_t *first = wal.head;
  wal_writer_t *last = first;
  size_t bytes = WAL_RECORD_HEADER_SIZE + first->len;

  while (last->next != nullptr) {
    const size_t rec = WAL_RECORD_HEADER_SIZE + last->next->len;

    if (bytes + rec > wal.group_max_bytes) {
      break;
    }

    bytes += rec;
    last = last->next;
  }

  const bool fail_flush = wal.dbg_fail_next_flush;
  wal.dbg_fail_next_flush = false;

  /* A group that does not fit goes to a new segment, unless the current
  one is still empty. */
  const batch_seq_t old_first = wal.current_first;
  const bool rotate = wal.offset > WAL_FILE_HEADER_SIZE &&
                      wal.offset + bytes > wal.segment_bytes;

  /* Encode, write and flush without the mutex, so new writers can queue
  up behind this group. */
  lock.unlock();

  dberr_t err = DB_SUCCESS;
  bool rotated = false;

  if (rotate) {
    err = wal_rotate(wal, first->seq);
    rotated = err == DB_SUCCESS;
  }

  if (err == DB_SUCCESS) {
    err = wal_encode_group(wal, *first, *last, bytes);
  }

  if (err == DB_SUCCESS) {
    err = os_file_write(wal.file, wal.buf, bytes, wal.offset);
  }

  if (err == DB_SUCCESS) {
    err = fail_flush ? DB_IO_ERROR : os_file_flush(wal.file);
  }

  lock.lock();

  /* The new segment becomes current; the old one is closed. */
  if (rotated) {
    wal.closed.push_back(old_first);
    wal.current_first = first->seq;
    wal.stats.segments++;
  }

  /* Complete every writer of the group. */
  uint64_t records = 0;
  wal_writer_t *w = first;

  for (;;) {
    wal_writer_t *next = w->next;

    w->err = err;
    w->done = true;

    if (w != first) {
      /* The waiter cannot return and free w before we release the mutex. */
      w->cv.notify_one();
    }

    ++records;

    if (w == last) {
      wal.head = next;
      break;
    }
    w = next;
  }

  if (err == DB_SUCCESS) {
    wal.offset += bytes;
    wal.stats.records += records;
    wal.stats.groups++;
    wal.stats.bytes += bytes;
  } else {
    /* Sequence numbers of the failed group are used up, so no later
    record may be written; writers queued behind it are rejected. */
    wal.fenced = true;

    while (wal.head != nullptr) {
      w = wal.head;
      wal.head = w->next;

      w->err = DB_FENCED;
      w->done = true;
      w->cv.notify_one();
    }
  }

  if (wal.head == nullptr) {
    wal.tail = nullptr;
  } else {
    /* Hand leadership to the new queue head. */
    wal.head->cv.notify_one();
  }
}

/** List the segments of a directory, oldest first, and remove leftovers of
segment creations that never finished.
@param[in]   dir     WAL directory
@param[out]  firsts  first sequence number of every segment
@return DB_SUCCESS or DB_IO_ERROR */
static dberr_t wal_list_segments(const std::string &dir,
                                 std::vector<batch_seq_t> &firsts) {
  firsts.clear();

  std::vector<std::string> names;

  dberr_t err = os_dir_list(dir.c_str(), names);
  if (err != DB_SUCCESS) {
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
      if (err != DB_SUCCESS) {
        return (err);
      }
    }
  }

  std::sort(firsts.begin(), firsts.end());

  return (DB_SUCCESS);
}

/** Own a descriptor until it is transferred to wal_t. */
struct wal_file_guard_t {
  /** Descriptor to close; OS_FILE_CLOSED once released. */
  os_file_t file{OS_FILE_CLOSED};

  /** Close a descriptor whose ownership was not transferred. */
  ~wal_file_guard_t() { (void)os_file_close(file); }

  /** Transfer the owned file descriptor without closing it.
  @return the descriptor previously owned by this guard; the guard is left
    closed */
  os_file_t release() {
    const os_file_t value = file;
    file = OS_FILE_CLOSED;

    return (value);
  }
};

/** Make a wal_t. The caller retains the segment until attachment succeeds.
@param[in]      dir              database or WAL directory
@param[in,out]  file             guard whose segment descriptor is transferred
                                 on success
@param[in]      group_max_bytes  maximum bytes in one WAL group; 0 selects the
                                 default
@param[out]     out              new WAL handle; unchanged on failure
@return DB_SUCCESS or DB_OUT_OF_MEMORY; descriptor ownership transfers only on
  success */
static dberr_t wal_attach(const std::string &dir, wal_file_guard_t &file,
                          size_t group_max_bytes, wal_t **out) {
  wal_t *wal = ut_new<wal_t>();

  if (wal == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  wal->dir = dir;

  /* Other fields start from their member initializers. */
  wal->file = file.release();

  if (group_max_bytes != 0) {
    wal->group_max_bytes = group_max_bytes;
  }

  *out = wal;

  return (DB_SUCCESS);
}

/** Create a new, empty WAL file in a directory.
@param[in]   dir              existing directory; must not contain a WAL
@param[in]   group_max_bytes  upper bound on bytes per group write, 0 for the
                              default; a single record may exceed it
@param[out]  out              new WAL, or NULL on failure
@param[in]   wal_id           stored database identity; 0 generates one
@return DB_SUCCESS, DB_IO_ERROR, DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
dberr_t wal_create(const char *dir, size_t group_max_bytes, wal_t **out,
                   uint64_t wal_id) {
  *out = nullptr;

  if (dir == nullptr) {
    return (DB_INVALID_ARGUMENT);
  }

  std::string directory;
  dberr_t err = os_dir_resolve(dir, directory);
  if (err != DB_SUCCESS) {
    return (err);
  }
  dir = directory.c_str();

  /* Only an empty directory gets a new WAL. */
  std::vector<batch_seq_t> firsts;
  err = wal_list_segments(dir, firsts);

  if (err == DB_SUCCESS && !firsts.empty()) {
    err = DB_IO_ERROR;
  }

  if (err == DB_SUCCESS && wal_id == 0) {
    err = os_random_id(wal_id);
  }
  wal_file_guard_t file;

  if (err == DB_SUCCESS) {
    err = wal_segment_create(dir, wal_id, 1, file.file);
  }

  if (err != DB_SUCCESS) {
    return (err);
  }

  err = wal_attach(dir, file, group_max_bytes, out);

  if (err == DB_SUCCESS) {
    (*out)->wal_id = wal_id;
    (*out)->stats.segments = 1;
  }

  return (err);
}

/** Check whether a directory holds a WAL.
@param[in]   dir     directory
@param[out]  exists  true if the WAL file exists
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_IO_ERROR */
dberr_t wal_exists(const char *dir, bool &exists) {
  exists = false;

  if (dir == nullptr) {
    return (DB_INVALID_ARGUMENT);
  }

  std::vector<batch_seq_t> firsts;

  const dberr_t err = wal_list_segments(dir, firsts);
  if (err != DB_SUCCESS) {
    return (err);
  }

  exists = !firsts.empty();

  return (DB_SUCCESS);
}

/** Check the database identity before callbacks or tail truncation.
@param[in]  file         open WAL segment descriptor
@param[in]  expected_id  expected WAL identity; 0 disables the identity check
@return DB_SUCCESS, DB_IO_ERROR, or DB_CORRUPTION for a missing/invalid header
  or identity mismatch */
static dberr_t wal_check_identity(os_file_t file, uint64_t expected_id) {
  uint64_t size = 0;
  dberr_t err = os_file_get_size(file, size);
  if (err != DB_SUCCESS) {
    return (err);
  }
  if (size < WAL_FILE_HEADER_SIZE) {
    return (DB_CORRUPTION);
  }

  unsigned char header[WAL_FILE_HEADER_SIZE];
  err = os_file_read(file, header, sizeof(header), 0);
  if (err != DB_SUCCESS) {
    return (err);
  }
  uint64_t id = 0;
  batch_seq_t first = 0;
  if (!wal_file_header_read(header, id, first) || id != expected_id) {
    return (DB_CORRUPTION);
  }

  return (DB_SUCCESS);
}

/** Recover the segments in order. On success the newest one is left open in
*file with its unfinished tail cut off.
@param[in]   dir          database or WAL directory
@param[in]   firsts       segment first sequence numbers, sorted from oldest
                          to newest
@param[in]   apply        record replay callback; may be NULL
@param[in]   ctx          context passed to apply
@param[out]  file         newest segment left open on success; closed on
                          failure
@param[out]  recv         recovery counters, final sequence and tail boundary
@param[in]   expected_id  expected WAL identity; 0 disables the identity check
@return DB_SUCCESS, a segment I/O/corruption error, or the replay callback
  error */
static dberr_t wal_recover_segments(const std::string &dir,
                                    const std::vector<batch_seq_t> &firsts,
                                    wal_apply_t apply, void *ctx,
                                    os_file_t &file, wal_recv_t &recv,
                                    uint64_t expected_id) {
  recv.first_seq = firsts[0];
  recv.last_seq = firsts[0] - 1;

  for (size_t i = 0; i < firsts.size(); ++i) {
    const bool is_last = i + 1 == firsts.size();

    /* Each segment continues where the previous one ended. */
    if (firsts[i] != recv.last_seq + 1) {
      return (DB_CORRUPTION);
    }

    const std::string path = wal_path(dir, wal_segment_name(firsts[i]));

    dberr_t err = os_file_open(path.c_str(), OS_FILE_OPEN, file);
    if (err != DB_SUCCESS) {
      return (err);
    }

    if (expected_id != 0) {
      err = wal_check_identity(file, expected_id);
      if (err != DB_SUCCESS) {
        (void)os_file_close(file);

        return (err);
      }
    }

    wal_recv_t seg;
    err = wal_recv_scan(file, firsts[i], is_last, apply, ctx, seg);

    if (err == DB_SUCCESS && i > 0 && seg.wal_id != recv.wal_id) {
      err = DB_CORRUPTION;
    }

    if (err != DB_SUCCESS || !is_last) {
      (void)os_file_close(file);
    }

    if (err != DB_SUCCESS) {
      return (err);
    }

    recv.wal_id = seg.wal_id;
    recv.records += seg.records;
    recv.last_seq = seg.last_seq;
    recv.end_offset = seg.end_offset;
    recv.tail_bytes = seg.tail_bytes;
    recv.segments++;
  }

  /* Cut off an unfinished group so new records directly follow the last
  recovered one. */
  if (recv.tail_bytes > 0) {
    dberr_t err = os_file_truncate(file, recv.end_offset);

    if (err == DB_SUCCESS) {
      err = os_file_flush(file);
    }

    if (err != DB_SUCCESS) {
      (void)os_file_close(file);

      return (err);
    }
  }

  return (DB_SUCCESS);
}

/** Open an existing WAL and recover it: replay every valid record through
apply, cut off an unfinished last group, and leave the WAL ready to append
after the last recovered record.
@param[in]   dir              directory holding the WAL
@param[in]   group_max_bytes  as for wal_create()
@param[in]   apply            callback per recovered record, or NULL
@param[in]   ctx              passed to apply
@param[out]  out              open WAL, or NULL on failure
@param[out]  recv             what recovery found
@param[in]   expected_id      database identity checked before replay; 0 for a
                              standalone WAL
@return DB_SUCCESS, DB_CORRUPTION, DB_IO_ERROR, DB_INVALID_ARGUMENT,
  DB_OUT_OF_MEMORY or the error returned by apply */
dberr_t wal_open(const char *dir, size_t group_max_bytes, wal_apply_t apply,
                 void *ctx, wal_t **out, wal_recv_t &recv,
                 uint64_t expected_id) {
  *out = nullptr;
  recv = wal_recv_t();

  if (dir == nullptr) {
    return (DB_INVALID_ARGUMENT);
  }

  std::string directory;
  dberr_t err = os_dir_resolve(dir, directory);
  if (err != DB_SUCCESS) {
    return (err);
  }
  dir = directory.c_str();

  std::vector<batch_seq_t> firsts;
  err = wal_list_segments(dir, firsts);

  if (err == DB_SUCCESS && firsts.empty()) {
    err = DB_IO_ERROR;
  }

  wal_file_guard_t file;

  if (err == DB_SUCCESS) {
    err = wal_recover_segments(dir, firsts, apply, ctx, file.file, recv,
                               expected_id);
  }

  if (err != DB_SUCCESS) {
    return (err);
  }

  err = wal_attach(dir, file, group_max_bytes, out);
  if (err != DB_SUCCESS) {
    return (err);
  }

  wal_t *wal = *out;
  wal->wal_id = recv.wal_id;
  wal->offset = recv.end_offset;
  wal->next_seq = recv.last_seq + 1;
  wal->current_first = firsts.back();
  wal->closed.assign(firsts.begin(), firsts.end() - 1);
  wal->stats.segments = firsts.size();

  return (DB_SUCCESS);
}

/** Close a WAL.
@param[in]  wal  WAL from wal_create() or wal_open(); may be NULL
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t wal_close(wal_t *wal) {
  if (wal == nullptr) {
    return (DB_SUCCESS);
  }

  wal->mutex.lock();
  ut_a(wal->head == nullptr);
  wal->mutex.unlock();

  const dberr_t err = os_file_close(wal->file);

  free(wal->buf);
  ut_delete(wal);

  return (err);
}

/** Set the size at which the next group starts a new segment.
@param[in]  wal    WAL
@param[in]  bytes  segment size */
void wal_set_segment_bytes(wal_t &wal, uint64_t bytes) {
  wal.mutex.lock();
  wal.segment_bytes = bytes;
  wal.mutex.unlock();
}

/** Delete the closed segments that hold only stored records.
@param[in]   wal          WAL
@param[in]   flushed_seq  every record up to here is flushed to storage
@param[out]  removed      segments deleted
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t wal_recycle(wal_t &wal, batch_seq_t flushed_seq, uint64_t &removed) {
  removed = 0;

  /* Only one recycler may choose and delete the oldest segment. Keep
  appenders free to run while deletion and directory sync are in progress. */
  std::lock_guard<std::mutex> recycle_lock(wal.recycle_mutex);

  for (;;) {
    batch_seq_t first;

    {
      std::lock_guard<std::mutex> lock(wal.mutex);

      if (wal.closed.empty()) {
        return (DB_SUCCESS);
      }

      const batch_seq_t next_first =
          wal.closed.size() > 1 ? wal.closed[1] : wal.current_first;

      /* A closed segment holds only stored records exactly when the
      next segment starts at or before the first not stored record. */
      if (next_first - 1 > flushed_seq) {
        return (DB_SUCCESS);
      }

      first = wal.closed.front();
    }

    const std::string path = wal_path(wal.dir, wal_segment_name(first));

    dberr_t err = os_file_delete_if_exists(path.c_str());
    if (err != DB_SUCCESS) {
      return (err);
    }

    /* Make each prefix deletion durable before deleting the next file.
    On failure keep this entry: retrying a missing file is harmless and
    retries its directory sync before advancing to a newer segment. */
    err = os_dir_flush(wal.dir.c_str());
    if (err != DB_SUCCESS) {
      return (err);
    }

    {
      std::lock_guard<std::mutex> lock(wal.mutex);
      ut_a(!wal.closed.empty() && wal.closed.front() == first);
      wal.closed.erase(wal.closed.begin());
      wal.stats.segments--;
    }

    removed++;
  }
}

/** Give a record its sequence number and queue it, without waiting.
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
                           wal_writer_t &w, batch_seq_t &seq) {
  seq = 0;

  if (payload == nullptr || len == 0 || len > WAL_MAX_PAYLOAD) {
    return (DB_INVALID_ARGUMENT);
  }

  /* The writer may be reused; reset everything but the condition
  variable, which cannot be copied. */
  w.payload = static_cast<const unsigned char *>(payload);
  w.len = len;
  w.seq = 0;
  w.done = false;
  w.err = DB_SUCCESS;
  w.next = nullptr;

  std::lock_guard<std::mutex> lock(wal.mutex);

  if (wal.fenced) {
    return (DB_FENCED);
  }

  /* Join the queue; the sequence number fixes the record's place in the
  file. */
  w.seq = wal.next_seq++;

  if (wal.tail == nullptr) {
    wal.head = &w;
  } else {
    wal.tail->next = &w;
  }
  wal.tail = &w;

  seq = w.seq;

  return (DB_SUCCESS);
}

/** Wait until a queued record is durable, writing a group as leader if this
writer reaches the head of the queue.
@param[in]      wal  WAL
@param[in,out]  w    entry from wal_append_enqueue()
@retval DB_SUCCESS durable
@retval DB_FENCED not written; an earlier record failed
@retval DB_IO_ERROR outcome unknown; the WAL is now fenced
@retval DB_OUT_OF_MEMORY not written; the WAL is now fenced */
dberr_t wal_append_wait(wal_t &wal, wal_writer_t &w) {
  std::unique_lock<std::mutex> lock(wal.mutex);

  /* Wait until a leader has written this record, or until this writer
  reaches the head of the queue and becomes the leader itself. The head
  is checked before waiting, so a hand-over made before this call is not
  missed. */
  while (!w.done && wal.head != &w) {
    w.cv.wait(lock);
  }

  if (!w.done) {
    wal_write_group(wal, lock);
  }

  ut_ad(w.done);

  return (w.err);
}

/** Read the counters.
@param[in]   wal    WAL
@param[out]  stats  counters */
void wal_get_stats(wal_t &wal, wal_stats_t &stats) {
  wal.mutex.lock();
  stats = wal.stats;
  wal.mutex.unlock();
}

#ifdef PT_DEBUG
/** Make the next group flush fail with DB_IO_ERROR after its data has been
written.
@param[in]  wal  WAL */
void wal_dbg_fail_next_flush(wal_t &wal) {
  wal.mutex.lock();
  wal.dbg_fail_next_flush = true;
  wal.mutex.unlock();
}
#endif
