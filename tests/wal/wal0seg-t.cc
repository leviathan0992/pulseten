/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal0seg-t.cc
 *    Tests for WAL segments: rotation, recovery across segments, recycling
 *    while appends run, and damaged or missing segments.
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "os0file.h"
#include "test0util.h"
#include "test0wal.h"
#include "ut0byte.h"
#include "wal0wal.h"

/** Small segments, so that a few records rotate. */
static const uint64_t SEGMENT_BYTES = 300;

/** Build the deterministic payload of a test record.
@param[in]  seq  write or record sequence number
@return the payload of record seq */
static std::string payload_for(uint64_t seq) {
  return ("record-" + std::to_string(seq));
}

/** Append records with the next sequence numbers up to last.
@param[in,out]  wal    write-ahead log
@param[in]      first  first record number, inclusive
@param[in]      last   last record number, inclusive */
static void append_upto(wal_t *wal, uint64_t first, uint64_t last) {
  for (uint64_t s = first; s <= last; ++s) {
    const std::string p = payload_for(s);
    batch_seq_t seq = 0;

    CHECK(append_record(*wal, p.data(), p.size(), seq) == DB_SUCCESS);
    CHECK(seq == s);
  }
}

struct collect_t {
  /** Recovered payloads in sequence order. */
  std::vector<std::string> payloads;

  /** Sequence number of payloads[0]. */
  batch_seq_t first{0};
};

/** Replay callback: collect consecutive payloads from the first one.
@param[in,out]  ctx      replay collector receiving record sequences and
                         payloads
@param[in]      seq      sequence number being replayed
@param[in]      payload  encoded record payload
@param[in]      len      input length in bytes
@return DB_SUCCESS, or DB_ERROR when this test injects a replay failure */
static dberr_t collect_apply(void *ctx, batch_seq_t seq,
                             const unsigned char *payload, size_t len) {
  collect_t *c = static_cast<collect_t *>(ctx);

  if (c->payloads.empty()) {
    c->first = seq;
  } else if (seq != c->first + c->payloads.size()) {
    return (DB_CORRUPTION);
  }

  c->payloads.push_back(std::string((const char *)payload, len));

  return (DB_SUCCESS);
}

/** Open, collect and close.
@param[in]      dir   database or WAL directory
@param[in,out]  c     collector receiving the recovered records
@param[out]     recv  WAL scan counters and boundaries
@return result of wal_open() */
static dberr_t recover(const test_dir_t &dir, collect_t *c, wal_recv_t *recv) {
  wal_t *wal = nullptr;

  const dberr_t err = wal_open(dir.path, 0, collect_apply, c, &wal, *recv);
  if (wal != nullptr) {
    CHECK(wal_close(wal) == DB_SUCCESS);
  }

  return (err);
}

/** Check that recovery produced exactly a consecutive record range.
@param[in]  c      recovered sequences and payloads to inspect
@param[in]  first  first record number, inclusive
@param[in]  last   last record number, inclusive
@return true if c holds exactly records first..last */
static bool has_range(const collect_t &c, uint64_t first, uint64_t last) {
  if (c.payloads.size() != last - first + 1 || c.first != first) {
    return (false);
  }

  for (uint64_t s = first; s <= last; ++s) {
    if (c.payloads[s - first] != payload_for(s)) {
      return (false);
    }
  }

  return (true);
}

/** Build the path of the segment starting at a given sequence number.
@param[in]  dir    database or WAL directory
@param[in]  first  first record number, inclusive
@return path of the segment that starts at first */
static std::string segment_path(const test_dir_t &dir, uint64_t first) {
  return (dir.file(wal_segment_name(first).c_str()));
}

/** List segment first sequence numbers in increasing order.
@param[in]  dir  database or WAL directory
@return first sequence numbers of the segments in the directory */
static std::vector<uint64_t> segments(const test_dir_t &dir) {
  std::vector<std::string> names;
  std::vector<uint64_t> firsts;

  CHECK(os_dir_list(dir.path, names) == DB_SUCCESS);

  for (size_t i = 0; i < names.size(); ++i) {
    batch_seq_t first = 0;
    if (wal_segment_parse_name(names[i].c_str(), first)) {
      firsts.push_back(first);
    }
  }

  std::sort(firsts.begin(), firsts.end());

  return (firsts);
}

/** Create a WAL with small segments holding records 1..n.
@param[in]  dir  database or WAL directory
@param[in]  n    number of records to create */
static void make_wal(const test_dir_t &dir, uint64_t n) {
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(wal, 1, n);
  CHECK(wal_close(wal) == DB_SUCCESS);
}

/** Appends start a new segment once one is full; reopening reads them all. */
static void test_rotation() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(wal, 1, 40);

  wal_stats_t stats;
  wal_get_stats(*wal, stats);
  CHECK(stats.segments > 3);
  CHECK(stats.segments == segments(dir).size());
  CHECK(wal_close(wal) == DB_SUCCESS);

  /* Recovery reads every segment in order. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_SUCCESS);
  CHECK(has_range(c, 1, 40));
  CHECK(recv.first_seq == 1 && recv.last_seq == 40);
  CHECK(recv.segments == stats.segments);

  /* Appending after a reopen continues the last segment, then rotates. */
  CHECK(wal_open(dir.path, 0, nullptr, nullptr, &wal, recv) == DB_SUCCESS);
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(wal, 41, 60);
  CHECK(wal_close(wal) == DB_SUCCESS);

  collect_t again;
  CHECK(recover(dir, &again, &recv) == DB_SUCCESS);
  CHECK(has_range(again, 1, 60));
}

/** Recycling deletes the closed segments whose records are all stored, never
the current one. */
static void test_recycle() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(wal, 1, 40);

  const std::vector<uint64_t> before = segments(dir);

  /* Records up to 15 are stored elsewhere: segments wholly below 16 go. */
  uint64_t removed = 0;
  CHECK(wal_recycle(*wal, 15, removed) == DB_SUCCESS);
  CHECK(removed > 0);

  const std::vector<uint64_t> after = segments(dir);
  CHECK(after.size() == before.size() - removed);
  CHECK(after.front() <= 16);

  /* Nothing more to remove for the same point. */
  CHECK(wal_recycle(*wal, 15, removed) == DB_SUCCESS);
  CHECK(removed == 0);

  wal_stats_t stats;
  wal_get_stats(*wal, stats);
  CHECK(stats.segments == after.size());
  CHECK(wal_close(wal) == DB_SUCCESS);

  /* Recovery starts at the oldest remaining segment. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_SUCCESS);
  CHECK(recv.first_seq == after.front());
  CHECK(has_range(c, after.front(), 40));

  /* With everything stored, only the current segment remains. */
  CHECK(wal_open(dir.path, 0, nullptr, nullptr, &wal, recv) == DB_SUCCESS);
  CHECK(wal_recycle(*wal, 1000, removed) == DB_SUCCESS);
  CHECK(segments(dir).size() == 1);
  append_upto(wal, 41, 45);
  CHECK(wal_close(wal) == DB_SUCCESS);

  collect_t last;
  CHECK(recover(dir, &last, &recv) == DB_SUCCESS);
  CHECK(last.payloads.back() == payload_for(45));
}

struct append_arg_t {
  /** WAL under test. */
  wal_t *wal{nullptr};

  /** Records to append. */
  int count{0};

  /** Calls that failed. */
  int errors{0};

  /** Appenders that have finished, shared by all of them. */
  int *finished{nullptr};

  /** Protects *finished. */
  std::mutex *mutex{nullptr};
};

/** Append count records, counting failures.
@param[in,out]  arg  append worker arguments and failure count */
static void append_main(append_arg_t *arg) {
  for (int i = 0; i < arg->count; ++i) {
    const std::string p = "concurrent-payload";
    batch_seq_t seq = 0;

    if (append_record(*arg->wal, p.data(), p.size(), seq) != DB_SUCCESS) {
      arg->errors++;
    }
  }

  std::lock_guard<std::mutex> lock(*arg->mutex);
  (*arg->finished)++;
}

/** Recycling runs safely while writers append and rotate segments. */
static void test_recycle_during_appends() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);

  std::vector<append_arg_t> args(4);
  std::vector<std::thread> threads;
  std::mutex mutex;
  int finished = 0;

  for (size_t t = 0; t < args.size(); ++t) {
    args[t].wal = wal;
    args[t].count = 200;
    args[t].finished = &finished;
    args[t].mutex = &mutex;
    threads.push_back(std::thread(append_main, &args[t]));
  }

  /* Recycle behind the appends until they finish, as flushes would. */
  uint64_t total_removed = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (finished == (int)args.size()) {
        break;
      }
    }

    wal_stats_t stats;
    wal_get_stats(*wal, stats);

    uint64_t removed = 0;
    CHECK(wal_recycle(*wal, stats.records / 2, removed) == DB_SUCCESS);
    total_removed += removed;
  }

  for (size_t t = 0; t < threads.size(); ++t) {
    threads[t].join();
    CHECK(args[t].errors == 0);
  }

  CHECK(total_removed > 0);
  CHECK(wal_close(wal) == DB_SUCCESS);
  printf("recycle during appends: %llu segments removed\n",
         (unsigned long long)total_removed);

  /* What remains is a gapless run ending at the last record. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_SUCCESS);
  CHECK(recv.last_seq == 800);
  CHECK(c.payloads.size() == 800 - recv.first_seq + 1);
}

/** A segment missing from the middle is corruption. */
static void test_missing_segment() {
  test_dir_t dir;
  make_wal(dir, 40);

  const std::vector<uint64_t> firsts = segments(dir);
  CHECK(firsts.size() > 3);

  /* A gap in the middle is corruption. */
  CHECK(unlink(segment_path(dir, firsts[1]).c_str()) == 0);

  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
}

/** Replace the contents of a file.
@param[in]  path  file or directory path
@param[in]  data  replacement file contents */
static void write_file(const std::string &path,
                       const std::vector<unsigned char> &data) {
  os_file_t file;
  CHECK(os_file_open(path.c_str(), OS_FILE_OPEN_OR_CREATE, file) == DB_SUCCESS);
  CHECK(os_file_truncate(file, 0) == DB_SUCCESS);
  if (!data.empty()) {
    CHECK(os_file_write(file, &data[0], data.size(), 0) == DB_SUCCESS);
  }
  CHECK(os_file_close(file) == DB_SUCCESS);
}

/** Read the bytes of a test file.
@param[in]  path  file or directory path
@return the bytes of a file */
static std::vector<unsigned char> read_file(const std::string &path) {
  std::vector<unsigned char> data;
  os_file_t file;
  uint64_t size = 0;

  CHECK(os_file_open(path.c_str(), OS_FILE_OPEN, file) == DB_SUCCESS);
  CHECK(os_file_get_size(file, size) == DB_SUCCESS);
  data.resize(size);
  CHECK(os_file_read(file, &data[0], size, 0) == DB_SUCCESS);
  CHECK(os_file_close(file) == DB_SUCCESS);

  return (data);
}

/** Damage or a cut in a closed segment is corruption, not an unfinished
write. */
static void test_damaged_closed_segment() {
  /* A cut at the end of a closed segment: unlike the last segment, it is
  not an unfinished write. */
  {
    test_dir_t dir;
    make_wal(dir, 40);

    const std::vector<uint64_t> firsts = segments(dir);
    std::vector<unsigned char> data = read_file(segment_path(dir, firsts[0]));
    data.resize(data.size() - 1);
    write_file(segment_path(dir, firsts[0]), data);

    collect_t c;
    wal_recv_t recv;
    CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
  }

  /* Bytes after the last record of a closed segment. */
  {
    test_dir_t dir;
    make_wal(dir, 40);

    const std::vector<uint64_t> firsts = segments(dir);
    std::vector<unsigned char> data = read_file(segment_path(dir, firsts[1]));
    data.push_back(0);
    write_file(segment_path(dir, firsts[1]), data);

    collect_t c;
    wal_recv_t recv;
    CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
  }

  /* A segment of another WAL. */
  {
    test_dir_t dir;
    make_wal(dir, 40);

    const std::vector<uint64_t> firsts = segments(dir);
    std::vector<unsigned char> data = read_file(segment_path(dir, firsts[1]));
    wal_encode_file_header(&data[0], ut_read_u64(&data[16]) + 1, firsts[1]);
    write_file(segment_path(dir, firsts[1]), data);

    collect_t c;
    wal_recv_t recv;
    CHECK(recover(dir, &c, &recv) == DB_CORRUPTION);
  }
}

/** A crash just after a rotation, leaving a header-only segment, recovers. */
static void test_crash_after_rotation() {
  test_dir_t dir;
  make_wal(dir, 20);

  /* A crash after a new segment was published but before its first group
  was written leaves a segment with only a header, and maybe an unfinished
  init file of the one after. */
  const std::vector<uint64_t> firsts = segments(dir);
  std::vector<unsigned char> header =
      read_file(segment_path(dir, firsts.back()));
  header.resize(WAL_FILE_HEADER_SIZE);
  wal_encode_file_header(&header[0], ut_read_u64(&header[16]), 21);

  write_file(segment_path(dir, 21), header);
  write_file(segment_path(dir, 30) + ".init", header);

  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, &c, &recv) == DB_SUCCESS);
  CHECK(has_range(c, 1, 20));
  CHECK(recv.last_seq == 20);

  struct stat st;
  CHECK(stat((segment_path(dir, 30) + ".init").c_str(), &st) != 0);

  /* Appends continue in the empty segment. */
  wal_t *wal = nullptr;
  CHECK(wal_open(dir.path, 0, nullptr, nullptr, &wal, recv) == DB_SUCCESS);
  append_upto(wal, 21, 22);
  CHECK(wal_close(wal) == DB_SUCCESS);

  collect_t again;
  CHECK(recover(dir, &again, &recv) == DB_SUCCESS);
  CHECK(has_range(again, 1, 22));
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_rotation();
  test_recycle();
  test_recycle_during_appends();
  test_missing_segment();
  test_damaged_closed_segment();
  test_crash_after_rotation();

  return (test_result("wal0seg-t"));
}
