/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_seg-t.cc
 *    Tests for WAL segments: rotation, recovery across segments, purging
 *    while appends run, and damaged or missing segments.
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "os_file.h"
#include "test_util.h"
#include "test_wal.h"
#include "ut.h"
#include "ut_byte.h"
#include "ut_err.h"
#include "wal_format.h"
#include "wal_recv.h"

/** Small segments, so that a few records rotate. */
static const uint64_t SEGMENT_BYTES = 300;

/** Build the deterministic payload of a test record.
@return the payload of record seq */
static std::string payload_for(uint64_t seq) {
  return ("record-" + std::to_string(seq));
}

/** Append records with the next sequence numbers up to last.
@param[in]  first  first record number, inclusive
@param[in]  last   last record number, inclusive */
static void append_upto(wal_t &wal, uint64_t first, uint64_t last) {
  for (uint64_t s = first; s <= last; ++s) {
    const std::string p = payload_for(s);
    batch_seq_t seq = 0;

    CHECK(append_record(wal, p.data(), p.size(), seq).ok());
    CHECK(seq == s);
  }
}

struct collect_t {
  /** Recovered payloads in sequence order. */
  std::vector<std::string> m_payloads;

  /** Sequence number of payloads[0]. */
  batch_seq_t m_first{0};
};

/** Replay callback: collect consecutive payloads from the first one.
@return DB_SUCCESS, or DB_ERROR when this test injects a replay failure */
static db_status_t collect_apply(void *ctx, batch_seq_t seq,
                                 const unsigned char *payload, size_t len) {
  collect_t *c = static_cast<collect_t *>(ctx);

  if (c->m_payloads.empty()) {
    c->m_first = seq;
  } else if (seq != c->m_first + c->m_payloads.size()) {
    return (DB_CORRUPTION);
  }

  c->m_payloads.push_back(
      std::string(reinterpret_cast<const char *>(payload), len));

  return (DB_SUCCESS);
}

/** Open, collect and close.
@return result of wal_open() */
static db_status_t recover(const test_dir_t &dir, collect_t &c,
                           wal_recv_t &recv) {
  wal_t *wal = nullptr;

  const db_status_t err =
      wal_open(dir.m_path, 0, collect_apply, &c, &wal, recv);
  if (wal != nullptr) {
    CHECK(wal_close(wal).ok());
  }

  return (err);
}

/** Check that recovery produced exactly a consecutive record range.
@param[in]  first  first record number, inclusive
@param[in]  last   last record number, inclusive
@return true if c holds exactly records first..last */
static bool has_range(const collect_t &c, uint64_t first, uint64_t last) {
  if (c.m_payloads.size() != last - first + 1 || c.m_first != first) {
    return (false);
  }

  for (uint64_t s = first; s <= last; ++s) {
    if (c.m_payloads[s - first] != payload_for(s)) {
      return (false);
    }
  }

  return (true);
}

/** Build the path of the segment starting at a given sequence number.
@param[in]  first  first record number, inclusive
@return path of the segment that starts at first */
static std::string segment_path(const test_dir_t &dir, uint64_t first) {
  return (dir.file(wal_segment_name(first).c_str()));
}

/** List segment first sequence numbers in increasing order.
@return first sequence numbers of the segments in the directory */
static std::vector<uint64_t> segments(const test_dir_t &dir) {
  std::vector<std::string> names;
  std::vector<uint64_t> firsts;

  CHECK(os_dir_list(dir.m_path, names).ok());

  for (size_t i = 0; i < names.size(); ++i) {
    batch_seq_t first = 0;
    if (wal_segment_parse_name(names[i].c_str(), first)) {
      firsts.push_back(first);
    }
  }

  std::sort(firsts.begin(), firsts.end());

  return (firsts);
}

/** Create a WAL with small segments holding records 1..n. */
static void make_wal(const test_dir_t &dir, uint64_t n) {
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.m_path, 0, &wal).ok());
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(*wal, 1, n);
  CHECK(wal_close(wal).ok());
}

/** Appends start a new segment once one is full; reopening reads them all. */
static void test_rotation() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.m_path, 0, &wal).ok());
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(*wal, 1, 40);

  wal_stats_t stats;
  wal_get_stats(*wal, stats);
  CHECK(stats.m_segments > 3);
  CHECK(stats.m_segments == segments(dir).size());
  CHECK(wal_close(wal).ok());

  /* Recovery reads every segment in order. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, c, recv).ok());
  CHECK(has_range(c, 1, 40));
  CHECK(recv.m_first_seq == 1 && recv.m_last_seq == 40);
  CHECK(recv.m_segments == stats.m_segments);

  /* Appending after a reopen continues the last segment, then rotates. */
  CHECK(wal_open(dir.m_path, 0, nullptr, nullptr, &wal, recv).ok());
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(*wal, 41, 60);
  CHECK(wal_close(wal).ok());

  collect_t again;
  CHECK(recover(dir, again, recv).ok());
  CHECK(has_range(again, 1, 60));
}

/** Purging deletes the closed segments whose records are all stored, never
the current one. */
static void test_purge() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.m_path, 0, &wal).ok());
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);
  append_upto(*wal, 1, 40);

  const std::vector<uint64_t> before = segments(dir);

  /* Records up to 15 are stored elsewhere: segments wholly below 16 go. */
  uint64_t removed = 0;
  CHECK(wal_purge(*wal, 15, removed).ok());
  CHECK(removed > 0);

  const std::vector<uint64_t> after = segments(dir);
  CHECK(after.size() == before.size() - removed);
  CHECK(after.front() <= 16);

  /* Nothing more to remove for the same point. */
  CHECK(wal_purge(*wal, 15, removed).ok());
  CHECK(removed == 0);

  wal_stats_t stats;
  wal_get_stats(*wal, stats);
  CHECK(stats.m_segments == after.size());
  CHECK(wal_close(wal).ok());

  /* Recovery starts at the oldest remaining segment. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, c, recv).ok());
  CHECK(recv.m_first_seq == after.front());
  CHECK(has_range(c, after.front(), 40));

  /* With everything stored, only the current segment remains. */
  CHECK(wal_open(dir.m_path, 0, nullptr, nullptr, &wal, recv).ok());
  CHECK(wal_purge(*wal, 40, removed).ok());
  CHECK(segments(dir).size() == 1);
  append_upto(*wal, 41, 45);
  CHECK(wal_close(wal).ok());

  collect_t last;
  CHECK(recover(dir, last, recv).ok());
  CHECK(last.m_payloads.back() == payload_for(45));
}

struct append_arg_t {
  /** WAL under test. */
  wal_t *m_wal{nullptr};

  /** Records to append. */
  int m_count{0};

  /** Calls that failed. */
  int m_errors{0};

  /** Appenders that have finished, shared by all of them. */
  int *m_finished{nullptr};

  /** Protects *finished. */
  std::mutex *m_mutex{nullptr};
};

/** Append count records, counting failures. */
static void append_main(append_arg_t *arg) {
  for (int i = 0; i < arg->m_count; ++i) {
    const std::string p = "concurrent-payload";
    batch_seq_t seq = 0;

    if (!append_record(*arg->m_wal, p.data(), p.size(), seq).ok()) {
      arg->m_errors++;
    }
  }

  std::lock_guard<std::mutex> lock(*arg->m_mutex);
  (*arg->m_finished)++;
}

/** Purging runs safely while writers append and rotate segments. */
static void test_purge_during_appends() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.m_path, 0, &wal).ok());
  wal_set_segment_bytes(*wal, SEGMENT_BYTES);

  std::vector<append_arg_t> args(4);
  std::vector<std::thread> threads;
  std::mutex mutex;
  int finished = 0;

  for (size_t t = 0; t < args.size(); ++t) {
    args[t].m_wal = wal;
    args[t].m_count = 200;
    args[t].m_finished = &finished;
    args[t].m_mutex = &mutex;
    threads.push_back(std::thread(append_main, &args[t]));
  }

  /* Purge behind the appends until they finish, as flushes would. */
  uint64_t total_removed = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (finished == static_cast<int>(args.size())) {
        break;
      }
    }

    wal_stats_t stats;
    wal_get_stats(*wal, stats);

    uint64_t removed = 0;
    CHECK(wal_purge(*wal, stats.m_records / 2, removed).ok());
    total_removed += removed;
  }

  for (size_t t = 0; t < threads.size(); ++t) {
    threads[t].join();
    CHECK(args[t].m_errors == 0);
  }

  CHECK(total_removed > 0);
  CHECK(wal_close(wal).ok());
  printf("purge during appends: %llu segments removed\n",
         static_cast<unsigned long long>(total_removed));

  /* What remains is a gapless run ending at the last record. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, c, recv).ok());
  CHECK(recv.m_last_seq == 800);
  CHECK(c.m_payloads.size() == 800 - recv.m_first_seq + 1);
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
  CHECK(recover(dir, c, recv).code() == DB_CORRUPTION);
}

/** Replace the contents of a file. */
static void write_file(const std::string &path,
                       const std::vector<unsigned char> &data) {
  os_file_t file;
  CHECK(os_file_open(path.c_str(), OS_FILE_OPEN_OR_CREATE, file).ok());
  CHECK(os_file_truncate(file, 0).ok());
  if (!data.empty()) {
    CHECK(os_file_write(file, &data[0], data.size(), 0).ok());
  }
  CHECK(os_file_close(file).ok());
}

/** Read the bytes of a test file.
@return the bytes of a file */
static std::vector<unsigned char> read_file(const std::string &path) {
  std::vector<unsigned char> data;
  os_file_t file;
  uint64_t size = 0;

  CHECK(os_file_open(path.c_str(), OS_FILE_OPEN, file).ok());
  CHECK(os_file_get_size(file, size).ok());
  data.resize(size);
  CHECK(os_file_read(file, &data[0], size, 0).ok());
  CHECK(os_file_close(file).ok());

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
    CHECK(recover(dir, c, recv).code() == DB_CORRUPTION);
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
    CHECK(recover(dir, c, recv).code() == DB_CORRUPTION);
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
    CHECK(recover(dir, c, recv).code() == DB_CORRUPTION);
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
  CHECK(recover(dir, c, recv).ok());
  CHECK(has_range(c, 1, 20));
  CHECK(recv.m_last_seq == 20);

  struct stat st;
  CHECK(stat((segment_path(dir, 30) + ".init").c_str(), &st) != 0);

  /* Appends continue in the empty segment. */
  wal_t *wal = nullptr;
  CHECK(wal_open(dir.m_path, 0, nullptr, nullptr, &wal, recv).ok());
  append_upto(*wal, 21, 22);
  CHECK(wal_close(wal).ok());

  collect_t again;
  CHECK(recover(dir, again, recv).ok());
  CHECK(has_range(again, 1, 22));
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_rotation();
  test_purge();
  test_purge_during_appends();
  test_missing_segment();
  test_damaged_closed_segment();
  test_crash_after_rotation();

  return (test_result("wal_seg-t"));
}
