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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

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
static void append_upto(wal &log, uint64_t first, uint64_t last) {
  for (uint64_t s = first; s <= last; ++s) {
    const std::string p = payload_for(s);
    batch_seq_t seq = s - 1;

    CHECK(append_record(log, p.data(), p.size(), seq).ok());
    CHECK(seq == s);
  }

  return;
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
@return result of wal::open() */
static db_status_t recover(const test_dir_t &dir, collect_t &c,
                           wal_recv_t &recv) {
  wal *log = nullptr;

  const db_status_t err = wal::open(dir.m_path, collect_apply, &c, &log, recv);
  if (log != nullptr) {
    CHECK(wal::destroy(log).ok());
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
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, SEGMENT_BYTES).ok());
  append_upto(*log, 1, n);
  CHECK(wal::destroy(log).ok());

  return;
}

/** Appends start a new segment once one is full; reopening reads them all. */
static void test_rotation() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, SEGMENT_BYTES).ok());
  append_upto(*log, 1, 40);

  wal_stats_t stats;
  log->read_stats(stats);
  CHECK(stats.m_segments > 3);
  CHECK(stats.m_segments == segments(dir).size());
  CHECK(wal::destroy(log).ok());

  /* Recovery reads every segment in order. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, c, recv).ok());
  CHECK(has_range(c, 1, 40));
  CHECK(recv.m_first_seq == 1 && recv.m_last_seq == 40);
  CHECK(recv.m_segments == stats.m_segments);

  /* Appending after a reopen continues the last segment, then rotates. */
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recv, 0, true,
                  SEGMENT_BYTES)
            .ok());
  append_upto(*log, 41, 60);
  CHECK(wal::destroy(log).ok());

  collect_t again;
  CHECK(recover(dir, again, recv).ok());
  CHECK(has_range(again, 1, 60));

  return;
}

/** Purging deletes the closed segments whose records are all stored, never
the current one. */
static void test_purge() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, SEGMENT_BYTES).ok());
  append_upto(*log, 1, 40);

  const std::vector<uint64_t> before = segments(dir);

  /* Records up to 15 are stored elsewhere: segments wholly below 16 go. */
  uint64_t removed = 0;
  CHECK(log->purge(15, removed).ok());
  CHECK(removed > 0);

  const std::vector<uint64_t> after = segments(dir);
  CHECK(after.size() == before.size() - removed);
  CHECK(after.front() <= 16);

  /* Nothing more to remove for the same point. */
  CHECK(log->purge(15, removed).ok());
  CHECK(removed == 0);

  wal_stats_t stats;
  log->read_stats(stats);
  CHECK(stats.m_segments == after.size());
  CHECK(wal::destroy(log).ok());

  /* Recovery starts at the oldest remaining segment. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, c, recv).ok());
  CHECK(recv.m_first_seq == after.front());
  CHECK(has_range(c, after.front(), 40));

  /* With everything stored, only the current segment remains. */
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recv).ok());
  CHECK(log->purge(40, removed).ok());
  CHECK(segments(dir).size() == 1);
  append_upto(*log, 41, 45);
  CHECK(wal::destroy(log).ok());

  collect_t last;
  CHECK(recover(dir, last, recv).ok());
  CHECK(last.m_payloads.back() == payload_for(45));

  return;
}

struct append_arg_t {
  /** WAL under test. */
  wal *m_wal{nullptr};

  /** Records to append. */
  int m_count{0};

  /** Calls that failed. */
  int m_errors{0};

  /** Appenders that have finished, shared by all of them. */
  int *m_finished{nullptr};

  /** Last appended sequence; protected by m_mutex. */
  batch_seq_t *m_sequence{nullptr};

  /** Protects sequence assignment, I/O and *finished. */
  std::mutex *m_mutex{nullptr};
};

/** Append count records, counting failures. */
static void append_main(append_arg_t *arg) {
  for (int i = 0; i < arg->m_count; ++i) {
    const std::string p = "concurrent-payload";
    std::lock_guard<std::mutex> lock(*arg->m_mutex);
    if (!append_record(*arg->m_wal, p.data(), p.size(), *arg->m_sequence)
             .ok()) {
      arg->m_errors++;
    }
  }

  std::lock_guard<std::mutex> lock(*arg->m_mutex);
  (*arg->m_finished)++;

  return;
}

/** Purging runs while writers append and rotate at a fixed threshold;
the retained records remain recoverable in sequence order. */
static void test_concurrent_purge_during_appends() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, SEGMENT_BYTES).ok());

  std::vector<append_arg_t> args(4);
  std::vector<std::thread> threads;
  std::mutex mutex;
  int finished = 0;
  batch_seq_t sequence = 0;

  for (size_t t = 0; t < args.size(); ++t) {
    args[t].m_wal = log;
    args[t].m_count = 200;
    args[t].m_finished = &finished;
    args[t].m_sequence = &sequence;
    args[t].m_mutex = &mutex;
    threads.push_back(std::thread(append_main, &args[t]));
  }

  /* Purge completed prefixes while appends rotate at the fixed threshold. */
  uint64_t total_removed = 0;
  uint64_t polls = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (finished == static_cast<int>(args.size())) {
        break;
      }
    }

    ++polls;

    wal_stats_t stats;
    log->read_stats(stats);

    uint64_t removed = 0;
    CHECK(log->purge(stats.m_records / 2, removed).ok());
    total_removed += removed;
  }

  for (size_t t = 0; t < threads.size(); ++t) {
    threads[t].join();
    CHECK(args[t].m_errors == 0);
  }

  CHECK(polls > 0);
  CHECK(total_removed > 0);
  CHECK(wal::destroy(log).ok());
  printf("purge during appends: %llu segments removed\n",
         static_cast<unsigned long long>(total_removed));

  /* What remains is a gapless run ending at the last record. */
  collect_t c;
  wal_recv_t recv;
  CHECK(recover(dir, c, recv).ok());
  CHECK(recv.m_last_seq == 800);
  CHECK(c.m_payloads.size() == 800 - recv.m_first_seq + 1);

  return;
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

  return;
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

  return;
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

  return;
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
  wal *log = nullptr;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recv).ok());
  append_upto(*log, 21, 22);
  CHECK(wal::destroy(log).ok());

  collect_t again;
  CHECK(recover(dir, again, recv).ok());
  CHECK(has_range(again, 1, 22));

  return;
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_rotation();
  test_purge();
  test_concurrent_purge_during_appends();
  test_missing_segment();
  test_damaged_closed_segment();
  test_crash_after_rotation();

  return (test_result("wal_seg-t"));
}
