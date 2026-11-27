/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_file-t.cc
 *    Tests for WAL files: rotation, recovery across files, purging while
 *    appends run, and damaged or missing files.
 *
 * Covers: src/wal/wal.cc, src/wal/wal_recovery.cc
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "os_file.h"
#include "test_fixture.h"
#include "test_util.h"
#include "test_wal.h"
#include "ut.h"
#include "wal_recovery.h"

/** Small files, so that a few records rotate. */
static const uint64_t FILE_SIZE_BYTES = 300;

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

/** Check that recovery produced exactly a consecutive record range.
@param[in]  first  first record number, inclusive
@param[in]  last   last record number, inclusive
@return true if c holds exactly records first..last */
static bool has_range(const test_wal_records_t &c, uint64_t first,
                      uint64_t last) {
  if (c.m_payloads.size() != last - first + 1 || c.m_first_seq != first) {
    return (false);
  }

  for (uint64_t s = first; s <= last; ++s) {
    if (c.m_payloads[s - first] != payload_for(s)) {
      return (false);
    }
  }

  return (true);
}

/** Build the path of the file starting at a given sequence number.
@param[in]  first  first record number, inclusive
@return path of the file that starts at first */
static std::string file_path(const test_dir_t &dir, uint64_t first) {
  return (dir.file(wal_file_name(first).c_str()));
}

/** List file first sequence numbers in increasing order.
@return first sequence numbers of the files in the directory */
static std::vector<uint64_t> files(const test_dir_t &dir) {
  std::vector<std::string> names;
  std::vector<uint64_t> file_first_seqs;

  CHECK(os_dir_list(dir.m_path, names).ok());

  for (size_t i = 0; i < names.size(); ++i) {
    batch_seq_t first = 0;
    if (wal_parse_file_name(names[i].c_str(), first)) {
      file_first_seqs.push_back(first);
    }
  }

  std::sort(file_first_seqs.begin(), file_first_seqs.end());

  return (file_first_seqs);
}

/** Create a WAL with small files holding records 1..n. */
static void make_wal(const test_dir_t &dir, uint64_t n) {
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, FILE_SIZE_BYTES).ok());

  append_upto(*log, 1, n);
  CHECK(wal::destroy(log).ok());

  return;
}

/** Appends start a new file once one is full; reopening reads them all. */
static void test_rotation() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, FILE_SIZE_BYTES).ok());

  append_upto(*log, 1, 40);

  wal::stats_t stats;
  log->stats(stats);
  CHECK(stats.m_file_count > 3);
  CHECK(stats.m_file_count == files(dir).size());
  CHECK(wal::destroy(log).ok());

  /* Recovery reads every file in order. */
  test_wal_records_t c;
  wal_recovery_t recovery;
  CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
  CHECK(has_range(c, 1, 40));
  CHECK(recovery.m_first_seq == 1 && recovery.m_last_seq == 40);

  /* Appending after a reopen continues the last file, then rotates. */
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recovery, 0, true,
                  FILE_SIZE_BYTES)
            .ok());
  log->stats(stats);
  CHECK(stats.m_file_count == files(dir).size());

  append_upto(*log, 41, 60);
  CHECK(wal::destroy(log).ok());

  test_wal_records_t again;
  CHECK(test_wal_recover(dir.m_path, again, recovery).ok());
  CHECK(has_range(again, 1, 60));

  return;
}

/** Purging deletes the closed files whose records are all stored, never
the current one. */
static void test_purge() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, FILE_SIZE_BYTES).ok());

  append_upto(*log, 1, 40);

  const std::vector<uint64_t> before = files(dir);
  REQUIRE(!before.empty() && before.front() == 1);

  /* Records up to 15 are stored elsewhere: files wholly below 16 go. */
  CHECK(log->purge(15).ok());
  const std::vector<uint64_t> after = files(dir);
  REQUIRE(!after.empty());
  const auto keep = std::upper_bound(before.begin(), before.end(), 16) - 1;
  CHECK(keep != before.begin());
  CHECK(after == std::vector<uint64_t>(keep, before.end()));

  /* Nothing more to remove for the same point. */
  CHECK(log->purge(15).ok());
  CHECK(files(dir) == after);

  wal::stats_t stats;
  log->stats(stats);
  CHECK(stats.m_file_count == after.size());
  CHECK(wal::destroy(log).ok());

  /* Recovery starts at the oldest remaining file. */
  test_wal_records_t c;
  wal_recovery_t recovery;
  CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
  CHECK(recovery.m_first_seq == after.front());
  CHECK(has_range(c, after.front(), 40));

  /* With everything stored, only the current file remains. */
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recovery).ok());
  CHECK(log->purge(40).ok());
  CHECK(files(dir).size() == 1);

  append_upto(*log, 41, 45);
  CHECK(wal::destroy(log).ok());

  test_wal_records_t last;
  CHECK(test_wal_recover(dir.m_path, last, recovery).ok());
  CHECK(last.m_payloads.back() == payload_for(45));

  return;
}

struct append_arg_t {
  /** WAL under test. */
  wal *m_wal{nullptr};

  /** Records to append. */
  int m_record_count{0};

  /** Calls that failed, plus a timed-out wait for the purge. */
  int m_errors{0};

  /** Appenders that have finished, shared by all of them. */
  int *m_finished{nullptr};

  /** Last appended sequence; protected by m_mutex. */
  batch_seq_t *m_last_seq{nullptr};

  /** Set once a purge has removed a file; protected by m_mutex. */
  bool *m_purged{nullptr};

  /** Signalled when *m_purged becomes true. */
  std::condition_variable *m_purged_cv{nullptr};

  /** Protects sequence assignment, I/O, *finished and *purged. */
  std::mutex *m_mutex{nullptr};
};

/** Append count records, counting failures. Halfway through, wait until a
purge has removed a file, so at least one purge falls between appends no
matter how the mutex is scheduled. */
static void append_main(append_arg_t *arg) {
  for (int i = 0; i < arg->m_record_count; ++i) {
    const std::string p = "concurrent-payload";
    std::unique_lock<std::mutex> lock(*arg->m_mutex);

    /* A timeout counts as an error; the main thread checks m_errors. */
    if (i == arg->m_record_count / 2 &&
        !arg->m_purged_cv->wait_for(lock, std::chrono::seconds(30),
                                    [arg] { return (*arg->m_purged); })) {
      arg->m_errors++;
    }

    if (!append_record(*arg->m_wal, p.data(), p.size(), *arg->m_last_seq)
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
  CHECK(wal::create(dir.m_path, &log, 0, true, FILE_SIZE_BYTES).ok());

  std::vector<append_arg_t> args(4);
  std::vector<std::thread> threads;
  std::mutex mutex;
  std::condition_variable purged_cv;
  bool purged = false;
  int finished = 0;
  batch_seq_t sequence = 0;

  for (size_t t = 0; t < args.size(); ++t) {
    args[t].m_wal = log;
    args[t].m_record_count = 200;
    args[t].m_finished = &finished;
    args[t].m_last_seq = &sequence;
    args[t].m_purged = &purged;
    args[t].m_purged_cv = &purged_cv;
    args[t].m_mutex = &mutex;
    threads.push_back(std::thread(append_main, &args[t]));
  }

  /* Purge completed prefixes while appends rotate at the fixed threshold. */
  const std::string oldest = file_path(dir, 1);
  uint64_t polls = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(mutex);

      if (finished == static_cast<int>(args.size())) {
        break;
      }
    }

    ++polls;

    batch_seq_t appended = 0;
    {
      std::lock_guard<std::mutex> lock(mutex);
      appended = sequence;
    }

    CHECK(log->purge(appended / 2).ok());
    if (!purged && access(oldest.c_str(), F_OK) != 0) {
      std::lock_guard<std::mutex> lock(mutex);
      purged = true;
      purged_cv.notify_all();
    }
  }

  for (size_t t = 0; t < threads.size(); ++t) {
    threads[t].join();
    CHECK(args[t].m_errors == 0);
  }

  CHECK(polls > 0);
  CHECK(purged);
  CHECK(wal::destroy(log).ok());

  /* What remains is a gapless run ending at the last record. */
  test_wal_records_t c;
  wal_recovery_t recovery;
  CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
  CHECK(recovery.m_last_seq == 800);
  CHECK(c.m_payloads.size() == 800 - recovery.m_first_seq + 1);

  return;
}

/** A file missing from the middle is corruption. */
static void test_missing_file() {
  test_dir_t dir;
  make_wal(dir, 40);

  const std::vector<uint64_t> file_first_seqs = files(dir);
  CHECK(file_first_seqs.size() > 3);

  /* A gap in the middle is corruption. */
  CHECK(unlink(file_path(dir, file_first_seqs[1]).c_str()) == 0);

  test_wal_records_t c;
  wal_recovery_t recovery;
  CHECK(test_wal_recover(dir.m_path, c, recovery).code() == DB_CORRUPTION);

  return;
}

/** The first retained WAL file must cover the write after storage. */
static void test_missing_prefix() {
  test_dir_t dir;
  make_wal(dir, 40);
  const std::vector<uint64_t> file_first_seqs = files(dir);
  REQUIRE(file_first_seqs.size() > 1);
  REQUIRE(unlink(file_path(dir, file_first_seqs.front()).c_str()) == 0);

  test_wal_records_t records;
  wal_recovery_t recovery;
  wal missing;
  CHECK(missing
            .open_low(dir.m_path, test_wal_collect, &records, recovery, 0, true,
                      0, 0)
            .code() == DB_CORRUPTION);
  CHECK(records.m_payloads.empty());

  const batch_seq_t stored_seq = file_first_seqs[1] - 1;
  wal covered;
  REQUIRE(covered
              .open_low(dir.m_path, test_wal_collect, &records, recovery, 0,
                        true, stored_seq, stored_seq)
              .ok());
  CHECK(has_range(records, stored_seq + 1, 40));
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
  CHECK(os_file_size(file, size).ok());

  data.resize(size);
  CHECK(os_file_read(file, &data[0], size, 0).ok());
  CHECK(os_file_close(file).ok());

  return (data);
}

/** Damage or a cut in a closed file is corruption, not an unfinished
write. */
static void test_damaged_closed_file() {
  /* A cut at the end of a closed file: unlike the last file, it is
  not an unfinished write. */
  {
    test_dir_t dir;
    make_wal(dir, 40);

    const std::vector<uint64_t> file_first_seqs = files(dir);
    std::vector<unsigned char> data =
        read_file(file_path(dir, file_first_seqs[0]));
    data.resize(data.size() - 1);
    write_file(file_path(dir, file_first_seqs[0]), data);

    test_wal_records_t c;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, c, recovery).code() == DB_CORRUPTION);
  }

  /* Bytes after the last record of a closed file. */
  {
    test_dir_t dir;
    make_wal(dir, 40);

    const std::vector<uint64_t> file_first_seqs = files(dir);
    std::vector<unsigned char> data =
        read_file(file_path(dir, file_first_seqs[1]));
    data.push_back(0);
    write_file(file_path(dir, file_first_seqs[1]), data);

    test_wal_records_t c;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, c, recovery).code() == DB_CORRUPTION);
  }

  /* A file of another WAL. */
  {
    test_dir_t dir;
    make_wal(dir, 40);

    const std::vector<uint64_t> file_first_seqs = files(dir);
    std::vector<unsigned char> data =
        read_file(file_path(dir, file_first_seqs[1]));
    wal_encode_file_header(&data[0], ut_read_u64(&data[16]) + 1,
                           file_first_seqs[1]);
    write_file(file_path(dir, file_first_seqs[1]), data);

    test_wal_records_t c;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, c, recovery).code() == DB_CORRUPTION);
  }

  return;
}

/** A crash just after a rotation, leaving a header-only file, recovers. */
static void test_crash_after_rotation() {
  test_dir_t dir;
  make_wal(dir, 20);

  /* A crash after a new file was published but before its first group
  was written leaves a file with only a header, and maybe an unfinished
  init file of the one after. */
  const std::vector<uint64_t> file_first_seqs = files(dir);
  std::vector<unsigned char> header =
      read_file(file_path(dir, file_first_seqs.back()));
  header.resize(WAL_FILE_HEADER_BYTES);
  wal_encode_file_header(&header[0], ut_read_u64(&header[16]), 21);

  write_file(file_path(dir, 21), header);
  write_file(file_path(dir, 30) + ".init", header);

  test_wal_records_t c;
  wal_recovery_t recovery;
  CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
  CHECK(has_range(c, 1, 20));
  CHECK(recovery.m_last_seq == 20);

  struct stat st;
  CHECK(stat((file_path(dir, 30) + ".init").c_str(), &st) != 0);

  /* Appends continue in the empty file. */
  wal *log = nullptr;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recovery).ok());

  append_upto(*log, 21, 22);
  CHECK(wal::destroy(log).ok());

  test_wal_records_t again;
  CHECK(test_wal_recover(dir.m_path, again, recovery).ok());
  CHECK(has_range(again, 1, 22));

  return;
}

/** Scanning preserves every file; cleanup removes only unpublished files. */
static void test_file_scan_and_cleanup() {
  test_dir_t dir;
  const std::vector<unsigned char> bytes(8, 0xa5);
  const std::string pending = file_path(dir, 5) + ".init";
  const std::string unrelated = dir.file("other.init");
  const std::string malformed = dir.file("wal-not-a-sequence.pt.init");
  const std::string suffix = file_path(dir, 7) + ".init.keep";

  write_file(file_path(dir, 4), bytes);
  write_file(file_path(dir, 1), bytes);
  write_file(pending, bytes);
  write_file(unrelated, bytes);
  write_file(malformed, bytes);
  write_file(suffix, bytes);

  std::vector<batch_seq_t> file_first_seqs(1, 999);
  CHECK(wal_scan_files(dir.m_path, file_first_seqs).ok());
  CHECK(file_first_seqs.size() == 2 && file_first_seqs[0] == 1 &&
        file_first_seqs[1] == 4);
  CHECK(read_file(pending) == bytes);
  CHECK(read_file(file_path(dir, 1)) == bytes);
  CHECK(read_file(file_path(dir, 4)) == bytes);

  CHECK(wal_scan_files(dir.m_path, file_first_seqs, true).ok());
  CHECK(file_first_seqs == std::vector<batch_seq_t>({1, 4}));

  struct stat st;
  CHECK(stat(pending.c_str(), &st) != 0);
  CHECK(read_file(file_path(dir, 1)) == bytes);
  CHECK(read_file(file_path(dir, 4)) == bytes);
  CHECK(read_file(unrelated) == bytes);
  CHECK(read_file(malformed) == bytes);
  CHECK(read_file(suffix) == bytes);

  CHECK(wal_scan_files(dir.m_path, file_first_seqs, true).ok());
  CHECK(file_first_seqs.size() == 2 && file_first_seqs[0] == 1 &&
        file_first_seqs[1] == 4);
}

/** Create and open report cleanup failures; creation retries after repair. */
static void test_file_cleanup_failure() {
  test_dir_t dir;
  const std::string pending = file_path(dir, 1) + ".init";
  CHECK(mkdir(pending.c_str(), 0700) == 0);

  std::vector<batch_seq_t> file_first_seqs;
  CHECK(wal_scan_files(dir.m_path, file_first_seqs).ok());
  CHECK(file_first_seqs.empty());
  CHECK(wal_scan_files(dir.m_path, file_first_seqs, true).code() ==
        DB_IO_ERROR);

  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).code() == DB_IO_ERROR);
  CHECK(log == nullptr);

  wal_recovery_t recovery;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recovery).code() ==
        DB_IO_ERROR);
  CHECK(log == nullptr);

  CHECK(rmdir(pending.c_str()) == 0);
  write_file(pending, std::vector<unsigned char>(8, 0xa5));

  CHECK(wal::create(dir.m_path, &log).ok());
  CHECK(wal::destroy(log).ok());

  struct stat st;
  CHECK(stat(pending.c_str(), &st) != 0);
}

/** Run WAL rotation, file cleanup and durability tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"rotation", test_rotation},
      {"purge", test_purge},
      {"concurrent_purge_during_appends", test_concurrent_purge_during_appends},
      {"missing_file", test_missing_file},
      {"missing_prefix", test_missing_prefix},
      {"damaged_closed_file", test_damaged_closed_file},
      {"crash_after_rotation", test_crash_after_rotation},
      {"file_scan_and_cleanup", test_file_scan_and_cleanup},
      {"file_cleanup_failure", test_file_cleanup_failure},
  };

  return (test_main("wal_file-t", argc, argv, cases));
}
