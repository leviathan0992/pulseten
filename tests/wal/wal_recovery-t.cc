/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_recovery-t.cc
 *    Tests for opening and recovering an existing WAL.
 *
 * Crash states are produced by editing the file to look like an interrupted
 * write: cut records, garbage tails and partly written groups. Killing a
 * writer process with SIGKILL is tested in crash/crash-t.
 *
 * Covers: src/wal/wal_recovery.cc
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "os_file.h"
#include "test_fixture.h"
#include "test_util.h"
#include "test_wal.h"
#include "ut.h"
#include "wal_recovery.h"

/** Build the deterministic payload of a test record.
@param[in]  i  zero-based row or record index
@return the payload of record i; lengths vary with i */
static std::string payload_for(int i) {
  char buf[64];
  snprintf(buf, sizeof(buf), "record-%d-%s", i,
           std::string(i % 50, 'x').c_str());

  return (buf);
}

/** Append records first..last (by payload_for) with a single writer, so every
record is a group of its own.
@param[in]  first  first payload index, inclusive
@param[in]  last   last payload index, inclusive */
static void append_range(wal &log, int first, int last) {
  for (int i = first; i <= last; ++i) {
    std::string p = payload_for(i);
    batch_seq_t seq = static_cast<batch_seq_t>(i - 1);
    CHECK(append_record(log, p.data(), p.size(), seq).ok());
    CHECK(seq == static_cast<batch_seq_t>(i));
  }

  return;
}

/** Create a WAL holding records 1..n. */
static void make_wal(const test_dir_t &dir, int n) {
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());

  append_range(*log, 1, n);
  CHECK(wal::destroy(log).ok());

  return;
}

/** Compute the WAL offset immediately after a generated record.
@param[in]  n  record number whose end offset is requested
@return file offset just after record n written by make_wal() */
static uint64_t end_of_record(int n) {
  uint64_t at = WAL_FILE_HEADER_BYTES;

  for (int i = 1; i <= n; ++i) {
    at += WAL_RECORD_HEADER_BYTES + payload_for(i).size();
  }

  return (at);
}

/** Read the bytes of a test file.
@return the bytes of the first file, or nothing if it cannot be read */
static std::vector<unsigned char> read_file(const test_dir_t &dir) {
  std::vector<unsigned char> data;
  os_file_t file;
  uint64_t size = 0;
  if (!os_file_open(dir.file(wal_file_name(1).c_str()).c_str(), OS_FILE_OPEN,
                    file)
           .ok()) {
    return (data);
  }

  if (os_file_size(file, size).ok() && size > 0) {
    data.resize(size);

    if (!os_file_read(file, &data[0], size, 0).ok()) {
      data.clear();
    }
  }

  static_cast<void>(os_file_close(file));

  return (data);
}

/** Replace the WAL file contents. */
static void write_file(const test_dir_t &dir,
                       const std::vector<unsigned char> &data) {
  os_file_t file;
  CHECK(os_file_open(dir.file(wal_file_name(1).c_str()).c_str(), OS_FILE_OPEN,
                     file)
            .ok());
  CHECK(os_file_truncate(file, 0).ok());

  if (!data.empty()) {
    CHECK(os_file_write(file, &data[0], data.size(), 0).ok());
  }

  CHECK(os_file_close(file).ok());

  return;
}

/** Append one encoded record to a byte vector. */
static void push_record(std::vector<unsigned char> &data, batch_seq_t seq,
                        uint32_t group_index, const std::string &payload) {
  const size_t at = data.size();
  data.resize(at + WAL_RECORD_HEADER_BYTES + payload.size());
  wal_encode_record(&data[at], seq, group_index,
                    reinterpret_cast<const unsigned char *>(payload.data()),
                    payload.size());

  return;
}

/** Check that recovery produced exactly the expected records.
@return true if c holds exactly records 1..n of payload_for() */
static bool has_records(const test_wal_records_t &c, int n) {
  if (c.m_payloads.size() != static_cast<size_t>(n) ||
      (n > 0 && c.m_first_seq != 1)) {
    return (false);
  }

  for (int i = 1; i <= n; ++i) {
    if (c.m_payloads[i - 1] != payload_for(i)) {
      return (false);
    }
  }

  return (true);
}

/** Reopening an empty WAL recovers nothing and appends from 1. */
static void test_reopen_empty() {
  test_dir_t dir;
  make_wal(dir, 0);

  test_wal_records_t c;
  wal_recovery_t recovery;
  wal *log = nullptr;
  CHECK(wal::open(dir.m_path, test_wal_collect, &c, &log, recovery).ok());
  CHECK(recovery.m_records == 0 && recovery.m_last_seq == 0);
  CHECK(recovery.m_end_offset == WAL_FILE_HEADER_BYTES &&
        recovery.m_tail_bytes == 0);

  append_range(*log, 1, 1);
  CHECK(wal::destroy(log).ok());

  return;
}

/** Reopening recovers every record, and appending continues after them. */
static void test_reopen_and_continue() {
  test_dir_t dir;
  make_wal(dir, 50);

  test_wal_records_t c;
  wal_recovery_t recovery;
  wal *log = nullptr;
  CHECK(wal::open(dir.m_path, test_wal_collect, &c, &log, recovery).ok());
  CHECK(has_records(c, 50));
  CHECK(recovery.m_last_seq == 50 &&
        recovery.m_end_offset == end_of_record(50));

  /* Sequence numbers continue after the recovered records. */
  append_range(*log, 51, 60);
  CHECK(wal::destroy(log).ok());

  test_wal_records_t again;
  CHECK(test_wal_recover(dir.m_path, again, recovery).ok());
  CHECK(has_records(again, 60));

  return;
}

/** A cut anywhere inside the last record drops just that record. */
static void test_cut_last_record() {
  test_dir_t dir;
  make_wal(dir, 20);

  const std::vector<unsigned char> full = read_file(dir);
  const uint64_t end19 = end_of_record(19);
  CHECK(full.size() == end_of_record(20));

  /* Cut the file at every byte inside the last record, as if the crash
  happened while it was being written. */
  for (uint64_t cut = end19; cut < full.size(); ++cut) {
    write_file(dir, std::vector<unsigned char>(
                        full.begin(), full.begin() + static_cast<long>(cut)));

    test_wal_records_t c;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
    CHECK(has_records(c, 19));
    CHECK(recovery.m_tail_bytes == cut - end19);

    /* The unfinished tail is cut off on open. */
    CHECK(read_file(dir).size() == end19);
  }

  return;
}

/** Zero-filled, incomplete and invalid headers are all discarded at the end. */
static void test_garbage_tail() {
  const size_t lengths[] = {1,
                            WAL_RECORD_HEADER_BYTES - 1,
                            WAL_RECORD_HEADER_BYTES,
                            WAL_RECORD_HEADER_BYTES + 1,
                            100,
                            4096};

  for (size_t k = 0; k < sizeof(lengths) / sizeof(lengths[0]); ++k) {
    for (int fill = 0; fill < 2; ++fill) {
      test_dir_t dir;
      make_wal(dir, 5);

      std::vector<unsigned char> data = read_file(dir);

      /* Zeros, as left by a file extended before its data reached disk,
      or arbitrary bytes. */
      for (size_t i = 0; i < lengths[k]; ++i) {
        data.push_back(fill == 0 ? 0 : static_cast<unsigned char>(i * 131 + 7));
      }

      write_file(dir, data);

      test_wal_records_t c;
      wal_recovery_t recovery;
      CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
      CHECK(has_records(c, 5));
      CHECK(recovery.m_tail_bytes == lengths[k]);
      CHECK(read_file(dir).size() == end_of_record(5));
    }
  }

  return;
}

/** Write records 1..5, then a group of records 6, 7, 8 in which record
bad_seq is damaged, as if the group's write was only partly persisted. */
static void make_torn_group(const test_dir_t &dir, batch_seq_t bad_seq) {
  make_wal(dir, 5);

  std::vector<unsigned char> data = read_file(dir);

  for (batch_seq_t seq = 6; seq <= 8; ++seq) {
    const size_t at = data.size();
    push_record(data, seq, static_cast<uint32_t>(seq - 6),
                payload_for(static_cast<int>(seq)));

    if (seq == bad_seq) {
      data[at + WAL_RECORD_HEADER_BYTES] ^= 0x01;
    }
  }

  write_file(dir, data);

  return;
}

/** Keep valid records before the damage, even within one append group. */
static void test_torn_group() {
  for (const int bad_seq : {6, 7}) {
    test_dir_t dir;
    make_torn_group(dir, bad_seq);

    test_wal_records_t c;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
    CHECK(has_records(c, bad_seq - 1));
    CHECK(read_file(dir).size() == end_of_record(bad_seq - 1));
  }

  return;
}

/** Even later valid groups are discarded after a bad record in the last
file. */
static void test_discard_suffix() {
  for (const int bad_seq : {1, 3, 10}) {
    test_dir_t dir;
    make_wal(dir, 10);

    std::vector<unsigned char> data = read_file(dir);
    const size_t end = end_of_record(bad_seq - 1);
    data[end + WAL_RECORD_HEADER_BYTES] ^= 1;
    write_file(dir, data);

    test_wal_records_t c;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
    CHECK(has_records(c, bad_seq - 1));
    CHECK(read_file(dir).size() == end);
    CHECK(recovery.m_tail_bytes == data.size() - end);
  }

  return;
}

/** Every damaged header byte ends the recovered prefix. */
static void test_record_header_damage() {
  test_dir_t dir;
  make_wal(dir, 6);

  const std::vector<unsigned char> original = read_file(dir);

  for (size_t byte = 0; byte < WAL_RECORD_HEADER_BYTES; ++byte) {
    std::vector<unsigned char> data = original;
    data[end_of_record(5) + byte] ^= 1;
    write_file(dir, data);

    test_wal_records_t c;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
    CHECK(has_records(c, 5));
    CHECK(read_file(dir).size() == end_of_record(5));
  }

  return;
}

/** A caller's required boundary is checked before any truncation. */
static void test_required_sequence() {
  test_dir_t dir;
  make_torn_group(dir, 6);

  const std::vector<unsigned char> before = read_file(dir);

  for (const batch_seq_t required : {6, 9}) {
    test_wal_records_t c;
    wal_recovery_t recovery;
    wal *log = nullptr;
    const db_status_t status =
        wal::open(dir.m_path, test_wal_collect, &c, &log, recovery, 0, false,
                  WAL_DEFAULT_FILE_SIZE_BYTES, required);
    CHECK(status.code() == DB_CORRUPTION);
    CHECK(status.message().find("before required sequence") !=
          std::string::npos);
    CHECK(log == nullptr);
    CHECK(read_file(dir) == before);
  }

  test_wal_records_t c;
  wal_recovery_t recovery;
  wal *log = nullptr;
  CHECK(wal::open(dir.m_path, test_wal_collect, &c, &log, recovery, 0, false,
                  WAL_DEFAULT_FILE_SIZE_BYTES, 5)
            .ok());
  CHECK(has_records(c, 5));
  CHECK(read_file(dir).size() == end_of_record(5));
  append_range(*log, 6, 6);
  CHECK(wal::destroy(log).ok());

  test_wal_records_t again;
  CHECK(test_wal_recover(dir.m_path, again, recovery).ok());
  CHECK(has_records(again, 6));

  return;
}

/** A damaged file header is corruption. */
static void test_bad_header() {
  test_dir_t dir;
  make_wal(dir, 3);

  std::vector<unsigned char> data = read_file(dir);
  data[10] ^= 0x01;
  write_file(dir, data);

  test_wal_records_t c;
  wal_recovery_t recovery;
  CHECK(test_wal_recover(dir.m_path, c, recovery).code() == DB_CORRUPTION);

  /* Shorter than a header. */
  write_file(dir, std::vector<unsigned char>(data.begin(), data.begin() + 10));
  CHECK(test_wal_recover(dir.m_path, c, recovery).code() == DB_CORRUPTION);

  return;
}

/** A sequence gap in the newest file also ends the recovered prefix. */
static void test_misplaced_record() {
  test_dir_t dir;
  make_wal(dir, 5);

  std::vector<unsigned char> data = read_file(dir);

  /* A valid record whose sequence number skips 6. */
  push_record(data, 7, 0, payload_for(7));
  write_file(dir, data);

  test_wal_records_t c;
  wal_recovery_t recovery;
  CHECK(test_wal_recover(dir.m_path, c, recovery).ok());
  CHECK(has_records(c, 5));
  CHECK(read_file(dir).size() == end_of_record(5));

  return;
}

/** An error from the replay callback stops the open and changes no file. */
static void test_apply_error() {
  test_dir_t dir;
  make_wal(dir, 5);

  const std::vector<unsigned char> before = read_file(dir);

  wal_recovery_t recovery;
  for (const dberr_t code :
       {DB_ERROR, DB_CORRUPTION, DB_IO_ERROR, DB_OUT_OF_MEMORY}) {
    test_wal_records_t c;
    c.m_fail_at = 3;
    c.m_failure = code;
    CHECK(test_wal_recover(dir.m_path, c, recovery).code() == code);
    CHECK(read_file(dir) == before);
  }

  test_wal_records_t ok;
  CHECK(test_wal_recover(dir.m_path, ok, recovery).ok());
  CHECK(has_records(ok, 5));

  return;
}

/** Tail recovery does not depend on the next open's synchronization option. */
static void test_tail_sync_modes() {
  for (const bool sync : {false, true}) {
    for (int damage = 0; damage < 3; ++damage) {
      test_dir_t dir;
      make_wal(dir, 5);

      std::vector<unsigned char> bytes = read_file(dir);
      const size_t start = bytes.size();
      push_record(bytes, 6, 0, payload_for(6));

      if (damage == 0) {
        bytes[start] ^= 1;
      } else if (damage == 1) {
        bytes.back() ^= 1;
      } else {
        /* Record 6 has a valid header but a missing body before group 7. */
        bytes.resize(start + WAL_RECORD_HEADER_BYTES);
        push_record(bytes, 7, 0, payload_for(7));
      }

      write_file(dir, bytes);

      wal *log = nullptr;
      test_wal_records_t collected;
      wal_recovery_t recovery;
      const db_status_t status = wal::open(dir.m_path, test_wal_collect,
                                           &collected, &log, recovery, 0, sync);
      CHECK(status.ok());
      CHECK(log != nullptr);
      CHECK(has_records(collected, 5));
      CHECK(read_file(dir).size() == start);
      CHECK(wal::destroy(log).ok());
    }
  }

  return;
}

/** A failed file read is an operation error, not a discarded record. */
static void test_read_failure() {
  test_dir_t dir;
  make_wal(dir, 3);

  const std::vector<unsigned char> before = read_file(dir);
  const std::string path = dir.file(wal_file_name(1).c_str());
  os_file_t file = open(path.c_str(), O_WRONLY);
  CHECK(file != OS_FILE_CLOSED);

  wal_recovery_t recovery;
  CHECK(wal_file_reader(file)
            .replay(1, true, nullptr, nullptr, recovery)
            .code() == DB_IO_ERROR);
  CHECK(os_file_close(file).ok());
  CHECK(read_file(dir) == before);

  return;
}

/** Run WAL replay, corruption and tail recovery tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"reopen_empty", test_reopen_empty},
      {"reopen_and_continue", test_reopen_and_continue},
      {"cut_last_record", test_cut_last_record},
      {"garbage_tail", test_garbage_tail},
      {"tail_sync_modes", test_tail_sync_modes},

      {"torn_group", test_torn_group},
      {"discard_suffix", test_discard_suffix},
      {"record_header_damage", test_record_header_damage},
      {"required_sequence", test_required_sequence},

      {"bad_header", test_bad_header},
      {"misplaced_record", test_misplaced_record},

      {"apply_error", test_apply_error},
      {"read_failure", test_read_failure},
  };

  return (test_main("wal_recovery-t", argc, argv, cases));
}
