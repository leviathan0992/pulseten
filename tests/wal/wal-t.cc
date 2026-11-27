/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal-t.cc
 *    Tests for WAL creation, record format, batch I/O and injected I/O
 *    failures while creating, rotating, purging and recovering.
 *
 * Covers: src/wal/wal.cc
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "os_file.h"
#include "test_fixture.h"
#include "test_util.h"
#include "test_wal.h"
#include "ut.h"

/** Read a little-endian 32-bit value from record bytes.
@return the little-endian 32-bit value at b */
static uint32_t read_u32(const unsigned char *b) {
  uint32_t v = 0;

  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(b[i]) << (8 * i);
  }

  return (v);
}

/** Read a little-endian 64-bit value from record bytes.
@return the little-endian 64-bit value at b */
static uint64_t read_u64(const unsigned char *b) {
  uint64_t v = 0;

  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(b[i]) << (8 * i);
  }

  return (v);
}

/** Check whether a test file exists.
@return true if a file exists at path */
static bool file_exists(const std::string &path) {
  struct stat st;

  return (stat(path.c_str(), &st) == 0);
}

/** Read a file into memory.
@param[out]  data  file bytes; cleared before reading
@return true if it could be read */
static bool read_file(const std::string &path,
                      std::vector<unsigned char> &data) {
  os_file_t file;
  if (!os_file_open(path.c_str(), OS_FILE_OPEN, file).ok()) {
    return (false);
  }

  uint64_t size = 0;
  if (!os_file_size(file, size).ok()) {
    static_cast<void>(os_file_close(file));

    return (false);
  }

  data.resize(size);

  if (size > 0 && !os_file_read(file, &data[0], size, 0).ok()) {
    static_cast<void>(os_file_close(file));

    return (false);
  }

  static_cast<void>(os_file_close(file));

  return (true);
}

/** Read every file of a WAL and check its format, independently of the
recovery code.
@param[out]  payloads  payload of record seq is payloads[seq - 1]
@return true if every file header and record is valid, the records cover
  every file completely, and sequence numbers run 1, 2, 3, ... across the
  files */
static bool read_wal(const test_dir_t &dir,
                     std::vector<std::string> &payloads) {
  payloads.clear();

  uint64_t wal_id = 0;

  for (int file_index = 0;; ++file_index) {
    /* The next file starts right after the last record read. */
    const uint64_t first = payloads.size() + 1;
    const std::string path = dir.file(wal_file_name(first).c_str());

    if (!file_exists(path)) {
      return (file_index > 0);
    }

    std::vector<unsigned char> data;

    if (!read_file(path, data) || data.size() < WAL_FILE_HEADER_BYTES) {
      return (false);
    }

    const unsigned char *h = &data[0];
    if (memcmp(h, "PTWAL001", 8) != 0 ||
        read_u32(h + 8) != WAL_FORMAT_VERSION ||
        read_u32(h + 12) != WAL_FILE_HEADER_BYTES ||
        read_u64(h + 24) != first || read_u32(h + 32) != 0 ||
        read_u32(h + 36) != ut_crc32c(h, 36, 0)) {
      return (false);
    }

    if (file_index > 0 && read_u64(h + 16) != wal_id) {
      return (false);
    }

    wal_id = read_u64(h + 16);

    const uint64_t size = data.size();
    uint64_t at = WAL_FILE_HEADER_BYTES;

    while (at < size) {
      if (size - at < WAL_RECORD_HEADER_BYTES) {
        return (false);
      }

      const unsigned char *r = &data[at];

      const uint32_t len = read_u32(r + 4);

      if (read_u32(r) != WAL_RECORD_MAGIC || len == 0 ||
          len > size - at - WAL_RECORD_HEADER_BYTES ||
          read_u64(r + 8) != payloads.size() + 1 ||
          read_u32(r + 20) != ut_crc32c(r, 20, 0)) {
        return (false);
      }

      uint32_t crc = ut_crc32c(r, 16, 0);
      crc = ut_crc32c(r + WAL_RECORD_HEADER_BYTES, len, crc);
      if (read_u32(r + 16) != crc) {
        return (false);
      }

      payloads.push_back(std::string(
          reinterpret_cast<const char *>(r) + WAL_RECORD_HEADER_BYTES, len));
      at += WAL_RECORD_HEADER_BYTES + len;
    }

    /* An empty file can only be the last one. */
    if (payloads.size() + 1 == first) {
      return (true);
    }
  }
}

/** A new WAL has one empty file and appends to it. */
static void test_create() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());
  CHECK(log != nullptr);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.empty());
  CHECK(!file_exists(dir.file((wal_file_name(1) + ".init").c_str())));

  /* A second WAL in the same directory is refused; the first is left
  untouched. */
  CHECK(wal::destroy(log).ok());

  wal *second = nullptr;
  CHECK(wal::create(dir.m_path, &second).code() == DB_INVALID_ARGUMENT);
  CHECK(second == nullptr);
  CHECK(read_wal(dir, payloads));

  return;
}

/** Records of many lengths come back in order with consecutive sequence
numbers. */
static void test_sequential() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());

  std::vector<std::string> expected;

  for (int i = 0; i < 100; ++i) {
    /* Lengths 1 to 300 bytes. */
    std::string payload(1 + (i * 37) % 300, static_cast<char>('a' + i % 26));
    batch_seq_t seq = static_cast<batch_seq_t>(i);
    CHECK(append_record(*log, payload.data(), payload.size(), seq).ok());
    CHECK(seq == static_cast<batch_seq_t>(i) + 1);

    expected.push_back(payload);
  }

  wal::stats_t stats;
  log->stats(stats);
  CHECK(stats.m_records == 100);

  /* One writer never has company, so every record is its own group. */
  CHECK(stats.m_groups == 100);
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads == expected);

  return;
}

/** Unsynced groups remain readable across rotation and reopening. */
static void test_unsynced() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, false, 128).ok());

  std::vector<std::string> expected;

  /* Disabling sync must bypass the group's sync failure point too. */
  ut_debug_global().arm("wal.sync", DB_IO_ERROR, -1);

  for (int i = 0; i < 20; ++i) {
    const std::string payload = "record-" + std::to_string(i);

    batch_seq_t seq = static_cast<batch_seq_t>(i);
    CHECK(append_record(*log, payload.data(), payload.size(), seq).ok());
    CHECK(seq == static_cast<batch_seq_t>(i) + 1);

    expected.push_back(payload);
  }

  CHECK_EQ(ut_debug_global().failures("wal.sync"), uint64_t{0});
  ut_debug_global().disarm("wal.sync");
  CHECK(log->sync().ok());

  wal::stats_t stats;
  log->stats(stats);
  CHECK(stats.m_file_count > 1);
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads == expected);

  wal_recovery_t recovery;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recovery, 0, false).ok());
  CHECK(recovery.m_last_seq == 20);

  ut_debug_global().arm("wal.sync", DB_IO_ERROR);

  batch_seq_t seq = recovery.m_last_seq;
  CHECK(append_record(*log, "reopened", 8, seq).ok());
  CHECK(seq == 21);
  CHECK_EQ(ut_debug_global().failures("wal.sync"), uint64_t{0});
  ut_debug_global().disarm("wal.sync");
  CHECK(wal::destroy(log).ok());

  /* Switching to sync mode may rotate before its first group is written. */
  CHECK(
      wal::open(dir.m_path, nullptr, nullptr, &log, recovery, 0, true, 1).ok());
  CHECK(append_record(*log, "synced", 6, seq).ok());
  CHECK(seq == 22);
  CHECK(wal::destroy(log).ok());

  expected.push_back("reopened");
  expected.push_back("synced");
  CHECK(read_wal(dir, payloads));
  CHECK(payloads == expected);

  return;
}

/** Empty and oversized payloads are rejected without consuming a sequence. */
static void test_invalid_argument() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());

  batch_seq_t seq = 99;
  CHECK(append_record(*log, "x", 0, seq).code() == DB_INVALID_ARGUMENT);
  CHECK(seq == 0);
  CHECK(append_record(*log, nullptr, 1, seq).code() == DB_INVALID_ARGUMENT);

  std::vector<char> big(WAL_MAX_PAYLOAD_BYTES + 1, 'x');
  CHECK(append_record(*log, &big[0], big.size(), seq).code() ==
        DB_INVALID_ARGUMENT);

  /* Rejected calls use no sequence numbers. */
  CHECK(append_record(*log, "ok", 2, seq).ok());
  CHECK(seq == 1);
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.size() == 1 && payloads[0] == "ok");

  return;
}

/** The largest payload is written, as a group of its own. */
static void test_max_payload() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());

  /* Larger than the default group limit; written as a group of one. */
  std::string payload(WAL_MAX_PAYLOAD_BYTES, 'm');
  batch_seq_t seq = 0;
  CHECK(append_record(*log, payload.data(), payload.size(), seq).ok());
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.size() == 1 && payloads[0] == payload);

  return;
}

/** Group starts select the sequence range for each batch. */
static void test_batch_append() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());

  const std::string payloads[] = {"a", "b", "c", "d", "e", "f"};
  wal_record_t records[3];

  for (size_t group = 0; group < 2; ++group) {
    for (size_t i = 0; i < 3; ++i) {
      const size_t n = group * 3 + i;
      records[i].m_payload =
          reinterpret_cast<const unsigned char *>(payloads[n].data());
      records[i].m_payload_bytes = payloads[n].size();
    }

    CHECK(log->append(group * 3 + 1, records, 3).ok());
  }

  wal::stats_t stats;
  log->stats(stats);
  CHECK(stats.m_groups == 2 && stats.m_records == 6);
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> recovered;
  CHECK(read_wal(dir, recovered));
  CHECK(recovered == std::vector<std::string>(payloads, payloads + 6));

  return;
}

/** Broken sequence contracts abort before any record reaches the file. */
static void test_sequence_invariants() {
  test_dir_t dir;
  wal *log = nullptr;
  REQUIRE(wal::create(dir.m_path, &log).ok());

  const std::string payload = "record";

  const wal_record_t record{
      reinterpret_cast<const unsigned char *>(payload.data()), payload.size()};
  CHECK_EQ(log->append(0, &record, 1).code(), DB_INVALID_ARGUMENT);
  CHECK_EQ(log->append(UINT64_MAX, &record, 1).code(), DB_INVALID_ARGUMENT);

  const pid_t pid = fork();
  REQUIRE(pid >= 0);
  if (pid == 0) {
    static_cast<void>(log->append(2, &record, 1));
    _exit(0);
  }

  int status = 0;
  REQUIRE(waitpid(pid, &status, 0) == pid);
  CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);

  std::vector<unsigned char> data;
  REQUIRE(read_file(dir.file(wal_file_name(1).c_str()), data));
  CHECK_EQ(data.size(), size_t{WAL_FILE_HEADER_BYTES});

  batch_seq_t seq = 0;
  REQUIRE(append_record(*log, payload.data(), payload.size(), seq).ok());
  CHECK_EQ(seq, batch_seq_t{1});
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> recovered;
  REQUIRE(read_wal(dir, recovered));
  CHECK(recovered == std::vector<std::string>{payload});

  return;
}

/** I/O failure returns its original diagnostic for the caller to retain. */
static void test_failure(bool partial) {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());

  batch_seq_t seq = 0;
  CHECK(append_record(*log, "one", 3, seq).ok());
  CHECK(seq == 1);

  if (partial) {
    ut_debug_global().arm("wal.partial_write", DB_IO_ERROR);
  } else {
    ut_debug_global().arm("wal.sync", DB_IO_ERROR);
  }

  const db_status_t failure = append_record(*log, "two", 3, seq);
  CHECK(failure.code() == DB_OUTCOME_UNKNOWN);
  CHECK(seq == 2);

  CHECK(failure.message().find(partial ? "cannot write WAL file"
                                       : "cannot sync WAL file") == 0);

  wal::stats_t stats;
  log->stats(stats);
  CHECK(stats.m_records == 1 && stats.m_groups == 1);
  CHECK(wal::destroy(log).ok());

  /* Recovery discards an incomplete group before the raw format check. */
  wal_recovery_t recovered;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recovered).ok());
  CHECK(partial ? recovered.m_last_seq == 1
                : recovered.m_last_seq >= 1 && recovered.m_last_seq <= 2);
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.size() >= 1 && payloads.size() <= 2);
  CHECK(payloads[0] == "one");

  return;
}

/** Rotation failed before the new group was written; its sequence is unused. */
static void test_rotation_failure() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log, 0, true, 1).ok());

  batch_seq_t seq = 0;
  CHECK(append_record(*log, "one", 3, seq).ok());

  const std::string blocked =
      std::string(dir.m_path) + "/" + wal_file_name(2) + ".init";
  CHECK(mkdir(blocked.c_str(), 0700) == 0);
  CHECK(append_record(*log, "two", 3, seq).code() == DB_IO_ERROR);
  CHECK(seq == 0);
  CHECK(wal::destroy(log).ok());
  CHECK(rmdir(blocked.c_str()) == 0);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.size() == 1 && payloads[0] == "one");
}

/** Global failure points also apply during creation: a failed header write
publishes no file, and a failed rotation leaves the WAL usable. */
static void test_file_failure() {
  test_dir_t dir;
  wal *log = nullptr;

  ut_debug_global().arm("wal.write_header", DB_IO_ERROR);
  CHECK_EQ(wal::create(dir.m_path, &log, 0, true, 1).code(), DB_IO_ERROR);
  CHECK(log == nullptr);

  std::vector<batch_seq_t> file_first_seqs;
  CHECK(wal_scan_files(dir.m_path, file_first_seqs).ok() &&
        file_first_seqs.empty());

  REQUIRE(wal::create(dir.m_path, &log, 0, true, 1).ok());

  batch_seq_t seq = 0;
  CHECK(append_record(*log, "one", 3, seq).ok());

  /* The second group needs a new file; nothing of it is written. */
  ut_debug_global().arm("wal.write_header", DB_IO_ERROR);
  CHECK_EQ(append_record(*log, "two", 3, seq).code(), DB_IO_ERROR);
  CHECK_EQ(seq, batch_seq_t{0});

  /* The unpublished init file does not block the retry. */
  seq = 1;
  CHECK(append_record(*log, "two", 3, seq).ok());
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads == std::vector<std::string>({"one", "two"}));

  return;
}

/** A failed file deletion removes nothing and can be retried. */
static void test_purge_failure() {
  test_dir_t dir;
  wal *log = nullptr;
  REQUIRE(wal::create(dir.m_path, &log, 0, false, 1).ok());

  batch_seq_t seq = 0;

  for (const char *payload : {"one", "two", "three"}) {
    CHECK(append_record(*log, payload, strlen(payload), seq).ok());
  }

  wal::stats_t stats;
  log->stats(stats);
  CHECK_EQ(stats.m_file_count, uint64_t{3});

  ut_debug_global().arm("wal.purge", DB_IO_ERROR);
  CHECK_EQ(log->purge(2).code(), DB_IO_ERROR);
  log->stats(stats);
  CHECK_EQ(stats.m_file_count, uint64_t{3});

  CHECK(log->purge(2).ok());
  log->stats(stats);
  CHECK_EQ(stats.m_file_count, uint64_t{1});
  CHECK(wal::destroy(log).ok());

  return;
}

/** Global failure points also apply during recovery: a failed truncation
of an invalid tail fails the open and leaves the file as it was. */
static void test_truncate_failure() {
  test_dir_t dir;
  wal *log = nullptr;
  REQUIRE(wal::create(dir.m_path, &log).ok());

  batch_seq_t seq = 0;
  CHECK(append_record(*log, "one", 3, seq).ok());
  CHECK(wal::destroy(log).ok());

  const std::string path = dir.file(wal_file_name(1).c_str());
  FILE *file = fopen(path.c_str(), "ab");
  REQUIRE(file != nullptr);
  CHECK(fputs("torn tail", file) >= 0);
  CHECK(fclose(file) == 0);

  std::vector<unsigned char> torn;
  CHECK(read_file(path, torn));

  ut_debug_global().arm("wal.truncate", DB_IO_ERROR);

  wal_recovery_t recovery;
  CHECK_EQ(wal::open(dir.m_path, nullptr, nullptr, &log, recovery, 0, true,
                     WAL_DEFAULT_FILE_SIZE_BYTES, 0)
               .code(),
           DB_IO_ERROR);
  CHECK(log == nullptr);

  std::vector<unsigned char> after;
  CHECK(read_file(path, after));
  CHECK(after == torn);

  REQUIRE(wal::open(dir.m_path, nullptr, nullptr, &log, recovery, 0, true,
                    WAL_DEFAULT_FILE_SIZE_BYTES, 0)
              .ok());

  CHECK_EQ(recovery.m_last_seq, batch_seq_t{1});
  CHECK(recovery.m_tail_bytes > 0);
  CHECK(wal::destroy(log).ok());

  return;
}

/** Run WAL creation, append, recovery and failure tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"create", test_create},
      {"sequential", test_sequential},
      {"unsynced", test_unsynced},
      {"invalid_argument", test_invalid_argument},
      {"max_payload", test_max_payload},
      {"batch_append", test_batch_append},
      {"sequence_invariants", test_sequence_invariants},
      {"failure/sync", [] { test_failure(false); }},
      {"failure/partial_write", [] { test_failure(true); }},
      {"rotation_failure", test_rotation_failure},
      {"file_failure", test_file_failure},
      {"purge_failure", test_purge_failure},
      {"truncate_failure", test_truncate_failure},
  };

  return (test_main("wal-t", argc, argv, cases));
}
