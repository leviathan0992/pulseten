/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal-t.cc
 *    Tests for WAL creation, record format and batch I/O.
 *
 * Each test works in a new directory under $TMPDIR (or /tmp).
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "os_file.h"
#include "test_util.h"
#include "test_wal.h"
#include "test_wal_access.h"
#include "ut.h"
#include "ut_crc32.h"
#include "ut_err.h"
#include "wal_format.h"

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
  if (!os_file_get_size(file, size).ok()) {
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

/** Read every segment of a WAL and check its format, independently of the
recovery code.
@param[out]  payloads  payload of record seq is payloads[seq - 1]
@return true if every segment header and record is valid, the records cover
  every segment completely, and sequence numbers run 1, 2, 3, ... across the
  segments */
static bool read_wal(const test_dir_t &dir,
                     std::vector<std::string> &payloads) {
  payloads.clear();
  uint64_t wal_id = 0;

  for (int segment = 0;; ++segment) {
    /* The next segment starts right after the last record read. */
    const uint64_t first = payloads.size() + 1;
    const std::string path = dir.file(wal_segment_name(first).c_str());

    if (!file_exists(path)) {
      return (segment > 0);
    }

    std::vector<unsigned char> data;
    if (!read_file(path, data) || data.size() < WAL_FILE_HEADER_SIZE) {
      return (false);
    }

    const unsigned char *h = &data[0];
    if (memcmp(h, "PTWAL001", 8) != 0 ||
        read_u32(h + 8) != WAL_FORMAT_VERSION ||
        read_u32(h + 12) != WAL_FILE_HEADER_SIZE || read_u64(h + 24) != first ||
        read_u32(h + 32) != 0 || read_u32(h + 36) != ut_crc32c(h, 36, 0)) {
      return (false);
    }

    if (segment > 0 && read_u64(h + 16) != wal_id) {
      return (false);
    }

    wal_id = read_u64(h + 16);

    const uint64_t size = data.size();
    uint64_t at = WAL_FILE_HEADER_SIZE;
    uint32_t prev_index = 0;

    while (at < size) {
      if (size - at < WAL_RECORD_HEADER_SIZE) {
        return (false);
      }

      const unsigned char *r = &data[at];
      const uint32_t len = read_u32(r + 4);

      /* A record starts a new group (position 0) or follows the previous
      record of its group, which is always in the same segment. */
      const uint32_t index = read_u32(r + 16);
      const bool index_ok =
          index == 0 || (at > WAL_FILE_HEADER_SIZE && index == prev_index + 1);

      if (read_u32(r) != WAL_RECORD_MAGIC || len == 0 ||
          len > size - at - WAL_RECORD_HEADER_SIZE ||
          read_u64(r + 8) != payloads.size() + 1 || !index_ok) {
        return (false);
      }

      prev_index = index;

      uint32_t crc = ut_crc32c(r, 20, 0);
      crc = ut_crc32c(r + WAL_RECORD_HEADER_SIZE, len, crc);
      if (read_u32(r + 20) != crc) {
        return (false);
      }

      payloads.push_back(std::string(
          reinterpret_cast<const char *>(r) + WAL_RECORD_HEADER_SIZE, len));
      at += WAL_RECORD_HEADER_SIZE + len;
    }

    /* An empty segment can only be the last one. */
    if (payloads.size() + 1 == first) {
      return (true);
    }
  }
}

/** A new WAL has one empty segment and appends to it. */
static void test_create() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());
  CHECK(log != nullptr);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.empty());
  CHECK(!file_exists(dir.file((wal_segment_name(1) + ".init").c_str())));

  /* A second WAL in the same directory is refused; the first is left
  untouched. */
  CHECK(wal::destroy(log).ok());

  wal *second = nullptr;
  CHECK(wal::create(dir.m_path, &second).code() == DB_IO_ERROR);
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

  wal_stats_t stats;
  log->read_stats(stats);
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
  for (int i = 0; i < 20; ++i) {
    const std::string payload = "record-" + std::to_string(i);
    /* Disabling sync must bypass the group's sync failure hook too. */
    wal_test_access::fail_next_sync(*log);
    batch_seq_t seq = static_cast<batch_seq_t>(i);
    CHECK(append_record(*log, payload.data(), payload.size(), seq).ok());
    CHECK(seq == static_cast<batch_seq_t>(i) + 1);
    expected.push_back(payload);
  }

  CHECK(log->sync().ok());
  wal_stats_t stats;
  log->read_stats(stats);
  CHECK(stats.m_segments > 1);
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads == expected);

  wal_recv_t recv;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recv, 0, false).ok());
  CHECK(recv.m_last_seq == 20);
  wal_test_access::fail_next_sync(*log);
  batch_seq_t seq = recv.m_last_seq;
  CHECK(append_record(*log, "reopened", 8, seq).ok());
  CHECK(seq == 21);
  CHECK(wal::destroy(log).ok());

  /* Switching to sync mode may rotate before its first group is written. */
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recv, 0, true, 1).ok());
  CHECK(append_record(*log, "synced", 6, seq).ok());
  CHECK(seq == 22);
  CHECK(wal::destroy(log).ok());
  expected.push_back("reopened");
  expected.push_back("synced");
  CHECK(read_wal(dir, payloads));
  CHECK(payloads == expected);

  return;
}

/** Empty and oversized payloads are rejected without using a sequence number.
 */
static void test_invalid_argument() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());

  batch_seq_t seq = 99;
  CHECK(append_record(*log, "x", 0, seq).code() == DB_INVALID_ARGUMENT);
  CHECK(seq == 0);
  CHECK(append_record(*log, nullptr, 1, seq).code() == DB_INVALID_ARGUMENT);
  std::vector<char> big(WAL_MAX_PAYLOAD + 1, 'x');
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
  std::string payload(WAL_MAX_PAYLOAD, 'm');
  batch_seq_t seq = 0;
  CHECK(append_record(*log, payload.data(), payload.size(), seq).ok());
  CHECK(wal::destroy(log).ok());

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.size() == 1 && payloads[0] == payload);

  return;
}

/** Group boundaries and record indices follow the batches chosen by db. */
static void test_batch_append() {
  test_dir_t dir;
  wal *log = nullptr;
  CHECK(wal::create(dir.m_path, &log).ok());
  const std::string payloads[] = {"a", "b", "c", "d", "e", "f"};
  wal_record_t records[3];
  for (size_t group = 0; group < 2; ++group) {
    for (size_t i = 0; i < 3; ++i) {
      const size_t n = group * 3 + i;
      records[i].m_seq = n + 1;
      records[i].m_payload =
          reinterpret_cast<const unsigned char *>(payloads[n].data());
      records[i].m_len = payloads[n].size();
    }

    CHECK(log->append(records, 3).ok());
  }

  wal_stats_t stats;
  log->read_stats(stats);
  CHECK(stats.m_groups == 2 && stats.m_records == 6);
  CHECK(wal::destroy(log).ok());
  std::vector<std::string> recovered;
  CHECK(read_wal(dir, recovered));
  CHECK(recovered == std::vector<std::string>(payloads, payloads + 6));

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
    wal_test_access::partial_write(*log);
  } else {
    wal_test_access::fail_next_sync(*log);
  }
  const db_status_t failure = append_record(*log, "two", 3, seq);
  CHECK(failure.code() == DB_OUTCOME_UNKNOWN);
  CHECK(seq == 2);

  CHECK(failure.message().find(partial ? "cannot write WAL segment"
                                       : "cannot sync WAL segment") == 0);

  wal_stats_t stats;
  log->read_stats(stats);
  CHECK(stats.m_records == 1 && stats.m_groups == 1);
  CHECK(wal::destroy(log).ok());

  /* Recovery discards an incomplete group before the raw format check. */
  wal_recv_t recovered;
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
      std::string(dir.m_path) + "/" + wal_segment_name(2) + ".init";
  CHECK(mkdir(blocked.c_str(), 0700) == 0);
  CHECK(append_record(*log, "two", 3, seq).code() == DB_IO_ERROR);
  CHECK(seq == 0);
  CHECK(wal::destroy(log).ok());
  CHECK(rmdir(blocked.c_str()) == 0);
  std::vector<std::string> payloads;
  CHECK(read_wal(dir, payloads));
  CHECK(payloads.size() == 1 && payloads[0] == "one");
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_create();
  test_sequential();
  test_unsynced();
  test_invalid_argument();
  test_max_payload();
  test_batch_append();
  test_failure(false);
  test_failure(true);
  test_rotation_failure();

  return (test_result("wal-t"));
}
