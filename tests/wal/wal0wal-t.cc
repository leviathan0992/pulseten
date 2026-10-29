/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal0wal-t.cc
 *    Tests for WAL creation, record format and group commit.
 *
 * Each test works in a new directory under $TMPDIR (or /tmp).
 *
 *-------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "os0file.h"
#include "test0util.h"
#include "test0wal.h"
#include "ut0crc32.h"
#include "wal0wal.h"

/** Read a little-endian 32-bit value from record bytes.
@param[in]  b  encoded input bytes
@return the little-endian 32-bit value at b */
static uint32_t read_u32(const unsigned char *b) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= (uint32_t)b[i] << (8 * i);
  }

  return (v);
}

/** Read a little-endian 64-bit value from record bytes.
@param[in]  b  encoded input bytes
@return the little-endian 64-bit value at b */
static uint64_t read_u64(const unsigned char *b) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= (uint64_t)b[i] << (8 * i);
  }

  return (v);
}

/** Check whether a test file exists.
@param[in]  path  file or directory path
@return true if a file exists at path */
static bool file_exists(const std::string &path) {
  struct stat st;

  return (stat(path.c_str(), &st) == 0);
}

/** Read a file into memory.
@param[in]   path  file or directory path
@param[out]  data  file bytes; cleared before reading
@return true if it could be read */
static bool read_file(const std::string &path,
                      std::vector<unsigned char> *data) {
  os_file_t file;
  if (os_file_open(path.c_str(), OS_FILE_OPEN, file) != DB_SUCCESS) {
    return (false);
  }

  uint64_t size = 0;
  bool ok = os_file_get_size(file, size) == DB_SUCCESS;

  if (ok) {
    data->resize(size);
    ok = size == 0 || os_file_read(file, &(*data)[0], size, 0) == DB_SUCCESS;
  }

  (void)os_file_close(file);

  return (ok);
}

/** Read every segment of a WAL and check its format, independently of the
recovery code.
@param[in]   dir       directory holding the WAL
@param[out]  payloads  payload of record seq is payloads[seq - 1]
@return true if every segment header and record is valid, the records cover
  every segment completely, and sequence numbers run 1, 2, 3, ... across the
  segments */
static bool read_wal(const test_dir_t &dir,
                     std::vector<std::string> *payloads) {
  payloads->clear();
  uint64_t wal_id = 0;

  for (int segment = 0;; ++segment) {
    /* The next segment starts right after the last record read. */
    const uint64_t first = payloads->size() + 1;
    const std::string path = dir.file(wal_segment_name(first).c_str());

    if (!file_exists(path)) {
      return (segment > 0);
    }

    std::vector<unsigned char> data;
    if (!read_file(path, &data) || data.size() < WAL_FILE_HEADER_SIZE) {
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
          read_u64(r + 8) != payloads->size() + 1 || !index_ok) {
        return (false);
      }

      prev_index = index;

      uint32_t crc = ut_crc32c(r, 20, 0);
      crc = ut_crc32c(r + WAL_RECORD_HEADER_SIZE, len, crc);
      if (read_u32(r + 20) != crc) {
        return (false);
      }

      payloads->push_back(
          std::string((const char *)r + WAL_RECORD_HEADER_SIZE, len));
      at += WAL_RECORD_HEADER_SIZE + len;
    }

    /* An empty segment can only be the last one. */
    if (payloads->size() + 1 == first) {
      return (true);
    }
  }
}

/** A new WAL has one empty segment and appends to it. */
static void test_create() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);
  CHECK(wal != nullptr);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, &payloads));
  CHECK(payloads.empty());
  CHECK(!file_exists(dir.file((wal_segment_name(1) + ".init").c_str())));

  /* A second WAL in the same directory is refused; the first is left
  untouched. */
  CHECK(wal_close(wal) == DB_SUCCESS);

  wal_t *second = nullptr;
  CHECK(wal_create(dir.path, 0, &second) == DB_IO_ERROR);
  CHECK(second == nullptr);
  CHECK(read_wal(dir, &payloads));
}

/** Records of many lengths come back in order with consecutive sequence
numbers. */
static void test_sequential() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);

  std::vector<std::string> expected;
  for (int i = 0; i < 100; ++i) {
    /* Lengths 1 to 300 bytes. */
    std::string payload(1 + (i * 37) % 300, (char)('a' + i % 26));
    batch_seq_t seq = 0;
    CHECK(append_record(*wal, payload.data(), payload.size(), seq) ==
          DB_SUCCESS);
    CHECK(seq == (batch_seq_t)i + 1);
    expected.push_back(payload);
  }

  wal_stats_t stats;
  wal_get_stats(*wal, stats);
  CHECK(stats.records == 100);
  /* One writer never has company, so every record is its own group. */
  CHECK(stats.groups == 100);
  CHECK(wal_close(wal) == DB_SUCCESS);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, &payloads));
  CHECK(payloads == expected);
}

/** Empty and oversized payloads are rejected without using a sequence number.
 */
static void test_invalid_argument() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);

  batch_seq_t seq = 99;
  CHECK(append_record(*wal, "x", 0, seq) == DB_INVALID_ARGUMENT);
  CHECK(seq == 0);
  CHECK(append_record(*wal, nullptr, 1, seq) == DB_INVALID_ARGUMENT);
  std::vector<char> big(WAL_MAX_PAYLOAD + 1, 'x');
  CHECK(append_record(*wal, &big[0], big.size(), seq) == DB_INVALID_ARGUMENT);

  /* Rejected calls use no sequence numbers. */
  CHECK(append_record(*wal, "ok", 2, seq) == DB_SUCCESS);
  CHECK(seq == 1);
  CHECK(wal_close(wal) == DB_SUCCESS);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, &payloads));
  CHECK(payloads.size() == 1 && payloads[0] == "ok");
}

/** The largest payload is written, as a group of its own. */
static void test_max_payload() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);
  /* Larger than the default group limit; written as a group of one. */
  std::string payload(WAL_MAX_PAYLOAD, 'm');
  batch_seq_t seq = 0;
  CHECK(append_record(*wal, payload.data(), payload.size(), seq) == DB_SUCCESS);
  CHECK(wal_close(wal) == DB_SUCCESS);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, &payloads));
  CHECK(payloads.size() == 1 && payloads[0] == payload);
}

/** Work for one thread of a concurrent test. */
struct writer_arg_t {
  /** WAL under test. */
  wal_t *wal{nullptr};

  /** Number of this thread; keeps its payloads apart from the others'. */
  int thread_no{0};

  /** Records to append. */
  int count{0};

  /** Sequence number returned for each append. */
  std::vector<batch_seq_t> seqs;

  /** Calls that failed. */
  int errors{0};
};

/** Build the deterministic payload of one writer's record.
@param[in]  thread_no  writer thread index
@param[in]  i          zero-based row or record index
@return the payload of record i of a writer thread */
static std::string writer_payload(int thread_no, int i) {
  char buf[64];
  snprintf(buf, sizeof(buf), "thread-%d-record-%d", thread_no, i);

  return (buf);
}

/** Append count records, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  for (int i = 0; i < arg->count; ++i) {
    std::string payload = writer_payload(arg->thread_no, i);
    batch_seq_t seq = 0;
    if (append_record(*arg->wal, payload.data(), payload.size(), seq) !=
        DB_SUCCESS) {
      arg->errors++;
    }
    arg->seqs.push_back(seq);
  }
}

/** Run threads appending concurrently and check every record.
@param[in]   threads          number of writer threads
@param[in]   count            appends per thread
@param[in]   group_max_bytes  group limit passed to wal_create()
@param[out]  stats            WAL counters after the run */
static void run_concurrent(int threads, int count, size_t group_max_bytes,
                           wal_stats_t *stats) {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, group_max_bytes, &wal) == DB_SUCCESS);

  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;
  for (int t = 0; t < threads; ++t) {
    args[t].wal = wal;
    args[t].thread_no = t;
    args[t].count = count;
    workers.push_back(std::thread(writer_main, &args[t]));
  }
  for (int t = 0; t < threads; ++t) {
    workers[t].join();
  }
  wal_get_stats(*wal, *stats);
  CHECK(wal_close(wal) == DB_SUCCESS);

  std::vector<std::string> payloads;
  CHECK(read_wal(dir, &payloads));
  CHECK(payloads.size() == (size_t)threads * count);
  CHECK(stats->records == (uint64_t)threads * count);
  for (int t = 0; t < threads; ++t) {
    CHECK(args[t].errors == 0);
    for (int i = 0; i < count; ++i) {
      const batch_seq_t seq = args[t].seqs[i];
      /* Each thread's records keep their order and hold its payload. */
      CHECK(i == 0 || seq > args[t].seqs[i - 1]);
      CHECK(seq >= 1 && seq <= payloads.size() &&
            payloads[seq - 1] == writer_payload(t, i));
    }
  }
}

/** Concurrent writers share group flushes. */
static void test_concurrent() {
  wal_stats_t stats;
  run_concurrent(8, 300, 0, &stats);
  printf("concurrent: %llu records in %llu groups\n",
         (unsigned long long)stats.records, (unsigned long long)stats.groups);
  /* Eight writers waiting on flushes must share some of them. */
  CHECK(stats.groups < stats.records);
}

/** A group limit below one record makes every group a single record. */
static void test_group_limit() {
  /* A limit below one record makes every group a single record. */
  wal_stats_t stats;
  run_concurrent(4, 100, 1, &stats);
  CHECK(stats.groups == stats.records);
}

#ifdef PT_DEBUG
/** A failed group flush fences the WAL: later appends are rejected. */
static void test_fence() {
  test_dir_t dir;
  wal_t *wal = nullptr;
  CHECK(wal_create(dir.path, 0, &wal) == DB_SUCCESS);

  batch_seq_t seq = 0;
  CHECK(append_record(*wal, "one", 3, seq) == DB_SUCCESS);
  CHECK(seq == 1);

  wal_dbg_fail_next_flush(*wal);
  CHECK(append_record(*wal, "two", 3, seq) == DB_IO_ERROR);
  CHECK(seq == 2);

  CHECK(append_record(*wal, "three", 5, seq) == DB_FENCED);
  CHECK(seq == 0);

  wal_stats_t stats;
  wal_get_stats(*wal, stats);
  CHECK(stats.records == 1 && stats.groups == 1);
  CHECK(wal_close(wal) == DB_SUCCESS);

  /* The failed record's data was written before the injected flush
  failure, so it may be present; the file must still be well formed. */
  std::vector<std::string> payloads;
  CHECK(read_wal(dir, &payloads));
  CHECK(payloads.size() >= 1 && payloads.size() <= 2);
  CHECK(payloads[0] == "one");
}
#endif

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_create();
  test_sequential();
  test_invalid_argument();
  test_max_payload();
  test_concurrent();
  test_group_limit();
#ifdef PT_DEBUG
  test_fence();
#endif

  return (test_result("wal0wal-t"));
}
