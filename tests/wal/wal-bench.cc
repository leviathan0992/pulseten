/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal-bench.cc
 *    WAL batch I/O benchmark; db-bench measures concurrent group commit.
 *
 * Usage: wal-bench <dir> [seconds] [payload_bytes] [records_per_group...]
 * Defaults: 3 seconds, 100-byte payloads, groups of 1, 4 and 16 records.
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "test_util.h"

/** Append caller-selected batches in a fresh WAL and report I/O latency. */
static int bench_run(const char *base, size_t count, double seconds,
                     size_t payload_bytes) {
  const std::string dir = std::string(base) + "/batch-" +
                          std::to_string(count) + "-" +
                          std::to_string(getpid());
  if (mkdir(dir.c_str(), 0755) != 0) {
    perror("mkdir");

    return (1);
  }

  wal *log = nullptr;
  if (!wal::create(dir.c_str(), &log).ok()) {
    return (1);
  }

  const std::string payload(payload_bytes, 'p');
  std::vector<wal_record_t> records(count);
  std::vector<double> latencies;
  batch_seq_t sequence = 0;
  const auto start = std::chrono::steady_clock::now();
  const auto deadline = start + std::chrono::duration<double>(seconds);
  db_status_t err;
  while (std::chrono::steady_clock::now() < deadline) {
    for (wal_record_t &record : records) {
      record.m_seq = ++sequence;
      record.m_payload =
          reinterpret_cast<const unsigned char *>(payload.data());
      record.m_len = payload.size();
    }

    const auto before = std::chrono::steady_clock::now();
    err = log->append(records.data(), records.size());
    if (!err.ok()) {
      break;
    }

    latencies.push_back(std::chrono::duration<double, std::micro>(
                            std::chrono::steady_clock::now() - before)
                            .count());
  }

  const double elapsed =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
          .count();
  wal_stats_t stats;
  log->read_stats(stats);
  const db_status_t closed = wal::destroy(log);
  if (!err.ok() || !closed.ok() || latencies.empty()) {
    fprintf(stderr, "WAL batch failed: %s\n", err.message().c_str());
    test_remove_dir(dir.c_str());

    return (1);
  }

  std::sort(latencies.begin(), latencies.end());
  printf("%7zu %12.0f %10.2f %10.1f %10.1f %10.1f\n", count,
         static_cast<double>(stats.m_records) / elapsed,
         static_cast<double>(stats.m_bytes) / elapsed / (1024.0 * 1024.0),
         latencies[latencies.size() / 2],
         latencies[(latencies.size() - 1) * 99 / 100], latencies.back());
  test_remove_dir(dir.c_str());

  return (0);
}

/** Run pure WAL I/O configurations without introducing a second writer queue.
 */
int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(
        stderr,
        "usage: %s <dir> [seconds] [payload_bytes] [records_per_group...]\n",
        argv[0]);

    return (2);
  }

  const double seconds = argc > 2 ? atof(argv[2]) : 3.0;
  const size_t bytes = argc > 3 ? static_cast<size_t>(atol(argv[3])) : 100;
  if (seconds <= 0 || bytes == 0 || bytes > WAL_MAX_PAYLOAD) {
    return (2);
  }

  std::vector<size_t> groups;
  for (int i = 4; i < argc; ++i) {
    uint64_t count = 0;
    if (!test_parse_positive_count(argv[i], count) || count > 65536) {
      return (2);
    }

    groups.push_back(static_cast<size_t>(count));
  }

  if (groups.empty()) {
    groups = {1, 4, 16};
  }

  printf("%7s %12s %10s %10s %10s %10s\n", "records", "records/s", "MiB/s",
         "p50_us", "p99_us", "max_us");
  for (size_t count : groups) {
    if (bench_run(argv[1], count, seconds, bytes) != 0) {
      return (1);
    }
  }

  return (0);
}
