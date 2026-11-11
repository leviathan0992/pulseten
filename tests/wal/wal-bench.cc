/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal-bench.cc
 *    Group commit benchmark.
 *
 * N threads call append_record() in a closed loop for a fixed time and report
 * throughput, latency percentiles and records per flush.
 *
 * Usage: wal-bench <dir> [seconds] [payload_bytes] [writers...]
 * Defaults: 3 seconds, 100-byte payloads, writers 1 4 16.
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <time.h>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "test_util.h"
#include "test_wal.h"
#include "ut.h"
#include "ut_err.h"

/** Read the monotonic clock in nanoseconds.
@return the monotonic clock in nanoseconds */
static uint64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
          static_cast<uint64_t>(ts.tv_nsec));
}

struct bench_arg_t {
  /** WAL under test. */
  wal_t *m_wal{nullptr};

  /** Size of each payload. */
  size_t m_payload_bytes{0};

  /** Stop at this monotonic time. */
  uint64_t m_deadline_ns{0};

  /** Latency of each append in nanoseconds. */
  std::vector<uint64_t> m_latencies;

  /** Calls that failed. */
  int m_errors{0};
};

/** Append payloads until the deadline, timing each append. */
static void bench_main(bench_arg_t *arg) {
  std::string payload(arg->m_payload_bytes, 'p');
  for (;;) {
    const uint64_t start = now_ns();
    if (start >= arg->m_deadline_ns) {
      break;
    }
    batch_seq_t seq;
    if (!append_record(*arg->m_wal, payload.data(), payload.size(), seq).ok()) {
      arg->m_errors++;
      break;
    }
    arg->m_latencies.push_back(now_ns() - start);
  }
}

/** Read a quantile of sorted latency samples in microseconds.
@param[in]  q  quantile fraction in the range [0, 1]
@return latency at fraction q of the sorted samples, in microseconds */
static double percentile_us(const std::vector<uint64_t> &sorted, double q) {
  size_t i = static_cast<size_t>(q * static_cast<double>(sorted.size() - 1));

  return (static_cast<double>(sorted[i]) / 1000.0);
}

/** Run one configuration in a fresh subdirectory.
@return 0 on success */
static int bench_run(const char *base, int writers, double seconds,
                     size_t payload_bytes) {
  char dir[1024];
  snprintf(dir, sizeof(dir), "%s/bench-%d-%d", base, writers,
           static_cast<int>(getpid()));
  if (mkdir(dir, 0755) != 0) {
    perror("mkdir");

    return (1);
  }
  wal_t *wal = nullptr;
  if (!wal_create(dir, 0, &wal).ok()) {
    fprintf(stderr, "wal_create failed\n");

    return (1);
  }

  std::vector<bench_arg_t> args(writers);
  std::vector<std::thread> threads;
  const uint64_t start = now_ns();
  const uint64_t deadline = start + static_cast<uint64_t>(seconds * 1e9);
  for (int t = 0; t < writers; ++t) {
    args[t].m_wal = wal;
    args[t].m_payload_bytes = payload_bytes;
    args[t].m_deadline_ns = deadline;
    args[t].m_latencies.reserve(1 << 20);
    threads.push_back(std::thread(bench_main, &args[t]));
  }
  int errors = 0;
  std::vector<uint64_t> all;
  for (int t = 0; t < writers; ++t) {
    threads[t].join();
    errors += args[t].m_errors;
    all.insert(all.end(), args[t].m_latencies.begin(),
               args[t].m_latencies.end());
  }
  const double elapsed = static_cast<double>(now_ns() - start) / 1e9;

  wal_stats_t stats;
  wal_get_stats(*wal, stats);
  if (!wal_close(wal).ok() || errors != 0 || all.empty()) {
    fprintf(stderr, "run failed: %d append errors\n", errors);

    return (1);
  }
  std::sort(all.begin(), all.end());

  printf("%7d %12.0f %10.2f %10.1f %10.1f %10.1f %12.2f\n", writers,
         static_cast<double>(all.size()) / elapsed,
         static_cast<double>(stats.m_bytes) / elapsed / (1024.0 * 1024.0),
         percentile_us(all, 0.50), percentile_us(all, 0.99),
         static_cast<double>(all.back()) / 1000.0,
         static_cast<double>(stats.m_records) /
             static_cast<double>(stats.m_groups));

  test_remove_dir(dir);

  return (0);
}

/** Run the benchmark.
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s <dir> [seconds] [payload_bytes] [writers...]\n",
            argv[0]);

    return (2);
  }
  const double seconds = argc > 2 ? atof(argv[2]) : 3.0;
  const size_t payload_bytes =
      argc > 3 ? static_cast<size_t>(atol(argv[3])) : 100;
  std::vector<int> writers;
  for (int i = 4; i < argc; ++i) {
    writers.push_back(atoi(argv[i]));
  }
  if (writers.empty()) {
    writers.push_back(1);
    writers.push_back(4);
    writers.push_back(16);
  }

  printf("payload %zu bytes, %.1f s per run\n", payload_bytes, seconds);
  printf("%7s %12s %10s %10s %10s %10s %12s\n", "writers", "appends/s", "MiB/s",
         "p50_us", "p99_us", "max_us", "recs/flush");
  for (size_t i = 0; i < writers.size(); ++i) {
    if (bench_run(argv[1], writers[i], seconds, payload_bytes) != 0) {
      return (1);
    }
  }

  return (0);
}
