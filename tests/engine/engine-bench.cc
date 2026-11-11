/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine-bench.cc
 *    Write path benchmark.
 *
 * N threads call engine_write() in a closed loop for a fixed time, each
 * write holding P points of one table (a host tag and two DOUBLE fields).
 * Reports writes and points per second and write latency, then stores
 * everything in one flush and reports the rows stored per second.
 *
 * Usage: engine-bench <dir> [seconds] [points_per_write] [writers...]
 * Defaults: 3 seconds, 1 point per write, writers 1 4 16.
 *
 *-------------------------------------------------------------------------
 */

#include "engine.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <time.h>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "ingest.h"
#include "test_point.h"
#include "test_util.h"
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
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Number of points submitted in each write call. */
  uint32_t m_points_per_write{1};

  /** Stop at this monotonic time. */
  uint64_t m_deadline_ns{0};

  /** Latency of each write in nanoseconds. */
  std::vector<uint64_t> m_latencies;

  /** Calls that failed. */
  int m_errors{0};
};

/** Write until the deadline, timing each write. */
static void bench_main(bench_arg_t *arg) {
  char host[32];
  snprintf(host, sizeof(host), "host-%d", arg->m_thread_no);

  ingest_tag_t tags[] = {tag("host", host)};
  ingest_field_t fields[] = {field_double("cpu", 0.5),
                             field_double("memory", 0.25)};

  std::vector<ingest_point_t> points;
  for (uint32_t k = 0; k < arg->m_points_per_write; ++k) {
    points.push_back(point(T0 + k, tags, 1, fields, 2));
  }

  for (;;) {
    const uint64_t start = now_ns();
    if (start >= arg->m_deadline_ns) {
      break;
    }

    batch_seq_t seq;
    if (!engine_write(*arg->m_engine, "metrics", 7, &points[0],
                      arg->m_points_per_write, seq)
             .ok()) {
      arg->m_errors++;
      break;
    }

    arg->m_latencies.push_back(now_ns() - start);
  }
}

/** Read a quantile of sorted latency samples in microseconds.
@param[in]  q  quantile fraction in the range [0, 1]
@return quantile q of sorted nanosecond latencies, in microseconds */
static double percentile_us(const std::vector<uint64_t> &sorted, double q) {
  const size_t i =
      static_cast<size_t>(q * static_cast<double>(sorted.size() - 1));

  return (static_cast<double>(sorted[i]) / 1000.0);
}

/** Run one configuration in a fresh subdirectory.
@return 0 on success */
static int bench_run(const char *base, int writers, double seconds,
                     uint32_t points_per_write) {
  char dir[1024];
  snprintf(dir, sizeof(dir), "%s/bench-%d-%d", base, writers,
           static_cast<int>(getpid()));

  if (mkdir(dir, 0755) != 0) {
    perror("mkdir");

    return (1);
  }

  engine_t *engine = nullptr;
  if (!engine_open(dir, nullptr, &engine).ok()) {
    fprintf(stderr, "engine_open failed\n");

    return (1);
  }

  std::vector<bench_arg_t> args(writers);
  std::vector<std::thread> threads;

  const uint64_t start = now_ns();
  const uint64_t deadline = start + static_cast<uint64_t>(seconds * 1e9);

  for (int t = 0; t < writers; ++t) {
    args[t].m_engine = engine;
    args[t].m_thread_no = t;
    args[t].m_points_per_write = points_per_write;
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

  /* Store everything written in one flush. */
  const uint64_t flush_start = now_ns();
  engine_flush_info_t info;
  const db_status_t flush_err = engine_flush(*engine, info);
  const double flush_s = static_cast<double>(now_ns() - flush_start) / 1e9;

  const db_status_t close_err = engine_close(engine);
  if (!flush_err.ok() || !close_err.ok() || errors != 0 || all.empty()) {
    fprintf(stderr, "run failed: %d write errors\n", errors);

    return (1);
  }

  std::sort(all.begin(), all.end());

  const double writes = static_cast<double>(all.size()) / elapsed;

  printf("%7d %12.0f %12.0f %10.1f %10.1f %12.0f\n", writers, writes,
         writes * points_per_write, percentile_us(all, 0.50),
         percentile_us(all, 0.99), static_cast<double>(info.m_rows) / flush_s);

  test_remove_dir(dir);

  return (0);
}

/** Run the benchmark.
@param[in]  argv  argument strings; argv[0] names the program
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr,
            "usage: %s <dir> [seconds] [points_per_write] [writers...]\n",
            argv[0]);

    return (2);
  }

  const double seconds = argc > 2 ? atof(argv[2]) : 3.0;
  const uint32_t points_per_write =
      argc > 3 ? static_cast<uint32_t>(atoi(argv[3])) : 1;

  std::vector<int> writers;
  for (int i = 4; i < argc; ++i) {
    writers.push_back(atoi(argv[i]));
  }

  if (writers.empty()) {
    writers.push_back(1);
    writers.push_back(4);
    writers.push_back(16);
  }

  printf("%u points per write, %.1f s per run\n", points_per_write, seconds);
  printf("%7s %12s %12s %10s %10s %12s\n", "writers", "writes/s", "points/s",
         "p50_us", "p99_us", "flush_rows/s");

  for (size_t i = 0; i < writers.size(); ++i) {
    if (bench_run(argv[1], writers[i], seconds, points_per_write) != 0) {
      return (1);
    }
  }

  return (0);
}
