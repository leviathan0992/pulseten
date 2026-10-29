/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine0flusher-t.cc
 *    Tests for background flushing: by interval and by size, WAL segments
 *    deleted after flushes, writers held back by the memtable limit, and
 *    SIGKILL while all of it runs.
 *
 *-------------------------------------------------------------------------
 */

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "duck0duck.h"
#include "test0engine.h"
#include "ut0time.h"

/** Options without a background thread, for checking results.
@return options with background flushing disabled */
static const engine_options_t *manual() {
  static engine_options_t options;
  options.background_flush = false;

  return (&options);
}

/** Wait up to timeout_ms for the memtable to be empty.
@param[in,out]  engine      database engine
@param[in]      timeout_ms  maximum wait in milliseconds
@return true if it became empty */
static bool wait_until_flushed(engine_t *engine, int timeout_ms) {
  for (int waited = 0; waited < timeout_ms; waited += 10) {
    memtable_stats_t stats;
    engine_get_memtable_stats(*engine, stats);

    if (stats.live_writes == 0) {
      return (true);
    }

    usleep(10 * 1000);
  }

  return (false);
}

/** Reopen without a background thread and return every stored or replayed id.
@param[in]   dir   database or WAL directory
@param[out]  recv  recovery counters from reopening the database
@return ids read from storage and replayed memtable batches */
static std::vector<int64_t> reopen_ids(const test_dir_t &dir,
                                       engine_recv_t *recv) {
  engine_t *engine = nullptr;
  std::vector<int64_t> ids;

  CHECK(engine_open(dir.path, manual(), &engine, recv, test_error) ==
        DB_SUCCESS);
  if (engine != nullptr) {
    ids = all_ids(engine);
    CHECK(engine_close(engine) == DB_SUCCESS);
  }

  return (ids);
}

/** The background thread flushes on its interval, with no explicit flush. */
static void test_interval_flush() {
  test_dir_t dir;
  engine_options_t options;
  options.flush_interval_ms = 20;

  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  for (int64_t v = 1; v <= 10; ++v) {
    CHECK(write_id(engine, v) == DB_SUCCESS);
  }

  /* No engine_flush() call: the interval stores everything. */
  CHECK(wait_until_flushed(engine, 3000));

  engine_stats_t stats;
  engine_get_stats(*engine, stats);
  CHECK(stats.background_flushes >= 1 && stats.background_errors == 0);
  CHECK(engine_close(engine) == DB_SUCCESS);

  engine_recv_t recv;
  CHECK(reopen_ids(dir, &recv).size() == 10);
  CHECK(recv.flushed_seq == 10 && recv.replayed == 0);
}

/** Reaching flush_bytes starts a flush before the interval is up. */
static void test_size_trigger() {
  test_dir_t dir;
  engine_options_t options;
  options.flush_interval_ms = 60 * 1000;
  options.flush_bytes = 4096;

  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  for (int64_t v = 1; v <= 200; ++v) {
    CHECK(write_id(engine, v) == DB_SUCCESS);
  }

  /* The interval is a minute; only the size can have started a flush. */
  bool flushed = false;

  for (int waited = 0; waited < 3000 && !flushed; waited += 10) {
    engine_stats_t stats;
    engine_get_stats(*engine, stats);
    flushed = stats.background_flushes >= 1;
    usleep(10 * 1000);
  }

  CHECK(flushed);
  CHECK(engine_close(engine) == DB_SUCCESS);

  engine_recv_t recv;
  CHECK(reopen_ids(dir, &recv).size() == 200);
  CHECK(recv.flushed_seq > 0);
}

/** A committed background flush is counted even if the following expiry
fails; expiry failures are counted separately. */
static void test_flush_count_when_expiry_fails() {
  test_dir_t dir;
  engine_options_t options;
  options.background_flush = false;
  options.partition_days = 1;

  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  ingest_field_t field = field_int("id", 1);
  ingest_point_t p = point(ut_now_us() - 3 * UT_DAY_US, nullptr, 0, &field, 1);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "k", 1, &p, 1, seq, test_error) == DB_SUCCESS);

  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
  CHECK(engine_close(engine) == DB_SUCCESS);

  options.background_flush = true;
  options.retention_days = 1;
  options.flush_interval_ms = 10 * 60 * 1000;
  options.flush_bytes = 1;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  /* Before waking the flusher, make expiry of the old partition fail.
  The foreign key blocks its catalog deletion, but not a later flush. */
  duck_t duck;
  CHECK(duckdb_connect(storage_get_database(*engine_get_storage(*engine)),
                       &duck.conn) == DuckDBSuccess);
  CHECK(duck_exec(duck,
                  "CREATE TABLE _pt_test_expiry_block (table_id UBIGINT, "
                  "first_day BIGINT, FOREIGN KEY(table_id, first_day) "
                  "REFERENCES _pt_partitions(table_id, first_day))") ==
        DB_SUCCESS);
  CHECK(duck_exec(duck,
                  "INSERT INTO _pt_test_expiry_block "
                  "SELECT table_id, first_day FROM _pt_partitions") ==
        DB_SUCCESS);
  duckdb_disconnect(&duck.conn);

  p.time_us = ut_now_us();
  CHECK(engine_write(*engine, "k", 1, &p, 1, seq, test_error) == DB_SUCCESS);

  engine_stats_t stats;
  for (int waited = 0; waited < 3000; waited += 10) {
    engine_get_stats(*engine, stats);
    if (stats.background_errors > 0) {
      break;
    }
    usleep(10 * 1000);
  }

  memtable_stats_t mt;
  engine_get_memtable_stats(*engine, mt);
  CHECK(mt.flushed_seq == 2 && mt.live_writes == 0);
  CHECK(stats.background_flushes == 1);
  CHECK(stats.background_errors == 1);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

struct writer_arg_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int thread_no{0};

  /** Writes to make. */
  int count{0};

  /** Points per write. */
  uint32_t points{1};

  /** Length of the padding string of each point. */
  size_t pad{2};

  /** Calls that failed. */
  int errors{0};
};

/** Write count batches with unique ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  const std::string pad(arg->pad, 'p');

  for (int i = 0; i < arg->count; ++i) {
    std::vector<ingest_field_t> fields;
    std::vector<ingest_point_t> points;

    /* Ids unique across threads, writes and points. */
    const int64_t base =
        ((int64_t)arg->thread_no * 100000 + i) * (int64_t)arg->points;

    fields.resize(2 * arg->points);
    for (uint32_t k = 0; k < arg->points; ++k) {
      fields[2 * k] = field_int("id", base + k);
      fields[2 * k + 1] = field_str("pad", pad.c_str());
    }

    for (uint32_t k = 0; k < arg->points; ++k) {
      points.push_back(point(T0 + k, nullptr, 0, &fields[2 * k], 2));
    }

    batch_seq_t seq = 0;
    if (engine_write(*arg->engine, "k", 1, &points[0], arg->points, seq,
                     test_error) != DB_SUCCESS) {
      arg->errors++;
    }
  }
}

/** Run writer threads to completion.
@param[in,out]  engine   database engine
@param[in]      threads  number of writer threads
@param[in]      count    writes per writer
@param[in]      points   points per batch
@param[in]      pad      padding bytes per point */
static void run_writers(engine_t *engine, int threads, int count,
                        uint32_t points, size_t pad) {
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].engine = engine;
    args[t].thread_no = t;
    args[t].count = count;
    args[t].points = points;
    args[t].pad = pad;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
    CHECK(args[t].errors == 0);
  }
}

/** Background flushes delete stored WAL segments; reopening replays nothing. */
static void test_wal_recycled() {
  test_dir_t dir;
  engine_options_t options;
  options.flush_interval_ms = 10;
  options.wal_segment_bytes = 4096;

  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  run_writers(engine, 8, 100, 1, 2);
  CHECK(wait_until_flushed(engine, 5000));

  /* Hundreds of small segments were written; flushed ones are gone. */
  engine_stats_t stats;
  engine_get_stats(*engine, stats);
  printf("wal recycled: %llu segments left after %llu background flushes\n",
         (unsigned long long)stats.wal_segments,
         (unsigned long long)stats.background_flushes);
  CHECK(stats.wal_segments <= 2);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Reopening finds every write in storage and replays nothing. */
  engine_recv_t recv;
  const std::vector<int64_t> ids = reopen_ids(dir, &recv);

  CHECK(ids.size() == 800);
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == 800);
  CHECK(recv.replayed == 0 && recv.flushed_seq == 800);
  CHECK(recv.wal_records < 800);
}

struct sampler_arg_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *stop{nullptr};

  /** Protects *stop. */
  std::mutex *mutex{nullptr};

  /** Largest memtable size seen. */
  uint64_t max_bytes{0};
};

/** Record the largest memtable size seen.
@param[in,out]  arg  sampling configuration and largest observed memtable size
*/
static void sampler_main(sampler_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->mutex);
      if (*arg->stop) {
        break;
      }
    }

    memtable_stats_t stats;
    engine_get_memtable_stats(*arg->engine, stats);

    if (stats.live_bytes > arg->max_bytes) {
      arg->max_bytes = stats.live_bytes;
    }

    usleep(500);
  }
}

/** Writers wait while the memtable is over its limit, so it stays near the
limit. */
static void test_throttle() {
  test_dir_t dir;

  /* Writes of 100 points with 200-byte strings, about 25 KiB each, against
  a 64 KiB limit; the interval alone would not flush in time. */
  engine_options_t options;
  options.flush_interval_ms = 60 * 1000;
  options.flush_bytes = 64 * 1024;
  options.memtable_limit_bytes = 64 * 1024;

  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, &options, &engine, nullptr, test_error) ==
        DB_SUCCESS);

  bool stop = false;
  std::mutex mutex;
  sampler_arg_t sampler;
  sampler.engine = engine;
  sampler.stop = &stop;
  sampler.mutex = &mutex;
  std::thread sampler_thread(sampler_main, &sampler);

  const int threads = 4;
  run_writers(engine, threads, 30, 100, 200);

  {
    std::lock_guard<std::mutex> lock(mutex);
    stop = true;
  }
  sampler_thread.join();

  engine_stats_t stats;
  engine_get_stats(*engine, stats);

  /* Each writer checks the limit before writing, so the memtable can pass
  it by at most one write per writer. */
  const uint64_t one_write = 100 * (8 + 8 + 200 + 4 + 3) + 1024;
  printf("throttle: %llu writes waited, largest memtable %llu bytes\n",
         (unsigned long long)stats.throttled_writes,
         (unsigned long long)sampler.max_bytes);
  CHECK(stats.throttled_writes > 0);
  CHECK(sampler.max_bytes <=
        options.memtable_limit_bytes + threads * one_write);
  CHECK(engine_close(engine) == DB_SUCCESS);

  engine_recv_t recv;
  CHECK(reopen_ids(dir, &recv).size() == (size_t)(threads * 30 * 100));
}

/** Writer in the kill test: report each acknowledged id. */
struct kill_writer_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** First id to write. */
  int64_t base{0};

  /** Pipe end the acknowledgements are written to. */
  int fd{-1};
};

/** Write ids from base until killed; report each acknowledged one.
@param[in,out]  w  writer configuration, id range and acknowledgment pipe */
static void kill_writer_main(kill_writer_t *w) {
  for (int64_t i = 0;; ++i) {
    const int64_t id = w->base + i;

    if (write_id(w->engine, id) != DB_SUCCESS) {
      _exit(3);
    }

    if (write(w->fd, &id, sizeof(id)) != (ssize_t)sizeof(id)) {
      _exit(4);
    }
  }
}

/** Child process: write from four threads; the background thread flushes
often and deletes small WAL segments.
@param[in]  dir    database or WAL directory
@param[in]  round  crash-test round used to assign unique ids
@param[in]  fd     pipe descriptor for reporting acknowledged writes */
static void kill_child(const char *dir, int round, int fd) {
  engine_options_t options;
  options.flush_interval_ms = 2;
  options.wal_segment_bytes = 8192;

  engine_t *engine = nullptr;
  if (engine_open(dir, &options, &engine, nullptr, test_error) != DB_SUCCESS) {
    _exit(2);
  }

  kill_writer_t writers[4];
  std::thread threads[4];

  for (int t = 0; t < 4; ++t) {
    writers[t].engine = engine;
    writers[t].base = (int64_t)round * 100000000 + t * 1000000;
    writers[t].fd = fd;
    threads[t] = std::thread(kill_writer_main, &writers[t]);
  }

  threads[0].join();
}

/** Kill a process whose background thread flushes and recycles the WAL: every
acknowledged id survives, none twice. */
static void test_kill_with_background() {
  test_dir_t dir;
  std::set<int64_t> acked;

  for (int round = 0; round < 8; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.path, round, fds[1]);
    }
    close(fds[1]);

    const int wanted = 50 + round * 25;
    int received = 0;
    int64_t id = 0;

    while (received < wanted &&
           read(fds[0], &id, sizeof(id)) == (ssize_t)sizeof(id)) {
      acked.insert(id);
      ++received;
    }

    kill(pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    while (read(fds[0], &id, sizeof(id)) == (ssize_t)sizeof(id)) {
      acked.insert(id);
    }
    close(fds[0]);
    CHECK(received == wanted);

    /* Every acknowledged id survives, none twice, even though segments
    were being deleted when the process died. */
    engine_recv_t recv;
    const std::vector<int64_t> ids = reopen_ids(dir, &recv);
    const std::set<int64_t> unique(ids.begin(), ids.end());
    CHECK(unique.size() == ids.size());

    std::set<int64_t>::iterator it;
    for (it = acked.begin(); it != acked.end(); ++it) {
      CHECK(unique.count(*it) == 1);
    }

    if (round == 7) {
      printf("kill test: %zu acknowledged writes, F = %llu, WAL from %llu\n",
             acked.size(), (unsigned long long)recv.flushed_seq,
             (unsigned long long)(recv.flushed_seq - recv.replayed + 1));
    }
  }
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_interval_flush();
  test_size_trigger();
  test_flush_count_when_expiry_fails();
  test_wal_recycled();
  test_throttle();
  test_kill_with_background();

  return (test_result("engine0flusher-t"));
}
