/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine0flush-t.cc
 *    Tests for flushing: writes move from the memtable to DuckDB, reopening
 *    replays only what is not stored, flushes run alongside writes, and
 *    SIGKILL during writes and flushes loses or duplicates nothing.
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

#include "engine0engine.h"
#include "test0engine.h"
#include "test0point.h"
#include "test0util.h"

/** Options with flushing only by engine_flush(), so tests control it.
@return options with background flushing disabled */
static const engine_options_t *manual() {
  static engine_options_t options;
  options.background_flush = false;

  return (&options);
}

/** A flush stores every visible write, and reopening replays only the writes
after F. */
static void test_flush_and_reopen() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_recv_t recv;

  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);

  for (int64_t v = 1; v <= 3; ++v) {
    CHECK(write_id(engine, v) == DB_SUCCESS);
  }

  /* Everything visible goes to storage and leaves the memtable. */
  engine_flush_info_t info;
  std::string error;
  CHECK(engine_flush(*engine, info, error) == DB_SUCCESS);
  CHECK(info.writes == 3 && info.rows == 3 && info.flushed_seq == 3);

  memtable_stats_t stats;
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.flushed_seq == 3 && stats.live_writes == 0);
  CHECK(stats.garbage_writes == 0);

  /* Nothing left: a flush with nothing to do. */
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
  CHECK(info.writes == 0 && info.flushed_seq == 3);

  CHECK(write_id(engine, 4) == DB_SUCCESS);
  CHECK(all_ids(engine).size() == 4);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Reopening replays only the write after F. */
  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);
  CHECK(recv.flushed_seq == 3 && recv.wal_records == 4 && recv.replayed == 1);

  std::vector<int64_t> ids = all_ids(engine);
  CHECK(ids.size() == 4);
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == 4);

  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
  CHECK(info.writes == 1 && info.flushed_seq == 4);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** A schema version stored and a newer one only in the WAL are recovered
together. */
static void test_schema_across_flush() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  ingest_field_t f1[] = {field_int("a", 1)};
  ingest_field_t f2[] = {field_int("a", 2), field_double("b", 0.5)};
  ingest_point_t p1 = point(T0, nullptr, 0, f1, 1);
  ingest_point_t p2 = point(T0, nullptr, 0, f2, 2);
  batch_seq_t seq = 0;

  /* Version 1 is stored; version 2 only lives in the WAL. */
  CHECK(engine_write(*engine, "m", 1, &p1, 1, seq, test_error) == DB_SUCCESS);
  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
  CHECK(engine_write(*engine, "m", 1, &p2, 1, seq, test_error) == DB_SUCCESS);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Replay adds version 2 to the stored version 1, and later writes plan
  against it. */
  engine_recv_t recv;
  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);
  CHECK(recv.replayed == 1);
  CHECK(engine_write(*engine, "m", 1, &p1, 1, seq, test_error) == DB_SUCCESS);
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
  CHECK(info.writes == 2);

  const storage_state_t *state =
      storage_get_state(*(engine_get_storage(*engine)));
  const storage_table_t &m = state->tables.find("m")->second;
  CHECK(m.versions.size() == 2 && m.versions[1].cols.size() == 3);

  int64_t nulls = -1;
  CHECK(storage_query_int(*(engine_get_storage(*engine)),
                          "SELECT count(*) FROM " +
                              state->partitions.begin()->second.phys_name +
                              " WHERE c2 IS NULL",
                          nulls) == DB_SUCCESS);
  CHECK(nulls == 2);

  CHECK(engine_close(engine) == DB_SUCCESS);
}

#ifdef PT_DEBUG
/** A flush failing before commit changes nothing; the next one works. */
static void test_flush_failure() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(write_id(engine, 1) == DB_SUCCESS);

  /* A failed flush rolls back and changes nothing; the next one works. */
  engine_flush_info_t info;
  std::string error;
  engine_dbg_fail_next_store(*engine);
  CHECK(engine_flush(*engine, info, error) == DB_ERROR);
  CHECK(!error.empty() && info.writes == 0);

  memtable_stats_t stats;
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.flushed_seq == 0 && stats.live_writes == 1);

  CHECK(write_id(engine, 2) == DB_SUCCESS);
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);
  CHECK(info.writes == 2);
  CHECK(engine_close(engine) == DB_SUCCESS);
}
#endif

struct writer_arg_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int thread_no{0};

  /** Writes to make. */
  int count{0};

  /** Calls that failed. */
  int errors{0};
};

/** Write count ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  for (int i = 0; i < arg->count; ++i) {
    if (write_id(arg->engine, arg->thread_no * 100000 + i) != DB_SUCCESS) {
      arg->errors++;
    }
  }
}

struct flusher_arg_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *stop{nullptr};

  /** Protects *stop. */
  std::mutex *stop_mutex{nullptr};

  /** Flushes that stored something. */
  int flushes{0};

  /** Calls that failed. */
  int errors{0};
};

/** Flush until told to stop, counting flushes and failures.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void flusher_main(flusher_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->stop_mutex);
      if (*arg->stop) {
        break;
      }
    }

    engine_flush_info_t info;
    if (engine_flush(*arg->engine, info, test_error) != DB_SUCCESS) {
      arg->errors++;
    } else if (info.writes > 0) {
      arg->flushes++;
    }

    usleep(2000);
  }
}

/** Flushes running alongside writers lose and duplicate nothing. */
static void test_flush_during_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  bool stop = false;
  std::mutex stop_mutex;
  flusher_arg_t flusher;
  flusher.engine = engine;
  flusher.stop = &stop;
  flusher.stop_mutex = &stop_mutex;
  std::thread flusher_thread(flusher_main, &flusher);

  const int threads = 4;
  const int count = 150;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].engine = engine;
    args[t].thread_no = t;
    args[t].count = count;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
    CHECK(args[t].errors == 0);
  }

  {
    std::lock_guard<std::mutex> lock(stop_mutex);
    stop = true;
  }
  flusher_thread.join();
  CHECK(flusher.errors == 0);
  printf("flush during writes: %d flushes\n", flusher.flushes);

  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info, test_error) == DB_SUCCESS);

  /* Every write is stored exactly once. */
  std::vector<int64_t> ids = all_ids(engine);
  CHECK(ids.size() == (size_t)(threads * count));
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == ids.size());
  CHECK(engine_close(engine) == DB_SUCCESS);

  engine_recv_t recv;
  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);
  CHECK(recv.replayed == 0);
  CHECK(all_ids(engine).size() == (size_t)(threads * count));
  CHECK(engine_close(engine) == DB_SUCCESS);
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

/** Flush continuously until killed.
@param[in,out]  engine  database engine */
static void kill_flusher_main(engine_t *engine) {
  for (;;) {
    engine_flush_info_t info;
    if (engine_flush(*engine, info, test_error) != DB_SUCCESS) {
      _exit(5);
    }
    usleep(1000);
  }
}

/** Child process: write from four threads while another flushes, until
killed.
@param[in]  dir    database or WAL directory
@param[in]  round  crash-test round used to assign unique ids
@param[in]  fd     pipe descriptor for reporting acknowledged writes */
static void kill_child(const char *dir, int round, int fd) {
  engine_t *engine = nullptr;
  if (engine_open(dir, manual(), &engine, nullptr, test_error) != DB_SUCCESS) {
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

  std::thread flusher(kill_flusher_main, engine);
  flusher.join();
}

/** Kill a process that writes and flushes, over and over: every acknowledged
id is stored or replayed, none twice. */
static void test_kill_during_flushes() {
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

    const int wanted = 30 + round * 20;
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

    /* Stored and replayed writes together: every acknowledged id, and no
    id twice, which a replay of stored writes would cause. */
    engine_t *engine = nullptr;
    engine_recv_t recv;
    std::string error;
    CHECK(engine_open(dir.path, manual(), &engine, &recv, error) == DB_SUCCESS);
    if (engine == nullptr) {
      fprintf(stderr, "reopen failed: %s\n", error.c_str());

      return;
    }

    const std::vector<int64_t> ids = all_ids(engine);
    const std::set<int64_t> unique(ids.begin(), ids.end());
    CHECK(unique.size() == ids.size());

    std::set<int64_t>::iterator it;
    for (it = acked.begin(); it != acked.end(); ++it) {
      CHECK(unique.count(*it) == 1);
    }

    if (round == 7) {
      printf("kill test: %zu acknowledged writes, F = %llu, %llu replayed\n",
             acked.size(), (unsigned long long)recv.flushed_seq,
             (unsigned long long)recv.replayed);
    }

    CHECK(engine_close(engine) == DB_SUCCESS);
  }
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_flush_and_reopen();
  test_schema_across_flush();
#ifdef PT_DEBUG
  test_flush_failure();
#endif
  test_flush_during_writes();
  test_kill_during_flushes();

  return (test_result("engine0flush-t"));
}
