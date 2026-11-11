/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine_flush-t.cc
 *    Tests for flushing: writes move from the memtable to DuckDB, reopening
 *    replays only what is not stored, flushes run alongside writes, and
 *    SIGKILL during writes and flushes loses or duplicates nothing.
 *
 *-------------------------------------------------------------------------
 */

#include "engine.h"

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <signal.h>
#include <string>
#include <thread>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "ingest.h"
#include "memtable.h"
#include "storage.h"
#include "test_engine.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"

/** A long background interval lets tests control internal flush boundaries.
@return options with a long background interval and no capacity limit */
static const engine_options_t *deferred_flush() {
  static engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;

  return (&options);
}

/** A flush stores every visible write, and reopening replays only the writes
after F. */
static void test_flush_and_reopen() {
  test_dir_t dir;
  engine_t *engine = nullptr;

  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  for (int64_t v = 1; v <= 3; ++v) {
    CHECK(write_id(*engine, v).ok());
  }

  /* Everything visible goes to storage and leaves the memtable. */
  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info).ok());
  CHECK(info.m_writes == 3 && info.m_rows == 3 && info.m_flushed_seq == 3);

  memtable_stats_t stats;
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_flushed_seq == 3 && stats.m_live_writes == 0);
  CHECK(stats.m_garbage_writes == 0);

  /* Nothing left: a flush with nothing to do. */
  CHECK(engine_flush(*engine, info).ok());
  CHECK(info.m_writes == 0 && info.m_flushed_seq == 3);

  CHECK(write_id(*engine, 4).ok());
  CHECK(all_ids(*engine).size() == 4);
  CHECK(engine_close(engine).ok());

  /* Reopening replays only the write after F. */
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_flushed_seq == 3 && stats.m_visible_seq == 4 &&
        stats.m_live_writes == 1);

  std::vector<int64_t> ids = all_ids(*engine);
  CHECK(ids.size() == 4);
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == 4);

  CHECK(engine_flush(*engine, info).ok());
  CHECK(info.m_writes == 1 && info.m_flushed_seq == 4);
  CHECK(engine_close(engine).ok());
}

/** A schema version stored and a newer one only in the WAL are recovered
together. */
static void test_schema_across_flush() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  ingest_field_t f1[] = {field_int("a", 1)};
  ingest_field_t f2[] = {field_int("a", 2), field_double("b", 0.5)};
  ingest_point_t p1 = point(T0, nullptr, 0, f1, 1);
  ingest_point_t p2 = point(T0, nullptr, 0, f2, 2);
  batch_seq_t seq = 0;

  /* Version 1 is stored; version 2 only lives in the WAL. */
  CHECK(engine_write(*engine, "m", 1, &p1, 1, seq).ok());
  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info).ok());
  CHECK(engine_write(*engine, "m", 1, &p2, 1, seq).ok());
  CHECK(engine_close(engine).ok());

  /* Replay adds version 2 to the stored version 1, and later writes plan
  against it. */
  memtable_stats_t stats;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_flushed_seq == 1 && stats.m_visible_seq == 2 &&
        stats.m_live_writes == 1);
  CHECK(engine_write(*engine, "m", 1, &p1, 1, seq).ok());
  CHECK(engine_flush(*engine, info).ok());
  CHECK(info.m_writes == 2);

  std::vector<schema_t> schemas;
  engine_copy_schemas(*engine, schemas);
  CHECK(schemas.size() == 1 && schemas[0].m_version == 2 &&
        schemas[0].m_cols.size() == 3);

  test_query_t result;
  CHECK(query_all(*engine, "SELECT count(*) FROM m WHERE b IS NULL", result)
            .ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_i == 2);
  CHECK(engine_close(engine).ok());

  /* Check the stored schema history after releasing the engine. */
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, STORAGE_DEFAULT_PARTITION_DAYS, &st).ok());
  if (st == nullptr) {
    return;
  }
  const storage_catalog_t &state = storage_catalog(*st);
  const storage_table_t &m = state.m_tables.find("m")->second;
  CHECK(m.m_versions.size() == 2 && m.m_versions[1].m_cols.size() == 3);

  int64_t nulls = -1;
  CHECK(storage_query_int(*st,
                          "SELECT count(*) FROM " +
                              state.m_partitions.begin()->second.m_phys_name +
                              " WHERE c2 IS NULL",
                          nulls)
            .ok());
  CHECK(nulls == 2);

  storage_close(st);
}

#ifdef PT_DEBUG
/** A flush failing before commit changes nothing; the next one works. */
static void test_flush_failure() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  CHECK(write_id(*engine, 1).ok());

  /* A failed flush rolls back and changes nothing; the next one works. */
  engine_flush_info_t info;
  engine_dbg_fail_next_flush(*engine);
  const db_status_t status = engine_flush(*engine, info);
  CHECK(status.code() == DB_ERROR);
  CHECK(!status.message().empty() && info.m_writes == 0);

  memtable_stats_t stats;
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_flushed_seq == 0 && stats.m_live_writes == 1);

  CHECK(write_id(*engine, 2).ok());
  CHECK(engine_flush(*engine, info).ok());
  CHECK(info.m_writes == 2);
  CHECK(engine_close(engine).ok());
}
#endif /* PT_DEBUG */

struct writer_arg_t {
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Writes to make. */
  int m_count{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Write count ids, counting failures.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  for (int i = 0; i < arg->m_count; ++i) {
    if (!write_id(*arg->m_engine, arg->m_thread_no * 100000 + i).ok()) {
      arg->m_errors++;
    }
  }
}

struct flusher_arg_t {
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *m_stop{nullptr};

  /** Protects *stop. */
  std::mutex *m_stop_mutex{nullptr};

  /** Flushes that stored something. */
  int m_flushes{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Flush until told to stop, counting flushes and failures.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void flusher_main(flusher_arg_t *arg) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->m_stop_mutex);
      if (*arg->m_stop) {
        break;
      }
    }

    engine_flush_info_t info;
    if (!engine_flush(*arg->m_engine, info).ok()) {
      arg->m_errors++;
    } else if (info.m_writes > 0) {
      arg->m_flushes++;
    }

    usleep(2000);
  }
}

/** Flushes running alongside writers lose and duplicate nothing. */
static void test_flush_during_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  bool stop = false;
  std::mutex stop_mutex;
  flusher_arg_t flusher;
  flusher.m_engine = engine;
  flusher.m_stop = &stop;
  flusher.m_stop_mutex = &stop_mutex;
  std::thread flusher_thread(flusher_main, &flusher);

  const int threads = 4;
  const int count = 150;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_engine = engine;
    args[t].m_thread_no = t;
    args[t].m_count = count;
    workers.push_back(std::thread(writer_main, &args[t]));
  }

  for (int t = 0; t < threads; ++t) {
    workers[t].join();
    CHECK(args[t].m_errors == 0);
  }

  {
    std::lock_guard<std::mutex> lock(stop_mutex);
    stop = true;
  }
  flusher_thread.join();
  CHECK(flusher.m_errors == 0);
  printf("flush during writes: %d flushes\n", flusher.m_flushes);

  engine_flush_info_t info;
  CHECK(engine_flush(*engine, info).ok());

  /* Every write is stored exactly once. */
  std::vector<int64_t> ids = all_ids(*engine);
  CHECK(ids.size() == static_cast<size_t>(threads * count));
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == ids.size());
  CHECK(engine_close(engine).ok());

  memtable_stats_t stats;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_flushed_seq == static_cast<batch_seq_t>(threads * count) &&
        stats.m_live_writes == 0);
  CHECK(all_ids(*engine).size() == static_cast<size_t>(threads * count));
  CHECK(engine_close(engine).ok());
}

/** Writer in the kill test: report each acknowledged id. */
struct kill_writer_t {
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** First id to write. */
  int64_t m_base{0};

  /** Pipe end the acknowledgements are written to. */
  int m_fd{-1};
};

/** Write ids from base until killed; report each acknowledged one. */
static void kill_writer_main(kill_writer_t *w) {
  for (int64_t i = 0;; ++i) {
    const int64_t id = w->m_base + i;

    if (!write_id(*w->m_engine, id).ok()) {
      _exit(3);
    }

    if (write(w->m_fd, &id, sizeof(id)) != static_cast<ssize_t>(sizeof(id))) {
      _exit(4);
    }
  }
}

/** Flush continuously until killed. */
static void kill_flusher_main(engine_t *engine) {
  for (;;) {
    engine_flush_info_t info;
    if (!engine_flush(*engine, info).ok()) {
      _exit(5);
    }
    usleep(1000);
  }
}

/** Child process: write from four threads while another flushes, until
killed. */
static void kill_child(const char *dir, int round, int fd) {
  engine_t *engine = nullptr;
  if (!engine_open(dir, deferred_flush(), &engine).ok()) {
    _exit(2);
  }

  kill_writer_t writers[4];
  std::thread threads[4];

  for (int t = 0; t < 4; ++t) {
    writers[t].m_engine = engine;
    writers[t].m_base = static_cast<int64_t>(round) * 100000000 + t * 1000000;
    writers[t].m_fd = fd;
    threads[t] = std::thread(kill_writer_main, &writers[t]);
  }

  std::thread flusher(kill_flusher_main, engine);
  flusher.join();
}

/** Kill a process that writes and flushes, over and over: every acknowledged
id is stored or replayed, none twice. */
static void test_kill_during_flushes() {
  db_status_t status;

  test_dir_t dir;
  std::set<int64_t> acked;

  for (int round = 0; round < 8; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.m_path, round, fds[1]);
    }
    close(fds[1]);

    const int wanted = 30 + round * 20;
    int received = 0;
    int64_t id = 0;

    while (received < wanted &&
           read(fds[0], &id, sizeof(id)) == static_cast<ssize_t>(sizeof(id))) {
      acked.insert(id);
      ++received;
    }

    kill(pid, SIGKILL);
    int child_status = 0;
    waitpid(pid, &child_status, 0);
    CHECK(WIFSIGNALED(child_status) && WTERMSIG(child_status) == SIGKILL);

    while (read(fds[0], &id, sizeof(id)) == static_cast<ssize_t>(sizeof(id))) {
      acked.insert(id);
    }
    close(fds[0]);
    CHECK(received == wanted);

    /* Stored and replayed writes together: every acknowledged id, and no
    id twice, which a replay of stored writes would cause. */
    engine_t *engine = nullptr;

    status = engine_open(dir.m_path, deferred_flush(), &engine);
    CHECK(status.ok());
    if (engine == nullptr) {
      fprintf(stderr, "reopen failed: %s\n", status.message().c_str());

      return;
    }

    const std::vector<int64_t> ids = all_ids(*engine);
    const std::set<int64_t> unique(ids.begin(), ids.end());
    CHECK(unique.size() == ids.size());

    std::set<int64_t>::iterator it;
    for (it = acked.begin(); it != acked.end(); ++it) {
      CHECK(unique.count(*it) == 1);
    }

    if (round == 7) {
      printf("kill test: %zu acknowledged writes, %zu recovered rows\n",
             acked.size(), ids.size());
    }

    CHECK(engine_close(engine).ok());
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
#endif /* PT_DEBUG */
  test_flush_during_writes();
  test_kill_during_flushes();

  return (test_result("engine_flush-t"));
}
