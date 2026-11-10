/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine-t.cc
 *    Tests for the write path: writes become visible, survive reopening
 *    and SIGKILL, concurrent schema changes agree, and failures fence.
 *
 *-------------------------------------------------------------------------
 */

#include "engine.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "batch.h"
#include "ingest.h"
#include "memtable.h"
#include "schema.h"
#include "storage.h"
#include "test_memtable.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "wal_format.h"

/** A long background interval lets tests control internal flush boundaries.
@return options with a long background interval and no capacity limit */
static const engine_options_t *deferred_flush() {
  static engine_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;

  return (&options);
}

/** Text of one value of a batch, "NULL" if absent.
@param[in]  r  zero-based row index
@return formatted value text, or "NULL" when the column is absent or the value
  is NULL */
static std::string value_text(const batch_t &b, uint32_t col_id, uint32_t r) {
  if (col_id >= b.m_cols.size() || b.m_cols[col_id].m_nulls[r]) {
    return ("NULL");
  }

  const batch_col_t &col = b.m_cols[col_id];
  char buf[64];

  switch (col.m_type) {
    case col_type_t::BOOLEAN:
      return (col.m_bools[r] ? "true" : "false");
    case col_type_t::BIGINT:
    case col_type_t::TIMESTAMP:
      snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(col.m_ints[r]));

      return (buf);
    case col_type_t::DOUBLE:
      snprintf(buf, sizeof(buf), "%.17g", col.m_doubles[r]);

      return (buf);
    case col_type_t::VARCHAR:
      return ("'" + str_at(col, r) + "'");
  }

  return ("?");
}

/** Everything visible in the engine as text, for comparing states.
@return a deterministic description of visible schemas and rows */
static std::string dump(engine_t &engine) {
  memtable_snapshot_t snap;
  snapshot_all(engine_memtable(engine), snap);

  std::string out = "visible " + std::to_string(snap.m_visible_seq) + "\n";
  std::map<std::string, memtable_table_view_t>::iterator it;

  for (it = snap.m_tables.begin(); it != snap.m_tables.end(); ++it) {
    const schema_t *schema = it->second.m_schema;

    out += "table " + it->first + " v" + std::to_string(schema->m_version);
    for (size_t c = 0; c < schema->m_cols.size(); ++c) {
      out += " " + schema->m_cols[c].m_name;
    }
    out += "\n";

    for (size_t i = 0; i < it->second.m_batches.size(); ++i) {
      const batch_t *b = it->second.m_batches[i];

      for (uint32_t r = 0; r < b->m_n_rows; ++r) {
        for (size_t c = 0; c < schema->m_cols.size(); ++c) {
          out += value_text(*b, static_cast<uint32_t>(c), r) + " ";
        }
        out += "\n";
      }
    }
  }

  memtable_snapshot_release(engine_memtable(engine), snap);

  return (out);
}

/** Write one point with the given tags and fields; checks success.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@param[in]  tags     input tags; may be NULL when n_tags is 0
@return the assigned sequence number; a failed write is recorded as a test
  failure */
static batch_seq_t write_one(engine_t &engine, const char *table,
                             int64_t time_us, const ingest_tag_t *tags,
                             uint32_t n_tags, const ingest_field_t *fields,
                             uint32_t n_fields) {
  db_status_t status;

  ingest_point_t p = point(time_us, tags, n_tags, fields, n_fields);
  batch_seq_t seq = 0;

  status = engine_write(engine, table, strlen(table), &p, 1, seq);
  const db_status_t err = status;
  CHECK(err.ok());
  if (!err.ok()) {
    fprintf(stderr, "write failed: %s: %s\n", err.message().c_str(),
            status.message().c_str());
  }

  return (seq);
}

/** Writes are visible at once, and reopening replays the WAL into the same
state and sequence numbers. */
static void test_write_and_reopen() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  memtable_stats_t stats;

  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_visible_seq == 0 && stats.m_live_writes == 0);
#ifdef PT_DEBUG
  /* Default writes do not execute the WAL group sync. */
  engine_dbg_fail_next_wal_sync(*engine);
#endif

  ingest_tag_t host_a[] = {tag("host", "a")};
  ingest_tag_t host_b[] = {tag("host", "b")};
  ingest_field_t cpu1[] = {field_double("usage", 0.25)};
  ingest_field_t cpu2[] = {field_double("usage", 0.5),
                           field_str("note", "\xE6\xB8\xA9")};
  ingest_field_t mem[] = {field_int("used", 1024), field_bool("ok", true)};

  CHECK(write_one(*engine, "cpu", T0, host_a, 1, cpu1, 1) == 1);
  CHECK(write_one(*engine, "CPU", T0 + 1, host_b, 1, cpu2, 2) == 2);
  CHECK(write_one(*engine, "mem", T0, nullptr, 0, mem, 2) == 3);

  memtable_snapshot_t snap;
  snapshot_all(engine_memtable(*engine), snap);
  CHECK(snap.m_visible_seq == 3 && snap.m_tables.size() == 2);
  CHECK(snap.m_tables["cpu"].m_schema->m_version == 2);
  memtable_snapshot_release(engine_memtable(*engine), snap);

  const std::string before = dump(*engine);
  CHECK(engine_close(engine).ok());

  /* Reopening replays the WAL into the same state. */
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_visible_seq == 3 && stats.m_live_writes == 3);
  CHECK(dump(*engine) == before);

  /* And writing continues after the recovered sequence numbers. */
  CHECK(write_one(*engine, "mem", T0 + 1, nullptr, 0, mem, 2) == 4);
  CHECK(engine_close(engine).ok());
}

/** A failed checkpoint sync must not advance durable storage past the WAL. */
static void test_unsynced_flush_failure() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  ingest_field_t fields[] = {field_int("v", 1)};
  CHECK(write_one(*engine, "m", T0, nullptr, 0, fields, 1) == 1);

  const std::string path = dir.file(wal_segment_name(1).c_str());
  const std::string hidden = path + ".hidden";
  CHECK(rename(path.c_str(), hidden.c_str()) == 0);

  engine_flush_info_t info;
  const db_status_t err = engine_flush(*engine, info);
  CHECK(err.code() == DB_IO_ERROR);
  CHECK(err.message().find("cannot sync WAL before flush") == 0);
  CHECK(storage_catalog(engine_storage(*engine)).m_flushed_seq == 0);
  memtable_stats_t stats;
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_visible_seq == 1 && stats.m_live_writes == 1);

  CHECK(rename(hidden.c_str(), path.c_str()) == 0);
  CHECK(engine_flush(*engine, info).ok());
  CHECK(info.m_flushed_seq == 1 && info.m_rows == 1);
  CHECK(engine_close(engine).ok());
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_flushed_seq == 1 && stats.m_visible_seq == 1);
  CHECK(engine_close(engine).ok());
}

/** Conflicting and invalid writes are rejected with a reason and use no
sequence number. */
static void test_rejected_writes() {
  db_status_t status;

  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  ingest_field_t num[] = {field_int("v", 1)};
  ingest_field_t text[] = {field_str("v", "x")};
  ingest_field_t reserved[] = {field_int("time", 1)};

  CHECK(write_one(*engine, "m", T0, nullptr, 0, num, 1) == 1);

  /* A conflict and an invalid name: rejected with a reason, no sequence
  number used. */
  const ingest_field_t *bad[] = {text, reserved};
  const dberr_t expected[] = {DB_SCHEMA_CONFLICT, DB_INVALID_ARGUMENT};

  for (int i = 0; i < 2; ++i) {
    ingest_point_t p = point(T0, nullptr, 0, bad[i], 1);
    batch_seq_t seq = 99;

    status = engine_write(*engine, "m", 1, &p, 1, seq);
    CHECK(status.code() == expected[i]);
    CHECK(seq == 0 && !status.message().empty());
  }

  ingest_point_t p = point(T0, nullptr, 0, num, 1);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "bad name", 8, &p, 1, seq).code() ==
        DB_INVALID_ARGUMENT);
  CHECK(engine_write(*engine, "_pt_meta", 8, &p, 1, seq).code() ==
        DB_INVALID_ARGUMENT);

  CHECK(write_one(*engine, "m", T0, nullptr, 0, num, 1) == 2);
  CHECK(engine_close(engine).ok());

  memtable_stats_t stats;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_visible_seq == 2 && stats.m_live_writes == 2);
  CHECK(engine_close(engine).ok());
}

/** A second open of the same directory is refused. */
static void test_second_open() {
  test_dir_t dir;
  engine_t *first = nullptr;
  engine_t *second = nullptr;

  CHECK(engine_open(dir.m_path, deferred_flush(), &first).ok());
  CHECK(engine_open(dir.m_path, deferred_flush(), &second).code() == DB_LOCKED);
  CHECK(second == nullptr);
  CHECK(engine_close(first).ok());
}

/** Invalid options are rejected before opening resources; failures after
locking release the directory so a later open can succeed. */
static void test_open_failure_cleanup() {
  db_status_t status;

  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options;

  options.m_flush_interval_ms = 0;
  status = engine_open(dir.m_path, &options, &engine);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(engine == nullptr);
  CHECK(status.message() ==
        "background flush interval must be greater than zero");
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;
  options.m_retention_days = ENGINE_MAX_RETENTION_DAYS + 1;
  status = engine_open(dir.m_path, &options, &engine);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(engine == nullptr);
  CHECK(status.message().find("retention period") != std::string::npos);
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  /* Partition options are rejected before taking the directory lock too. */
  options.m_retention_days = 0;
  options.m_partition_days = 0;
  status = engine_open(dir.m_path, &options, &engine);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(engine == nullptr && !status.message().empty());
  CHECK(access(dir.file("LOCK").c_str(), F_OK) != 0);

  options.m_partition_days = STORAGE_DEFAULT_PARTITION_DAYS;
  status = engine_open(dir.m_path, &options, &engine);
  CHECK(status.ok());
  CHECK(engine != nullptr && status.message().empty());
  CHECK(engine_close(engine).ok());

  /* A bad WAL fails recovery after storage and the memtable are open. */
  const std::string wal = dir.file(wal_segment_name(1).c_str());
  const std::string saved_wal = dir.file("saved-wal");
  CHECK(rename(wal.c_str(), saved_wal.c_str()) == 0);
  FILE *empty_wal = fopen(wal.c_str(), "wb");
  CHECK(empty_wal != nullptr);
  if (empty_wal != nullptr) {
    CHECK(fclose(empty_wal) == 0);
  }

  status = engine_open(dir.m_path, &options, &engine);
  CHECK(status.code() == DB_CORRUPTION);
  CHECK(engine == nullptr && !status.message().empty());

  CHECK(unlink(wal.c_str()) == 0);
  CHECK(rename(saved_wal.c_str(), wal.c_str()) == 0);
  status = engine_open(dir.m_path, &options, &engine);
  CHECK(status.ok());
  CHECK(engine != nullptr && status.message().empty());
  CHECK(engine_close(engine).ok());
}

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

/** Each write has 10 points. Every 20th write adds a field whose name all
threads share, so threads race to add the same column. Odd threads also write
to a table of their own.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  char shared[32];
  char own[32];
  snprintf(own, sizeof(own), "own_%d", arg->m_thread_no);

  for (int i = 0; i < arg->m_count; ++i) {
    snprintf(shared, sizeof(shared), "g%d", i / 20);

    ingest_field_t f[] = {field_int("n", i), field_double(shared, 1.5)};
    ingest_tag_t t[] = {tag("thread", own)};
    std::vector<ingest_point_t> points;

    for (int k = 0; k < 10; ++k) {
      points.push_back(point(T0 + k, t, 1, f, 2));
    }

    const char *table = (arg->m_thread_no % 2 == 1 && i % 2 == 1) ? own : "m";
    batch_seq_t seq = 0;

    if (!engine_write(*arg->m_engine, table, strlen(table), &points[0], 10, seq)
             .ok()) {
      arg->m_errors++;
    }
  }
}

struct reader_arg_t {
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *m_stop{nullptr};

  /** Protects *stop. */
  std::mutex *m_stop_mutex{nullptr};

  /** Snapshots taken. */
  int m_snapshots{0};

  /** Calls that failed. */
  int m_errors{0};
};

/** Take snapshots while writes run; each must hold exactly one batch per
visible write, and visibility must only grow.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void reader_main(reader_arg_t *arg) {
  batch_seq_t last = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->m_stop_mutex);
      if (*arg->m_stop) {
        break;
      }
    }

    memtable_snapshot_t snap;
    snapshot_all(engine_memtable(*arg->m_engine), snap);

    size_t batches = 0;
    std::map<std::string, memtable_table_view_t>::iterator it;
    for (it = snap.m_tables.begin(); it != snap.m_tables.end(); ++it) {
      batches += it->second.m_batches.size();
    }

    if (batches != snap.m_visible_seq || snap.m_visible_seq < last) {
      arg->m_errors++;
    }

    last = snap.m_visible_seq;
    memtable_snapshot_release(engine_memtable(*arg->m_engine), snap);
    arg->m_snapshots++;
  }
}

/** Threads writing at once, some racing to add the same columns, lose
nothing. */
static void test_concurrent_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

  const int threads = 8;
  const int count = 100;

  bool stop = false;
  std::mutex stop_mutex;
  reader_arg_t reader;
  reader.m_engine = engine;
  reader.m_stop = &stop;
  reader.m_stop_mutex = &stop_mutex;
  std::thread reader_thread(reader_main, &reader);

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
  reader_thread.join();
  CHECK(reader.m_errors == 0);
  printf("concurrent: %d snapshots taken during writes\n", reader.m_snapshots);

  /* Table m: time, thread, n, and g0..g4 added in races. */
  memtable_snapshot_t snap;
  snapshot_all(engine_memtable(*engine), snap);
  CHECK(snap.m_visible_seq == static_cast<batch_seq_t>(threads * count));
  CHECK(snap.m_tables["m"].m_schema->m_cols.size() == 3 + count / 20);
  memtable_snapshot_release(engine_memtable(*engine), snap);

  const std::string before = dump(*engine);
  CHECK(engine_close(engine).ok());

  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  CHECK(dump(*engine) == before);
  CHECK(engine_close(engine).ok());
}

#ifdef PT_DEBUG
/** A failed WAL sync fences the engine; what was visible stays, and
reopening recovers. */
static void test_fence() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_options_t options = *deferred_flush();
  options.m_wal_sync = true;
  CHECK(engine_open(dir.m_path, &options, &engine).ok());

  ingest_field_t f[] = {field_int("v", 1)};
  CHECK(write_one(*engine, "m", T0, nullptr, 0, f, 1) == 1);

  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  batch_seq_t seq = 0;

  engine_dbg_fail_next_wal_sync(*engine);
  CHECK(engine_write(*engine, "m", 1, &p, 1, seq).code() == DB_OUTCOME_UNKNOWN);
  CHECK(seq == 2);

  CHECK(engine_write(*engine, "m", 1, &p, 1, seq).code() == DB_FENCED);
  CHECK(seq == 0);

  /* The fence keeps the failure that caused it. */
  const db_status_t reason_status = engine_fence_status(*engine);
  const std::string &reason = reason_status.message();
  CHECK(reason.find("cannot sync WAL segment") == 0);

  /* What was visible before stays visible. */
  memtable_snapshot_t snap;
  snapshot_all(engine_memtable(*engine), snap);
  CHECK(snap.m_visible_seq == 1);
  memtable_snapshot_release(engine_memtable(*engine), snap);
  CHECK(engine_close(engine).ok());

  /* Reopening recovers; the unknown write may or may not be there. */
  memtable_stats_t stats;
  CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());
  engine_get_memtable_stats(*engine, stats);
  CHECK(stats.m_visible_seq >= 1 && stats.m_visible_seq <= 2);
  CHECK(stats.m_live_writes == stats.m_visible_seq);
  CHECK(write_one(*engine, "m", T0, nullptr, 0, f, 1) ==
        stats.m_visible_seq + 1);
  CHECK(engine_close(engine).ok());
}
#endif

/** Acknowledgement from the writer process. */
struct kill_ack_t {
  /** Id of an acknowledged write. */
  int64_t m_id{0};
};

struct kill_writer_t {
  /** Engine under test. */
  engine_t *m_engine{nullptr};

  /** Kill round; keeps ids apart across rounds. */
  int m_round{0};

  /** Number of this thread; keeps its ids apart from the others'. */
  int m_thread_no{0};

  /** Pipe end the acknowledgements are written to. */
  int m_fd{-1};
};

/** Write points with unique ids until killed; report each acknowledged write.
 */
static void kill_writer_main(kill_writer_t *w) {
  for (int i = 0;; ++i) {
    kill_ack_t ack;
    ack.m_id = static_cast<int64_t>(w->m_round) * 100000000 +
               w->m_thread_no * 1000000 + i;

    ingest_field_t f[] = {field_int("id", ack.m_id), field_str("pad", "xyz")};
    ingest_point_t p = point(T0 + i, nullptr, 0, f, 2);
    batch_seq_t seq = 0;

    if (!engine_write(*w->m_engine, "k", 1, &p, 1, seq).ok()) {
      _exit(3);
    }

    if (write(w->m_fd, &ack, sizeof(ack)) !=
        static_cast<ssize_t>(sizeof(ack))) {
      _exit(4);
    }
  }
}

/** Child process: open the directory and write from four threads until
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
    writers[t].m_round = round;
    writers[t].m_thread_no = t;
    writers[t].m_fd = fd;
    threads[t] = std::thread(kill_writer_main, &writers[t]);
  }

  for (int t = 0; t < 4; ++t) {
    threads[t].join();
  }

  _exit(0);
}

/** Read the ids recovered into the memtable.
@return every value of column "id" of table "k" */
static std::vector<int64_t> recovered_ids(engine_t &engine) {
  std::vector<int64_t> ids;
  memtable_snapshot_t snap;
  snapshot_all(engine_memtable(engine), snap);

  if (snap.m_tables.count("k") == 1) {
    const memtable_table_view_t &view = snap.m_tables["k"];
    const col_def_t *id_col = schema_find_col(view.m_schema->m_cols, "id");
    CHECK(id_col != nullptr);

    for (size_t i = 0; id_col != nullptr && i < view.m_batches.size(); ++i) {
      const batch_col_t &col = view.m_batches[i]->m_cols[id_col->m_id];
      for (uint32_t r = 0; r < view.m_batches[i]->m_n_rows; ++r) {
        ids.push_back(col.m_ints[r]);
      }
    }
  }

  memtable_snapshot_release(engine_memtable(engine), snap);

  return (ids);
}

/** Kill a writing process, over and over: every acknowledged write is
recovered, none twice. */
static void test_kill_during_writes() {
  test_dir_t dir;
  std::set<int64_t> acked;

  for (int round = 0; round < 10; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.m_path, round, fds[1]);
    }
    close(fds[1]);

    /* Let some writes finish, then kill the writer mid-write. */
    const int wanted = 20 + round * 13;
    int received = 0;
    kill_ack_t ack;

    while (received < wanted && read(fds[0], &ack, sizeof(ack)) ==
                                    static_cast<ssize_t>(sizeof(ack))) {
      acked.insert(ack.m_id);
      ++received;
    }

    kill(pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    while (read(fds[0], &ack, sizeof(ack)) ==
           static_cast<ssize_t>(sizeof(ack))) {
      acked.insert(ack.m_id);
    }
    close(fds[0]);
    CHECK(received == wanted);

    /* Every acknowledged write is recovered, and nothing twice. */
    engine_t *engine = nullptr;
    CHECK(engine_open(dir.m_path, deferred_flush(), &engine).ok());

    const std::vector<int64_t> ids = recovered_ids(*engine);
    const std::set<int64_t> unique(ids.begin(), ids.end());
    CHECK(unique.size() == ids.size());

    std::set<int64_t>::iterator it;
    for (it = acked.begin(); it != acked.end(); ++it) {
      CHECK(unique.count(*it) == 1);
    }

    CHECK(engine_close(engine).ok());
  }

  printf("kill test: %zu acknowledged writes survived 10 kills\n",
         acked.size());
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_write_and_reopen();
  test_unsynced_flush_failure();
  test_rejected_writes();
  test_second_open();
  test_open_failure_cleanup();
  test_concurrent_writes();
#ifdef PT_DEBUG
  test_fence();
#endif
  test_kill_during_writes();

  return (test_result("engine-t"));
}
