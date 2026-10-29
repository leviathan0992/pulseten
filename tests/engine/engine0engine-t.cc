/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine0engine-t.cc
 *    Tests for the write path: writes become visible, survive reopening
 *    and SIGKILL, concurrent schema changes agree, and failures fence.
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
#include "test0memtable.h"
#include "test0point.h"
#include "test0util.h"

/** Options with flushing only by engine_flush(), so tests control it.
@return options with background flushing disabled */
static const engine_options_t *manual() {
  static engine_options_t options;
  options.background_flush = false;

  return (&options);
}

/** Text of one value of a batch, "NULL" if absent.
@param[in]  b       immutable batch holding the value
@param[in]  col_id  column id in the schema
@param[in]  r       zero-based row index
@return formatted value text, or "NULL" when the column is absent or the value
  is NULL */
static std::string value_text(const batch_t *b, uint32_t col_id, uint32_t r) {
  if (col_id >= b->cols.size() || b->cols[col_id].nulls[r]) {
    return ("NULL");
  }

  const batch_col_t &col = b->cols[col_id];
  char buf[64];

  switch (col.type) {
    case COL_BOOLEAN:
      return (col.bools[r] ? "true" : "false");
    case COL_BIGINT:
    case COL_TIMESTAMP:
      snprintf(buf, sizeof(buf), "%lld", (long long)col.ints[r]);

      return (buf);
    case COL_DOUBLE:
      snprintf(buf, sizeof(buf), "%.17g", col.doubles[r]);

      return (buf);
    case COL_VARCHAR:
      return ("'" + str_at(col, r) + "'");
  }

  return ("?");
}

/** Everything visible in the engine as text, for comparing states.
@param[in,out]  engine  database engine
@return a deterministic description of visible schemas and rows */
static std::string dump(engine_t *engine) {
  memtable_snapshot_t snap;
  snapshot_all(*engine_get_memtable(*engine), snap);

  std::string out = "visible " + std::to_string(snap.visible_seq) + "\n";
  std::map<std::string, memtable_table_view_t>::iterator it;

  for (it = snap.tables.begin(); it != snap.tables.end(); ++it) {
    const schema_t *schema = it->second.schema;

    out += "table " + it->first + " v" + std::to_string(schema->version);
    for (size_t c = 0; c < schema->cols.size(); ++c) {
      out += " " + schema->cols[c].name;
    }
    out += "\n";

    for (size_t i = 0; i < it->second.batches.size(); ++i) {
      const batch_t *b = it->second.batches[i];

      for (uint32_t r = 0; r < b->n_rows; ++r) {
        for (size_t c = 0; c < schema->cols.size(); ++c) {
          out += value_text(b, (uint32_t)c, r) + " ";
        }
        out += "\n";
      }
    }
  }

  memtable_snapshot_release(*engine_get_memtable(*engine), snap);

  return (out);
}

/** Write one point with the given tags and fields; checks success.
@param[in,out]  engine    database engine
@param[in]      table     logical table name
@param[in]      time_us   UTC timestamp in microseconds since 1970-01-01
@param[in]      tags      input tags; may be NULL when n_tags is 0
@param[in]      n_tags    number of tags
@param[in]      fields    input fields
@param[in]      n_fields  number of fields
@return the assigned sequence number; a failed write is recorded as a test
  failure */
static batch_seq_t write_one(engine_t *engine, const char *table,
                             int64_t time_us, const ingest_tag_t *tags,
                             uint32_t n_tags, const ingest_field_t *fields,
                             uint32_t n_fields) {
  ingest_point_t p = point(time_us, tags, n_tags, fields, n_fields);
  batch_seq_t seq = 0;
  std::string error;

  const dberr_t err =
      engine_write(*engine, table, strlen(table), &p, 1, seq, error);
  CHECK(err == DB_SUCCESS);
  if (err != DB_SUCCESS) {
    fprintf(stderr, "write failed: %s: %s\n", ut_strerr(err), error.c_str());
  }

  return (seq);
}

/** Writes are visible at once, and reopening replays the WAL into the same
state and sequence numbers. */
static void test_write_and_reopen() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  engine_recv_t recv;

  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);
  CHECK(recv.wal_records == 0);

  ingest_tag_t host_a[] = {tag("host", "a")};
  ingest_tag_t host_b[] = {tag("host", "b")};
  ingest_field_t cpu1[] = {field_double("usage", 0.25)};
  ingest_field_t cpu2[] = {field_double("usage", 0.5),
                           field_str("note", "\xE6\xB8\xA9")};
  ingest_field_t mem[] = {field_int("used", 1024), field_bool("ok", true)};

  CHECK(write_one(engine, "cpu", T0, host_a, 1, cpu1, 1) == 1);
  CHECK(write_one(engine, "CPU", T0 + 1, host_b, 1, cpu2, 2) == 2);
  CHECK(write_one(engine, "mem", T0, nullptr, 0, mem, 2) == 3);

  memtable_snapshot_t snap;
  snapshot_all(*engine_get_memtable(*engine), snap);
  CHECK(snap.visible_seq == 3 && snap.tables.size() == 2);
  CHECK(snap.tables["cpu"].schema->version == 2);
  memtable_snapshot_release(*engine_get_memtable(*engine), snap);

  const std::string before = dump(engine);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Reopening replays the WAL into the same state. */
  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);
  CHECK(recv.wal_records == 3);
  CHECK(dump(engine) == before);

  /* And writing continues after the recovered sequence numbers. */
  CHECK(write_one(engine, "mem", T0 + 1, nullptr, 0, mem, 2) == 4);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** Conflicting and invalid writes are rejected with a reason and use no
sequence number. */
static void test_rejected_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  ingest_field_t num[] = {field_int("v", 1)};
  ingest_field_t text[] = {field_str("v", "x")};
  ingest_field_t reserved[] = {field_int("time", 1)};

  CHECK(write_one(engine, "m", T0, nullptr, 0, num, 1) == 1);

  /* A conflict and an invalid name: rejected with a reason, no sequence
  number used. */
  const ingest_field_t *bad[] = {text, reserved};
  const dberr_t expected[] = {DB_SCHEMA_CONFLICT, DB_INVALID_ARGUMENT};

  for (int i = 0; i < 2; ++i) {
    ingest_point_t p = point(T0, nullptr, 0, bad[i], 1);
    batch_seq_t seq = 99;
    std::string error;

    CHECK(engine_write(*engine, "m", 1, &p, 1, seq, error) == expected[i]);
    CHECK(seq == 0 && !error.empty());
  }

  ingest_point_t p = point(T0, nullptr, 0, num, 1);
  batch_seq_t seq = 0;
  CHECK(engine_write(*engine, "bad name", 8, &p, 1, seq, test_error) ==
        DB_INVALID_ARGUMENT);

  CHECK(write_one(engine, "m", T0, nullptr, 0, num, 1) == 2);
  CHECK(engine_close(engine) == DB_SUCCESS);

  engine_recv_t recv;
  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);
  CHECK(recv.wal_records == 2);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

/** A second open of the same directory is refused. */
static void test_second_open() {
  test_dir_t dir;
  engine_t *first = nullptr;
  engine_t *second = nullptr;

  CHECK(engine_open(dir.path, manual(), &first, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(engine_open(dir.path, manual(), &second, nullptr, test_error) ==
        DB_LOCKED);
  CHECK(second == nullptr);
  CHECK(engine_close(first) == DB_SUCCESS);
}

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

/** Each write has 10 points. Every 20th write adds a field whose name all
threads share, so threads race to add the same column. Odd threads also write
to a table of their own.
@param[in,out]  arg  writer configuration and counters; must outlive the
                     worker */
static void writer_main(writer_arg_t *arg) {
  char shared[32];
  char own[32];
  snprintf(own, sizeof(own), "own_%d", arg->thread_no);

  for (int i = 0; i < arg->count; ++i) {
    snprintf(shared, sizeof(shared), "g%d", i / 20);

    ingest_field_t f[] = {field_int("n", i), field_double(shared, 1.5)};
    ingest_tag_t t[] = {tag("thread", own)};
    std::vector<ingest_point_t> points;

    for (int k = 0; k < 10; ++k) {
      points.push_back(point(T0 + k, t, 1, f, 2));
    }

    const char *table = (arg->thread_no % 2 == 1 && i % 2 == 1) ? own : "m";
    batch_seq_t seq = 0;

    if (engine_write(*arg->engine, table, strlen(table), &points[0], 10, seq,
                     test_error) != DB_SUCCESS) {
      arg->errors++;
    }
  }
}

struct reader_arg_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Set when the thread should stop; protected by the mutex. */
  bool *stop{nullptr};

  /** Protects *stop. */
  std::mutex *stop_mutex{nullptr};

  /** Snapshots taken. */
  int snapshots{0};

  /** Calls that failed. */
  int errors{0};
};

/** Take snapshots while writes run; each must hold exactly one batch per
visible write, and visibility must only grow.
@param[in,out]  arg  worker configuration and counters; the caller keeps it
                     alive until join */
static void reader_main(reader_arg_t *arg) {
  batch_seq_t last = 0;

  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*arg->stop_mutex);
      if (*arg->stop) {
        break;
      }
    }

    memtable_snapshot_t snap;
    snapshot_all(*engine_get_memtable(*arg->engine), snap);

    size_t batches = 0;
    std::map<std::string, memtable_table_view_t>::iterator it;
    for (it = snap.tables.begin(); it != snap.tables.end(); ++it) {
      batches += it->second.batches.size();
    }

    if (batches != snap.visible_seq || snap.visible_seq < last) {
      arg->errors++;
    }

    last = snap.visible_seq;
    memtable_snapshot_release(*engine_get_memtable(*arg->engine), snap);
    arg->snapshots++;
  }
}

/** Threads writing at once, some racing to add the same columns, lose
nothing. */
static void test_concurrent_writes() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  const int threads = 8;
  const int count = 100;

  bool stop = false;
  std::mutex stop_mutex;
  reader_arg_t reader;
  reader.engine = engine;
  reader.stop = &stop;
  reader.stop_mutex = &stop_mutex;
  std::thread reader_thread(reader_main, &reader);

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
  reader_thread.join();
  CHECK(reader.errors == 0);
  printf("concurrent: %d snapshots taken during writes\n", reader.snapshots);

  /* Table m: time, thread, n, and g0..g4 added in races. */
  memtable_snapshot_t snap;
  snapshot_all(*engine_get_memtable(*engine), snap);
  CHECK(snap.visible_seq == (batch_seq_t)(threads * count));
  CHECK(snap.tables["m"].schema->cols.size() == 3 + count / 20);
  memtable_snapshot_release(*engine_get_memtable(*engine), snap);

  const std::string before = dump(engine);
  CHECK(engine_close(engine) == DB_SUCCESS);

  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);
  CHECK(dump(engine) == before);
  CHECK(engine_close(engine) == DB_SUCCESS);
}

#ifdef PT_DEBUG
/** A failed WAL flush fences the engine; what was visible stays, and
reopening recovers. */
static void test_fence() {
  test_dir_t dir;
  engine_t *engine = nullptr;
  CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
        DB_SUCCESS);

  ingest_field_t f[] = {field_int("v", 1)};
  CHECK(write_one(engine, "m", T0, nullptr, 0, f, 1) == 1);

  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  batch_seq_t seq = 0;

  engine_dbg_fail_next_flush(*engine);
  CHECK(engine_write(*engine, "m", 1, &p, 1, seq, test_error) ==
        DB_OUTCOME_UNKNOWN);
  CHECK(seq == 2);

  CHECK(engine_write(*engine, "m", 1, &p, 1, seq, test_error) == DB_FENCED);
  CHECK(seq == 0);

  /* What was visible before stays visible. */
  memtable_snapshot_t snap;
  snapshot_all(*engine_get_memtable(*engine), snap);
  CHECK(snap.visible_seq == 1);
  memtable_snapshot_release(*engine_get_memtable(*engine), snap);
  CHECK(engine_close(engine) == DB_SUCCESS);

  /* Reopening recovers; the unknown write may or may not be there. */
  engine_recv_t recv;
  CHECK(engine_open(dir.path, manual(), &engine, &recv, test_error) ==
        DB_SUCCESS);
  CHECK(recv.wal_records >= 1 && recv.wal_records <= 2);
  CHECK(write_one(engine, "m", T0, nullptr, 0, f, 1) == recv.wal_records + 1);
  CHECK(engine_close(engine) == DB_SUCCESS);
}
#endif

/** Acknowledgement from the writer process. */
struct kill_ack_t {
  /** Id of an acknowledged write. */
  int64_t id{0};
};

struct kill_writer_t {
  /** Engine under test. */
  engine_t *engine{nullptr};

  /** Kill round; keeps ids apart across rounds. */
  int round{0};

  /** Number of this thread; keeps its ids apart from the others'. */
  int thread_no{0};

  /** Pipe end the acknowledgements are written to. */
  int fd{-1};
};

/** Write points with unique ids until killed; report each acknowledged write.
@param[in,out]  w  writer configuration, id range and acknowledgment pipe */
static void kill_writer_main(kill_writer_t *w) {
  for (int i = 0;; ++i) {
    kill_ack_t ack;
    ack.id = (int64_t)w->round * 100000000 + w->thread_no * 1000000 + i;

    ingest_field_t f[] = {field_int("id", ack.id), field_str("pad", "xyz")};
    ingest_point_t p = point(T0 + i, nullptr, 0, f, 2);
    batch_seq_t seq = 0;

    if (engine_write(*w->engine, "k", 1, &p, 1, seq, test_error) !=
        DB_SUCCESS) {
      _exit(3);
    }

    if (write(w->fd, &ack, sizeof(ack)) != (ssize_t)sizeof(ack)) {
      _exit(4);
    }
  }
}

/** Child process: open the directory and write from four threads until
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
    writers[t].round = round;
    writers[t].thread_no = t;
    writers[t].fd = fd;
    threads[t] = std::thread(kill_writer_main, &writers[t]);
  }

  for (int t = 0; t < 4; ++t) {
    threads[t].join();
  }

  _exit(0);
}

/** Read the ids recovered into the memtable.
@param[in,out]  engine  database engine
@return every value of column "id" of table "k" */
static std::vector<int64_t> recovered_ids(engine_t *engine) {
  std::vector<int64_t> ids;
  memtable_snapshot_t snap;
  snapshot_all(*engine_get_memtable(*engine), snap);

  if (snap.tables.count("k") == 1) {
    const memtable_table_view_t &view = snap.tables["k"];
    const col_def_t *id_col = schema_find_col(view.schema->cols, "id");
    CHECK(id_col != nullptr);

    for (size_t i = 0; id_col != nullptr && i < view.batches.size(); ++i) {
      const batch_col_t &col = view.batches[i]->cols[id_col->id];
      for (uint32_t r = 0; r < view.batches[i]->n_rows; ++r) {
        ids.push_back(col.ints[r]);
      }
    }
  }

  memtable_snapshot_release(*engine_get_memtable(*engine), snap);

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
      kill_child(dir.path, round, fds[1]);
    }
    close(fds[1]);

    /* Let some writes finish, then kill the writer mid-write. */
    const int wanted = 20 + round * 13;
    int received = 0;
    kill_ack_t ack;

    while (received < wanted &&
           read(fds[0], &ack, sizeof(ack)) == (ssize_t)sizeof(ack)) {
      acked.insert(ack.id);
      ++received;
    }

    kill(pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    while (read(fds[0], &ack, sizeof(ack)) == (ssize_t)sizeof(ack)) {
      acked.insert(ack.id);
    }
    close(fds[0]);
    CHECK(received == wanted);

    /* Every acknowledged write is recovered, and nothing twice. */
    engine_t *engine = nullptr;
    CHECK(engine_open(dir.path, manual(), &engine, nullptr, test_error) ==
          DB_SUCCESS);

    const std::vector<int64_t> ids = recovered_ids(engine);
    const std::set<int64_t> unique(ids.begin(), ids.end());
    CHECK(unique.size() == ids.size());

    std::set<int64_t>::iterator it;
    for (it = acked.begin(); it != acked.end(); ++it) {
      CHECK(unique.count(*it) == 1);
    }

    CHECK(engine_close(engine) == DB_SUCCESS);
  }

  printf("kill test: %zu acknowledged writes survived 10 kills\n",
         acked.size());
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_write_and_reopen();
  test_rejected_writes();
  test_second_open();
  test_concurrent_writes();
#ifdef PT_DEBUG
  test_fence();
#endif
  test_kill_during_writes();

  return (test_result("engine0engine-t"));
}
