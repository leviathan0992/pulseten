/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db::flush-t.cc
 *    Tests for flushing: writes move from the memtable to DuckDB, reopening
 *    replays only what is not stored, flushes run alongside writes, and
 *    SIGKILL during writes and flushes loses or duplicates nothing.
 *
 *-------------------------------------------------------------------------
 */

#include "db.h"

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <signal.h>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "ingest.h"
#include "ingest_codec.h"
#include "memtable.h"
#include "storage.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_point.h"
#include "test_storage.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "wal.h"

/** A long background interval lets tests control internal flush boundaries.
@return options with a long background interval and no capacity limit */
static const db_options_t *deferred_flush() {
  static db_options_t options;
  options.m_flush_interval_ms = 24 * 60 * 60 * 1000;
  options.m_memtable_limit_bytes = 0;

  return (&options);
}

/** A flush stores every visible write, and reopening replays only the writes
after F. */
static void test_flush_and_reopen() {
  test_dir_t dir;
  db *database = nullptr;

  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());

  for (int64_t v = 1; v <= 3; ++v) {
    CHECK(write_id(*database, v).ok());
  }

  /* Everything visible goes to storage and leaves the memtable. */
  db_flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 3 && info.m_rows == 3 && info.m_flushed_seq == 3);

  memtable_stats_t stats;
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_flushed_seq == 3 && stats.m_live_writes == 0);
  CHECK(stats.m_retired_writes == 0);

  /* Nothing left: a flush with nothing to do. */
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 0 && info.m_flushed_seq == 3);

  CHECK(write_id(*database, 4).ok());
  CHECK(all_ids(*database).size() == 4);
  db_close(database);

  /* Reopening replays only the write after F. */
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_flushed_seq == 3 && stats.m_visible_seq == 4 &&
        stats.m_live_writes == 1);

  std::vector<int64_t> ids = all_ids(*database);
  CHECK(ids.size() == 4);
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == 4);

  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 1 && info.m_flushed_seq == 4);
  db_close(database);

  return;
}

/** A WAL batch using a column prefix fills the missing columns with NULL
through recovery, flush and reopen. */
static void test_recover_short_prefix() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  ingest_field_t a[] = {field_int("a", 1)};
  ingest_point_t p1 = point(T0, nullptr, 0, a, 1);
  ingest_plan_t create;
  CHECK(plan_for(nullptr, "m", &p1, 1, create).ok());
  const schema_t base = schema_after(nullptr, create);
  ingest_plan_t prefix;
  CHECK(plan_for(&base, "m", &p1, 1, prefix).ok());
  batch_seq_t seq = 0;
  CHECK(database->write("m", 1, &p1, 1, seq).ok());
  ingest_field_t b[] = {field_int("a", 2), field_double("b", 0.5)};
  ingest_point_t p2 = point(T0, nullptr, 0, b, 2);
  CHECK(database->write("m", 1, &p2, 1, seq).ok());
  db_flush_info_t info;
  CHECK(database->flush(info).ok());
  db_close(database);

  /* Encode a valid current-format batch that omits the trailing column. */
  std::string payload;
  ingest_encode(prefix, payload);
  wal *log = nullptr;
  wal_recv_t recv;
  CHECK(wal::open(dir.m_path, nullptr, nullptr, &log, recv).ok());
  wal_record_t record;
  record.m_seq = recv.m_last_seq + 1;
  record.m_payload = reinterpret_cast<const unsigned char *>(payload.data());
  record.m_len = payload.size();
  CHECK(record.m_seq == 3 && log->append(&record, 1).ok());
  CHECK(wal::destroy(log).ok());

  for (int round = 0; round < 2; ++round) {
    CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
    test_query_t result;
    CHECK(
        query_all(*database, "SELECT count(*), count(b) FROM m", result).ok());
    CHECK(result.m_rows.size() == 1);
    if (result.m_rows.size() == 1) {
      CHECK(result.m_rows[0][0].m_i == 3 && result.m_rows[0][1].m_i == 1);
    }

    CHECK(database->flush(info).ok());
    db_close(database);
  }

  return;
}

/** A stored column directory and additions only in the WAL are recovered
together. */
static void test_schema_across_flush() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());

  ingest_field_t f1[] = {field_int("a", 1)};
  ingest_field_t f2[] = {field_int("a", 2), field_double("b", 0.5)};
  ingest_point_t p1 = point(T0, nullptr, 0, f1, 1);
  ingest_point_t p2 = point(T0, nullptr, 0, f2, 2);
  batch_seq_t seq = 0;

  /* The initial columns are stored; the new column only lives in the WAL. */
  CHECK(database->write("m", 1, &p1, 1, seq).ok());
  db_flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(database->write("m", 1, &p2, 1, seq).ok());
  db_close(database);

  /* Replay appends the WAL column to the stored directory, and later writes
  plan against that directory. */
  memtable_stats_t stats;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_flushed_seq == 1 && stats.m_visible_seq == 2 &&
        stats.m_live_writes == 1);
  CHECK(database->write("m", 1, &p1, 1, seq).ok());
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 2);

  std::vector<schema_t> schemas;
  database->copy_schemas(schemas);
  CHECK(schemas.size() == 1 && schemas[0].m_cols.size() == 3);

  test_query_t result;
  CHECK(query_all(*database, "SELECT count(*) FROM m WHERE b IS NULL", result)
            .ok());
  CHECK(result.m_rows.size() == 1 && result.m_rows[0][0].m_i == 2);
  db_close(database);

  /* Check the latest stored column directory after releasing the database. */
  storage *st = nullptr;
  CHECK(storage::open(dir.m_path, STORAGE_DEFAULT_PARTITION_DAYS, &st).ok());
  if (st == nullptr) {
    return;
  }

  const storage_catalog_t &state = st->catalog();
  const storage_table_t &m = state.m_tables.find("m")->second;
  CHECK(m.m_schema.m_cols.size() == 3);

  int64_t nulls = -1;
  CHECK(test_storage_query_int(
            *st,
            "SELECT count(*) FROM " +
                state.m_partitions.begin()->second.m_phys_name +
                " WHERE c2 IS NULL",
            nulls)
            .ok());
  CHECK(nulls == 2);

  storage::destroy(st);

  return;
}

/** A flush failing before commit changes nothing; the next one works. */
static void test_flush_failure() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  CHECK(write_id(*database, 1).ok());

  /* A failed flush rolls back and changes nothing; the next one works. */
  db_flush_info_t info;
  db_test_access::fail_next_flush(*database);
  const db_status_t status = database->flush(info);
  CHECK(status.code() == DB_ERROR);
  CHECK(!status.message().empty() && info.m_writes == 0);

  memtable_stats_t stats;
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_flushed_seq == 0 && stats.m_live_writes == 1);

  CHECK(write_id(*database, 2).ok());
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 2);
  db_close(database);

  return;
}

struct writer_arg_t {
  /** Engine under test. */
  db *m_db{nullptr};

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
    if (!write_id(*arg->m_db, arg->m_thread_no * 100000 + i).ok()) {
      arg->m_errors++;
    }
  }

  return;
}

struct flusher_arg_t {
  /** Engine under test. */
  db *m_db{nullptr};

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

    db_flush_info_t info;
    if (!arg->m_db->flush(info).ok()) {
      arg->m_errors++;
    } else if (info.m_writes > 0) {
      arg->m_flushes++;
    }

    usleep(2000);
  }

  return;
}

/** Flushes running alongside writers lose and duplicate nothing. */
static void test_flush_during_writes() {
  test_dir_t dir;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());

  bool stop = false;
  std::mutex stop_mutex;
  flusher_arg_t flusher;
  flusher.m_db = database;
  flusher.m_stop = &stop;
  flusher.m_stop_mutex = &stop_mutex;
  std::thread flusher_thread(flusher_main, &flusher);

  const int threads = 4;
  const int count = 150;
  std::vector<writer_arg_t> args(threads);
  std::vector<std::thread> workers;

  for (int t = 0; t < threads; ++t) {
    args[t].m_db = database;
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

  db_flush_info_t info;
  CHECK(database->flush(info).ok());

  /* Every write is stored exactly once. */
  std::vector<int64_t> ids = all_ids(*database);
  CHECK(ids.size() == static_cast<size_t>(threads * count));
  CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == ids.size());
  db_close(database);

  memtable_stats_t stats;
  CHECK(db_open(dir.m_path, deferred_flush(), &database).ok());
  db_test_access::memtable_handle(*database).read_stats(stats);
  CHECK(stats.m_flushed_seq == static_cast<batch_seq_t>(threads * count) &&
        stats.m_live_writes == 0);
  CHECK(all_ids(*database).size() == static_cast<size_t>(threads * count));
  db_close(database);

  return;
}

/** Writer in the kill test: report each acknowledged id. */
struct kill_writer_t {
  /** Engine under test. */
  db *m_db{nullptr};

  /** First id to write. */
  int64_t m_base{0};

  /** Pipe end the acknowledgements are written to. */
  int m_fd{-1};
};

/** Write ids from base until killed; report each acknowledged one. */
static void kill_writer_main(kill_writer_t *w) {
  for (int64_t i = 0;; ++i) {
    const int64_t id = w->m_base + i;

    if (!write_id(*w->m_db, id).ok()) {
      _exit(3);
    }

    if (write(w->m_fd, &id, sizeof(id)) != static_cast<ssize_t>(sizeof(id))) {
      _exit(4);
    }
  }

  return;
}

/** Flush continuously until killed. */
static void kill_flusher_main(db *database) {
  for (;;) {
    db_flush_info_t info;
    if (!database->flush(info).ok()) {
      _exit(5);
    }

    usleep(1000);
  }

  return;
}

/** Child process: write from four threads while another flushes, until
killed. */
static void kill_child(const char *dir, int round, int fd) {
  db *database = nullptr;
  if (!db_open(dir, deferred_flush(), &database).ok()) {
    _exit(2);
  }

  kill_writer_t writers[4];
  std::thread threads[4];

  for (int t = 0; t < 4; ++t) {
    writers[t].m_db = database;
    writers[t].m_base = static_cast<int64_t>(round) * 100000000 + t * 1000000;
    writers[t].m_fd = fd;
    threads[t] = std::thread(kill_writer_main, &writers[t]);
  }

  std::thread flusher(kill_flusher_main, database);
  flusher.join();

  return;
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
    db *database = nullptr;

    status = db_open(dir.m_path, deferred_flush(), &database);
    CHECK(status.ok());
    if (database == nullptr) {
      fprintf(stderr, "reopen failed: %s\n", status.message().c_str());

      return;
    }

    const std::vector<int64_t> ids = all_ids(*database);
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

    db_close(database);
  }

  return;
}

/** Cleanup failure does not undo a committed flush; a later pass retries it. */
static void test_cleanup_failure() {
  test_dir_t dir;
  db_options_t options = *deferred_flush();
  options.m_wal_segment_size_bytes = 1;
  db *database = nullptr;
  CHECK(db_open(dir.m_path, &options, &database).ok());
  const ingest_field_t f = field_int("v", 1);
  const ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  batch_seq_t seq = 0;
  CHECK(database->write("m", 1, &p, 1, seq).ok());
  CHECK(database->write("m", 1, &p, 1, seq).ok());
  const std::string path = std::string(dir.m_path) + "/" + wal_segment_name(1);
  const std::string held = path + ".held";
  CHECK(rename(path.c_str(), held.c_str()) == 0);
  CHECK(mkdir(path.c_str(), 0700) == 0);
  db_flush_info_t info;
  CHECK(database->flush(info).ok());
  CHECK(info.m_flushed_seq == 2 && info.m_writes == 2);
  CHECK(info.m_cleanup_status.code() == DB_IO_ERROR);
  CHECK(rmdir(path.c_str()) == 0);
  CHECK(rename(held.c_str(), path.c_str()) == 0);
  CHECK(database->flush(info).ok());
  CHECK(info.m_writes == 0 && info.m_cleanup_status.ok());
  CHECK(info.m_segments_removed == 1);
  CHECK(database->write("m", 1, &p, 1, seq).ok() && seq == 3);
  db_close(database);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_cleanup_failure();
  test_flush_and_reopen();
  test_schema_across_flush();
  test_recover_short_prefix();
  test_flush_failure();
  test_flush_during_writes();
  test_kill_during_flushes();

  return (test_result("db_flush-t"));
}
