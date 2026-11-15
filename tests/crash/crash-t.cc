/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * crash-t.cc
 *    Tests for crash recovery: a process is killed with SIGKILL, round after
 *    round, while it appends to the WAL, writes, flushes, or flushes and
 *    purges in the background; every acknowledged change survives and
 *    nothing is applied twice.
 *
 * Each case reuses one directory for all its rounds, so later rounds recover
 * from the damage of earlier ones.
 *
 * Covers: src/wal/wal.cc, src/wal/wal_recovery.cc, src/db/db.cc,
 *         src/store/store.cc
 *
 *-------------------------------------------------------------------------
 */

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "db.h"
#include "db_directory.h"
#include "ingest.h"
#include "store.h"
#include "test_crash.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_store.h"
#include "test_util.h"
#include "test_wal.h"
#include "ut.h"
#include "wal.h"

/* =========================== WAL appends ============================ */

/** Acknowledgement of one appended record. */
struct append_ack_t {
  /** Sequence number of the acknowledged record. */
  uint64_t m_seq{0};

  /** Thread that wrote it. */
  int32_t m_thread_index{0};

  /** Its index among that thread's records. */
  int32_t m_integer{0};
};

/** Build a payload identifying the crash round, writer and record.
@return the payload of record i of a thread in a kill round */
static std::string append_payload(int round, int thread_index, int i) {
  char buf[64];
  snprintf(buf, sizeof(buf), "round-%d-thread-%d-record-%d", round,
           thread_index, i);

  return (buf);
}

/** One appending thread of the child process. */
struct appender_t {
  /** WAL under test. */
  wal *m_wal{nullptr};

  /** Serializes sequence assignment and appends. */
  std::mutex *m_mutex{nullptr};

  /** Last appended sequence, protected by m_mutex. */
  batch_seq_t *m_last_seq{nullptr};

  /** Kill round; keeps payloads apart across rounds. */
  int m_round{0};

  /** Number of this thread; keeps its payloads apart from the others'. */
  int m_thread_index{0};

  /** Pipe end the acknowledgements are written to. */
  int m_fd{-1};
};

/** Append until killed, acknowledging every record. */
static void appender_main(appender_t *a) {
  for (int i = 0;; ++i) {
    const std::string p = append_payload(a->m_round, a->m_thread_index, i);
    append_ack_t ack;

    {
      std::lock_guard<std::mutex> lock(*a->m_mutex);

      if (!append_record(*a->m_wal, p.data(), p.size(), *a->m_last_seq).ok()) {
        _exit(3);
      }

      ack.m_seq = *a->m_last_seq;
    }

    ack.m_thread_index = a->m_thread_index;
    ack.m_integer = i;
    test_crash_ack(a->m_fd, ack);
  }

  return;
}

/** Child process: open the WAL and append from four threads until killed. */
static void append_child_main(const char *dir, int round, int fd) {
  wal *log = nullptr;
  wal_recovery_t recovery;
  if (!wal::open(dir, nullptr, nullptr, &log, recovery).ok()) {
    _exit(2);
  }

  std::mutex mutex;
  batch_seq_t sequence = recovery.m_last_seq;
  appender_t appenders[4];
  std::thread threads[4];

  for (int t = 0; t < 4; ++t) {
    appenders[t].m_wal = log;
    appenders[t].m_mutex = &mutex;
    appenders[t].m_last_seq = &sequence;
    appenders[t].m_round = round;
    appenders[t].m_thread_index = t;
    appenders[t].m_fd = fd;
    threads[t] = std::thread(appender_main, &appenders[t]);
  }

  for (std::thread &thread : threads) {
    thread.join();
  }

  return;
}

/** Kill a process appending from several threads: every acknowledged record
is recovered with its payload. */
static void test_wal_appends() {
  test_dir_t dir;
  wal *log = nullptr;
  REQUIRE(wal::create(dir.m_path, &log).ok());
  CHECK(wal::destroy(log).ok());

  /* Every record acknowledged so far, from all rounds: (seq, payload). */
  std::vector<std::pair<uint64_t, std::string>> acked;

  for (int round = 0; round < 10; ++round) {
    test_kill_child<append_ack_t>(
        20 + round * 17,
        [&](int fd) { append_child_main(dir.m_path, round, fd); },
        [&](const append_ack_t &ack) {
          acked.emplace_back(
              ack.m_seq,
              append_payload(round, ack.m_thread_index, ack.m_integer));
        });

    test_wal_records_t records;
    wal_recovery_t recovery;
    CHECK(test_wal_recover(dir.m_path, records, recovery).ok());
    CHECK(records.m_payloads.empty() || records.m_first_seq == 1);

    const std::vector<std::string> &payloads = records.m_payloads;

    for (const std::pair<uint64_t, std::string> &record : acked) {
      CHECK(record.first >= 1 && record.first <= payloads.size() &&
            payloads[record.first - 1] == record.second);
    }
  }

  printf("wal appends: %zu acknowledged records survived 10 kills\n",
         acked.size());

  return;
}

/* ========================= Database writes ========================== */

/** One writing thread of the child process. */
struct id_writer_t {
  /** Database under test. */
  db *m_db{nullptr};

  /** First id to write; unique to the round and thread. */
  int64_t m_base{0};

  /** Pipe end the acknowledgements are written to. */
  int m_fd{-1};
};

/** Write ids from base until killed, acknowledging each one. */
static void id_writer_main(id_writer_t *w) {
  for (int64_t id = w->m_base;; ++id) {
    if (!write_id(*w->m_db, id).ok()) {
      _exit(3);
    }

    test_crash_ack(w->m_fd, id);
  }

  return;
}

/** Start four writers whose ids do not repeat across threads or rounds. */
static void start_id_writers(db &database, int round, int fd,
                             id_writer_t (&writers)[4],
                             std::thread (&threads)[4]) {
  for (int t = 0; t < 4; ++t) {
    writers[t].m_db = &database;
    writers[t].m_base = static_cast<int64_t>(round) * 100000000 + t * 1000000;
    writers[t].m_fd = fd;
    threads[t] = std::thread(id_writer_main, &writers[t]);
  }

  return;
}

/** Reopen the database and check that every acknowledged id is recovered and
no id appears twice.
@return number of recovered rows */
static size_t check_acked_ids(const test_dir_t &dir,
                              const std::set<int64_t> &acked) {
  const std::vector<int64_t> ids = reopen_ids(dir);
  const std::set<int64_t> unique(ids.begin(), ids.end());
  CHECK_EQ(unique.size(), ids.size());

  for (const int64_t id : acked) {
    CHECK_EQ(unique.count(id), 1);
  }

  return (ids.size());
}

/** Child process: write from four threads until killed. */
static void write_child_main(const char *dir, int round, int fd) {
  db *database = nullptr;
  if (!db_open(dir, deferred_flush_options(), &database).ok()) {
    _exit(2);
  }

  id_writer_t writers[4];
  std::thread threads[4];
  start_id_writers(*database, round, fd, writers, threads);

  for (std::thread &thread : threads) {
    thread.join();
  }

  return;
}

/** Kill a writing process: every acknowledged write is recovered, none
twice. */
static void test_db_writes() {
  test_dir_t dir;
  std::set<int64_t> acked;

  for (int round = 0; round < 10; ++round) {
    test_kill_child<int64_t>(
        20 + round * 13,
        [&](int fd) { write_child_main(dir.m_path, round, fd); },
        [&](int64_t id) { acked.insert(id); });

    check_acked_ids(dir, acked);
  }

  printf("db writes: %zu acknowledged writes survived 10 kills\n",
         acked.size());

  return;
}

/** Child process: write from four threads while another flushes, until
killed. */
static void flush_child_main(const char *dir, int round, int fd) {
  db *database = nullptr;
  if (!db_open(dir, deferred_flush_options(), &database).ok()) {
    _exit(2);
  }

  id_writer_t writers[4];
  std::thread threads[4];
  start_id_writers(*database, round, fd, writers, threads);

  for (;;) {
    db::flush_info_t info;
    if (!database->flush(info).ok()) {
      _exit(5);
    }

    usleep(1000);
  }

  return;
}

/** Kill a process that writes and flushes: every acknowledged id is stored
or replayed, and none twice, which replaying a stored write would cause. */
static void test_db_flushes() {
  test_dir_t dir;
  std::set<int64_t> acked;
  size_t rows = 0;

  for (int round = 0; round < 8; ++round) {
    test_kill_child<int64_t>(
        30 + round * 20,
        [&](int fd) { flush_child_main(dir.m_path, round, fd); },
        [&](int64_t id) { acked.insert(id); });

    rows = check_acked_ids(dir, acked);
  }

  printf("db flushes: %zu acknowledged writes, %zu recovered rows\n",
         acked.size(), rows);

  return;
}

/** Recover a stored batch and a WAL-only batch after SIGKILL. Only the
WAL-only batch needs its payload, and an invalid final batch is truncated. */
static void test_db_recovery_skips_stored_payload() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_wal_sync = true;
  int ready = 0;
  test_kill_child<int>(
      1,
      [&](int fd) {
        db *database = nullptr;
        if (!db_open(dir.m_path, &options, &database).ok() ||
            !write_id(*database, 1).ok()) {
          _exit(2);
        }

        db::flush_info_t info;
        /* The final record will stand in for a damaged tail after the kill. */
        if (!database->flush(info).ok() || info.m_flushed_seq != 1 ||
            !write_id(*database, 2).ok() || !write_id(*database, 3).ok()) {
          _exit(3);
        }

        test_crash_ack(fd, 1);

        for (;;) {
          pause();
        }
      },
      [&](int ack) { ready = ack; });
  REQUIRE(ready == 1);

  const std::string path = dir.file(wal_file_name(1).c_str());
  os_file_t file = OS_FILE_CLOSED;
  REQUIRE(os_file_open(path.c_str(), OS_FILE_OPEN, file).ok());
  uint64_t size = 0;
  REQUIRE(os_file_size(file, size).ok());
  std::vector<unsigned char> bytes(size);
  REQUIRE(os_file_read(file, bytes.data(), bytes.size(), 0).ok());

  size_t end = WAL_FILE_HEADER_BYTES;
  for (batch_seq_t expected : {1, 2}) {
    REQUIRE(end + WAL_RECORD_HEADER_BYTES < bytes.size());
    batch_seq_t seq = 0;
    uint32_t index = 0;
    size_t length = 0;
    REQUIRE(wal_decode_record_header(bytes.data() + end, seq, index, length));
    REQUIRE(seq == expected);
    end += WAL_RECORD_HEADER_BYTES + length;
  }
  REQUIRE(end + WAL_RECORD_HEADER_BYTES < bytes.size());

  /* A bad header at F still prevents recovery and must not trigger repair. */
  const size_t header_crc = WAL_FILE_HEADER_BYTES + WAL_RECORD_HEADER_BYTES - 1;
  const unsigned char bad_crc = bytes[header_crc] ^ 1;
  REQUIRE(os_file_write(file, &bad_crc, 1, header_crc).ok());
  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).code() == DB_CORRUPTION);
  CHECK(database == nullptr);
  REQUIRE(os_file_size(file, size).ok());
  CHECK_EQ(size, bytes.size());

  /* Damage only the payloads at F and at the end; record 2 remains intact. */
  bytes[WAL_FILE_HEADER_BYTES + WAL_RECORD_HEADER_BYTES] ^= 1;
  bytes.back() ^= 1;
  REQUIRE(os_file_write(file, bytes.data(), bytes.size(), 0).ok());
  REQUIRE(db_open(dir.m_path, &options, &database).ok());
  db::stats_t stats;
  database->stats(stats);
  CHECK_EQ(stats.m_flushed_seq, batch_seq_t{1});
  CHECK_EQ(stats.m_visible_seq, batch_seq_t{2});
  std::vector<int64_t> ids = all_ids(*database);
  std::sort(ids.begin(), ids.end());
  CHECK(ids == std::vector<int64_t>({1, 2}));
  REQUIRE(os_file_size(file, size).ok());
  CHECK_EQ(size, end);

  bytes.resize(end);
  std::vector<unsigned char> prefix(end);
  REQUIRE(os_file_read(file, prefix.data(), prefix.size(), 0).ok());
  CHECK(prefix == bytes);
  CHECK(os_file_close(file).ok());

  CHECK(write_id(*database, 4).ok());
  db_close(database);
  ids = reopen_ids(dir);
  std::sort(ids.begin(), ids.end());
  CHECK(ids == std::vector<int64_t>({1, 2, 4}));
}

/** Child process: write from four threads; the background thread flushes
often and deletes small WAL files. */
static void background_child_main(const char *dir, int round, int fd) {
  db::options_t options;
  options.m_flush_interval_ms = 2;
  options.m_wal_file_size_bytes = 8192;

  db *database = nullptr;
  if (!db_open(dir, &options, &database).ok()) {
    _exit(2);
  }

  id_writer_t writers[4];
  std::thread threads[4];
  start_id_writers(*database, round, fd, writers, threads);

  for (std::thread &thread : threads) {
    thread.join();
  }

  return;
}

/** Kill a process whose background thread flushes and purges the WAL: every
acknowledged id survives, none twice, though files were being deleted
when the process died. */
static void test_db_background() {
  test_dir_t dir;
  std::set<int64_t> acked;
  size_t rows = 0;

  for (int round = 0; round < 8; ++round) {
    test_kill_child<int64_t>(
        50 + round * 25,
        [&](int fd) { background_child_main(dir.m_path, round, fd); },
        [&](int64_t id) { acked.insert(id); });

    rows = check_acked_ids(dir, acked);
  }

  printf("db background: %zu acknowledged writes, %zu recovered rows\n",
         acked.size(), rows);

  return;
}

/* ========================== Store flushes =========================== */

/** Rows of write seq in the store case.
@return number of rows generated for that write */
static uint32_t store_row_count(batch_seq_t seq) {
  return (static_cast<uint32_t>(seq % 5) + 1);
}

/** Child process: flush writes after F, one per flush, until killed. Every
10th write adds a column. Acknowledges each committed F. */
static void store_child_main(const char *dir, int fd) {
  store *st = nullptr;
  if (!store::open(dir, 24, &st).ok()) {
    _exit(2);
  }

  test_catalog_t cat;
  catalog_load(cat, st->catalog());

  for (;;) {
    const batch_seq_t seq = st->catalog().m_flushed_seq + 1;

    char extra[32];
    snprintf(extra, sizeof(extra), "x%llu",
             static_cast<unsigned long long>(seq));

    ingest_field_t f[] = {field_int("seq", static_cast<int64_t>(seq)),
                          field_int(extra, 1)};

    const uint32_t n_fields = seq % 10 == 0 ? 2 : 1;

    std::vector<ingest_point_t> points;

    for (uint32_t r = 0; r < store_row_count(seq); ++r) {
      points.push_back(
          point(TEST_BASE_TIME_US + static_cast<int64_t>(seq) * 1000 + r,
                nullptr, 0, f, n_fields));
    }

    store_write_t w = prepare_catalog_write(
        cat, seq, "k", &points[0], static_cast<uint32_t>(points.size()));

    if (!st->append(&w, 1).ok()) {
      _exit(3);
    }

    test_crash_ack(fd, seq);
  }

  return;
}

/** Kill a process while it flushes to the store: F covers every committed
flush, and the stored rows are exactly those of writes 1 .. F. */
static void test_store_flushes() {
  test_dir_t dir;
  batch_seq_t acked = 0;

  for (int round = 0; round < 8; ++round) {
    test_kill_child<batch_seq_t>(
        5 + round * 3, [&](int fd) { store_child_main(dir.m_path, fd); },
        [&](batch_seq_t seq) { acked = seq; });

    store *st = nullptr;
    const db_status_t status = store::open(dir.m_path, 24, &st);
    if (!status.ok()) {
      fprintf(stderr, "reopen failed: %s\n", status.message().c_str());
    }

    REQUIRE(st != nullptr);

    const store_catalog_t &state = st->catalog();
    CHECK(state.m_flushed_seq >= acked);

    uint64_t expected = 0;

    for (batch_seq_t s = 1; s <= state.m_flushed_seq; ++s) {
      expected += store_row_count(s);
    }

    uint64_t actual = 0;

    for (const auto &[name, table] : state.m_tables) {
      for (const auto &[hour, partition] : table.m_partitions) {
        int64_t rows = -1;
        CHECK(test_store_query_int(
                  *st, "SELECT count(*) FROM " + partition.m_table_name, rows)
                  .ok());
        actual += static_cast<uint64_t>(rows);
      }
    }

    CHECK_EQ(actual, expected);

    store::destroy(st);
  }

  printf("store flushes: %llu flushes committed across 8 kills\n",
         static_cast<unsigned long long>(acked));

  return;
}

/** A committed override survives SIGKILL before its table schema is flushed. */
static void test_table_options_recovery() {
  test_dir_t dir;
  db::options_t options = *deferred_flush_options();
  options.m_wal_sync = true;
  options.m_partition_duration_hours = 24;

  const int64_t now = ut_now_us();
  int acknowledgements = 0;
  test_kill_child<int>(
      1,
      [&](int fd) {
        db *database = nullptr;
        if (!db_open(dir.m_path, &options, &database).ok()) {
          _exit(2);
        }

        db_test_access::stop_flusher(*database);

        ingest_field_t f = field_int("v", 1);
        ingest_point_t points[] = {
            point(now - 2 * UT_HOUR_US, nullptr, 0, &f, 1),
            point(now, nullptr, 0, &f, 1)};

        batch_seq_t seq = 0;
        if (!database->write("m", 1, points, 2, seq).ok() ||
            !database->set_table_options("m", {1}).ok()) {
          _exit(3);
        }

        test_crash_ack(fd, 1);

        for (;;) {
          pause();
        }
      },
      [&](int ack) { acknowledgements += ack; });
  REQUIRE(acknowledgements == 1);

  store *st = nullptr;
  REQUIRE(store::open(dir.m_path, 24, &st).ok());
  CHECK_EQ(st->catalog().m_flushed_seq, batch_seq_t{0});
  CHECK(!st->catalog().m_tables.at("m").m_initialized);
  CHECK(st->catalog().m_tables.at("m").m_retention_hours == 1);
  store::destroy(st);

  db *database = nullptr;
  REQUIRE(db_open(dir.m_path, &options, &database).ok());
  db_test_access::stop_flusher(*database);

  test_query_t result;
  REQUIRE(query_all(*database, "SELECT count(*) FROM m", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{1});
  CHECK(database->set_table_options("m", {0}).ok());
  REQUIRE(query_all(*database, "SELECT count(*) FROM m", result).ok());
  CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{2});

  db::flush_info_t info;
  CHECK(database->flush(info).ok());

  db_close(database);
}

/** Explicit declarations recover with and without a WAL-only first schema. */
static void test_create_table_recovery() {
  for (bool initialized : {false, true}) {
    test_dir_t dir;
    db::options_t options = *deferred_flush_options();
    options.m_wal_sync = true;

    const int64_t now = ut_now_us();
    int acknowledgements = 0;
    test_kill_child<int>(
        1,
        [&](int fd) {
          db *database = nullptr;
          if (!db_open(dir.m_path, &options, &database).ok()) {
            _exit(2);
          }

          db_test_access::stop_flusher(*database);

          if (!database->create_table("m", {5, 1}).ok()) {
            _exit(3);
          }

          if (initialized) {
            ingest_field_t f = field_int("v", 1);
            ingest_point_t p = point(now, nullptr, 0, &f, 1);
            batch_seq_t seq = 0;
            if (!database->write("m", 1, &p, 1, seq).ok()) {
              _exit(4);
            }
          }

          test_crash_ack(fd, 1);

          for (;;) {
            pause();
          }
        },
        [&](int ack) { acknowledgements += ack; });
    REQUIRE(acknowledgements == 1);

    db *database = nullptr;
    options.m_partition_duration_hours = 1;
    REQUIRE(db_open(dir.m_path, &options, &database).ok());
    db_test_access::stop_flusher(*database);

    const store_catalog_t &catalog =
        db_test_access::store_handle(*database).catalog();
    CHECK_EQ(catalog.m_flushed_seq, batch_seq_t{0});
    CHECK(!catalog.m_tables.at("m").m_initialized);
    CHECK(catalog.m_tables.at("m").m_retention_hours == 1);

    std::vector<db_table_info_t> tables;
    database->collect_tables(tables);
    REQUIRE(tables.size() == 1);
    CHECK_EQ(tables[0].m_partition_duration_hours, uint32_t{5});
    CHECK((tables[0].m_schema != nullptr) == initialized);

    db_cursor *cursor = nullptr;
    const db_status_t queried =
        database->open_cursor("SELECT count(*) FROM m", false, &cursor);

    if (initialized) {
      CHECK(queried.ok());
      db_cursor::destroy(cursor);
    } else {
      CHECK(queried.code() == DB_TABLE_NOT_INITIALIZED && cursor == nullptr);

      ingest_field_t f = field_int("v", 1);
      ingest_point_t p = point(now, nullptr, 0, &f, 1);
      batch_seq_t seq = 0;
      CHECK(database->write("m", 1, &p, 1, seq).ok());
    }

    db::flush_info_t info;
    CHECK(database->flush(info).ok());
    CHECK(catalog.m_tables.at("m").m_initialized);

    test_query_t result;
    CHECK(query_all(*database, "SELECT count(*) FROM m", result).ok());
    REQUIRE(result.m_rows.size() == 1);
    CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{1});

    db_close(database);
  }
}

/** Kills around the flush/drop boundary never revive old rows in a new
table. */
static void test_drop_table_recovery() {
  for (int stage = 0; stage < 6; ++stage) {
    ut_debug_scope debug_scope;

    test_dir_t dir;
    db::options_t options = *deferred_flush_options();
    options.m_wal_sync = true;
    options.m_partition_duration_hours = 24;

    const int64_t now = ut_now_us();
    int acknowledgements = 0;
    test_kill_child<int>(
        1,
        [&](int fd) {
          db *database = nullptr;
          if (!db_open(dir.m_path, &options, &database).ok()) {
            _exit(2);
          }

          db_test_access::stop_flusher(*database);

          if (!database->create_table("m", {5, 0}).ok()) {
            _exit(3);
          }

          ingest_field_t f = field_int("v", 1);
          ingest_point_t p = point(now, nullptr, 0, &f, 1);
          batch_seq_t seq = 0;
          if (stage != 5 && !database->write("m", 1, &p, 1, seq).ok()) {
            _exit(4);
          }

          if (stage == 0) {
            ut_debug_global().set(
                "db.before_drop_table",
                [&] {
                  test_crash_ack(fd, 1);

                  for (;;) {
                    pause();
                  }
                },
                1);
          }

          if (!database->drop_table("m").ok()) {
            _exit(5);
          }

          if (stage == 2 || stage == 3) {
            if (!database->create_table("m", {1, 0}).ok()) {
              _exit(6);
            }
          }

          if (stage == 3 || stage == 4) {
            f = field_str("v", "new");
            p = point(now, nullptr, 0, &f, 1);
            if (!database->write("m", 1, &p, 1, seq).ok()) {
              _exit(7);
            }
          }

          test_crash_ack(fd, 1);

          for (;;) {
            pause();
          }
        },
        [&](int ack) { acknowledgements += ack; });
    REQUIRE(acknowledgements == 1);

    db *database = nullptr;
    REQUIRE(db_open(dir.m_path, &options, &database).ok());
    db_test_access::stop_flusher(*database);

    const store_catalog_t &catalog =
        db_test_access::store_handle(*database).catalog();
    CHECK_EQ(catalog.m_flushed_seq,
             stage == 5 ? batch_seq_t{0} : batch_seq_t{1});

    db_cursor *cursor = nullptr;
    test_query_t result;

    if (stage == 0) {
      CHECK(query_all(*database, "SELECT v FROM m", result).ok());
      REQUIRE(result.m_rows.size() == 1);
      CHECK_EQ(result.m_rows[0][0].m_integer, int64_t{1});
      CHECK_EQ(catalog.m_tables.at("m").m_table_id, uint64_t{1});
    } else if (stage == 1 || stage == 5) {
      CHECK(database->open_cursor("SELECT * FROM m", false, &cursor).code() ==
            DB_ERROR);
      CHECK(cursor == nullptr && catalog.m_tables.count("m") == 0);
    } else if (stage == 2) {
      CHECK(database->open_cursor("SELECT * FROM m", false, &cursor).code() ==
            DB_TABLE_NOT_INITIALIZED);
      CHECK(cursor == nullptr);
      CHECK_EQ(catalog.m_tables.at("m").m_table_id, uint64_t{2});
      CHECK(!catalog.m_tables.at("m").m_initialized);
    } else {
      CHECK(query_all(*database, "SELECT v FROM m", result).ok());
      REQUIRE(result.m_rows.size() == 1);
      CHECK(result.m_rows[0][0].m_string == "new");

      db::flush_info_t flushed;
      CHECK(database->flush(flushed).ok());
      CHECK_EQ(catalog.m_tables.at("m").m_table_id, uint64_t{2});
      CHECK_EQ(catalog.m_tables.at("m").m_partition_duration_hours,
               stage == 3 ? uint32_t{1} : uint32_t{24});
    }

    db_close(database);
  }
}

/** A kill leaves an intact database or files that Drop can clean up. */
static void test_drop_database_recovery() {
  const char *points[] = {
      "directory.drop.before_remove", "directory.drop.storage_removed",
      "directory.drop.file_removed", "directory.drop.before_finish",
      "directory.drop.empty"};

  for (int stage = 0; stage < 6; ++stage) {
    test_dir_t parent;
    const std::string path = parent.file("database");
    int acknowledged = 0;
    test_kill_child<int>(
        1,
        [&](int fd) {
          db::options_t options = *deferred_flush_options();
          options.m_wal_sync = true;

          db *database = nullptr;
          if (!db_open(path.c_str(), &options, &database).ok()) {
            _exit(2);
          }

          db_test_access::stop_flusher(*database);

          if (!write_id(*database, 1).ok()) {
            _exit(3);
          }

          db_close(database);

          ut_debug_scope debug_scope;

          if (stage < 5) {
            ut_debug_global().set(
                points[stage],
                [&] {
                  test_crash_ack(fd, 1);

                  for (;;) {
                    pause();
                  }
                },
                1);
          }

          if (!db_drop_database(path.c_str()).ok()) {
            _exit(4);
          }

          test_crash_ack(fd, 1);

          for (;;) {
            pause();
          }
        },
        [&](int ack) {
          acknowledged += ack;

          if (stage < 5) {
            db *locked = nullptr;
            CHECK(db_open(path.c_str(), deferred_flush_options(), &locked)
                      .code() == DB_LOCKED);
            CHECK(locked == nullptr);
          }
        });
    REQUIRE(acknowledged == 1);

    if (stage == 0) {
      db *database = nullptr;
      REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());
      CHECK(all_ids(*database) == std::vector<int64_t>({1}));

      db_close(database);
    } else if (stage < 3) {
      db *database = nullptr;
      CHECK(db_open(path.c_str(), deferred_flush_options(), &database).code() ==
            DB_CORRUPTION);
      CHECK(database == nullptr);
    }

    CHECK(db_drop_database(path.c_str()).ok());
    CHECK(access(path.c_str(), F_OK) != 0);

    db *database = nullptr;
    REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());

    std::vector<db_table_info_t> tables;
    database->collect_tables(tables);
    CHECK(tables.empty());

    db_close(database);
    CHECK(db_drop_database(path.c_str()).ok());
  }
}

/** Kills during first Open leave either recoverable or removable storage. */
static void test_initialization_recovery() {
  for (int stage = 0; stage < 3; ++stage) {
    ut_debug_scope debug_scope;

    test_dir_t parent;
    const std::string path = parent.file("database");
    int acknowledged = 0;
    test_kill_child<int>(
        1,
        [&](int fd) {
          db database;

          ut_debug_global().set(
              stage == 1 ? "db.after_open_store" : "db.after_prepare_open",
              [&] {
                if (stage == 2) {
                  os_file_t file = OS_FILE_CLOSED;
                  if (!os_file_open((path + "/_pt_.duckdb").c_str(),
                                    OS_FILE_CREATE, file)
                           .ok() ||
                      !os_file_write(file, "broken", 6, 0).ok() ||
                      !os_file_sync(file).ok()) {
                    _exit(3);
                  }
                }

                test_crash_ack(fd, 1);

                for (;;) {
                  pause();
                }
              },
              1);

          if (!database.open(path.c_str(), *deferred_flush_options()).ok()) {
            _exit(2);
          }
        },
        [&](int ack) {
          acknowledged += ack;
          CHECK(db_drop_database(path.c_str()).code() == DB_LOCKED);
        });
    REQUIRE(acknowledged == 1);

    db *database = nullptr;

    if (stage == 2) {
      CHECK(!db_open(path.c_str(), deferred_flush_options(), &database).ok());
      CHECK(database == nullptr);
    } else {
      REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());
      CHECK(db_test_access::store_handle(*database).catalog().m_wal_id != 0);
      CHECK(access((path + "/INITIALIZING").c_str(), F_OK) != 0);
      CHECK(access((path + "/IDENTITY").c_str(), F_OK) != 0);

      db_close(database);
    }

    /* Drop also accepts a directory whose store is damaged. */
    CHECK(db_drop_database(path.c_str()).ok());
    database = nullptr;
    REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());
    CHECK(write_id(*database, 9).ok());

    db_close(database);
    REQUIRE(db_open(path.c_str(), deferred_flush_options(), &database).ok());
    CHECK(all_ids(*database) == std::vector<int64_t>({9}));

    db_close(database);
  }
}

/** Remove actual pinned-DuckDB spill files left behind by a killed sort. */
static void test_drop_database_spill() {
  test_dir_t parent;
  const std::string path = parent.file("database");
  int files = 0;
  test_kill_child<int>(
      1,
      [&](int fd) {
        db::options_t options = *deferred_flush_options();
        options.m_duckdb_memory_limit_bytes = 32 * 1024 * 1024;
        options.m_duckdb_threads = 1;

        db *database = nullptr;
        if (!db_open(path.c_str(), &options, &database).ok()) {
          _exit(2);
        }

        db_test_access::stop_flusher(*database);
        if (!database
                 ->exec("CREATE TABLE source AS SELECT i, "
                        "repeat(md5(i::VARCHAR), 4) v FROM range(600000) r(i)")
                 .ok()) {
          _exit(3);
        }

        db_cursor *cursor = nullptr;
        if (!database
                 ->open_cursor("SELECT v FROM source ORDER BY v DESC", false,
                               &cursor)
                 .ok()) {
          _exit(4);
        }

        query_rows_t rows;
        const db_status_t queried = cursor->fetch(rows);
        if (!queried.ok() || rows.empty()) {
          fprintf(stderr, "spill query: %s\n", queried.message().c_str());
          _exit(5);
        }

        std::vector<std::string> names;
        if (!os_dir_list((path + "/_pt_.duckdb.tmp").c_str(), names).ok() ||
            names.empty()) {
          _exit(6);
        }

        test_crash_ack(fd, static_cast<int>(names.size()));

        for (;;) {
          pause();
        }
      },
      [&](int count) { files = count; });
  REQUIRE(files > 0);

  const db_status_t removed = db_drop_database(path.c_str());
  if (!removed.ok()) {
    fprintf(stderr, "spill drop %s: %s\n", path.c_str(),
            removed.message().c_str());
  }

  CHECK(removed.ok());
  CHECK(access(path.c_str(), F_OK) != 0);
}

/** Run the SIGKILL recovery tests. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"wal_appends", test_wal_appends},
      {"db_writes", test_db_writes},
      {"db_flushes", test_db_flushes},
      {"db_recovery_skips_stored_payload",
       test_db_recovery_skips_stored_payload},
      {"db_background", test_db_background},
      {"store_flushes", test_store_flushes},
      {"table_options_recovery", test_table_options_recovery},
      {"create_table_recovery", test_create_table_recovery},
      {"drop_table_recovery", test_drop_table_recovery},
      {"drop_database_recovery", test_drop_database_recovery},
      {"drop_database_spill", test_drop_database_spill},
      {"initialization_recovery", test_initialization_recovery},
  };

  return (test_main("crash-t", argc, argv, cases));
}
