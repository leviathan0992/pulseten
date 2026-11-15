/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * model-t.cc
 *    Model-based test of writes, flushes, WAL cleanup, reopening, injected
 *    failures and SIGKILL, checked against the set of ids that must exist.
 *
 * A run is a series of epochs. In each, a child process opens the database
 * and runs seeded random steps, checking every query against its model and
 * reporting each step and each write on a pipe. The parent kills most
 * children with SIGKILL at a seeded step, then reopens the database and
 * checks it against what the child reported:
 *   - a write reported successful is present, whole;
 *   - a write rejected with nothing written is absent;
 *   - a write whose outcome is unknown, or that was cut short by the kill, is
 *     either present whole or absent;
 *   - nothing else is present, and nothing present before is lost.
 * Ids are never reused, so each write can be told apart.
 *
 * Covers: src/db/db.cc, src/wal/wal.cc, src/store/store.cc,
 *         src/store/pt_duckdb.cc, src/memtable/memtable.cc
 *
 *-------------------------------------------------------------------------
 */

#include <algorithm>
#include <cinttypes>
#include <csignal>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "db.h"
#include "ingest.h"
#include "test_crash.h"
#include "test_db.h"
#include "test_db_access.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_stress.h"
#include "test_util.h"
#include "ut.h"

/** Tables written by the test. */
static const char *const model_tables[] = {"t0", "t1", "t2"};

/** Number of tables. */
static const int MODEL_TABLES = 3;

/** Ids of each table. */
typedef std::set<int64_t> model_ids_t[MODEL_TABLES];

/** A failure point the child may arm, and the code it returns. */
struct model_fault_t {
  /** Point name. */
  const char *m_name;

  /** Code returned when it fires. */
  dberr_t m_code;
};

/** Points armed during an epoch. wal.sync fires only in synced epochs. */
static const model_fault_t model_faults[] = {
    {"wal.partial_write", DB_IO_ERROR}, {"wal.sync", DB_IO_ERROR},
    {"wal.write_header", DB_IO_ERROR},  {"wal.purge", DB_IO_ERROR},
    {"store.append", DB_ERROR},         {"store.append", DB_OUT_OF_MEMORY},
    {"store.commit", DB_IO_ERROR},      {"store.rollback", DB_ERROR},
};

/** Points armed only for an open. */
static const model_fault_t model_open_faults[] = {
    {"wal.truncate", DB_IO_ERROR},
    {"wal.write_header", DB_IO_ERROR},
    {"store.commit", DB_IO_ERROR},
};

/** Kinds of child reports. */
enum model_ack_kind_t : int32_t {
  /** A step begins; m_step is its index in the epoch. */
  MODEL_STEP = 1,

  /** A write of m_count ids from m_first_id to table m_table begins. */
  MODEL_WRITE_BEGIN,

  /** The write ended with m_code. */
  MODEL_WRITE_DONE,

  /** After a reopen, an uncertain write was found present or absent. */
  MODEL_RESOLVED,

  /** A check failed; the child exits with status 3. */
  MODEL_FAIL
};

/** One report from the child; written atomically. */
struct model_ack_t {
  /** A model_ack_kind_t. */
  int32_t m_kind{0};

  /** Table index of a write. */
  int32_t m_table{0};

  /** Result code of a write. */
  int32_t m_code{0};

  /** Whether a resolved write is present. */
  int32_t m_present{0};

  /** Step index within the epoch. */
  int64_t m_step{0};

  /** First id of a write. */
  int64_t m_first_id{0};

  /** Ids in a write. */
  int64_t m_count{0};
};

/** What the parent knows about one write. */
enum class model_write_t {
  /** Begun, with no result reported. */
  BEGUN,

  /** Present whole. */
  PRESENT,

  /** Absent. */
  ABSENT,

  /** Either present whole or absent. */
  UNCERTAIN
};

/** Writes whose result leaves nothing written. */
static bool model_rejected(dberr_t code) {
  return (code != DB_SUCCESS && code != DB_OUTCOME_UNKNOWN &&
          code != DB_CORRUPTION);
}

/** Time of an id: ids spread over ten one-day partitions. */
static int64_t model_time(int64_t id) {
  return (TEST_BASE_TIME_US + (id * 7919) % (10 * 86400) * 1000000);
}

/** Read every id of every table.
@return DB_SUCCESS or the query failure */
static db_status_t model_read(db &database, model_ids_t &ids) {
  std::vector<schema_ref_t> schemas;
  database.collect_schemas(schemas);

  for (int t = 0; t < MODEL_TABLES; ++t) {
    ids[t].clear();

    bool exists = false;

    for (const schema_ref_t &schema : schemas) {
      exists = exists || schema->m_name == model_tables[t];
    }

    if (!exists) {
      continue;
    }

    test_query_t result;
    const db_status_t err = query_all(
        database, std::string("SELECT id FROM ") + model_tables[t], result);

    if (!err.ok()) {
      return (err);
    }

    for (const std::vector<query_value_t> &row : result.m_rows) {
      ids[t].insert(row[0].m_integer);
    }
  }

  return (DB_SUCCESS);
}

/** Business settings shared by every open. */
static db::options_t model_options(bool wal_sync) {
  db::options_t options = daily_partition_options();
  options.m_wal_sync = wal_sync;
  options.m_wal_file_size_bytes = 64 * 1024;

  return (options);
}

/** The child side of one epoch. It never returns. */
class model_child_t {
 public:
  /** Prepare an epoch over the verified state ids; next_id is unused. */
  model_child_t(int fd, const char *dir, uint64_t seed, bool wal_sync,
                int64_t steps, const model_ids_t &ids, int64_t next_id)
      : m_fd(fd),
        m_dir(dir),
        m_rng(seed),
        m_wal_sync(wal_sync),
        m_steps(steps),
        m_next_id(next_id) {
    for (int t = 0; t < MODEL_TABLES; ++t) {
      m_ids[t] = ids[t];
    }
  }

  /** Run the steps, close the database and exit with 0. */
  [[noreturn]] void run();

 private:
  /** A write whose outcome is not known yet. */
  struct pending_t {
    /** Table index. */
    int m_table{0};

    /** First id. */
    int64_t m_first_id{0};

    /** Number of ids. */
    int64_t m_count{0};
  };

  /** Return a random integer in [0, n). */
  int pick(int n) {
    return (static_cast<int>(m_rng() % static_cast<uint64_t>(n)));
  }

  /** Report one event. */
  void ack(model_ack_t a) {
    a.m_step = m_step;
    test_crash_ack(m_fd, a);
  }

  /** Report a failed check and exit. */
  [[noreturn]] void fail(const char *format, ...);

  /** Open the database, retrying without faults if an injected one fails. */
  void open(bool inject);

  /** Close and reopen, then resolve pending writes and check every id. */
  void reopen(bool inject);

  /** Write one batch of new ids. */
  void write();

  /** Compare one table's aggregate, or all its ids, with the model. */
  void check_query();

  /** Pipe to the parent. */
  const int m_fd;

  /** Database directory. */
  const char *const m_dir;

  /** Every random choice of the epoch. */
  std::mt19937_64 m_rng;

  /** Sync each WAL group. */
  const bool m_wal_sync;

  /** Steps to run. */
  const int64_t m_steps;

  /** Current step. */
  int64_t m_step{0};

  /** Next unused id. */
  int64_t m_next_id;

  /** Ids that must exist. */
  model_ids_t m_ids;

  /** Writes with an unknown outcome since the last reopen. */
  std::vector<pending_t> m_pending;

  /** Reset between epochs, retaining controls across reopens in this epoch. */
  ut_debug_scope m_debug_scope;

  /** The open database. */
  db *m_db{nullptr};
};

/** Print why, tell the parent, and exit without cleanup. */
void model_child_t::fail(const char *format, ...) {
  fprintf(stderr, "model-t child: step %" PRId64 ": ", m_step);

  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fputc('\n', stderr);

  model_ack_t a;
  a.m_kind = MODEL_FAIL;
  ack(a);
  _exit(3);
}

/** An injected open failure must leave the directory openable. */
void model_child_t::open(bool inject) {
  if (inject) {
    const model_fault_t &fault = model_open_faults[pick(
        sizeof(model_open_faults) / sizeof(model_open_faults[0]))];
    ut_debug_global().arm(fault.m_name, fault.m_code);
  }

  const db::options_t options = model_options(m_wal_sync);
  db_status_t opened = db_open(m_dir, &options, &m_db);

  /* Points armed for this open, or left armed earlier, are cleared. */
  for (const model_fault_t &fault : model_faults) {
    ut_debug_global().disarm(fault.m_name);
  }

  for (const model_fault_t &fault : model_open_faults) {
    ut_debug_global().disarm(fault.m_name);
  }

  if (!opened.ok()) {
    opened = db_open(m_dir, &options, &m_db);
  }

  if (!opened.ok()) {
    fail("open failed: %s", opened.message().c_str());
  }

  return;
}

/** Each pending write must be present whole or absent; then the database
must hold exactly the model. */
void model_child_t::reopen(bool inject) {
  db_close(m_db);
  m_db = nullptr;
  open(inject);

  model_ids_t actual;
  if (const db_status_t err = model_read(*m_db, actual); !err.ok()) {
    fail("read failed: %s", err.message().c_str());
  }

  for (const pending_t &write : m_pending) {
    int64_t found = 0;

    for (int64_t i = 0; i < write.m_count; ++i) {
      found += actual[write.m_table].count(write.m_first_id + i);
    }

    if (found != 0 && found != write.m_count) {
      fail("uncertain write of %" PRId64 " ids from %" PRId64
           " is partly present: %" PRId64,
           write.m_count, write.m_first_id, found);
    }

    for (int64_t i = 0; found != 0 && i < write.m_count; ++i) {
      m_ids[write.m_table].insert(write.m_first_id + i);
    }

    model_ack_t a;
    a.m_kind = MODEL_RESOLVED;
    a.m_table = write.m_table;
    a.m_first_id = write.m_first_id;
    a.m_count = write.m_count;
    a.m_present = found != 0;
    ack(a);
  }

  m_pending.clear();

  for (int t = 0; t < MODEL_TABLES; ++t) {
    if (actual[t] != m_ids[t]) {
      fail("table %s holds %zu ids, model %zu", model_tables[t],
           actual[t].size(), m_ids[t].size());
    }
  }

  return;
}

/** A batch of 1 to 20 new ids in one table. */
void model_child_t::write() {
  const int t = pick(MODEL_TABLES);
  const int64_t count = 1 + pick(20);
  const int64_t first = m_next_id;
  m_next_id += count;

  model_ack_t a;
  a.m_kind = MODEL_WRITE_BEGIN;
  a.m_table = t;
  a.m_first_id = first;
  a.m_count = count;
  ack(a);

  std::vector<ingest_field_t> fields;
  std::vector<ingest_point_t> points;
  fields.reserve(static_cast<size_t>(count) * 2);

  for (int64_t i = 0; i < count; ++i) {
    fields.push_back(field_int("id", first + i));
    fields.push_back(field_str("pad", "model"));
    points.push_back(point(model_time(first + i), nullptr, 0,
                           &fields[fields.size() - 2], 2));
  }

  batch_seq_t seq = 0;
  const db_status_t written =
      m_db->write(model_tables[t], strlen(model_tables[t]), points.data(),
                  static_cast<uint32_t>(count), seq);

  a.m_kind = MODEL_WRITE_DONE;
  a.m_code = written.code();
  ack(a);

  if (written.ok()) {
    for (int64_t i = 0; i < count; ++i) {
      m_ids[t].insert(first + i);
    }
  } else if (!model_rejected(written.code())) {
    m_pending.push_back({t, first, count});
  }

  /* After any failure the database may be fenced; start again. */
  if (!written.ok()) {
    reopen(false);
  }

  return;
}

/** Aggregates or the full id list of one table, against the model. */
void model_child_t::check_query() {
  const int t = pick(MODEL_TABLES);
  const std::set<int64_t> &ids = m_ids[t];

  model_ids_t actual;

  if (pick(10) == 0) {
    if (const db_status_t err = model_read(*m_db, actual); !err.ok()) {
      fail("read failed: %s", err.message().c_str());
    }

    if (actual[t] != ids) {
      fail("table %s holds %zu ids, model %zu", model_tables[t],
           actual[t].size(), ids.size());
    }

    return;
  }

  if (ids.empty()) {
    return;
  }

  int64_t sum = 0;

  for (int64_t id : ids) {
    sum += id;
  }

  test_query_t result;
  const db_status_t err = query_all(
      *m_db,
      std::string("SELECT count(*), sum(id)::BIGINT, min(id), max(id) FROM ") +
          model_tables[t] + " WHERE time >= TIMESTAMP '2026-01-01'",
      result);

  if (!err.ok()) {
    fail("query failed: %s", err.message().c_str());
  }

  const std::vector<query_value_t> &row = result.m_rows[0];
  if (row[0].m_integer != static_cast<int64_t>(ids.size()) ||
      row[1].m_integer != sum || row[2].m_integer != *ids.begin() ||
      row[3].m_integer != *ids.rbegin()) {
    fail("table %s: count %" PRId64 " sum %" PRId64 ", model %zu %" PRId64,
         model_tables[t], row[0].m_integer, row[1].m_integer, ids.size(), sum);
  }

  return;
}

/** Writes, flushes, cleanup, reopens, faults and queries; then close. */
void model_child_t::run() {
  open(false);

  for (m_step = 0; m_step < m_steps; ++m_step) {
    model_ack_t a;
    a.m_kind = MODEL_STEP;
    ack(a);

    const int action = pick(100);
    if (action < 68) {
      write();
    } else if (action < 74) {
      db::flush_info_t info;
      if (!m_db->flush(info).ok()) {
        reopen(false);
      }
    } else if (action < 77) {
      /* A failed cleanup keeps every file; only a fence needs reopening. */
      if (db_test_access::purge_wal(*m_db).code() == DB_FENCED) {
        reopen(false);
      }
    } else if (action < 79) {
      reopen(pick(3) == 0);
    } else if (action < 85) {
      const model_fault_t &fault =
          model_faults[pick(sizeof(model_faults) / sizeof(model_faults[0]))];
      ut_debug_global().arm(fault.m_name, fault.m_code);
    } else {
      check_query();
    }
  }

  /* Faults left armed must not fail the clean close. */
  db_close(m_db);
  _exit(0);
}

/** Result of one epoch, as seen by the parent. */
struct model_epoch_t {
  /** Steps the child began. */
  int64_t m_steps{0};

  /** The child reported a failed check or exited abnormally. */
  bool m_failed{false};
};

/** One seeded run in the parent. */
class model_run_t {
 public:
  /** Prepare a run of settings.m_steps steps. */
  explicit model_run_t(const test_stress_t &settings, const char *case_name)
      : m_settings(settings), m_case(case_name), m_rng(settings.m_seed) {}

  /** Run every epoch, checking the database after each.
  @return true if every check passed */
  bool run();

 private:
  /** Return a random integer in [0, n). */
  int pick(int n) {
    return (static_cast<int>(m_rng() % static_cast<uint64_t>(n)));
  }

  /** Fork one epoch, kill it at kill_step unless negative, and collect its
  reports into m_writes. */
  model_epoch_t epoch(int64_t steps, int64_t kill_step, bool wal_sync);

  /** Reopen and check the database against the reports of an epoch.
  @return true if it matches */
  bool verify();

  /** Run settings. */
  const test_stress_t m_settings;

  /** Case name for the repeat command. */
  const char *const m_case;

  /** Epoch lengths, kill steps and child seeds. */
  std::mt19937_64 m_rng;

  /** Database directory. */
  test_dir_t m_dir;

  /** Verified ids. */
  model_ids_t m_ids;

  /** Next id no epoch has used. */
  int64_t m_next_id{1};

  /** Writes reported by the current epoch, by first id. */
  std::map<int64_t, std::pair<model_ack_t, model_write_t>> m_writes;

  /** Counters for the summary. */
  uint64_t m_epochs{0};
  uint64_t m_kills{0};
  uint64_t m_uncertain{0};
};

/** Children inherit the verified ids and the next unused id. */
model_epoch_t model_run_t::epoch(int64_t steps, int64_t kill_step,
                                 bool wal_sync) {
  model_epoch_t result;
  m_writes.clear();

  const uint64_t child_seed = m_rng();

  int fds[2];

  if (!test_check(pipe(fds) == 0, "pipe(fds) == 0", __FILE__, __LINE__)) {
    result.m_failed = true;

    return (result);
  }

  fflush(nullptr);

  const pid_t pid = fork();
  if (pid == 0) {
    close(fds[0]);

    model_child_t child(fds[1], m_dir.m_path, child_seed, wal_sync, steps,
                        m_ids, m_next_id);
    child.run();
  }

  close(fds[1]);

  if (pid < 0) {
    close(fds[0]);
    result.m_failed = true;

    return (result);
  }

  bool killed = false;
  model_ack_t a;
  while (read(fds[0], &a, sizeof(a)) == static_cast<ssize_t>(sizeof(a))) {
    switch (a.m_kind) {
      case MODEL_STEP:
        result.m_steps = a.m_step + 1;

        if (a.m_step == kill_step && !killed) {
          kill(pid, SIGKILL);
          killed = true;
        }

        break;
      case MODEL_WRITE_BEGIN:
        m_writes[a.m_first_id] = {a, model_write_t::BEGUN};
        m_next_id = std::max(m_next_id, a.m_first_id + a.m_count);
        break;
      case MODEL_WRITE_DONE: {
        const dberr_t code = static_cast<dberr_t>(a.m_code);
        m_writes[a.m_first_id].second =
            code == DB_SUCCESS     ? model_write_t::PRESENT
            : model_rejected(code) ? model_write_t::ABSENT
                                   : model_write_t::UNCERTAIN;
        break;
      }
      case MODEL_RESOLVED:
        m_writes[a.m_first_id].second =
            a.m_present ? model_write_t::PRESENT : model_write_t::ABSENT;
        break;
      default:
        result.m_failed = true;
        break;
    }
  }

  close(fds[0]);

  int status = 0;
  CHECK_EQ(waitpid(pid, &status, 0), pid);

  if (killed) {
    ++m_kills;

    /* The child may have finished just before the kill arrived. */
    const bool died = WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    const bool finished = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    result.m_failed = result.m_failed || (!died && !finished);
  } else {
    result.m_failed =
        result.m_failed || !WIFEXITED(status) || WEXITSTATUS(status) != 0;
  }

  return (result);
}

/** Every reported write must match its known state, earlier ids must stay,
and no other id may appear. */
bool model_run_t::verify() {
  db *database = nullptr;
  const db::options_t options = model_options(false);
  const db_status_t opened = db_open(m_dir.m_path, &options, &database);
  if (!opened.ok()) {
    fprintf(stderr, "model-t: reopen after epoch failed: %s\n",
            opened.message().c_str());

    return (false);
  }

  model_ids_t actual;
  const db_status_t read = model_read(*database, actual);

  db_close(database);

  if (!read.ok()) {
    fprintf(stderr, "model-t: read failed: %s\n", read.message().c_str());

    return (false);
  }

  model_ids_t expected;

  for (int t = 0; t < MODEL_TABLES; ++t) {
    expected[t] = m_ids[t];
  }

  for (const auto &entry : m_writes) {
    const model_ack_t &w = entry.second.first;
    const model_write_t state = entry.second.second;
    int64_t found = 0;

    for (int64_t i = 0; i < w.m_count; ++i) {
      found += actual[w.m_table].count(w.m_first_id + i);
    }

    const bool whole = found == w.m_count;
    const bool ok = state == model_write_t::PRESENT  ? whole
                    : state == model_write_t::ABSENT ? found == 0
                                                     : whole || found == 0;

    if (!ok) {
      fprintf(stderr,
              "model-t: write of %" PRId64 " ids from %" PRId64
              " to %s (state %d) has %" PRId64 " present\n",
              w.m_count, w.m_first_id, model_tables[w.m_table],
              static_cast<int>(state), found);

      return (false);
    }

    if (state == model_write_t::BEGUN || state == model_write_t::UNCERTAIN) {
      ++m_uncertain;
    }

    for (int64_t i = 0; whole && i < w.m_count; ++i) {
      expected[w.m_table].insert(w.m_first_id + i);
    }
  }

  for (int t = 0; t < MODEL_TABLES; ++t) {
    if (actual[t] != expected[t]) {
      fprintf(stderr, "model-t: table %s holds %zu ids, expected %zu\n",
              model_tables[t], actual[t].size(), expected[t].size());

      return (false);
    }

    m_ids[t] = actual[t];
  }

  return (true);
}

/** Epochs of 100 to 1000 steps; seven in ten end with SIGKILL. */
bool model_run_t::run() {
  int64_t done = 0;
  const int64_t total = static_cast<int64_t>(m_settings.m_steps);

  while (done < total) {
    const int64_t steps = std::min<int64_t>(100 + pick(901), total - done);
    const int64_t kill_step = pick(10) < 7 ? pick(static_cast<int>(steps)) : -1;
    const bool wal_sync = pick(10) == 0;

    const model_epoch_t result = epoch(steps, kill_step, wal_sync);
    ++m_epochs;

    if (result.m_failed || !verify()) {
      fprintf(stderr,
              "model-t: epoch %" PRIu64 " failed at step %" PRId64
              " of the run\n",
              m_epochs, done + result.m_steps);
      test_stress_repeat("model-t", m_case, m_settings);

      return (false);
    }

    done += result.m_steps > 0 ? result.m_steps : steps;
  }

  size_t ids = 0;

  for (int t = 0; t < MODEL_TABLES; ++t) {
    ids += m_ids[t].size();
  }

  printf("model-t: seed %" PRIu64 ", %" PRId64 " steps, %" PRIu64
         " epochs, %" PRIu64 " kills, %" PRIu64 " uncertain writes, %zu ids\n",
         m_settings.m_seed, done, m_epochs, m_kills, m_uncertain, ids);

  return (true);
}

/** Run one seed for the default number of steps. */
static void test_seed(uint64_t seed, const char *case_name) {
  const test_stress_t settings = test_stress_settings(seed, 2500);
  model_run_t run(settings, case_name);
  CHECK(run.run());

  return;
}

/** Run the model test with four seeds. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"seed/1", [] { test_seed(1, "seed/1"); }},
      {"seed/2", [] { test_seed(2, "seed/2"); }},
      {"seed/3", [] { test_seed(3, "seed/3"); }},
      {"seed/4", [] { test_seed(4, "seed/4"); }},
  };

  return (test_main("model-t", argc, argv, cases));
}
