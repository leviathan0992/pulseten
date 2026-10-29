/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage0storage-t.cc
 *    Tests for the DuckDB storage: creating the file, flushing writes with
 *    schema changes and day partitions, reloading, rollback, and SIGKILL
 *    during flushes.
 *
 *-------------------------------------------------------------------------
 */

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <map>
#include <string>
#include <vector>

#include "storage0storage.h"
#include "test0point.h"
#include "test0util.h"
#include "ut0time.h"

/** Schema versions of the tables written by a test, standing in for the
memtable. */
struct test_catalog_t {
  /** versions[name][i] has version i + 1. */
  std::map<std::string, std::vector<schema_t *>> versions;

  /** Plans whose batches are being flushed. */
  std::vector<ingest_plan_t *> plans;

  /** Destroy the schema versions and batches owned by the test catalog. */
  ~test_catalog_t() {
    std::map<std::string, std::vector<schema_t *>>::iterator it;
    for (it = versions.begin(); it != versions.end(); ++it) {
      for (size_t i = 0; i < it->second.size(); ++i) {
        delete it->second[i];
      }
    }

    for (size_t i = 0; i < plans.size(); ++i) {
      delete plans[i];
    }
  }
};

/** Take the schema versions of a storage file as the starting point.
@param[out]  cat    test catalog receiving stored schema versions
@param[in]   state  stored catalog to copy */
static void catalog_load(test_catalog_t *cat, const storage_state_t *state) {
  std::map<std::string, storage_table_t>::const_iterator it;

  for (it = state->tables.begin(); it != state->tables.end(); ++it) {
    for (size_t v = 0; v < it->second.versions.size(); ++v) {
      cat->versions[it->first].push_back(new schema_t(it->second.versions[v]));
    }
  }
}

/** Plan a write against the newest schema and describe it for flushing.
@param[in,out]  cat     test catalog receiving schema versions and an owned
                        batch
@param[in]      seq     write or record sequence number
@param[in]      table   logical table name
@param[in]      points  input points
@param[in]      n       number of input points
@return write description borrowing the batch and schema now owned by cat */
static storage_write_t catalog_write(test_catalog_t *cat, batch_seq_t seq,
                                     const char *table,
                                     const ingest_point_t *points, uint32_t n) {
  std::vector<schema_t *> &versions = cat->versions[table];
  const schema_t *latest = versions.empty() ? nullptr : versions.back();

  ingest_plan_t *plan = new ingest_plan_t;
  cat->plans.push_back(plan);
  CHECK(plan_for(latest, table, points, n, plan) == DB_SUCCESS);

  if (!plan->new_cols.empty()) {
    versions.push_back(new schema_t(schema_after(latest, *plan)));
  }

  storage_write_t w;
  w.seq = seq;
  w.batch = &plan->batch;
  w.schema = versions[plan->batch.schema_version - 1];

  return (w);
}

/** Execute a query and read its single integer result.
@param[in,out]  st   analytical storage
@param[in]      sql  SQL statement to check or execute
@return the single integer a query returns, or -1 */
static int64_t query_int(storage_t *st, const std::string &sql) {
  int64_t v = -1;
  CHECK(storage_query_int(*st, sql, v) == DB_SUCCESS);

  return (v);
}

/** Compare the metadata describing two stored database states.
@param[in]  a  first stored state to compare
@param[in]  b  second stored state to compare
@return true if two states describe the same file contents */
static bool states_equal(const storage_state_t *a, const storage_state_t *b) {
  if (a->flushed_seq != b->flushed_seq ||
      a->next_table_id != b->next_table_id ||
      a->tables.size() != b->tables.size() ||
      a->partitions.size() != b->partitions.size()) {
    return (false);
  }

  std::map<std::string, storage_table_t>::const_iterator t;
  for (t = a->tables.begin(); t != a->tables.end(); ++t) {
    if (b->tables.count(t->first) == 0) {
      return (false);
    }

    const storage_table_t &x = t->second;
    const storage_table_t &y = b->tables.find(t->first)->second;

    if (x.table_id != y.table_id || x.versions.size() != y.versions.size()) {
      return (false);
    }

    for (size_t v = 0; v < x.versions.size(); ++v) {
      if (x.versions[v].cols.size() != y.versions[v].cols.size()) {
        return (false);
      }
      for (size_t c = 0; c < x.versions[v].cols.size(); ++c) {
        const col_def_t &p = x.versions[v].cols[c];
        const col_def_t &q = y.versions[v].cols[c];
        if (p.name != q.name || p.role != q.role || p.type != q.type) {
          return (false);
        }
      }
    }
  }

  std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator p;
  for (p = a->partitions.begin(); p != a->partitions.end(); ++p) {
    if (b->partitions.count(p->first) == 0) {
      return (false);
    }

    const storage_partition_t &q = b->partitions.find(p->first)->second;
    if (p->second.phys_name != q.phys_name || p->second.n_cols != q.n_cols ||
        p->second.rows != q.rows) {
      return (false);
    }
  }

  return (true);
}

/** A new file has the internal tables and an empty state. */
static void test_open_new() {
  test_dir_t dir;
  storage_t *st = nullptr;
  std::string error;

  CHECK(storage_open(dir.path, 1, &st, error) == DB_SUCCESS);
  CHECK(error.empty());

  const storage_state_t *state = storage_get_state(*st);
  CHECK(state->flushed_seq == 0 && state->next_table_id == 1);
  CHECK(state->tables.empty() && state->partitions.empty());
  storage_close(st);

  CHECK(storage_open(dir.path, 1, &st, error) == DB_SUCCESS);
  CHECK(storage_get_state(*st)->flushed_seq == 0);
  storage_close(st);
}

/** Flushed writes, added columns and exact times survive a reload, which
gives the same state. */
static void test_flush_and_reload() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.path, 1, &st, test_error) == DB_SUCCESS);

  test_catalog_t cat;

  /* Three cpu points, then one that adds a column, then a mem point. */
  ingest_tag_t host[] = {tag("host", "a")};
  ingest_field_t usage[] = {field_double("usage", 0.5)};
  ingest_field_t more[] = {field_double("usage", 0.75), field_int("cores", 8)};
  ingest_field_t used[] = {field_int("used", 1024)};

  ingest_point_t p1[] = {point(T0, host, 1, usage, 1),
                         point(T0 + 1, host, 1, usage, 1),
                         point(T0 + 2, host, 1, usage, 1)};
  ingest_point_t p2 = point(T0 + 3, host, 1, more, 2);
  ingest_point_t p3 = point(T0, nullptr, 0, used, 1);

  storage_write_t writes[3];
  writes[0] = catalog_write(&cat, 1, "cpu", p1, 3);
  writes[1] = catalog_write(&cat, 2, "cpu", &p2, 1);
  writes[2] = catalog_write(&cat, 3, "mem", &p3, 1);

  /* The first write alone, then the other two. */
  std::string error;
  CHECK(storage_flush(*st, writes, 1, error) == DB_SUCCESS);
  CHECK(storage_get_state(*st)->flushed_seq == 1);
  CHECK(storage_flush(*st, writes + 1, 2, error) == DB_SUCCESS);

  const storage_state_t *state = storage_get_state(*st);
  CHECK(state->flushed_seq == 3);
  CHECK(state->tables.size() == 2);
  CHECK(state->tables.find("cpu")->second.versions.size() == 2);
  CHECK(state->partitions.size() == 2);

  /* cpu: time, host, usage, cores. The partition was widened for cores;
  the first three rows have NULL there. */
  const std::string cpu = storage_partition_name(1, ut_utc_day(T0));
  const storage_partition_t &part =
      state->partitions.find(std::make_pair((uint64_t)1, ut_utc_day(T0)))
          ->second;
  CHECK(part.phys_name == cpu && part.n_cols == 4 && part.rows == 4);

  CHECK(query_int(st, "SELECT count(*) FROM " + cpu) == 4);
  CHECK(query_int(st, "SELECT count(*) FROM " + cpu + " WHERE c3 IS NULL") ==
        3);
  CHECK(query_int(st, "SELECT max(c3) FROM " + cpu) == 8);
  CHECK(query_int(st, "SELECT count(*) FROM " + cpu + " WHERE c1 = 'a'") == 4);
  CHECK(query_int(st, "SELECT max(_pt_seq) FROM " + cpu) == 2);
  CHECK(query_int(
            st, "SELECT max(_pt_row) FROM " + cpu + " WHERE _pt_seq = 1") == 2);

  /* The exact microseconds survive. */
  CHECK(query_int(st, "SELECT epoch_us(max(c0)) FROM " + cpu) == T0 + 3);

  storage_state_t before = *state;
  storage_close(st);

  /* Reloading gives the same state, schema versions included. */
  std::string reload_error;
  CHECK(storage_open(dir.path, 1, &st, reload_error) == DB_SUCCESS);
  CHECK(reload_error.empty());
  CHECK(states_equal(&before, storage_get_state(*st)));

  const std::vector<schema_t> &v =
      storage_get_state(*st)->tables.find("cpu")->second.versions;
  CHECK(v[0].cols.size() == 3 && v[1].cols.size() == 4);
  CHECK(v[1].cols[3].name == "cores" && v[1].cols[3].type == COL_BIGINT);

  storage_close(st);
}

/** One write spanning three UTC days, one before 1970, lands in three
partitions. */
static void test_day_partitions() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.path, 1, &st, test_error) == DB_SUCCESS);

  /* One write across three UTC days, one of them before 1970. */
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t points[] = {point(-1, nullptr, 0, f, 1),
                             point(0, nullptr, 0, f, 1),
                             point(UT_DAY_US - 1, nullptr, 0, f, 1),
                             point(UT_DAY_US, nullptr, 0, f, 1)};

  test_catalog_t cat;
  storage_write_t w = catalog_write(&cat, 1, "m", points, 4);
  CHECK(storage_flush(*st, &w, 1, test_error) == DB_SUCCESS);

  const storage_state_t *state = storage_get_state(*st);
  CHECK(state->partitions.size() == 3);
  CHECK(query_int(st, "SELECT count(*) FROM _pt_t1_n1") == 1);
  CHECK(query_int(st, "SELECT count(*) FROM _pt_t1_p0") == 2);
  CHECK(query_int(st, "SELECT count(*) FROM _pt_t1_p1") == 1);

  storage_close(st);
}

/** A flush whose sequence numbers do not continue from F is refused. */
static void test_bad_arguments() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.path, 1, &st, test_error) == DB_SUCCESS);

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);

  /* Sequence numbers must continue from F. */
  storage_write_t w = catalog_write(&cat, 2, "m", &p, 1);
  std::string error;
  CHECK(storage_flush(*st, &w, 1, error) == DB_INVALID_ARGUMENT);
  CHECK(!error.empty());
  CHECK(storage_flush(*st, &w, 0, error) == DB_INVALID_ARGUMENT);

  w.seq = 1;
  CHECK(storage_flush(*st, &w, 1, error) == DB_SUCCESS);
  CHECK(storage_flush(*st, &w, 1, error) == DB_INVALID_ARGUMENT);

  storage_close(st);
}

#ifdef PT_DEBUG
/** A flush failing before commit leaves nothing, and the same write succeeds
afterwards. */
static void test_rollback() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.path, 1, &st, test_error) == DB_SUCCESS);

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  storage_write_t w = catalog_write(&cat, 1, "m", &p, 1);

  /* Fails after the table, partition and rows were written. */
  storage_dbg_fail_next_flush(*st);
  CHECK(storage_flush(*st, &w, 1, test_error) == DB_ERROR);
  CHECK(storage_get_state(*st)->flushed_seq == 0);

  /* Nothing of it is left: no rows, no partition table, no catalog rows.
  This shows that appender rows belong to the transaction. */
  CHECK(query_int(st,
                  "SELECT count(*) FROM information_schema.tables "
                  "WHERE table_schema = 'main'") == 4);
  CHECK(query_int(st, "SELECT count(*) FROM _pt_tables") == 0);

  /* The same write succeeds afterwards. */
  CHECK(storage_flush(*st, &w, 1, test_error) == DB_SUCCESS);
  storage_close(st);

  CHECK(storage_open(dir.path, 1, &st, test_error) == DB_SUCCESS);
  CHECK(storage_get_state(*st)->flushed_seq == 1);
  storage_close(st);
}
#endif

/** Rows of write seq in the kill test.
@param[in]  seq  write sequence number in the crash test
@return number of rows generated for that write */
static uint32_t kill_rows(batch_seq_t seq) {
  return ((uint32_t)(seq % 5) + 1);
}

/** Child process: flush writes after F, one per flush, until killed. Every
10th write adds a column. Reports each committed F on the pipe.
@param[in]  dir  database or WAL directory
@param[in]  fd   pipe descriptor for reporting acknowledged writes */
static void kill_child(const char *dir, int fd) {
  storage_t *st = nullptr;
  if (storage_open(dir, 1, &st, test_error) != DB_SUCCESS) {
    _exit(2);
  }

  test_catalog_t cat;
  catalog_load(&cat, storage_get_state(*st));

  for (;;) {
    const batch_seq_t seq = storage_get_state(*st)->flushed_seq + 1;

    char extra[32];
    snprintf(extra, sizeof(extra), "x%llu", (unsigned long long)seq);
    ingest_field_t f[] = {field_int("seq", (int64_t)seq), field_int(extra, 1)};
    const uint32_t n_fields = seq % 10 == 0 ? 2 : 1;

    std::vector<ingest_point_t> points;
    for (uint32_t r = 0; r < kill_rows(seq); ++r) {
      points.push_back(
          point(T0 + (int64_t)seq * 1000 + r, nullptr, 0, f, n_fields));
    }

    storage_write_t w =
        catalog_write(&cat, seq, "k", &points[0], (uint32_t)points.size());

    if (storage_flush(*st, &w, 1, test_error) != DB_SUCCESS) {
      _exit(3);
    }

    if (write(fd, &seq, sizeof(seq)) != (ssize_t)sizeof(seq)) {
      _exit(4);
    }
  }
}

/** Kill a process while it flushes, over and over: F covers every committed
flush and nothing else is stored. */
static void test_kill_during_flush() {
  test_dir_t dir;
  batch_seq_t acked = 0;

  for (int round = 0; round < 8; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.path, fds[1]);
    }
    close(fds[1]);

    const int wanted = 5 + round * 3;
    int received = 0;
    batch_seq_t seq = 0;

    while (received < wanted &&
           read(fds[0], &seq, sizeof(seq)) == (ssize_t)sizeof(seq)) {
      acked = seq;
      ++received;
    }

    kill(pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);

    while (read(fds[0], &seq, sizeof(seq)) == (ssize_t)sizeof(seq)) {
      acked = seq;
    }
    close(fds[0]);
    CHECK(received == wanted);

    /* F covers every committed flush, and the stored rows are exactly
    those of writes 1 .. F. */
    storage_t *st = nullptr;
    std::string error;
    CHECK(storage_open(dir.path, 1, &st, error) == DB_SUCCESS);
    if (st == nullptr) {
      fprintf(stderr, "reopen failed: %s\n", error.c_str());

      return;
    }

    const storage_state_t *state = storage_get_state(*st);
    CHECK(state->flushed_seq >= acked);

    uint64_t expected = 0;
    for (batch_seq_t s = 1; s <= state->flushed_seq; ++s) {
      expected += kill_rows(s);
    }

    uint64_t counted = 0;
    uint64_t actual = 0;
    std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
        it;
    for (it = state->partitions.begin(); it != state->partitions.end(); ++it) {
      counted += it->second.rows;
      actual += (uint64_t)query_int(
          st, "SELECT count(*) FROM " + it->second.phys_name);
    }

    CHECK(counted == expected);
    CHECK(actual == expected);
    storage_close(st);
  }

  printf("kill test: %llu flushes committed across 8 kills\n",
         (unsigned long long)acked);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_open_new();
  test_flush_and_reload();
  test_day_partitions();
  test_bad_arguments();
#ifdef PT_DEBUG
  test_rollback();
#endif
  test_kill_during_flush();

  return (test_result("storage0storage-t"));
}
