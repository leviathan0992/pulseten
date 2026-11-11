/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage-t.cc
 *    Tests for the DuckDB storage: creating the file, flushing writes with
 *    schema changes and day partitions, reloading, rollback, and SIGKILL
 *    during flushes.
 *
 *-------------------------------------------------------------------------
 */

#include "storage.h"

#include <cstdint>
#include <cstdio>
#include <limits>
#include <map>
#include <signal.h>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "duckdb_database.h"
#include "ingest.h"
#include "schema.h"
#include "test_point.h"
#include "test_util.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_time.h"

/** Prepare and commit a flush for a storage test.
@return the preparation or commit status */
static db_status_t flush_writes(storage_t &st, const storage_write_t *writes,
                                size_t n) {
  const db_status_t err = storage_flush_prepare(st, writes, n);
  if (!err.ok()) {
    return (err);
  }

  return (storage_flush_commit(st));
}

/** Schema versions of the tables written by a test, standing in for the
memtable. */
struct test_catalog_t {
  /** versions[name][i] has version i + 1. */
  std::map<std::string, std::vector<schema_t *>> m_versions;

  /** Plans whose batches are being flushed. */
  std::vector<ingest_plan_t *> m_plans;

  /** Destroy the schema versions and batches owned by the test catalog. */
  ~test_catalog_t() {
    std::map<std::string, std::vector<schema_t *>>::iterator it;
    for (it = m_versions.begin(); it != m_versions.end(); ++it) {
      for (size_t i = 0; i < it->second.size(); ++i) {
        delete it->second[i];
      }
    }

    for (size_t i = 0; i < m_plans.size(); ++i) {
      delete m_plans[i];
    }
  }
};

/** Take the schema versions of a storage file as the starting point. */
static void catalog_load(test_catalog_t &cat, const storage_catalog_t &state) {
  std::map<std::string, storage_table_t>::const_iterator it;

  for (it = state.m_tables.begin(); it != state.m_tables.end(); ++it) {
    for (size_t v = 0; v < it->second.m_versions.size(); ++v) {
      cat.m_versions[it->first].push_back(
          new schema_t(it->second.m_versions[v]));
    }
  }
}

/** Plan a write against the newest schema and describe it for flushing.
@param[in,out]  cat  test catalog receiving schema versions and an owned
                     batch
@return write description borrowing the batch and schema now owned by cat */
static storage_write_t catalog_write(test_catalog_t &cat, batch_seq_t seq,
                                     const char *table_name,
                                     const ingest_point_t *points, uint32_t n) {
  std::vector<schema_t *> &versions = cat.m_versions[table_name];
  const schema_t *latest = versions.empty() ? nullptr : versions.back();

  ingest_plan_t *plan = new ingest_plan_t;
  cat.m_plans.push_back(plan);
  CHECK(plan_for(latest, table_name, points, n, *plan).ok());

  if (!plan->m_new_cols.empty()) {
    versions.push_back(new schema_t(schema_after(latest, *plan)));
  }

  storage_write_t w;
  w.m_seq = seq;
  w.m_batch = &plan->m_batch;
  w.m_schema = versions[plan->m_batch.m_schema_version - 1];

  return (w);
}

/** Execute a query and read its single integer result.
@return the single integer a query returns, or -1 */
static int64_t query_int(storage_t &st, const std::string &sql) {
  int64_t v = -1;
  CHECK(storage_query_int(st, sql, v).ok());

  return (v);
}

/** Compare the metadata describing two stored database states.
@return true if two states describe the same file contents */
static bool states_equal(const storage_catalog_t &a,
                         const storage_catalog_t &b) {
  if (a.m_flushed_seq != b.m_flushed_seq ||
      a.m_next_table_id != b.m_next_table_id ||
      a.m_tables.size() != b.m_tables.size() ||
      a.m_partitions.size() != b.m_partitions.size()) {
    return (false);
  }

  std::map<std::string, storage_table_t>::const_iterator t;
  for (t = a.m_tables.begin(); t != a.m_tables.end(); ++t) {
    if (b.m_tables.count(t->first) == 0) {
      return (false);
    }

    const storage_table_t &x = t->second;
    const storage_table_t &y = b.m_tables.find(t->first)->second;

    if (x.m_table_id != y.m_table_id ||
        x.m_versions.size() != y.m_versions.size()) {
      return (false);
    }

    for (size_t v = 0; v < x.m_versions.size(); ++v) {
      if (x.m_versions[v].m_cols.size() != y.m_versions[v].m_cols.size()) {
        return (false);
      }
      for (size_t c = 0; c < x.m_versions[v].m_cols.size(); ++c) {
        const col_def_t &p = x.m_versions[v].m_cols[c];
        const col_def_t &q = y.m_versions[v].m_cols[c];
        if (p.m_name != q.m_name || p.m_kind != q.m_kind ||
            p.m_type != q.m_type) {
          return (false);
        }
      }
    }
  }

  std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator p;
  for (p = a.m_partitions.begin(); p != a.m_partitions.end(); ++p) {
    if (b.m_partitions.count(p->first) == 0) {
      return (false);
    }

    const storage_partition_t &q = b.m_partitions.find(p->first)->second;
    if (p->second.m_phys_name != q.m_phys_name ||
        p->second.m_n_cols != q.m_n_cols || p->second.m_rows != q.m_rows) {
      return (false);
    }
  }

  return (true);
}

/** A new file has the internal tables and an empty state. */
static void test_open_new() {
  db_status_t status;

  test_dir_t dir;
  storage_t *st = nullptr;

  status = storage_open(dir.m_path, 1, &st);
  CHECK(status.ok());
  CHECK(status.message().empty());

  const storage_catalog_t &state = storage_catalog(*st);
  CHECK(state.m_flushed_seq == 0 && state.m_next_table_id == 1);
  CHECK(state.m_tables.empty() && state.m_partitions.empty());
  storage_close(st);

  status = storage_open(dir.m_path, 1, &st);
  CHECK(status.ok());
  CHECK(storage_catalog(*st).m_flushed_seq == 0);
  storage_close(st);
}

/** Invalid metadata is rejected before the next write can use table id zero. */
static void test_zero_table_id() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());
  CHECK(duckdb_database_exec(*st->m_duckdb,
                             "UPDATE _pt_state SET next_table_id = 0")
            .ok());
  storage_close(st);
  CHECK(storage_open(dir.m_path, 1, &st).code() == DB_CORRUPTION);
  CHECK(st == nullptr);
  storage_close(st);
}

/** Table id exhaustion rolls back a flush without wrapping the counter;
existing tables remain writable and the catalog can still be reopened. */
static void test_table_id_exhaustion() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());
  const uint64_t exhausted = std::numeric_limits<uint64_t>::max();
  CHECK(duckdb_database_exec(*st->m_duckdb,
                             "UPDATE _pt_state SET next_table_id = " +
                                 std::to_string(exhausted - 1))
            .ok());
  storage_close(st);
  CHECK(storage_open(dir.m_path, 1, &st).ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  storage_write_t writes[] = {catalog_write(cat, 1, "last", &p, 1),
                              catalog_write(cat, 2, "extra", &p, 1)};
  const db_status_t exhausted_status = flush_writes(*st, writes, 2);
  CHECK(exhausted_status.code() == DB_LIMIT_EXCEEDED);
  if (exhausted_status.code() != DB_LIMIT_EXCEEDED) {
    storage_close(st);

    return;
  }
  CHECK(storage_catalog(*st).m_flushed_seq == 0);
  CHECK(storage_catalog(*st).m_next_table_id == exhausted - 1);
  CHECK(storage_catalog(*st).m_tables.empty());
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 0);

  CHECK(flush_writes(*st, writes, 1).ok());
  CHECK(storage_catalog(*st).m_next_table_id == exhausted);
  CHECK(storage_catalog(*st).m_tables.at("last").m_table_id == exhausted - 1);
  storage_write_t existing = catalog_write(cat, 2, "last", &p, 1);
  CHECK(flush_writes(*st, &existing, 1).ok());
  storage_write_t extra = catalog_write(cat, 3, "extra", &p, 1);
  CHECK(flush_writes(*st, &extra, 1).code() == DB_LIMIT_EXCEEDED);
  CHECK(storage_catalog(*st).m_flushed_seq == 2);
  CHECK(storage_catalog(*st).m_next_table_id == exhausted);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 1);
  storage_close(st);

  CHECK(storage_open(dir.m_path, 1, &st).ok());
  CHECK(storage_catalog(*st).m_flushed_seq == 2);
  CHECK(storage_catalog(*st).m_next_table_id == exhausted);
  CHECK(storage_catalog(*st).m_tables.size() == 1);
  const std::string physical =
      storage_catalog(*st).m_partitions.begin()->second.m_phys_name;
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);
  storage_close(st);
}

/** Flushed writes, added columns and exact times survive a reload, which
gives the same state. */
static void test_flush_and_reload() {
  db_status_t reload_status;

  test_dir_t dir;
  storage_t *st = nullptr;
  reload_status = storage_open(dir.m_path, 1, &st);
  CHECK(reload_status.ok());

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
  writes[0] = catalog_write(cat, 1, "cpu", p1, 3);
  writes[1] = catalog_write(cat, 2, "cpu", &p2, 1);
  writes[2] = catalog_write(cat, 3, "mem", &p3, 1);

  /* The first write alone, then the other two. */
  CHECK(flush_writes(*st, writes, 1).ok());
  CHECK(storage_catalog(*st).m_flushed_seq == 1);
  CHECK(flush_writes(*st, writes + 1, 2).ok());

  const storage_catalog_t &state = storage_catalog(*st);
  CHECK(state.m_flushed_seq == 3);
  CHECK(state.m_tables.size() == 2);
  CHECK(state.m_tables.find("cpu")->second.m_versions.size() == 2);
  CHECK(state.m_partitions.size() == 2);

  /* cpu: time, host, usage, cores. The partition was widened for cores;
  the first three rows have NULL there. */
  const std::string cpu = storage_partition_name(1, ut_utc_day(T0));
  const storage_partition_t &part =
      state.m_partitions
          .find(std::make_pair(static_cast<uint64_t>(1), ut_utc_day(T0)))
          ->second;
  CHECK(part.m_phys_name == cpu && part.m_n_cols == 4 && part.m_rows == 4);

  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu) == 4);
  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu + " WHERE c3 IS NULL") ==
        3);
  CHECK(query_int(*st, "SELECT max(c3) FROM " + cpu) == 8);
  CHECK(query_int(*st, "SELECT count(*) FROM " + cpu + " WHERE c1 = 'a'") == 4);
  CHECK(query_int(*st, "SELECT max(_pt_seq) FROM " + cpu) == 2);
  CHECK(query_int(*st, "SELECT max(_pt_row) FROM " + cpu +
                           " WHERE _pt_seq = 1") == 2);

  /* The exact microseconds survive. */
  CHECK(query_int(*st, "SELECT epoch_us(max(c0)) FROM " + cpu) == T0 + 3);

  storage_catalog_t before = state;
  storage_close(st);

  /* Reloading gives the same state, schema versions included. */

  reload_status = storage_open(dir.m_path, 1, &st);
  CHECK(reload_status.ok());
  CHECK(reload_status.message().empty());
  CHECK(states_equal(before, storage_catalog(*st)));

  const std::vector<schema_t> &v =
      storage_catalog(*st).m_tables.find("cpu")->second.m_versions;
  CHECK(v[0].m_cols.size() == 3 && v[1].m_cols.size() == 4);
  CHECK(v[1].m_cols[3].m_name == "cores" &&
        v[1].m_cols[3].m_type == col_type_t::BIGINT);

  storage_close(st);
}

/** One write spanning three UTC days, one before 1970, lands in three
partitions. */
static void test_day_partitions() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());

  /* One write across three UTC days, one of them before 1970. */
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t points[] = {point(-1, nullptr, 0, f, 1),
                             point(0, nullptr, 0, f, 1),
                             point(UT_DAY_US - 1, nullptr, 0, f, 1),
                             point(UT_DAY_US, nullptr, 0, f, 1)};

  test_catalog_t cat;
  storage_write_t w = catalog_write(cat, 1, "m", points, 4);
  CHECK(flush_writes(*st, &w, 1).ok());

  const storage_catalog_t &state = storage_catalog(*st);
  CHECK(state.m_partitions.size() == 3);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_n1") == 1);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_p0") == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_t1_p1") == 1);

  storage_close(st);
}

/** A flush whose sequence numbers do not continue from F is refused. */
static void test_bad_arguments() {
  db_status_t status;

  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);

  /* Sequence numbers must continue from F. */
  storage_write_t w = catalog_write(cat, 2, "m", &p, 1);

  status = flush_writes(*st, &w, 1);
  CHECK(status.code() == DB_INVALID_ARGUMENT);
  CHECK(!status.message().empty());
  status = flush_writes(*st, &w, 0);
  CHECK(status.code() == DB_INVALID_ARGUMENT);

  w.m_seq = 1;
  status = flush_writes(*st, &w, 1);
  CHECK(status.ok());
  status = flush_writes(*st, &w, 1);
  CHECK(status.code() == DB_INVALID_ARGUMENT);

  storage_close(st);
}

/** Snapshot catalog reads share partition decoding with open, without loading
ordinary names; reopening still performs complete physical catalog checks. */
static void test_snapshot_catalog() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());
  test_catalog_t cat;
  ingest_field_t field = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &field, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);
  CHECK(flush_writes(*st, &w, 1).ok());

  duckdb_database_t database;
  database.m_conn = storage_connection(*st);
  CHECK(
      duckdb_database_exec(database, "CREATE TABLE ordinary (v BIGINT)").ok());
  CHECK(duckdb_database_exec(database, "BEGIN TRANSACTION").ok());
  storage_catalog_t snapshot;
  CHECK(storage_read_catalog(database.m_conn, snapshot).ok());
  CHECK(snapshot.m_flushed_seq == 1);
  CHECK(snapshot.m_tables.size() == 1);
  CHECK(snapshot.m_partitions.size() == 1);
  CHECK(snapshot.m_user_tables.empty());
  CHECK(duckdb_database_exec(database, "COMMIT").ok());
  storage_close(st);

  CHECK(storage_open(dir.m_path, 1, &st).ok());
  CHECK(storage_catalog(*st).m_user_tables.count("ordinary") == 1);
  CHECK(storage_catalog(*st).m_partitions.size() == 1);
  storage_close(st);
}

/** An appender constraint failure rolls back every row and catalog change;
the same sequence can be retried after removing the test constraint. */
static void test_appender_failure() {
  db_status_t test_status;

  test_dir_t dir;
  storage_t *st = nullptr;
  test_status = storage_open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  test_catalog_t cat;
  ingest_field_t field = field_int("v", 1);
  ingest_point_t point_value = point(T0, nullptr, 0, &field, 1);
  storage_write_t first = catalog_write(cat, 1, "m", &point_value, 1);
  test_status = flush_writes(*st, &first, 1);
  CHECK(test_status.ok());

  const std::string physical =
      storage_catalog(*st).m_partitions.begin()->second.m_phys_name;
  CHECK(duckdb_database_exec(
            *st->m_duckdb,
            "CREATE UNIQUE INDEX _pt_test_unique ON " + physical + " (c1)")
            .ok());
  storage_write_t next = catalog_write(cat, 2, "m", &point_value, 1);
  test_status = flush_writes(*st, &next, 1);
  CHECK(test_status.code() == DB_ERROR);
  CHECK(!test_status.message().empty());
  CHECK(storage_catalog(*st).m_flushed_seq == 1);
  CHECK(storage_catalog(*st).m_partitions.begin()->second.m_rows == 1);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 1);

  CHECK(duckdb_database_exec(*st->m_duckdb, "DROP INDEX _pt_test_unique").ok());
  test_status = flush_writes(*st, &next, 1);
  CHECK(test_status.ok());
  CHECK(storage_catalog(*st).m_flushed_seq == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);
  storage_close(st);

  test_status = storage_open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  CHECK(storage_catalog(*st).m_flushed_seq == 2);
  CHECK(query_int(*st, "SELECT count(*) FROM " + physical) == 2);
  storage_close(st);
}

/** COMMIT and the best-effort ROLLBACK can both fail. Preserve the first
diagnostic and the unknown-outcome classification. */
static void test_commit_rollback_error() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());
  duckdb_database_t database;
  database.m_conn = storage_connection(*st);

  const db_status_t status = duckdb_database_commit(database);
  CHECK(status.code() == DB_OUTCOME_UNKNOWN);
  CHECK(status.message().find("commit") != std::string::npos);
  CHECK(status.message().find("rollback:") != std::string::npos);
  const std::string original = status.message();
  CHECK(duckdb_database_exec(database, "BEGIN TRANSACTION").ok());
  CHECK(duckdb_database_rollback(database).ok());
  CHECK(status.message() == original);
  storage_close(st);
}

/** An expiry that cannot begin must leave the existing transaction intact. */
static void test_expire_begin_failure() {
  db_status_t test_status;

  test_dir_t dir;
  storage_t *st = nullptr;
  test_status = storage_open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);
  test_status = flush_writes(*st, &w, 1);
  CHECK(test_status.ok());

  CHECK(duckdb_database_exec(*st->m_duckdb, "BEGIN TRANSACTION").ok());
  uint64_t removed = 0;
  test_status = storage_expire(*st, ut_utc_day(T0) + 1, removed);
  CHECK(test_status.code() == DB_ERROR);
  CHECK(removed == 0);
  CHECK(storage_catalog(*st).m_partitions.size() == 1);
  CHECK(test_status.message().find("rollback:") == std::string::npos);
  CHECK(duckdb_database_rollback(*st->m_duckdb).ok());
  storage_close(st);
}

/** A failed rollback retains the operation's diagnostic; a successful one
leaves it intact for the caller. */
static void test_rollback_diagnostic() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());

  duckdb_database_t database;
  database.m_conn = storage_connection(*st);

  const db_status_t cause(DB_ERROR, "original failure");
  const db_status_t failed = duckdb_database_rollback(database, cause);
  CHECK(failed.code() == DB_OUTCOME_UNKNOWN);
  CHECK(failed.message().find("original failure; rollback:") == 0);

  CHECK(duckdb_database_exec(database, "BEGIN TRANSACTION").ok());
  const db_status_t restored = duckdb_database_rollback(database, cause);
  CHECK(restored.code() == DB_ERROR);
  CHECK(restored.message() == "original failure");
  storage_close(st);
}

#ifdef PT_DEBUG
/** A failed flush rollback reports an unknown outcome; closing discards its
still-open transaction, and reopening reads the committed catalog. */
static void test_flush_rollback_failure() {
  db_status_t test_status;

  test_dir_t dir;
  storage_t *st = nullptr;
  test_status = storage_open(dir.m_path, 1, &st);
  CHECK(test_status.ok());

  test_catalog_t cat;
  ingest_field_t f = field_int("v", 1);
  ingest_point_t p = point(T0, nullptr, 0, &f, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);

  storage_dbg_fail_next_flush(*st);
  st->m_duckdb->m_dbg_fail_next_rollback = true;
  test_status = storage_flush_prepare(*st, &w, 1);
  CHECK(test_status.code() == DB_OUTCOME_UNKNOWN);
  CHECK(test_status.message().find("injected flush failure") !=
        std::string::npos);
  CHECK(test_status.message().find("rollback: injected rollback failure") !=
        std::string::npos);
  CHECK(!st->m_prepared);
  CHECK(storage_catalog(*st).m_flushed_seq == 0);
  storage_close(st);

  test_status = storage_open(dir.m_path, 1, &st);
  CHECK(test_status.ok());
  CHECK(storage_catalog(*st).m_flushed_seq == 0);
  CHECK(storage_catalog(*st).m_tables.empty());
  CHECK(storage_catalog(*st).m_partitions.empty());
  storage_close(st);
}

/** A flush failing before commit leaves nothing, and the same write succeeds
afterwards. */
static void test_rollback() {
  test_dir_t dir;
  storage_t *st = nullptr;
  CHECK(storage_open(dir.m_path, 1, &st).ok());

  test_catalog_t cat;
  ingest_field_t f[] = {field_int("v", 1)};
  ingest_point_t p = point(T0, nullptr, 0, f, 1);
  storage_write_t w = catalog_write(cat, 1, "m", &p, 1);

  /* Fails after the table, partition and rows were written. */
  storage_dbg_fail_next_flush(*st);
  CHECK(flush_writes(*st, &w, 1).code() == DB_ERROR);
  CHECK(storage_catalog(*st).m_flushed_seq == 0);

  /* Nothing of it is left: no rows, no partition table, no catalog rows.
  This shows that appender rows belong to the transaction. */
  CHECK(query_int(*st,
                  "SELECT count(*) FROM information_schema.tables "
                  "WHERE table_schema = 'main'") == 4);
  CHECK(query_int(*st, "SELECT count(*) FROM _pt_tables") == 0);

  /* The same write succeeds afterwards. */
  CHECK(flush_writes(*st, &w, 1).ok());
  storage_close(st);

  CHECK(storage_open(dir.m_path, 1, &st).ok());
  CHECK(storage_catalog(*st).m_flushed_seq == 1);
  storage_close(st);
}
#endif /* PT_DEBUG */

/** Rows of write seq in the kill test.
@return number of rows generated for that write */
static uint32_t kill_rows(batch_seq_t seq) {
  return (static_cast<uint32_t>(seq % 5) + 1);
}

/** Child process: flush writes after F, one per flush, until killed. Every
10th write adds a column. Reports each committed F on the pipe. */
static void kill_child(const char *dir, int fd) {
  storage_t *st = nullptr;
  if (!storage_open(dir, 1, &st).ok()) {
    _exit(2);
  }

  test_catalog_t cat;
  catalog_load(cat, storage_catalog(*st));

  for (;;) {
    const batch_seq_t seq = storage_catalog(*st).m_flushed_seq + 1;

    char extra[32];
    snprintf(extra, sizeof(extra), "x%llu",
             static_cast<unsigned long long>(seq));
    ingest_field_t f[] = {field_int("seq", static_cast<int64_t>(seq)),
                          field_int(extra, 1)};
    const uint32_t n_fields = seq % 10 == 0 ? 2 : 1;

    std::vector<ingest_point_t> points;
    for (uint32_t r = 0; r < kill_rows(seq); ++r) {
      points.push_back(point(T0 + static_cast<int64_t>(seq) * 1000 + r, nullptr,
                             0, f, n_fields));
    }

    storage_write_t w = catalog_write(cat, seq, "k", &points[0],
                                      static_cast<uint32_t>(points.size()));

    if (!flush_writes(*st, &w, 1).ok()) {
      _exit(3);
    }

    if (write(fd, &seq, sizeof(seq)) != static_cast<ssize_t>(sizeof(seq))) {
      _exit(4);
    }
  }
}

/** Kill a process while it flushes, over and over: F covers every committed
flush and nothing else is stored. */
static void test_kill_during_flush() {
  db_status_t status;

  test_dir_t dir;
  batch_seq_t acked = 0;

  for (int round = 0; round < 8; ++round) {
    int fds[2];
    CHECK(pipe(fds) == 0);

    const pid_t pid = fork();
    if (pid == 0) {
      close(fds[0]);
      kill_child(dir.m_path, fds[1]);
    }
    close(fds[1]);

    const int wanted = 5 + round * 3;
    int received = 0;
    batch_seq_t seq = 0;

    while (received < wanted && read(fds[0], &seq, sizeof(seq)) ==
                                    static_cast<ssize_t>(sizeof(seq))) {
      acked = seq;
      ++received;
    }

    kill(pid, SIGKILL);
    int child_status = 0;
    waitpid(pid, &child_status, 0);

    while (read(fds[0], &seq, sizeof(seq)) ==
           static_cast<ssize_t>(sizeof(seq))) {
      acked = seq;
    }
    close(fds[0]);
    CHECK(received == wanted);

    /* F covers every committed flush, and the stored rows are exactly
    those of writes 1 .. F. */
    storage_t *st = nullptr;

    status = storage_open(dir.m_path, 1, &st);
    CHECK(status.ok());
    if (st == nullptr) {
      fprintf(stderr, "reopen failed: %s\n", status.message().c_str());

      return;
    }

    const storage_catalog_t &state = storage_catalog(*st);
    CHECK(state.m_flushed_seq >= acked);

    uint64_t expected = 0;
    for (batch_seq_t s = 1; s <= state.m_flushed_seq; ++s) {
      expected += kill_rows(s);
    }

    uint64_t counted = 0;
    uint64_t actual = 0;
    std::map<std::pair<uint64_t, int64_t>, storage_partition_t>::const_iterator
        it;
    for (it = state.m_partitions.begin(); it != state.m_partitions.end();
         ++it) {
      counted += it->second.m_rows;
      actual += static_cast<uint64_t>(
          query_int(*st, "SELECT count(*) FROM " + it->second.m_phys_name));
    }

    CHECK(counted == expected);
    CHECK(actual == expected);
    storage_close(st);
  }

  printf("kill test: %llu flushes committed across 8 kills\n",
         static_cast<unsigned long long>(acked));
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_open_new();
  test_zero_table_id();
  test_table_id_exhaustion();
  test_flush_and_reload();
  test_day_partitions();
  test_bad_arguments();
  test_snapshot_catalog();
  test_appender_failure();
  test_commit_rollback_error();
  test_expire_begin_failure();
  test_rollback_diagnostic();
#ifdef PT_DEBUG
  test_flush_rollback_failure();
  test_rollback();
#endif /* PT_DEBUG */
  test_kill_during_flush();

  return (test_result("storage-t"));
}
