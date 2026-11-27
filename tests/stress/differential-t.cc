/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * differential-t.cc
 *    Differential test of queries: random writes go to PulseTen and to plain
 *    tables of an in-memory DuckDB, and random queries must return the same
 *    rows from both. Flushes and reopens move data between the memtable and
 *    stored partitions in between.
 *
 * Comparison rules:
 *   - Each query runs as SELECT COLUMNS(*)::VARCHAR FROM (q) ORDER BY ALL, so
 *     both sides return sorted text rows. A LIMIT inside q follows ORDER BY
 *     ALL, so it selects the same rows on both sides.
 *   - DOUBLE values are multiples of 1/4 far below 2^50, so sums and averages
 *     are exact in any order and need no tolerance.
 *   - A column added later (tag zone, field extra) is NULL in older rows on
 *     both sides; queries name it only after it exists in PulseTen.
 *
 * Covers: src/query/query.cc, src/query/query_duckdb.cc,
 *         src/query/query_memtable.cc, src/db/db.cc
 *
 *-------------------------------------------------------------------------
 */

#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <random>
#include <string>
#include <vector>

#include <duckdb.h>

#include "db.h"
#include "ingest.h"
#include "test_db.h"
#include "test_fixture.h"
#include "test_point.h"
#include "test_stress.h"
#include "test_util.h"
#include "ut.h"

/** Days the data spans; one partition each. */
static const int DIFF_DAYS = 40;

/** Host tag values. */
static const char *const diff_hosts[] = {"h0", "h1", "h2", "h3", "h4"};

/** Zone tag values, written only after the schema grows. */
static const char *const diff_zones[] = {"z0", "z1", "z2"};

/** Note field values, including one that needs SQL quoting. */
static const char *const diff_notes[] = {"a", "b", "c", "dd", "it's"};

/** One generated row; absent columns are NULL. */
struct diff_row_t {
  /** UTC time in microseconds. */
  int64_t m_time_us{0};

  /** Index into diff_hosts. */
  int m_host{0};

  /** Index into diff_zones, or -1. */
  int m_zone{-1};

  /** BIGINT field v, always present. */
  int64_t m_v{0};

  /** DOUBLE field x, a multiple of 1/4, if m_has_x. */
  double m_x{0};

  /** Field x is present. */
  bool m_has_x{false};

  /** BOOLEAN field ok: 0 false, 1 true, -1 absent. */
  int m_ok{-1};

  /** Index into diff_notes, or -1. */
  int m_note{-1};

  /** DOUBLE field extra, written only after the schema grows. */
  double m_extra{0};

  /** Field extra is present. */
  bool m_has_extra{false};
};

/** Rows of one result as text; NULL is a value no cast produces. */
typedef std::vector<std::vector<std::string>> diff_rows_t;

/** Stands for NULL in diff_rows_t. */
static const char *const DIFF_NULL = "\x01NULL";

/** State of one differential run. */
class diff_run_t {
 public:
  /** Prepare a run; open() must succeed before step(). */
  explicit diff_run_t(const test_stress_t &settings)
      : m_settings(settings), m_rng(settings.m_seed) {}

  /** Close both databases. */
  ~diff_run_t() {
    if (m_db != nullptr) {
      db_close(m_db);
    }

    if (m_ref_conn != nullptr) {
      duckdb_disconnect(&m_ref_conn);
    }

    if (m_ref != nullptr) {
      duckdb_close(&m_ref);
    }
  }

  /** Open PulseTen and the reference with empty tables.
  @return true on success */
  bool open();

  /** Run queries until the configured number has passed or one differs.
  @return true if every query matched */
  bool run();

  /** Writes, flushes and reopens done so far, for the summary. */
  void summary() const {
    printf("differential-t: seed %" PRIu64 ", %" PRIu64 " queries, %" PRIu64
           " write batches, %" PRIu64 " rows, %" PRIu64 " flushes, %" PRIu64
           " reopens\n",
           m_settings.m_seed, m_queries, m_batches, m_rows, m_flushes,
           m_reopens);
    printf("  %" PRIu64
           " queries returned rows; seconds: writes %.1f, "
           "flushes %.1f, reopens %.1f, PulseTen queries %.1f, reference "
           "queries %.1f\n",
           m_nonempty, m_write_s, m_flush_s, m_reopen_s, m_pulseten_s,
           m_reference_s);
  }

 private:
  /** Return a random integer in [0, n). */
  int pick(int n) {
    return (static_cast<int>(m_rng() % static_cast<uint64_t>(n)));
  }

  /** Return true with probability percent / 100. */
  bool chance(int percent) { return (pick(100) < percent); }

  /** How write_batch() chooses its table. */
  enum class target_t {
    /** cpu or mem at random. */
    ANY,

    /** mem. */
    MEM,

    /** cpu. */
    CPU,

    /** cpu, with zone and extra in the first row. */
    GROW_CPU
  };

  /** Write one random batch to both sides.
  @return true on success */
  bool write_batch(target_t target = target_t::ANY);

  /** Flush or reopen PulseTen.
  @return true on success */
  bool flush();
  bool reopen();

  /** Generate one query and compare both results.
  @return true if they match */
  bool query();

  /** Run a query on one side.
  @return true on success */
  bool run_pulseten(const std::string &sql, diff_rows_t &rows,
                    std::string &error);

  bool run_reference(const std::string &sql, diff_rows_t &rows,
                     std::string &error);

  /** Report a mismatch with what is needed to repeat it. */
  void report(const std::string &sql, const diff_rows_t &got,
              const diff_rows_t &expected, const std::string &error);

  /** Build a random predicate over table t; never empty. */
  std::string predicate(const char *t, bool narrow);

  /** Format microseconds as a TIMESTAMP literal. */
  static std::string timestamp(int64_t us);

  /** Run reference DDL or inserts.
  @return true on success */
  bool ref_exec(const std::string &sql);

  /** Run settings. */
  const test_stress_t m_settings;

  /** Every random choice. */
  std::mt19937_64 m_rng;

  /** Database directory. */
  test_dir_t m_dir;

  /** PulseTen database. */
  db *m_db{nullptr};

  /** Reference database and its connection. */
  duckdb_database m_ref{nullptr};
  duckdb_connection m_ref_conn{nullptr};

  /** Newest day written so far; late batches go to earlier days. */
  int m_day{0};

  /** cpu has tag zone and field extra in PulseTen. */
  bool m_grown{false};

  /** Counters for the summary. */
  uint64_t m_queries{0};
  uint64_t m_batches{0};
  uint64_t m_rows{0};
  uint64_t m_flushes{0};
  uint64_t m_reopens{0};

  /** Queries whose result had at least one row. */
  uint64_t m_nonempty{0};

  /** Time spent per kind of step, in seconds. */
  double m_write_s{0};
  double m_flush_s{0};
  double m_reopen_s{0};
  double m_pulseten_s{0};
  double m_reference_s{0};
};

/** Seconds since an arbitrary fixed point. */
static double diff_now() {
  return (std::chrono::duration<double>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count());
}

/** Both tables exist on the reference side from the start with every column;
PulseTen creates them on the first write. */
bool diff_run_t::open() {
  const db::options_t options = daily_partition_options();
  if (!db_open(m_dir.m_path, &options, &m_db).ok()) {
    return (false);
  }

  if (duckdb_open(nullptr, &m_ref) != DuckDBSuccess ||
      duckdb_connect(m_ref, &m_ref_conn) != DuckDBSuccess) {
    return (false);
  }

  for (const char *t : {"cpu", "mem"}) {
    if (!ref_exec(std::string("CREATE TABLE ") + t +
                  " (time TIMESTAMP, host VARCHAR, zone VARCHAR, v BIGINT, "
                  "x DOUBLE, ok BOOLEAN, note VARCHAR, extra DOUBLE)")) {
      return (false);
    }
  }

  return (true);
}

/** Run reference SQL whose result is not needed. */
bool diff_run_t::ref_exec(const std::string &sql) {
  duckdb_result result;
  const bool ok =
      duckdb_query(m_ref_conn, sql.c_str(), &result) == DuckDBSuccess;

  if (!ok) {
    fprintf(stderr, "reference failed: %s\n  %s\n",
            duckdb_result_error(&result), sql.c_str());
  }

  duckdb_destroy_result(&result);

  return (ok);
}

/** Format with second and microsecond parts, as DuckDB parses them. */
std::string diff_run_t::timestamp(int64_t us) {
  const time_t seconds = static_cast<time_t>(us / 1000000);
  struct tm civil;
  gmtime_r(&seconds, &civil);

  char text[64];
  snprintf(text, sizeof(text), "TIMESTAMP '%04d-%02d-%02d %02d:%02d:%02d.%06d'",
           civil.tm_year + 1900, civil.tm_mon + 1, civil.tm_mday, civil.tm_hour,
           civil.tm_min, civil.tm_sec, static_cast<int>(us % 1000000));

  return (text);
}

/** One batch for one table, within one day. Most batches go to the newest
day; some go back to an earlier, possibly already stored, day. */
bool diff_run_t::write_batch(target_t target) {
  const bool grow = target == target_t::GROW_CPU;
  const bool cpu =
      target == target_t::ANY ? chance(60) : target != target_t::MEM;

  const char *table = cpu ? "cpu" : "mem";

  if (m_day < DIFF_DAYS - 1 && chance(15)) {
    ++m_day;
  }

  const int day = chance(20) ? pick(m_day + 1) : m_day;
  const int n = 1 + pick(16);

  std::vector<diff_row_t> rows(static_cast<size_t>(n));
  for (diff_row_t &row : rows) {
    /* Whole seconds make equal times, and so ties, likely enough. */
    row.m_time_us = TEST_BASE_TIME_US + day * UT_DAY_US +
                    static_cast<int64_t>(pick(86400)) * 1000000;
    row.m_host = pick(5);
    row.m_v = pick(2001) - 1000;
    row.m_has_x = chance(80);
    row.m_x = (pick(8001) - 4000) / 4.0;
    row.m_ok = chance(85) ? pick(2) : -1;
    row.m_note = chance(70) ? pick(5) : -1;

    if (cpu && m_grown) {
      row.m_zone = chance(75) ? pick(3) : -1;
      row.m_has_extra = chance(60);
      row.m_extra = (pick(400) - 200) / 4.0;
    }
  }

  if (grow) {
    rows[0].m_zone = pick(3);
    rows[0].m_has_extra = true;
  }

  /* PulseTen: tags and fields per point. */
  std::vector<std::vector<ingest_tag_t>> tags(rows.size());
  std::vector<std::vector<ingest_field_t>> fields(rows.size());
  std::vector<ingest_point_t> points;

  for (size_t i = 0; i < rows.size(); ++i) {
    const diff_row_t &row = rows[i];
    tags[i].push_back(tag("host", diff_hosts[row.m_host]));

    if (row.m_zone >= 0) {
      tags[i].push_back(tag("zone", diff_zones[row.m_zone]));
    }

    fields[i].push_back(field_int("v", row.m_v));

    if (row.m_has_x) {
      fields[i].push_back(field_double("x", row.m_x));
    }

    if (row.m_ok >= 0) {
      fields[i].push_back(field_bool("ok", row.m_ok == 1));
    }

    if (row.m_note >= 0) {
      fields[i].push_back(field_str("note", diff_notes[row.m_note]));
    }

    if (row.m_has_extra) {
      fields[i].push_back(field_double("extra", row.m_extra));
    }

    points.push_back(point(
        row.m_time_us, tags[i].data(), static_cast<uint32_t>(tags[i].size()),
        fields[i].data(), static_cast<uint32_t>(fields[i].size())));
  }

  const db_status_t written = m_db->write(table, strlen(table), points.data(),
                                          static_cast<uint32_t>(points.size()));

  if (!written.ok()) {
    fprintf(stderr, "write failed: %s\n", written.message().c_str());

    return (false);
  }

  /* Reference: one INSERT with literal values. */
  std::string sql = std::string("INSERT INTO ") + table + " VALUES ";
  char number[64];

  for (size_t i = 0; i < rows.size(); ++i) {
    const diff_row_t &row = rows[i];
    sql += i == 0 ? "(" : ", (";
    sql += timestamp(row.m_time_us) + ", '" + diff_hosts[row.m_host] + "', ";
    sql += row.m_zone >= 0 ? std::string("'") + diff_zones[row.m_zone] + "'"
                           : "NULL";
    sql += ", " + std::to_string(row.m_v) + ", ";
    snprintf(number, sizeof(number), "%.17g", row.m_x);
    sql += row.m_has_x ? number : "NULL";
    sql += row.m_ok < 0 ? ", NULL" : row.m_ok == 1 ? ", true" : ", false";
    sql += row.m_note < 0
               ? ", NULL"
               : std::string(", '") +
                     (row.m_note == 4 ? "it''s" : diff_notes[row.m_note]) + "'";
    snprintf(number, sizeof(number), "%.17g", row.m_extra);
    sql += row.m_has_extra ? std::string(", ") + number : ", NULL";
    sql += ")";
  }

  ++m_batches;
  m_rows += rows.size();

  return (ref_exec(sql));
}

/** Store every visible write; later writes start a new memtable. */
bool diff_run_t::flush() {
  db::flush_info_t info;
  const db_status_t flushed = m_db->flush(info);
  if (!flushed.ok()) {
    fprintf(stderr, "flush failed: %s\n", flushed.message().c_str());

    return (false);
  }

  ++m_flushes;

  return (true);
}

/** Close and reopen, replaying unflushed writes from the WAL. */
bool diff_run_t::reopen() {
  db_close(m_db);
  m_db = nullptr;

  const db::options_t options = daily_partition_options();
  const db_status_t opened = db_open(m_dir.m_path, &options, &m_db);
  if (!opened.ok()) {
    fprintf(stderr, "reopen failed: %s\n", opened.message().c_str());

    return (false);
  }

  ++m_reopens;

  return (true);
}

/** A time condition in one of several forms, a tag condition and a field
condition, each optional; narrow keeps the time range within three days. */
std::string diff_run_t::predicate(const char *t, bool narrow) {
  std::vector<std::string> parts;
  const std::string col = std::string(t) + ".";

  const int span = narrow ? 1 + pick(3) : 1 + pick(DIFF_DAYS);
  const int64_t lo = TEST_BASE_TIME_US +
                     static_cast<int64_t>(pick(DIFF_DAYS)) * UT_DAY_US +
                     static_cast<int64_t>(pick(86400)) * 1000000;

  const int64_t hi = lo + static_cast<int64_t>(span) * UT_DAY_US -
                     static_cast<int64_t>(pick(86400)) * 1000000;

  switch (narrow ? pick(4) : pick(6)) {
    case 0:
      parts.push_back(col + "time >= " + timestamp(lo) + " AND " + col +
                      "time < " + timestamp(hi));
      break;
    case 1:
      parts.push_back(col + "time BETWEEN " + timestamp(lo) + " AND " +
                      timestamp(hi));
      break;
    case 2:
      parts.push_back("epoch_us(" + col + "time) >= " + std::to_string(lo) +
                      " AND epoch_us(" + col + "time) < " + std::to_string(hi));
      break;
    case 3:
      parts.push_back("date_trunc('day', " + col + "time) = CAST(" +
                      timestamp(lo) + " AS DATE)");
      break;
    case 4:
      parts.push_back(col + "time > " + timestamp(lo));
      break;
    default:
      break;
  }

  switch (pick(5)) {
    case 0:
      parts.push_back(col + "host = '" + diff_hosts[pick(5)] + "'");
      break;
    case 1:
      parts.push_back(col + "host IN ('" + diff_hosts[pick(5)] + "', '" +
                      diff_hosts[pick(5)] + "')");
      break;
    case 2:
      parts.push_back(col + "host <> '" + diff_hosts[pick(5)] + "'");
      break;
    default:
      break;
  }

  switch (pick(6)) {
    case 0:
      parts.push_back(col + "v > " + std::to_string(pick(2001) - 1000));
      break;
    case 1:
      parts.push_back(col + "x <= " + std::to_string(pick(2001) - 1000));
      break;
    case 2:
      parts.push_back(col + "ok");
      break;
    case 3:
      parts.push_back(col + "note IS NULL");
      break;
    default:
      break;
  }

  if (parts.empty()) {
    return ("true");
  }

  std::string text = parts[0];

  for (size_t i = 1; i < parts.size(); ++i) {
    text += " AND " + parts[i];
  }

  return (text);
}

/** Read a PulseTen result as text rows. */
bool diff_run_t::run_pulseten(const std::string &sql, diff_rows_t &rows,
                              std::string &error) {
  test_query_t result;
  const db_status_t status = query_all(*m_db, sql, result);
  if (!status.ok()) {
    error = "PulseTen: " + status.message();

    return (false);
  }

  rows.clear();

  for (const std::vector<query_value_t> &row : result.m_rows) {
    std::vector<std::string> text;

    for (const query_value_t &value : row) {
      text.push_back(value.m_is_null ? DIFF_NULL : value.m_string);
    }

    rows.push_back(std::move(text));
  }

  return (true);
}

/** Read a reference result as text rows. */
bool diff_run_t::run_reference(const std::string &sql, diff_rows_t &rows,
                               std::string &error) {
  duckdb_result result;
  if (duckdb_query(m_ref_conn, sql.c_str(), &result) != DuckDBSuccess) {
    error = std::string("reference: ") + duckdb_result_error(&result);
    duckdb_destroy_result(&result);

    return (false);
  }

  rows.clear();

  const idx_t columns = duckdb_column_count(&result);
  const idx_t count = duckdb_row_count(&result);
  for (idx_t r = 0; r < count; ++r) {
    std::vector<std::string> text;

    for (idx_t c = 0; c < columns; ++c) {
      if (duckdb_value_is_null(&result, c, r)) {
        text.push_back(DIFF_NULL);
        continue;
      }

      char *value = duckdb_value_varchar(&result, c, r);
      text.push_back(value);
      duckdb_free(value);
    }

    rows.push_back(std::move(text));
  }

  duckdb_destroy_result(&result);

  return (true);
}

/** Pick a query shape, fill in tables and predicates, and compare. */
bool diff_run_t::query() {
  const char *t = chance(60) ? "cpu" : "mem";
  const char *other = t[0] == 'c' ? "mem" : "cpu";
  const int shapes = m_grown ? 13 : 12;
  const int shape = pick(shapes);

  std::string q;

  switch (shape) {
    case 0:
      q = std::string(
              "SELECT count(*), count(x), count(note), sum(v), min(v), "
              "max(v), sum(x), avg(x), avg(v) FROM ") +
          t + " WHERE " + predicate(t, false);
      break;
    case 1:
      q = std::string("SELECT host, count(*), sum(v), min(x), max(x) FROM ") +
          t + " WHERE " + predicate(t, false) + " GROUP BY host";
      break;
    case 2:
      q = std::string(
              "SELECT time_bucket(INTERVAL '1 day', time) AS d, "
              "count(*), sum(x) FROM ") +
          t + " WHERE " + predicate(t, false) + " GROUP BY d";
      break;
    case 3:
      q = std::string("SELECT time, host, v, x, ok, note FROM ") + t +
          " WHERE " + predicate(t, false) + " ORDER BY ALL LIMIT " +
          std::to_string(1 + pick(60));
      break;
    case 4:
      q = std::string("SELECT ok, count(*), min(time), max(time) FROM ") + t +
          " WHERE " + predicate(t, false) + " GROUP BY ok";
      break;
    case 5:
      q = std::string("SELECT ") + t + ".host, count(*), sum(" + other +
          ".v) FROM " + t + " JOIN " + other + " ON " + t + ".host = " + other +
          ".host AND date_trunc('hour', " + t + ".time) = date_trunc('hour', " +
          other + ".time) WHERE " + predicate(t, false) + " GROUP BY " + t +
          ".host";
      break;
    case 6:
      q = std::string("SELECT count(*) FROM ") + t + " WHERE " +
          predicate(t, false) + " AND v > (SELECT avg(v) FROM " + other +
          " WHERE " + predicate(other, false) + ")";
      break;
    case 7:
      q = std::string("SELECT host, max(time) FROM ") + t + " WHERE " +
          predicate(t, false) + " GROUP BY host HAVING count(*) > " +
          std::to_string(pick(50));
      break;
    case 8:
      q = std::string("SELECT host, v FROM ") + t + " WHERE " +
          predicate(t, true) + " UNION ALL SELECT host, v FROM " + other +
          " WHERE " + predicate(other, true);
      break;
    case 9:
      q = std::string(
              "SELECT count(DISTINCT host), count(DISTINCT note), "
              "count(DISTINCT ok) FROM ") +
          t + " WHERE " + predicate(t, false);
      break;
    case 10:
      q = std::string(
              "SELECT host, time, v, row_number() OVER (PARTITION BY "
              "host ORDER BY time, v, x, ok, note) FROM ") +
          t + " WHERE " + predicate(t, true);
      break;
    case 11:
      q = std::string("WITH w AS (SELECT host, v, x FROM ") + t + " WHERE " +
          predicate(t, false) +
          ") SELECT host, count(*), sum(v) FROM w GROUP BY host";
      break;
    default:
      q = std::string(
              "SELECT zone, count(*), count(extra), sum(extra), "
              "max(x) FROM cpu WHERE ") +
          predicate("cpu", false) + " GROUP BY zone";
      break;
  }

  const std::string sql =
      "SELECT COLUMNS(*)::VARCHAR FROM (" + q + ") ORDER BY ALL";

  diff_rows_t got;
  diff_rows_t expected;
  std::string error;
  ++m_queries;

  const double start = diff_now();
  const bool pulseten_ok = run_pulseten(sql, got, error);
  const double middle = diff_now();
  const bool reference_ok = pulseten_ok && run_reference(sql, expected, error);
  m_pulseten_s += middle - start;
  m_reference_s += diff_now() - middle;

  if (!reference_ok) {
    report(sql, got, expected, error);

    return (false);
  }

  if (!expected.empty() && !(expected.size() == 1 && expected[0].size() > 0 &&
                             expected[0][0] == "0")) {
    ++m_nonempty;
  }

  if (got != expected) {
    report(sql, got, expected, "results differ");

    return (false);
  }

  return (true);
}

/** Print the query, both results around the first difference and the command
that repeats the run. */
void diff_run_t::report(const std::string &sql, const diff_rows_t &got,
                        const diff_rows_t &expected, const std::string &error) {
  fprintf(stderr, "query %" PRIu64 ": %s\n  %s\n", m_queries, error.c_str(),
          sql.c_str());
  fprintf(stderr, "  PulseTen %zu rows, reference %zu rows\n", got.size(),
          expected.size());

  size_t first = 0;
  while (first < got.size() && first < expected.size() &&
         got[first] == expected[first]) {
    ++first;
  }

  auto print = [](const char *side, const diff_rows_t &rows, size_t at) {
    if (at >= rows.size()) {
      fprintf(stderr, "  %s row %zu: none\n", side, at);

      return;
    }

    std::string text;

    for (const std::string &value : rows[at]) {
      text += (text.empty() ? "" : " | ") +
              (value == DIFF_NULL ? std::string("NULL") : value);
    }

    fprintf(stderr, "  %s row %zu: %s\n", side, at, text.c_str());
  };

  if (error == "results differ") {
    print("PulseTen ", got, first);
    print("reference", expected, first);
  }

  test_stress_repeat("differential-t", "random_queries", m_settings);
  summary();

  return;
}

/** Interleave writes, flushes, reopens and queries; the schema of cpu grows
after a third of the queries. */
bool diff_run_t::run() {
  /* Both tables exist in PulseTen before the first query. */
  if (!write_batch(target_t::CPU) || !write_batch(target_t::MEM)) {
    return (false);
  }

  while (m_queries < m_settings.m_steps) {
    if (!m_grown && m_queries >= m_settings.m_steps / 3) {
      m_grown = true;

      /* Add zone and extra to cpu in PulseTen before any query names them. */
      if (!write_batch(target_t::GROW_CPU)) {
        return (false);
      }
    }

    const int action = pick(100);
    bool ok = true;
    const double start = diff_now();

    if (action < 50) {
      ok = write_batch();
      m_write_s += diff_now() - start;
    } else if (action < 53) {
      ok = flush();
      m_flush_s += diff_now() - start;
    } else if (action < 54) {
      ok = reopen();
      m_reopen_s += diff_now() - start;
    } else {
      ok = query();
    }

    if (!ok) {
      return (false);
    }
  }

  summary();

  return (true);
}

/** Compare a seeded run of random queries against plain DuckDB tables. */
static void test_random_queries() {
  const test_stress_t settings = test_stress_settings(1, 3000);
  diff_run_t run(settings);
  REQUIRE(run.open());
  CHECK(run.run());

  return;
}

/** Run the differential query test. */
int main(int argc, char **argv) {
  static const test_case_t cases[] = {
      {"random_queries", test_random_queries},
  };

  return (test_main("differential-t", argc, argv, cases));
}
