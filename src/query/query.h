/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query.h
 *    SQL over stored and unflushed writes, through DuckDB.
 *
 * A query sees one snapshot: the stored partitions as of F, read in a
 * DuckDB transaction that started at F, and the memtable batches after F.
 * When DuckDB binds a table name it does not know, a replacement scan turns
 * it into a subquery over the native partition tables and a scan of the
 * memtable batches:
 *
 *   SELECT c0 AS "time", c1 AS "host", ... FROM _pt_t1_p20454
 *   WHERE _pt_seq <= F
 *   UNION ALL ...
 *   UNION ALL SELECT "time", "host", ... FROM _pt_memtable_scan(<token>, 'cpu')
 *
 * DuckDB's own optimizer and executor run the result: filters and
 * projections reach the partition scans, which skip data with their zone
 * maps. A partition lacking columns added later supplies NULLs.
 *
 * Before the query runs, simple conditions on "time" bound the times it
 * may read from each table, and only the partitions overlapping those
 * times are put into the subquery. The bounds are conservative: they may
 * keep a partition the query does not need, never drop one it does. The
 * WHERE clause itself is still applied by DuckDB.
 *
 * A query accepts one SELECT. It may name managed tables of the snapshot,
 * user tables visible in its DuckDB transaction,
 * common table expressions and subqueries, but no table functions, no
 * qualified names and no internal or system objects.
 *
 *-------------------------------------------------------------------------
 */

#ifndef query_h
#define query_h

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include <duckdb.h>

#include "batch.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_noncopyable.h"

/** Most rows one fetch returns: one DuckDB vector. */
#define QUERY_CHUNK_ROWS 2048

/** Times, in microseconds, a query may read from a table: [lo, hi].
Empty when lo > hi. */
struct query_range_t {
  /** Earliest time, inclusive. */
  int64_t m_lo{INT64_MIN};

  /** Latest time, inclusive. */
  int64_t m_hi{INT64_MAX};
};

/** Time ranges by normalized table name; an absent table is unbounded. */
typedef std::map<std::string, query_range_t> query_ranges_t;

/** A table as one query sees it. */
struct query_table_t {
  /** Logical columns visible in the snapshot. */
  schema_t m_schema;

  /** Stored partitions. */
  std::vector<storage_partition_t> m_partitions;

  /** Unflushed batches, in sequence order. */
  std::vector<const batch_t *> m_batches;
};

/** What one query sees. */
struct query_snapshot_t {
  /** Writes up to here are read from the partitions, later ones from the
  batches. */
  batch_seq_t m_flushed_seq{0};

  /** Days covered by one stored partition. */
  uint32_t m_partition_duration_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Tables by normalized name. */
  std::map<std::string, query_table_t> m_tables;
};

/** One value of a result. */
struct query_value_t {
  /** The value is NULL; the members below are then unused. */
  bool m_is_null{true};

  /** BOOLEAN. */
  bool m_b{false};

  /** BIGINT, and TIMESTAMP in microseconds. */
  int64_t m_i{0};

  /** DOUBLE. */
  double m_d{0};

  /** VARCHAR. */
  std::string m_s;
};

/** One column of a result. */
struct query_column_t {
  /** Name as DuckDB gives it. */
  std::string m_name;

  /** Type the values are returned as. */
  col_type_t m_type{col_type_t::BIGINT};
};

/** Rows of a result: rows[r][c] is the value of column c in row r. */
typedef std::vector<std::vector<query_value_t>> query_rows_t;

/** Internal conversion rules for result columns. */
class query_reader;

/** Install native query hooks on the database before background work starts.
The caller borrows the storage connection exclusively during opening. */
db_status_t query_prepare_database(duckdb_connection conn);

/** Analyze a SELECT: validate it and collect time ranges and base-table names.
Candidates may include CTE names; the database locks only time-series sources.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
db_status_t query_analyze(duckdb_connection conn, const std::string &sql,
                          query_ranges_t &ranges,
                          std::vector<std::string> &tables);

/** Cancellation of one query open. The caller keeps this object alive until
opening returns and no concurrent request() remains. A request is permanent. */
class query_cancel final : private ut_noncopyable {
 public:
  /** Request cancellation; interrupt the attached connection if any. */
  void request();

  /** Read the cancellation state while waiting for an engine lock.
  @return true after request() has been called */
  bool requested() const { return (m_requested.load()); }

  /** Attach one borrowed connection; only one query may use this object. */
  void attach(duckdb_connection conn);

  /** Stop borrowing the connection before it can be disconnected. */
  void detach();

 private:
  /** Allows lock waiters to observe cancellation without taking a mutex. */
  std::atomic<bool> m_requested{false};

  /** Serializes interrupt with connection attachment and detachment. */
  std::mutex m_mutex;

  /** Connection borrowed only while the query is opening. */
  duckdb_connection m_conn{nullptr};
};

/** Owns prepared and streaming execution resources; borrows its connection.
The connection and snapshot must outlive the cursor. */
class query_cursor final : private ut_noncopyable {
 public:
  /** Construct an empty cursor. */
  query_cursor();

  /** Stop execution before releasing its snapshot registration. */
  ~query_cursor();

  /** Allocate and open a checked SELECT in the borrowed snapshot connection. */
  static db_status_t open(duckdb_connection conn,
                          const query_snapshot_t &snapshot,
                          const std::string &sql, bool explain,
                          const query_ranges_t &ranges, query_cursor **out);

  /** Read at most QUERY_CHUNK_ROWS; every terminal error remains sticky. */
  db_status_t fetch(query_rows_t &rows);

  /** Request interruption concurrently with fetch, never with close. */
  void interrupt();

  /** Borrow result metadata fixed while opening. */
  const std::vector<query_column_t> &columns() const;

  /** Destroy an allocated cursor; NULL is accepted. */
  static void destroy(query_cursor *cursor);

 private:
  /** Release query resources; safe after a partial open. */
  void close();

  /** Configure the prepared statement result columns. */
  db_status_t open_columns();

  /** Fill a new cursor; the factory destroys it on failure. */
  db_status_t open_low(duckdb_connection conn, const query_snapshot_t &snapshot,
                       const std::string &sql, bool explain,
                       const query_ranges_t &ranges);

  /** Convert one native data chunk to output rows. */
  db_status_t read_chunk(duckdb_data_chunk chunk, query_rows_t &rows);

  /** End execution and retain its error, including interruption. */
  db_status_t end_failed();

  /** Registry token; 0 when not registered. */
  uint64_t m_token{0};

  /** Times each table may be read at; the registry points here. */
  query_ranges_t m_ranges;

  /** Connection the statement runs on. */
  duckdb_connection m_conn{nullptr};

  /** The bound and planned statement. */
  duckdb_prepared_statement m_prepared{nullptr};

  /** Pending execution; the first fetch consumes and releases this handle. */
  duckdb_pending_result m_pending{nullptr};

  /** Empty until execution starts; always safe to destroy. */
  duckdb_result m_result{};

  /** All rows were read, or the query failed. */
  bool m_done{false};

  /** Set by interrupt(), from any thread. */
  std::atomic<bool> m_interrupt_requested{false};

  /** Terminal result; success means normal EOF once m_done is true. */
  db_status_t m_terminal_status;

  /** Result columns, from the prepared statement. */
  std::vector<query_column_t> m_columns;

  /** How to read each result column. */
  std::vector<query_reader> m_readers;
};

#endif /* query_h */
