/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query.h
 *    Snapshot queries over time-series data, and checked SQL execution,
 *    through DuckDB.
 *
 * A query sees one snapshot: the stored partitions as of F, read in a
 * DuckDB transaction that started at F, and the memtable batches after F.
 * When DuckDB binds a table name it does not know, a replacement scan turns
 * it into a subquery over the native partition tables and a scan of the
 * memtable batches:
 *
 *   SELECT c0 AS "time", c1 AS "host", ... FROM _pt_t1_p490896
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
 * A query accepts one SELECT. It may name time-series tables of the
 * snapshot, unqualified or as main.name, SQL tables, qualified or not,
 * common table expressions and subqueries, but no table functions, SHOW
 * sources, or internal or system objects.
 *
 * query_command runs SQL for exec. It checks parsed object names
 * before each statement: SQL cannot reference internal objects, nor create,
 * replace or rename a time-series name or the main schema.
 *
 *-------------------------------------------------------------------------
 */

#ifndef query_h
#define query_h

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <duckdb.h>

#include "db_batch.h"
#include "db_schema.h"
#include "memtable.h"
#include "store.h"
#include "ut.h"

/** Most rows one fetch returns: one DuckDB vector. */
#define QUERY_MAX_CHUNK_ROWS 2048

/** Inclusive time range a query may read from a table.
Empty when m_start_time_us > m_end_time_us. */
struct query_range_t {
  /** Earliest timestamp in microseconds, inclusive. */
  int64_t m_start_time_us{INT64_MIN};

  /** Latest timestamp in microseconds, inclusive. */
  int64_t m_end_time_us{INT64_MAX};
};

/** Time ranges by normalized table name; an absent table is unbounded. */
typedef std::map<std::string, query_range_t> query_ranges_t;

/** A table as one query sees it. */
struct query_table_t {
  /** Fixed hours covered by this table's stored partitions. */
  uint32_t m_partition_duration_hours{STORE_DEFAULT_PARTITION_HOURS};

  /** Retention cutoff fixed for this query; INT64_MIN means unlimited. */
  int64_t m_cutoff_us{INT64_MIN};

  /** Logical columns visible in the snapshot. */
  schema_t m_schema;

  /** Stored partitions. */
  std::vector<store_partition_t> m_partitions;

  /** Pinned memtable interval; NULL if this table has no in-memory rows. */
  const memtable::snapshot_t *m_memtable_snapshot{nullptr};
};

/** Stored and in-memory data visible to one query. */
struct query_snapshot_t {
  /** Wall-clock time shared by all table retention cutoffs. */
  int64_t m_now_us{0};

  /** Writes at or below this sequence come from the partitions, later ones
  from the batches. */
  batch_seq_t m_flushed_seq{0};

  /** Tables by normalized name. */
  std::map<std::string, query_table_t> m_tables;
};

/** One value of a result. */
struct query_value_t {
  /** The value is NULL; the members below are then unused. */
  bool m_is_null{true};

  /** BOOLEAN. */
  bool m_boolean{false};

  /** BIGINT, and TIMESTAMP in microseconds. */
  int64_t m_integer{0};

  /** DOUBLE. */
  double m_double{0};

  /** VARCHAR. */
  std::string m_string;
};

/** One column of a result. */
struct query_column_t {
  /** Name as DuckDB gives it. */
  std::string m_name;

  /** Type the values are returned as. */
  data_type_t m_data_type{data_type_t::BIGINT};
};

/** Rows of a result: rows[r][c] is the value of column c in row r. */
typedef std::vector<std::vector<query_value_t>> query_rows_t;

/** Configured decoding of one DuckDB result column. */
class query_column_reader final {
 public:
  /** Initialize the output type and conversion rules. */
  bool init(duckdb_prepared_statement prepared, idx_t col, data_type_t &out);

  /** Read one value using the configured conversion rules. */
  bool read(duckdb_vector vector, idx_t row, query_value_t &out) const;

 private:
  /** DuckDB type of the column. */
  duckdb_type m_data_type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: the integer type it is stored as. */
  duckdb_type m_decimal_type{DUCKDB_TYPE_INVALID};

  /** DECIMAL: 10 to the power of its scale. */
  double m_decimal_divisor{1};
};

/** Install native query hooks on the database before background work starts.
Called during database open with exclusive use of the storage connection. */
db_status_t query_init(duckdb_connection conn);

/** Analyze a SELECT: validate it and collect time ranges and base-table names.
Candidates may include CTE names; the database locks only time-series sources.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
db_status_t query_analyze(duckdb_connection conn, const std::string &sql,
                          query_ranges_t &ranges,
                          std::vector<std::string> &tables);

/** Cancellation of one SQL execution or query open. The caller keeps this
object alive until the call returns and no concurrent request() remains. */
class query_cancel final : private ut_noncopyable {
 public:
  /** Request cancellation; interrupt the attached connection if any. */
  void request();

  /** Read the cancellation state while waiting for an engine lock.
  @return true after request() has been called */
  bool requested() const { return (m_requested.load()); }

  /** Attach the connection used by this operation. */
  void attach(duckdb_connection conn);

  /** Detach the connection before its owner disconnects it. */
  void detach();

 private:
  /* =================== Atomic cancellation state ==================== */

  /** Allows lock waiters to observe cancellation without taking a mutex. */
  std::atomic<bool> m_requested{false};

  /* ====================== Attached connection ======================= */

  /** Protects this group; serializes interrupt, attach and detach. */
  std::mutex m_mutex;

  /** Connection receiving cancellation requests during the operation. */
  duckdb_connection m_connection{nullptr};
};

/** Parsed SQL and its per-call connection. */
class query_command final : private ut_noncopyable {
 public:
  /** Release statements, detach cancellation and roll back unfinished work. */
  ~query_command();

  /** Connect, parse all statements and determine whether namespace locking is
  needed, without executing SQL. Call once, then execute once on success.
  Cancellation must outlive this command. */
  db_status_t init(duckdb_database database, const std::string &sql,
                   query_cancel *cancel);

  /** Check whether this call needs exclusive access to the namespace. */
  bool changes_namespace() const { return (m_changes_namespace); }

  /** Validate and execute the parsed statements. For namespace changes, the
  caller holds the namespace lock until this command is destroyed and supplies
  time-series names collected after acquiring that lock. */
  db_status_t execute(const std::set<std::string> &tables);

 private:
  /** Execute one parsed statement and release its temporary native handles. */
  db_status_t execute_statement(idx_t index);

  /** Connection shared by all statements of this call. */
  duckdb_connection m_connection{nullptr};

  /** Syntax trees consumed by statement preparation. */
  duckdb_extracted_statements m_statements{nullptr};

  /** Cancellation attached to this connection until destruction. */
  query_cancel *m_cancel{nullptr};

  /** At least one statement may change the table namespace. */
  bool m_changes_namespace{false};
};

/** Manages statement preparation and streaming results.
The connection and snapshot must outlive the cursor. */
class query_cursor final : private ut_noncopyable {
 public:
  query_cursor();

  /** Stop execution before releasing its snapshot registration. */
  ~query_cursor();

  /** Create a cursor; open a checked SELECT in the snapshot transaction. */
  static db_status_t open(duckdb_connection conn,
                          const query_snapshot_t &snapshot,
                          const std::string &sql, bool explain,
                          const query_ranges_t &ranges, query_cursor **out);

  /** Read at most QUERY_MAX_CHUNK_ROWS; every terminal error remains sticky. */
  db_status_t fetch(query_rows_t &rows);

  /** Request interruption concurrently with fetch, never with close. */
  void interrupt();

  /** Return the result metadata fixed when the cursor opened. */
  const std::vector<query_column_t> &columns() const;

  /** Destroy an allocated cursor; NULL is accepted. */
  static void destroy(query_cursor *cursor);

 private:
  /** Release query resources; safe after a partial open. */
  void close();

  /** Configure the prepared statement result columns. */
  db_status_t init_columns();

  /** Register the snapshot, prepare the statement and start streaming; the
  factory destroys the cursor on failure. */
  db_status_t open_low(duckdb_connection conn, const query_snapshot_t &snapshot,
                       const std::string &sql, bool explain,
                       const query_ranges_t &ranges);

  /** Convert one native data chunk to output rows. */
  db_status_t read_chunk(duckdb_data_chunk chunk, query_rows_t &rows);

  /** Record a DuckDB result error or interruption as the terminal status. */
  db_status_t fail_from_result();

  /* ======================== Query connection ======================== */

  /** Stable while open; interrupt() may read it concurrently with fetch().
  Close must wait for both operations to finish. */
  duckdb_connection m_connection{nullptr};

  /* ================== Cursor-owned execution state ================== */

  /* Open, fetch and close are serialized by the caller. */

  /** Registry token; 0 when not registered. */
  uint64_t m_token{0};

  /** Times each table may be read at; the registry points here. */
  query_ranges_t m_ranges;

  /** The bound and planned statement. */
  duckdb_prepared_statement m_prepared{nullptr};

  /** Pending execution; the first fetch consumes and releases this handle. */
  duckdb_pending_result m_pending{nullptr};

  /** Empty until execution starts; always safe to destroy. */
  duckdb_result m_result{};

  /** Unset while fetching can continue. DB_SUCCESS marks normal EOF. */
  std::optional<db_status_t> m_status;

  /** Result columns, from the prepared statement. */
  std::vector<query_column_t> m_columns;

  /** How to read each result column. */
  std::vector<query_column_reader> m_column_readers;

  /* =================== Atomic interruption state ==================== */

  /** Request to interrupt the current query, set by interrupt(). */
  std::atomic<bool> m_interrupt_requested{false};
};

/** Memtable scan function name in DuckDB. */
#define QUERY_MEMTABLE_SCAN_NAME "_pt_memtable_scan"

/** A connection id is local to its DatabaseInstance. The database address
is stable while the connection owns the instance. */
using query_connection_key_t = std::pair<uintptr_t, uint64_t>;

/** Get the composite connection identity. C++ adapter; no allocation.
@return the database-instance address and database-local connection id */
query_connection_key_t query_connection_key(duckdb_connection conn);

/** A registered query. */
struct query_entry_t {
  /* ============= Protected by the owning registry mutex ============= */

  /** Connection the query runs on. */
  query_connection_key_t m_connection_key{0, 0};

  /** Query snapshot owned by the database cursor. */
  const query_snapshot_t *m_snapshot{nullptr};

  /** Times each table may be read at; owned by its query cursor. */
  const query_ranges_t *m_ranges{nullptr};

  /** First failure a callback reported; DuckDB passes it on only as text. */
  db_status_t m_callback_status;
};

/** Running queries indexed by token and connection.
Registration lasts from cursor open to destruction; its snapshot stays valid
while DuckDB may invoke callbacks. */
class query_registry final : private ut_noncopyable {
 public:
  /** Access the registry retained through process teardown. */
  static query_registry &instance();

  /** Register a query and return its nonzero token. */
  uint64_t register_query(const query_connection_key_t &connection,
                          const query_snapshot_t &snapshot,
                          const query_ranges_t &ranges);

  /** Remove both indexes of a query, ignoring an unknown token. */
  void unregister_query(uint64_t token);

  /** Find a query in the connection index. */
  const query_snapshot_t *find_by_connection(
      const query_connection_key_t &connection, uint64_t &token,
      const query_ranges_t *&ranges);

  /** Retain the first failure a callback reports for this query. */
  void record_error(uint64_t token, const db_status_t &error);

  /** Prefer a recorded callback failure over DuckDB's text for it. */
  db_status_t restore_error(uint64_t token, const db_status_t &status);

  /** Find a query in the token index. */
  const query_snapshot_t *find_by_token(uint64_t token);

 private:
  /* ======================= Registered queries ======================= */

  /** Protects this group; no other lock is acquired while it is held. */
  std::mutex m_mutex;

  /** Queries indexed by their process-wide token. */
  std::map<uint64_t, query_entry_t> m_by_token;

  /** Token of the query running on each connection. */
  std::map<query_connection_key_t, uint64_t> m_by_connection;

  /** Next token to assign. */
  uint64_t m_next_token{1};
};

/** Install the memtable scan table function on the connection's database.
Call once during db opening, before registering the table replacement callback.
@param[in,out]  conn  connection to the open database; NULL is rejected
@return DB_SUCCESS or DB_ERROR if DuckDB refuses registration */
db_status_t query_install_memtable_scan(duckdb_connection conn);

/** Let plans retain immutable memtable bindings for the current snapshot.
Call before registering the table function. */
void query_wrap_memtable_bind(duckdb_table_function function);

/** Prepare a private, single-execution handle in an already fixed transaction.
The connection and snapshot belong exclusively to this registered cursor.
@return DuckDBSuccess, or DuckDBError with the prepared handle's diagnostic */
duckdb_state query_prepare(duckdb_connection connection, const char *sql,
                           uint64_t token, const query_snapshot_t &snapshot,
                           duckdb_prepared_statement *out);

/** Register table replacement and plan retention after the memtable scan.
Call once during db opening, before user queries or background work.
@param[in,out]  conn  non-NULL connection to the target database */
void query_register_callbacks(duckdb_connection conn);

/** Build the SELECT that stands for a table: its partitions that overlap a
time range, and its memtable batches.
@param[in]  name   normalized table name
@param[in]  range  times the query may read from the table
@return the SQL text */
std::string query_build_scan_sql(uint64_t token, const std::string &name,
                                 const query_table_t &table,
                                 const query_range_t &range,
                                 batch_seq_t flushed_seq = UINT64_MAX);

#endif /* query_h */
