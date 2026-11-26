/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db.h
 *    Coordinates writes, WAL, memtables and DuckDB storage.
 *
 * Writes plan and encode in parallel, then take sequences and queue under
 * m_write_mutex. Table X locks protect schema changes. Writes become visible
 * after WAL group commit; m_wal_sync also makes them durable before return.
 *
 * Flush syncs the WAL, stores visible writes in DuckDB, advances the stored
 * sequence F and retires memtable batches, without holding m_write_mutex
 * while storing. Recovery replays only WAL records after F. The background
 * thread runs flush and retention expiry; memtable pressure can wake it sooner.
 *
 * Queries combine stored data up to F with memtable writes up to the visible
 * sequence V. m_snapshot_mutex orders snapshot creation against write
 * publication, retention expiry and table metadata changes.
 *
 * db_schema and db_batch are the shared data model, not part of this
 * coordinator: ingest, memtable, duckdb and query build on them.
 *
 * Lock order. A lock may be taken while holding only locks above it:
 *
 *   1. db::m_namespace_lock        X by namespace-changing SQL, S by first
 *                                  time-series writes, explicit creation and
 * drop
 *   2. db_table::m_lock            table S/X, several in name order;
 *                                  db_tables::m_mutex is released first
 *   3. db::m_flush_mutex           store mutations and WAL purge
 *   4. db::m_snapshot_mutex        S for query/catalog snapshots, X for write
 *                                  publication, expiry and table metadata
 *   5. wal::m_io_mutex,            never under db::m_write_mutex
 *      wal::m_purge_mutex
 *   6. wal::m_state_mutex
 *   7. db::m_write_mutex           write queue and database status
 *   8. db_table::m_schema_mutex
 *   9. memtable::m_mutex
 *
 * Leaf mutexes do not enclose waits for the locks above: db_tables::m_mutex,
 * db::m_catalog_snapshot_mutex, db::m_flusher_mutex, query_cancel::m_mutex,
 * query_registry::m_mutex and the internal mutexes of ut_event_t and
 * ut_rwlock.
 *
 *-------------------------------------------------------------------------
 */

#ifndef db_h
#define db_h

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <pthread.h>

#include "db_directory.h"
#include "db_schema.h"
#include "db_table.h"
#include "ingest.h"
#include "memtable.h"
#include "os_file.h"
#include "query.h"
#include "store.h"
#include "ut.h"
#include "ut_debug.h"
#include "wal.h"

/** Maximum bytes selected by one database group leader; a single write may
exceed it. */
#define DB_MAX_WRITE_GROUP_BYTES (1024 * 1024)

/** Longest retention period, in hours: about 300 years. */
#define DB_MAX_RETENTION_HOURS 2640000

/** An open database directory. */
class db;

/** A query being read chunk by chunk. */
class db_cursor;

/** Owns one open database and its coordinated operations. */
class db final : private ut_noncopyable {
 public:
  /** Settings of a database. */
  struct options_t {
    /** Validate options before the directory is touched. */
    db_status_t validate() const;

    /** Allow creation of a database at a missing or uninitialized path. */
    bool m_create_if_missing{true};

    /** Idle wait between background passes, in milliseconds; must be greater
    than 0. Work and scheduling add to elapsed time; writers waiting for
    memtable capacity may wake the thread sooner, except during an OOM retry
    delay. */
    uint32_t m_flush_interval_ms{200};

    /** Maximum wait, in milliseconds, for each table or namespace lock taken
    by a write, create, set or drop; 0 waits indefinitely. Default: 0. A
    timeout rejects the write before it enters the WAL. */
    uint32_t m_write_lock_timeout_ms{0};

    /** New writers wait while unretired column data is at or above this many
    bytes; 0 disables waiting. Excludes planning buffers and retired batches
    held by snapshots. */
    uint64_t m_memtable_limit_bytes{128 * 1024 * 1024};

    /** Size at which the WAL starts a new file. */
    uint64_t m_wal_file_size_bytes{WAL_DEFAULT_FILE_SIZE_BYTES};

    /** Default hours covered by one time partition, 1 to
    STORE_MAX_PARTITION_HOURS. Used when the directory is created; an existing
    one keeps its own. */
    uint32_t m_partition_duration_hours{STORE_DEFAULT_PARTITION_HOURS};

    /** Default retention in hours, at most DB_MAX_RETENTION_HOURS; 0 keeps
    everything. Table overrides take precedence. Queries exclude expired rows;
    physical cleanup drops only whole partitions. */
    uint32_t m_retention_hours{0};

    /** Maximum future timestamp offset in seconds for new writes. Zero
    disables this check; applies to this open only, never to WAL replay. */
    uint32_t m_max_future_skew_seconds{0};

    /** Sync each WAL group before acknowledging its writes, and DuckDB commits
    past the drive's cache. When false, the most recent acknowledged writes may
    be lost on a system crash or power loss. */
    bool m_wal_sync{false};

    /** Shared DuckDB budget in bytes. 0 uses DuckDB's default. */
    uint64_t m_duckdb_memory_limit_bytes{0};

    /** DuckDB execution threads; 0 keeps its platform default for this open. */
    uint32_t m_duckdb_threads{0};
  };

  /** Mutable table settings; omitted retention inherits the open default. */
  struct table_options_t {
    std::optional<uint32_t> m_retention_hours;
  };

  /** Initial settings; zero partition hours selects the durable default. */
  struct create_table_options_t {
    uint32_t m_partition_duration_hours{0};
    std::optional<uint32_t> m_retention_hours;
  };

  /** Persist a table declaration; the first write defines its columns. */
  db_status_t create_table(const std::string &name,
                           const create_table_options_t &options);

  /** Drop a managed table durably after a global flush. */
  db_status_t drop_table(const std::string &name);

  /** Replace a table's mutable settings without flushing its data. */
  db_status_t set_table_options(const std::string &name,
                                const table_options_t &options);

  /** Counters of a database. */
  struct stats_t {
    /** Newest write stored in DuckDB. */
    batch_seq_t m_flushed_seq{0};

    /** Newest write visible to queries. */
    batch_seq_t m_visible_seq{0};

    /** Column data bytes in unretired memtable writes. */
    uint64_t m_memtable_bytes{0};

    /** Background flushes that stored something. */
    uint64_t m_background_flushes{0};

    /** Background passes with a new flush, WAL cleanup or expiry failure; at
    most one per pass. Rejections after a fatal error are not new failures. */
    uint64_t m_background_errors{0};

    /** Writes that had to wait for room in the memtable. */
    uint64_t m_throttled_writes{0};

    /** Current number of WAL files. */
    uint64_t m_wal_file_count{0};

    /** Partitions dropped for being past the retention period. */
    uint64_t m_expired_partitions{0};

    /** Shared DuckDB budget selected at open, in bytes. */
    uint64_t m_duckdb_memory_limit_bytes{0};

    /** Effective DuckDB execution thread setting selected at open. */
    uint32_t m_duckdb_threads{0};
  };

  /** Result of one flush. */
  struct flush_info_t {
    /** Writes stored by this flush. */
    uint64_t m_writes{0};

    /** Rows stored by this flush. */
    uint64_t m_rows{0};

    /** Last stored sequence after this flush. */
    batch_seq_t m_flushed_seq{0};
  };

  db() = default;

  ~db();

  /** Validate dir and options, then open or create the database in dir.
  REQUIRES: a new object; the caller destroys it if opening fails. */
  db_status_t open(const char *dir, const options_t &options);

  /** Stop background work and free resources. No call or cursor may remain. */
  void close();

  /** Write a batch of points to a table; thread-safe. Returns once the write is
  logged and visible to snapshots; m_wal_sync also makes it durable.
  @param[out]  seq  sequence number of the write; set on DB_SUCCESS and
                    DB_OUTCOME_UNKNOWN, otherwise 0
  @retval DB_SUCCESS logged and visible
  @retval DB_INVALID_ARGUMENT invalid input; nothing written
  @retval DB_SCHEMA_CONFLICT incompatible columns; nothing written
  @retval DB_LIMIT_EXCEEDED write exceeds a limit; nothing written
  @retval DB_LOCK_TIMEOUT table lock wait expired; nothing written
  @retval DB_OUT_OF_MEMORY allocation failed; nothing written
  @retval DB_ERROR write target lookup failed; nothing written
  @retval DB_IO_ERROR WAL preparation or maintenance failed; nothing written;
    reopen
  @retval DB_FENCED rejected; the database has failed, reopen
  @retval DB_CORRUPTION inconsistent catalog or write metadata
  @retval DB_OUTCOME_UNKNOWN the write may or may not be durable; do not retry
    blindly; reopen the database */
  db_status_t write(const char *table_name, size_t table_name_length,
                    const ingest_point_t *points, uint32_t n_points,
                    batch_seq_t &seq);

  /** Store every visible, unflushed write in DuckDB in one transaction and
  retire it from the memtable. Thread-safe; one flush runs at a time; writes are
  not blocked.
  @retval DB_SUCCESS stored, or nothing to store
  @retval DB_ERROR storage operation failed; reopen
  @retval DB_OUT_OF_MEMORY rolled back for lack of memory; safe to retry
  @retval DB_IO_ERROR WAL sync or storage I/O failed; reopen
  @retval DB_CORRUPTION stored metadata disagrees with the open state; reopen
  @retval DB_FENCED an earlier failure stopped the database
  @retval DB_OUTCOME_UNKNOWN commit or rollback failed; reopen to determine
    what was stored */
  db_status_t flush(flush_info_t &info);

  /** Retain the newest schema of every table, flushed or not, by name.
  Replaces the output. Thread-safe; retained schemas remain immutable and valid
  after later writes or database close. */
  void collect_schemas(std::vector<schema_ref_t> &schemas);

  /** Copy declared and initialized table metadata, sorted by name. */
  void collect_tables(std::vector<db_table_info_t> &tables);

  /** Read failures from the latest failed background pass, labeled by task.
  Successful passes and rejections after a fatal error preserve this result.
  @return the latest failed pass, or success before any background failure */
  db_status_t background_status();

  /** Read the database counters. */
  void stats(stats_t &stats);

  /** Execute DuckDB SQL on a temporary connection and discard result rows.
  Protected names are checked before execution. Transactions and session state
  stay within this call; disconnect rolls back any unfinished transaction.
  Cancellation stops waiting or execution, but does not undo commits.
  Parameter values require a single SELECT, INSERT, UPDATE, DELETE or MERGE. */
  db_status_t exec(const std::string &sql, query_cancel *cancel = nullptr,
                   query_params_t params = {});

  /** Start a SELECT, or EXPLAIN of it, over one consistent snapshot. The
  statement is planned here and runs as rows are fetched.
  Until closed, a cursor holds a DuckDB connection, a read transaction, a
  memtable snapshot and S locks on its time-series tables; the batches it
  reads outlive their flush. Thread-safe. One thread uses a cursor at a time,
  but use and close may move between threads. Close every cursor before
  db_close().
  @param[out] out open cursor, or NULL on failure
  @param[in] cancel optional opening cancellation, alive until this call returns
  @return DB_SUCCESS, DB_INVALID_ARGUMENT for a refused statement, parameter
    values or result type, DB_INTERRUPTED when opening is cancelled, or a DuckDB
  failure */
  db_status_t open_cursor(const std::string &sql, bool explain, db_cursor **out,
                          query_cancel *cancel = nullptr,
                          query_params_t params = {});

  /** Show DuckDB's plan for a SELECT, as a cursor would run it.
  @param[out]  plan  plan text; empty on failure
  @return DB_SUCCESS or the error of planning or fetching the EXPLAIN */
  db_status_t explain(const std::string &sql, std::string &plan,
                      query_params_t params = {});

 private:
  /** Drop the stored partitions whose hours have all passed the retention
  period, measured back from a given time. The background thread calls this with
  the current time after each flush. Thread-safe; does nothing without a
  retention period.
  @retval DB_SUCCESS dropped, or nothing to drop
  @retval DB_ERROR rolled back; nothing changed
  @retval DB_FENCED an earlier failure stopped the database
  @retval DB_OUTCOME_UNKNOWN commit or rollback failed; the database must
    be reopened */
  db_status_t expire(int64_t now_us, uint64_t &removed);

#ifdef PT_TESTING
  /** Test-only access is implemented outside the production library. */
  friend class db_test_access;
#endif /* PT_TESTING */

  /** State retained while one write plans, queues and commits. */
  class write_context;

  /** Replay one recovered WAL payload into the memtable. */
  static db_status_t replay(void *context, batch_seq_t seq,
                            const unsigned char *payload, size_t len);

  /** Cursors coordinate their snapshots with this database. */
  friend class db_cursor;

  /** Table-lock acquisition checks failure while waiting. */
  friend class db_table_lock;

  /** Create the memtable at F and restore the current column directories. */
  db_status_t init_memtable();

  /** Open or create the WAL, replay writes after F and verify its boundaries.
  The table directory must already contain the stored schemas. */
  db_status_t open_wal(const char *dir);

  /** Reject recovered time-series names that conflict with SQL tables.
  @return DB_SUCCESS, DB_CORRUPTION or a catalog query error */
  db_status_t validate_table_names();

  /** Find a table's stable entry and acquire its requested mode.
  @param[in] name normalized table name
  @param[in,out] lock empty guard; owns the table lock on success
  @param[in] cancel optional query cancellation, alive until this call returns
  @param[in] timeout_ms wait limit; 0 waits without a deadline
  @return DB_SUCCESS, DB_FENCED, DB_INTERRUPTED or DB_OUT_OF_MEMORY */
  db_status_t lock_table(const std::string &name, db_table_lock_mode_t mode,
                         db_table_lock &lock,
                         const query_cancel *cancel = nullptr,
                         uint32_t timeout_ms = 0);

  /** Lock an existing incarnation for Set or Drop, rejecting stale waiters.
  Drop must acquire namespace S before calling this helper. */
  db_status_t lock_existing_table(const std::string &name, db_table_lock &lock);

  /** Start background work after storage, WAL and scans are ready. */
  db_status_t start_flusher();

  /** Reject new work after a database failure, retaining its diagnostic.
  @return DB_SUCCESS or DB_FENCED with the first recorded cause */
  db_status_t status_low() const;

  /** Record the first fatal failure and stop new writes and publication.
  REQUIRES: m_write_mutex.
  @param[in]  status  failure; later calls preserve the first cause */
  void set_status_low(const db_status_t &status);

  /** Take m_write_mutex, then record the failure as set_status_low() does.
  @param[in]  status  failure; later calls preserve the first cause */
  void set_status(const db_status_t &status);

  /** Check the database failure state under the write lock.
  @return DB_SUCCESS or DB_FENCED with the first recorded cause */
  db_status_t status();

  /** Reject writes to SQL tables in the current DuckDB catalog.
  @return DB_SUCCESS, DB_INVALID_ARGUMENT, or a connection/catalog error */
  db_status_t validate_write_table(const std::string &name);

  /** Acquire namespace access, checking cancellation or the write deadline.
  The caller adopts the acquired mode in a standard lock guard. */
  db_status_t lock_namespace(bool exclusive, const query_cancel *cancel);

  /** Wait for memtable capacity, waking the background flusher as needed.
  @return DB_SUCCESS if admission is allowed, or DB_FENCED */
  db_status_t wait_for_memtable_space();

  /** Check the cached time bounds against the target's current retention and
  the future skew, locating a point only on rejection.
  REQUIRES: w holds its table lock, so the retention cannot change. */
  db_status_t validate_write_times(const write_context &w) const;

  /** Reserve memory and enqueue one write under the database write lock.
  Every accepted write must reach commit_write() to release its queue node.
  @return DB_SUCCESS, DB_FENCED, or a validation or allocation error */
  db_status_t reserve_write(write_context &w);

  /** Select a queue prefix within the group limit, always including its head.
  The write mutex must be held; queued writes keep the payloads alive. */
  void build_write_group(std::vector<wal_record_t> &records);

  /** Complete the selected writes after WAL I/O finishes. */
  void complete_writes(size_t count, const db_status_t &status,
                       bool keep_sequence);

  /** Publish or fail only the active WAL group after its I/O has returned;
  the caller wakes the next head. */
  void finish_write_group(size_t count, batch_seq_t last_seq,
                          const db_status_t &status);

  /** Wait for the group commit of a reserved write, then make it visible.
  @param[in,out]  seq  sequence number of the write; reset to 0 if the write
                       was certainly not written
  @retval DB_SUCCESS logged and visible
  @retval DB_OUTCOME_UNKNOWN the write may or may not be durable; reopen
  @return otherwise the error of a write that was certainly not written */
  db_status_t commit_write(write_context &w, batch_seq_t &seq);

  /** Remove stored WAL files; failures can be retried independently. */
  db_status_t purge_wal();

  /** Store every visible, unflushed write.
  @return DB_SUCCESS, a rolled-back preparation error, DB_CORRUPTION, DB_FENCED,
    or DB_OUTCOME_UNKNOWN after commit or rollback cannot be confirmed */
  db_status_t flush_low(flush_info_t &info);

  /** Wait for a wakeup or the interval; backoff ignores early writer wakeups.
  @return false if the flusher must stop */
  bool flusher_wait(bool backoff);

  /** Flush writes, clean up the WAL, expire partitions and record failures.
  The caller must not hold m_flusher_mutex.
  @return the flush result, used to delay retries after OOM */
  db_status_t flusher_run();

  /** Background flusher: flush every interval, or when woken.
  @return NULL */
  static void *flusher_main(void *arg);

  /** Reuse metadata for F, or read it in the cursor's own transaction.
  @return DB_SUCCESS or the catalog query/validation error */
  db_status_t read_query_catalog(duckdb_connection conn,
                                 batch_seq_t flushed_seq,
                                 std::shared_ptr<const store_catalog_t> &out);

  /** Allocate a cursor with its own DuckDB connection; NULL on failure.
  @return DB_SUCCESS, DB_OUT_OF_MEMORY or DB_ERROR */
  db_status_t create_cursor(db_cursor **out, query_cancel *cancel);

  /** Open the query on a new cursor left in *cursor, which the caller
  destroys if opening fails. */
  db_status_t open_cursor_low(const std::string &sql, bool explain,
                              db_cursor **cursor, query_cancel *cancel,
                              query_params_t params);

  /** Find known time-series sources and acquire their shared locks by name.
  @param[in,out] locks empty on entry; retains acquired locks, even on failure
  @param[in] cancel optional query cancellation, alive until this call returns
  @return DB_SUCCESS, DB_FENCED, DB_INTERRUPTED or DB_OUT_OF_MEMORY */
  db_status_t lock_query_tables(const std::vector<std::string> &names,
                                db_table_locks &locks,
                                const query_cancel *cancel);

  /* ======================== Owned resources ========================= */

  /* Initialized before publication; released only after all users stop. */

  /** Excludes Open/Drop until storage and WAL resources have closed. */
  db_directory m_directory;

  /** The write-ahead log. */
  wal *m_wal{nullptr};

  /** Unflushed writes and batches retained by query snapshots. */
  memtable *m_memtable{nullptr};

  /** The DuckDB file with the writes up to F. */
  store *m_store{nullptr};

  /** Settings given to db_open(), checked. */
  options_t m_options;

  /** Background flusher thread. */
  pthread_t m_flusher_thread{};

  /** The thread was started and still needs joining, even if it has exited. */
  bool m_flusher_started{false};

  /* ================ Write queue and database status ================= */

  /** Protects this group and orders reservation and group publication.
  Taken after table S/X locks and before WAL and memtable locks, and never held
  while waiting for a group commit. */
  ut_mutex_t m_write_mutex;

  /** Database status; initially successful, then retains the first fatal error.
  A failure rejects new writes, queries and maintenance until reopen;
  existing cursor snapshots remain readable. */
  db_status_t m_status;

  /** Next database-assigned write sequence, restored from recovery. */
  batch_seq_t m_next_seq{1};

  /** Caller-owned writes in order; the front writer becomes group leader. */
  std::deque<write_context *> m_writes;

  /** WAL I/O is using the selected queue prefix. */
  bool m_write_active{false};

  /* ============ Internally synchronized table directory ============= */

  /** Stable table objects, owned until close; lookup releases the cache lock
  before any table S/X acquisition. */
  db_tables m_tables;

  /* ====================== Reader catalog cache ====================== */

  /** Protects this group; never held across DuckDB calls.
  Taken after m_snapshot_mutex when both are needed. */
  std::mutex m_catalog_snapshot_mutex;

  /** Immutable metadata read in a query transaction, matched by F.
  Expiry clears it because deleting partitions does not change F. */
  std::shared_ptr<const store_catalog_t> m_catalog_snapshot;

  /* ==================== Operation serialization ===================== */

  /* These locks order whole operations; they protect no field group. */

  /** Namespace-changing SQL holds X through commit or rollback; first
  time-series writes hold S through publication. Taken before table locks and
  m_write_mutex. Pure DML, queries and flush do not take it. */
  ut_rwlock m_namespace_lock;

  /** Allows one flush or expiry transaction at a time. Taken before
  m_snapshot_mutex and m_write_mutex. */
  ut_mutex_t m_flush_mutex;

  /** Shared while a cursor fixes its snapshot; exclusive for write
  publication, partition deletion and table metadata changes. Taken after
  m_flush_mutex and before m_write_mutex. Never held across WAL I/O. */
  ut_shared_mutex_t m_snapshot_mutex;

  /* ======================== Background state ======================== */

  /** Protects this group; a leaf lock. */
  std::mutex m_flusher_mutex;

  /** close() asks the flusher to exit. */
  bool m_flusher_stop{false};

  /** Historical diagnostic only; success does not erase the last failure. */
  db_status_t m_last_background_error;

  /** Counters; m_wal_file_count is read from the WAL when asked for. */
  stats_t m_stats;

  /* ============= Internally synchronized notifications ============== */

  /** Wakes the flusher early; the event retains requests during a pass. */
  ut_event_t m_flusher_event;

  /** Wakes space waiters to recheck memtable capacity or database failure. */
  ut_event_t m_memtable_space_event;
};

/** Open a database directory: create its WAL and storage if there are none,
otherwise load the storage and replay the WAL records after F.
@param[in]   dir      database directory; created if absent, parent must exist
@param[in]   options  settings, or NULL for the defaults
@param[out]  out      open database, or NULL on failure
@return DB_SUCCESS, DB_LOCKED, DB_FENCED for pending deletion, DB_CORRUPTION,
  DB_IO_ERROR, DB_ERROR, DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
db_status_t db_open(const char *dir, const db::options_t *options, db **out);

/** Remove a closed database directory; an open database returns DB_LOCKED. */
db_status_t db_drop_database(const char *dir);

/** Close a database. No call may be in progress.
@param[in]  database  database; may be NULL */
void db_close(db *database);

/** Owns a query transaction, snapshot and table locks. */
class db_cursor final : private ut_noncopyable {
 public:
  db_cursor() = default;

  ~db_cursor();

  /** Request interruption of an active cursor from any thread while it is
  open. The running or next fetch observes the request; after it returns
  DB_INTERRUPTED, every later fetch does too. Normal end is reached when
  db_cursor::fetch() returns DB_SUCCESS with empty rows; interrupting after
  that leaves later fetches successful and empty. Do not call concurrently with
  db_cursor::destroy(). */
  void interrupt();

  /** Column metadata fixed when the cursor opens; valid until close. */
  const std::vector<query_column_t> &columns() const;

  /** Read the next rows of a cursor, at most QUERY_MAX_CHUNK_ROWS.
  @param[out]  rows  the rows; empty once every row was read
  @return as query_cursor::fetch() */
  db_status_t fetch(query_rows_t &rows);

  /** Stop a query and free its cursor.
  @param[in]  cursor  cursor; may be NULL */
  static void destroy(db_cursor *cursor);

 private:
  /** Release owned resources; safe after partial initialization. */
  void close();

  /** The database creates cursors and coordinates their snapshots. */
  friend class db;

  /** Take the snapshot of a cursor: the DuckDB transaction at F and the
  memtable writes after F. A deletion of stored data waits until it is fixed.
  @return DB_SUCCESS, DB_FENCED, DB_CORRUPTION or a DuckDB snapshot error */
  db_status_t take_snapshot(const std::vector<std::string> &names);

  /** Begin the read transaction and fix its stored sequence boundary. */
  db_status_t begin_snapshot(batch_seq_t &flushed_seq);

  /** Combine stored metadata with this cursor's pinned memtable snapshot. */
  void build_tables(const store_catalog_t &catalog,
                    const std::vector<std::string> &names);

  /** Check that every managed source was locked before fixing the snapshot. */
  bool tables_locked(const std::vector<std::string> &names) const;

  /** Database the cursor reads. */
  db *m_db{nullptr};

  /** Connection running the snapshot transaction. */
  duckdb_connection m_connection{nullptr};

  /** Cancellation state; must remain valid until open_cursor() returns. */
  query_cancel *m_cancel{nullptr};

  /** Memtable writes after F. */
  memtable::snapshot_t m_memtable_snapshot;

  /** What the query sees; the query registry points here. */
  query_snapshot_t m_snapshot;

  /** The running statement; NULL until it is opened. */
  query_cursor *m_query{nullptr};

  /** Time-series S locks released only after cursor resources are stopped. */
  db_table_locks m_table_locks;
};

#endif /* db_h */
