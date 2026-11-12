/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db.h
 *    Coordinates writes, WAL, memtables and DuckDB storage.
 *
 * Writes plan and encode in parallel, then assign sequences and queue under
 * write_mutex. Table X locks protect schema changes. Writes become visible
 * after WAL group commit; m_wal_sync also makes them durable before return.
 *
 * Flush syncs the WAL, stores visible writes in DuckDB, advances the stored
 * sequence F, and retires memtable batches without taking write_mutex.
 * Recovery replays only WAL records after F. The background thread runs
 * flush and retention expiry; memtable pressure can wake it sooner.
 *
 * Queries combine stored data at F with visible memtable writes through S.
 * snapshot_mutex coordinates snapshot creation with retention expiry, which
 * drops expired partitions while preserving schemas.
 *
 * Table-cache lookup releases tables.m_mutex before taking a table S/X lock.
 * Acquire table modes by name before flush_mutex, snapshot_mutex, write_mutex,
 * then WAL and memtable mutexes. flusher_mutex is a leaf.
 *
 *-------------------------------------------------------------------------
 */

#ifndef db_h
#define db_h

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <pthread.h>

#include "db_table.h"
#include "ingest.h"
#include "memtable.h"
#include "os_file.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_mutex.h"
#include "ut_noncopyable.h"
#include "ut_shared_mutex.h"
#include "wal.h"

/** Maximum bytes selected by one database group leader; a single write may
 * exceed it. */
#define DB_WRITE_GROUP_BYTES (1024 * 1024)

/** Longest retention period, in days: about 300 years. */
#define DB_MAX_RETENTION_DAYS 110000

/** An open database directory. */
class db;

/** Settings of a database. */
struct db_options_t {
  /** Validate options before allocating resources or touching the directory. */
  db_status_t validate() const;

  /** Idle wait between background passes, in milliseconds; must be greater
  than 0. Work and scheduling add to elapsed time; writers waiting for memtable
  capacity may wake the thread sooner. */
  uint32_t m_flush_interval_ms{200};

  /** New writers wait while unretired column data is at or above this many
  bytes; 0 disables waiting. Excludes planning buffers
  and retired batches held by snapshots. */
  uint64_t m_memtable_limit_bytes{256 * 1024 * 1024};

  /** Size at which the WAL starts a new segment. */
  uint64_t m_wal_segment_size_bytes{WAL_DEFAULT_SEGMENT_BYTES};

  /** Days covered by one time partition, 1 to STORAGE_MAX_PARTITION_DAYS.
  Used when the directory is created; an existing one keeps its own. */
  uint32_t m_partition_duration_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Keep this many days before the current time, at most
  DB_MAX_RETENTION_DAYS; 0 keeps everything.
  Older writes are rejected, and stored partitions that hold only older
  times are dropped. A partition is dropped whole, so up to
  m_partition_duration_days more are kept. */
  uint32_t m_retention_days{0};

  /** Sync each WAL group before acknowledging its writes. When false,
  acknowledged writes may be lost on a system crash or power loss. */
  bool m_wal_sync{false};

  /** Shared DuckDB budget in bytes; 0 resolves automatically at open.
  Separate from memtable capacity and process RSS. */
  uint64_t m_duckdb_memory_limit_bytes{0};
};

/** Counters of a database. */
struct db_stats_t {
  /** Newest write stored in DuckDB. */
  batch_seq_t m_flushed_seq{0};

  /** Newest write visible to queries. */
  batch_seq_t m_visible_seq{0};

  /** Column data bytes in unretired memtable writes. */
  uint64_t m_memtable_bytes{0};

  /** Background flushes that stored something. */
  uint64_t m_background_flushes{0};

  /** Background passes with a new flush, WAL cleanup or expiry failure; at most
  one per pass. Rejections after a fatal error are not new failures. */
  uint64_t m_background_errors{0};

  /** Writes that had to wait for room in the memtable. */
  uint64_t m_throttled_writes{0};

  /** WAL segment files now. */
  uint64_t m_wal_segment_count{0};

  /** Partitions dropped for being past the retention period. */
  uint64_t m_expired_partitions{0};

  /** Effective shared DuckDB budget for this open, in bytes. */
  uint64_t m_duckdb_memory_limit_bytes{0};
};

/** What a flush did. */
struct db_flush_info_t {
  /** Writes stored by this flush. */
  uint64_t m_writes{0};

  /** Rows stored by this flush. */
  uint64_t m_rows{0};

  /** F after the flush. */
  batch_seq_t m_flushed_seq{0};

  /** WAL segments deleted after the flush. */
  uint64_t m_segments_removed{0};

  /** WAL cleanup failure after storage succeeded; safe to retry. */
  db_status_t m_cleanup_status;
};

/** Open a database directory: create its WAL and storage if there are none,
otherwise load the storage and replay the WAL records after F.
@param[in]   dir      database directory; created if absent, parent must exist
@param[in]   options  settings, or NULL for the defaults
@param[out]  out      open database, or NULL on failure
@return DB_SUCCESS, DB_LOCKED, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR,
  DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
db_status_t db_open(const char *dir, const db_options_t *options, db **out);

/** Close a database. No call may be in progress.
@param[in]  database  database; may be NULL */
void db_close(db *database);

/** A query being read chunk by chunk. */
class db_cursor;

/** Owns one open database and its coordinated operations. */
class db final : private ut_noncopyable {
 public:
  /** Construct an empty, unopened object. */
  db() = default;

  /** Release every resource still owned by the object. */
  ~db();

  /** Open a fresh instance with a validated path and settings. */
  db_status_t open(const char *dir, const db_options_t &options);

  /** Stop background work and release resources. No call or cursor may remain.
   */
  void close();

  /** Write a batch of points to a table; thread-safe. Returns once the write is
  logged and visible to snapshots; m_wal_sync also makes it durable.
  @param[out]  seq  sequence number of the write; set on DB_SUCCESS and
                    DB_OUTCOME_UNKNOWN, otherwise 0
  @retval DB_SUCCESS logged and visible
  @retval DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT, DB_LIMIT_EXCEEDED,
    DB_OUT_OF_MEMORY rejected; nothing written
  @retval DB_FENCED rejected; an earlier write failed, reopen
  @retval DB_ERROR rejected while the memtable was full and its last flush
  failed
  @retval DB_CORRUPTION the reserved plan disagrees with memtable metadata
  @retval DB_OUTCOME_UNKNOWN the write may or may not be durable; do not retry
    blindly; reopen the database */
  db_status_t write(const char *table_name, size_t table_name_length,
                    const ingest_point_t *points, uint32_t n_points,
                    batch_seq_t &seq);

  /** Store every visible, unflushed write in DuckDB in one transaction and
  retire it from the memtable. Thread-safe; one flush runs at a time; writes are
  not blocked.
  @retval DB_SUCCESS stored, or nothing to store
  @retval DB_ERROR, DB_OUT_OF_MEMORY rolled back; nothing changed; may be
    retried
  @retval DB_CORRUPTION stored metadata disagrees with the open state; the
  database must be reopened
  @retval DB_FENCED an earlier failure stopped the database
  @retval DB_OUTCOME_UNKNOWN commit or rollback failed; reopen to determine
  what was stored */
  db_status_t flush(db_flush_info_t &info);

  /** Copy the newest schema of every table, flushed or not, by name.
  Thread-safe. */
  void copy_schemas(std::vector<schema_t> &schemas);

  /** Read the diagnostics of the latest failed background pass, labeled by
  task. Both failures are retained; a repeated rejection does not replace them.
  @return the latest failed pass, or success before any background failure */
  db_status_t background_status();

  /** Read the database counters. */
  void read_stats(db_stats_t &stats);

  /** Start a SELECT, or EXPLAIN of it, over one consistent snapshot, for
  reading its rows chunk by chunk. The statement is planned here and runs during
  the fetches. Thread-safe; each cursor is used by one thread at a time. Until
  the cursor is closed it holds a DuckDB connection, a read transaction and a
  memtable snapshot and S locks on referenced time-series tables. Its selected
  batches stay alive after flushing; unrelated batches may be reclaimed
  independently. Sequential use and close may move between threads. Close every
  cursor before db_close().
  @param[in] cancel optional opening cancellation, alive until this call returns
  @param[out]  out  open cursor, or NULL on failure
  @return DB_SUCCESS, DB_INVALID_ARGUMENT for a refused statement or result
    type, DB_INTERRUPTED when opening is cancelled, or a DuckDB failure */
  db_status_t cursor_open(const std::string &sql, bool explain, db_cursor **out,
                          query_cancel *cancel = nullptr);

  /** Show DuckDB's plan for a SELECT, as a cursor would run it.
  @param[out]  plan  plan text; empty on failure
  @return DB_SUCCESS or the error from planning or fetching the EXPLAIN result
  */
  db_status_t explain(const std::string &sql, std::string &plan);

 private:
  /** Drop the stored partitions whose days have all passed the retention
  period, measured back from a given time. The background thread calls this with
  the current time after each flush. Thread-safe; does nothing without a
  retention period.
  @retval DB_SUCCESS dropped, or nothing to drop
  @retval DB_ERROR rolled back; nothing changed
  @retval DB_FENCED an earlier failure stopped the database
  @retval DB_OUTCOME_UNKNOWN commit or rollback failed; the database must be
  reopened
  */
  db_status_t expire(int64_t now_us, uint64_t &removed);

#ifdef PT_TESTING
  /** Test-only access is implemented outside the production library. */
  friend class db_test_access;
#endif /* PT_TESTING */

  /** State retained while one write plans, queues and commits. */
  class write_context;

  /** Owns a cursor until opening transfers it to the caller. */
  class cursor_guard;

  /** Replay one recovered WAL payload into the memtable. */
  static db_status_t replay(void *context, batch_seq_t seq,
                            const unsigned char *payload, size_t len);

  /** Acquire this instance's database directory lock. */
  db_status_t lock_dir(const char *dir);

  /** Cursors coordinate their snapshots with this database. */
  friend class db_cursor;

  /** Table-lock acquisition checks failure while waiting. */
  friend class db_table_lock;

  /** Create the memtable at F and restore the current column directories. */
  db_status_t restore_memtable();

  /** Open or create the WAL, replay writes after F and verify its boundaries.
  The memtable must already contain the stored column directories. */
  db_status_t open_wal(const char *dir);

  /** Register recovered time-series schemas and check their names against
  DuckDB. User-table names are inspected temporarily, never cached as database
  state.
  @return DB_SUCCESS, DB_OUT_OF_MEMORY, DB_CORRUPTION or a catalog query error
  */
  db_status_t restore_tables();

  /** Find a table's stable entry and acquire its requested mode.
  @param[in] name normalized table name
  @param[in,out] lock empty guard; owns the table lock on success
  @param[in] cancel optional query cancellation, alive until this call returns
  @return DB_SUCCESS, DB_FENCED, DB_INTERRUPTED or DB_OUT_OF_MEMORY */
  db_status_t lock_table(const std::string &name, db_table_lock_mode_t mode,
                         db_table_lock &lock,
                         const query_cancel *cancel = nullptr);

  /** Start background work after storage, WAL and scans are ready. */
  db_status_t start_flusher();

  /** Reject new work after a database failure, retaining its diagnostic.
  @return DB_SUCCESS or DB_FENCED with the first recorded cause */
  db_status_t check_status_low() const;

  /** Record the first fatal failure and stop new writes and publication.
  @param[in]  status  failure; later calls preserve the first cause */
  void fail_low(const db_status_t &status);

  /** Record the first fatal failure and stop new writes and publication.
  @param[in]  status  failure; later calls preserve the first cause */
  void fail(const db_status_t &status);

  /** Check the database failure state under the write lock.
  @return DB_SUCCESS or DB_FENCED with the first recorded cause */
  db_status_t check_status();

  /** Reject a user table using DuckDB's current catalog while the name is
  S-locked. The common existing time-series path needs no catalog connection.
  @return DB_SUCCESS, DB_INVALID_ARGUMENT or a connection/catalog error */
  db_status_t check_write_target(const db_table_t &table,
                                 const std::string &name);

  /** Wait for memtable capacity, waking the background flusher as needed.
  @return DB_SUCCESS if admission is allowed, DB_FENCED, or the last flush error
    if the memtable is still full */
  db_status_t wait_for_memtable_space();

  /** Refuse points older than the retention period; they would only be
  dropped again.
  @return DB_SUCCESS or DB_INVALID_ARGUMENT */
  db_status_t check_retention(const ingest_point_t *points,
                              uint32_t n_points) const;

  /** Reserve memory and enqueue one write under the database write lock.
  Every accepted write must reach commit_write() to release its queue node.
  @return DB_SUCCESS, a validation/allocation error before enqueue, or DB_FENCED
*/
  db_status_t reserve_write(write_context &w);

  /** Select a queue prefix within the group limit, always including its head.
  The write mutex must be held; records borrow the queued payloads. */
  void build_write_group(std::vector<wal_record_t> &records);

  /** Complete the selected prefix after the WAL call stops borrowing it. */
  void complete_writes(size_t count, const db_status_t &status,
                       bool keep_sequence);

  /** Publish or fail only the active WAL group; the caller wakes the next head.
  Active WAL I/O has returned. */
  void finish_write_group(size_t count, batch_seq_t last_seq,
                          const db_status_t &status);

  /** Wait for the group commit of a reserved write, then make it visible.
  @param[in,out]  seq  sequence number of the write; reset to 0 if the write
                       was certainly not written
  @retval DB_SUCCESS logged and visible
  @retval DB_OUTCOME_UNKNOWN the write may or may not be durable; the database
  must be reopened
  @return otherwise the error of a write that was certainly not written */
  db_status_t commit_write(write_context &w, batch_seq_t &seq);

  /** Reclaim stored WAL segments even when a flush has no new batches. A failed
  deletion or directory sync is retried on the next flush. */
  void purge_wal(db_flush_info_t &info);

  /** Store every visible, unflushed write.
  @return DB_SUCCESS, a rolled-back preparation error, DB_CORRUPTION, DB_FENCED,
    or DB_OUTCOME_UNKNOWN after commit or rollback cannot be confirmed */
  db_status_t flush_low(db_flush_info_t &info);

  /** Wait until the next round is due.
  @param[in,out]  lock  holds db.m_flusher_mutex
  @return false if the flusher must stop */
  bool flusher_wait(std::unique_lock<std::mutex> &lock);

  /** Flush, expire old partitions, and record the round's result.
  REQUIRES: db.m_flusher_mutex is not held. */
  void flusher_flush_and_expire();

  /** Background flusher: flush every interval, or when woken.
  @return NULL */
  static void *flusher_main(void *arg);

  /** Reuse metadata for F, or read it in the cursor's own transaction.
  @return DB_SUCCESS or the catalog query/validation error */
  db_status_t query_catalog(duckdb_connection conn, batch_seq_t flushed_seq,
                            std::shared_ptr<const storage_catalog_t> &out);

  /** Allocate a cursor with a DuckDB connection of its own, owned by a guard.
  @return DB_SUCCESS, DB_OUT_OF_MEMORY or DB_ERROR */
  db_status_t cursor_create(cursor_guard &owner, query_cancel *cancel);

  /** Find known time-series sources and acquire their shared locks by name.
  @param[in,out] locks empty on entry; retains acquired locks, even on failure
  @param[in] cancel optional query cancellation, alive until this call returns
  @return DB_SUCCESS, DB_FENCED, DB_INTERRUPTED or DB_OUT_OF_MEMORY */
  db_status_t lock_query_tables(const std::vector<std::string> &names,
                                db_table_locks &locks,
                                const query_cancel *cancel);

  /* Immutable after db_open() returns. */

  /** Holds the directory's lock. */
  os_file_t m_lock_file{OS_FILE_CLOSED};

  /** The write-ahead log. */
  wal *m_wal{nullptr};

  /** Writes after F and every table's schema. */
  memtable *m_mt{nullptr};

  /** The DuckDB file with the writes up to F. */
  storage *m_storage{nullptr};

  /** Settings given to db_open(), checked. */
  db_options_t m_options;

  /** Background flusher thread. */
  pthread_t m_flusher_thread{};

  /** m_flusher_thread runs. */
  bool m_flusher_started{false};

  /* Protected by m_write_mutex. */

  /** Orders database queueing, memtable reservation and group publication.
  Taken after table S/X locks and before WAL and memtable locks, and never held
  while waiting for a group commit. */
  ut_mutex_t m_write_mutex;

  /** Database status; initially successful, then retains the first fatal error.
  A failure rejects new writes, queries and maintenance until reopen;
  existing cursor snapshots remain readable. */
  db_status_t m_db_status;

  /** Next database-assigned write sequence, restored from recovery. */
  batch_seq_t m_next_seq{1};

  /** Caller-owned writes in order; the front writer becomes group leader. */
  std::deque<write_context *> m_writes;

  /** A WAL operation still borrows the selected queue prefix. */
  bool m_write_active{false};

#ifdef PT_TESTING
  /** Called outside the write lock after selecting an active WAL group. */
  void (*m_test_before_wal)(void *){nullptr};

  /** Context borrowed by the test WAL callback. */
  void *m_test_wal_context{nullptr};

  /** One-shot checkpoint before a full-memtable writer begins waiting. */
  void (*m_test_before_space_wait)(db &){nullptr};
#endif /* PT_TESTING */

  /** Stable table objects, owned until close; lookup releases the cache lock
  before any table S/X acquisition. */
  db_tables m_tables;

  /* Protected by m_catalog_snapshot_mutex. */

  /** Guards the cached reader catalog; never held across DuckDB calls.
  Taken after m_snapshot_mutex when both are needed. */
  std::mutex m_catalog_snapshot_mutex;

  /** Immutable metadata read in a query transaction, matched by F.
  Expiry clears it because deleting partitions does not change F. */
  std::shared_ptr<const storage_catalog_t> m_catalog_snapshot;

  /* Locks that protect work, not fields. */

  /** Allows one flush or expiry transaction at a time. Taken before
  m_snapshot_mutex and m_write_mutex. */
  ut_mutex_t m_flush_mutex;

  /** Held shared while a cursor fixes its snapshot, and exclusively during
  partition deletion. Taken after m_flush_mutex and
  before m_write_mutex. */
  ut_shared_mutex_t m_snapshot_mutex;

  /* Protected by m_flusher_mutex, a leaf lock. */

  /** Protects the fields below. */
  std::mutex m_flusher_mutex;

  /** Wakes the flusher early. */
  std::condition_variable m_flusher_wake_cv;

  /** Wakes writers waiting for room after a flush. */
  std::condition_variable m_memtable_space_cv;

  /** Changes after flush completion or the first fatal error. */
  uint64_t m_progress_epoch{0};

  /** close() asks the flusher to exit. */
  bool m_flusher_stop{false};

  /** Someone asked the flusher to flush now. */
  bool m_flusher_wake{false};

  /** Historical diagnostic only; success does not erase the last failure. */
  db_status_t m_last_background_error;

  /** Last flush result, replaced on every attempt including success.
  Capacity waiters use it only while the database is healthy. */
  db_status_t m_last_flush_status{DB_SUCCESS};

  /** Counters; m_wal_segment_count is read from the WAL when asked for. */
  db_stats_t m_stats;
};

/** Owns a query transaction, snapshot and table locks. */
class db_cursor final : private ut_noncopyable {
 public:
  /** Construct an empty, unopened object. */
  db_cursor() = default;

  /** Release every resource still owned by the object. */
  ~db_cursor();

  /** Request interruption of an active cursor from any thread while it is
  open. The running or next fetch observes the request; after it returns
  DB_INTERRUPTED, every later fetch does too. Normal end is reached when
  db_cursor::fetch() returns DB_SUCCESS with empty rows; interrupting after
  that leaves later fetches successful and empty. Do not call concurrently with
  db_cursor::destroy(). */
  void interrupt();

  /** Read the column metadata of a cursor.
  @return the result columns of a cursor */
  const std::vector<query_column_t> &columns() const;

  /** Read the next rows of a cursor, at most QUERY_CHUNK_ROWS.
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

  /** Acquire shared snapshot access, observing cancellation during the wait.
  @param[in,out] lock deferred guard associated with the database snapshot mutex
  @return DB_SUCCESS with the lock held, or DB_INTERRUPTED without ownership */
  db_status_t lock_snapshot(std::shared_lock<ut_shared_mutex_t> &lock);

  /** Take the snapshot of a cursor: the DuckDB transaction at F and the
  memtable writes after F. A deletion of stored data waits until it is fixed.
  @return DB_SUCCESS, DB_FENCED, DB_CORRUPTION or a DuckDB snapshot error */
  db_status_t take_snapshot();

  /** Begin the read transaction and fix its stored sequence boundary. */
  db_status_t begin_snapshot(batch_seq_t &flushed_seq);

  /** Combine stored metadata with this cursor's pinned memtable snapshot. */
  void build_tables(const storage_catalog_t &catalog);

  /** Every managed source in this snapshot was locked before fixing it. */
  bool tables_locked(const std::vector<std::string> &names) const;

  /** Database the cursor reads. */
  db *m_db{nullptr};

  /** Connection running the snapshot transaction. */
  duckdb_connection m_conn{nullptr};

  /** Cancellation borrowed only until cursor_open() returns. */
  query_cancel *m_cancel{nullptr};

  /** Memtable writes after F. */
  memtable_snapshot_t m_memtable_snapshot;

  /** What the query sees; the query registry points here. */
  query_snapshot_t m_snapshot;

  /** The running statement; NULL until it is opened. */
  query_cursor *m_query{nullptr};

  /** Time-series S locks released only after cursor resources are stopped. */
  db_table_locks m_table_locks;
};

#endif /* db_h */
