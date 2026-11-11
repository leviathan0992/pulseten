/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine.h
 *    The write path: ingest, WAL and memtable together.
 *
 * A write is planned against the newest schema, encoded, queued in the
 * WAL, and reserved in the memtable; then it waits for its group commit
 * and becomes visible. Planning and encoding run without the engine lock,
 * so concurrent writes plan in parallel. Queuing and reserving happen under
 * the lock, so the WAL order and the memtable order are the same and every
 * schema change is decided against the schema just before it. A schema
 * change planned against a version that another write has moved past is
 * planned again under the lock. Waiting for the group commit happens
 * without the lock, so it can gather many writes.
 *
 * A write becomes visible after its WAL group is written. With WAL sync
 * enabled, the group is also durable before publication. Otherwise a flush
 * syncs the WAL before storing visible writes, so durable F never passes the
 * durable WAL prefix.
 *
 * engine_flush() stores the visible writes in DuckDB and then retires them
 * from the memtable. It does not take the write lock, so writes go on while
 * it runs. Opening a directory loads the stored tables and F, the last
 * stored write, and replays only the WAL records after F.
 *
 * Between flush-and-expiry passes, the background thread waits up to
 * flush_interval_ms unless a writer waiting for memtable capacity wakes it.
 * Flush work and scheduling add to elapsed time. Each flush also deletes WAL
 * segments that hold only stored writes. New writers wait while unretired
 * column data is at or above memtable_limit_bytes;
 * 0 disables waiting. This throttles writes, without bounding total process
 * memory.
 * A full memtable returns the latest flush error instead of waiting for a
 * failed flush indefinitely. A successful flush clears that error.
 *
 * With a retention period, writes older than it are rejected, and after
 * each background flush the stored partitions whose days have all passed
 * it are dropped whole.
 *
 * Managed time-series tables cannot be dropped through engine_drop_table().
 * Retention removes expired partitions while preserving each table's schema.
 * engine_exec_ddl() creates and drops ordinary tables. Ordinary DDL uses DuckDB
 * transactions, preserves F, and reserves its target name against concurrent
 * time-series writes. CTAS may read the same snapshot as SELECT.
 *
 * A cursor runs SQL over a consistent snapshot: the stored data at F
 * and the memtable writes F < seq <= S. Readers reference every visible
 * memtable batch before starting their DuckDB transaction, read its metadata,
 * then keep the batches after F. Flush COMMIT holds no reader gate.
 *
 * Expiry holds snapshot_mutex exclusively while deleting stored partitions.
 * Readers hold it shared while fixing their snapshot, so no snapshot spans a
 * partition deletion; snapshots fixed earlier keep their DuckDB MVCC view.
 * Ordinary DDL commit also holds snapshot_mutex exclusively, so a reader
 * cannot combine an earlier memtable view with a later ordinary-table view.
 * Ordinary table names and the active DDL target use catalog_mutex: name
 * checks hold it shared, and DDL changes hold it exclusively. Ordinary DDL
 * reserves its target under write_mutex and catalog_mutex. Its execution
 * holds no write, catalog, flush or snapshot lock; only commit and catalog
 * updates hold flush_mutex, snapshot_mutex and write_mutex. Writes check names
 * before planning and again under write_mutex before queuing in the WAL.
 *
 * Lock order: ddl_mutex, then flush_mutex, then snapshot_mutex, then
 * write_mutex, then catalog_mutex, then the WAL and memtable mutexes.
 * The background lock flusher_mutex is a leaf.
 *
 *-------------------------------------------------------------------------
 */

#ifndef engine_h
#define engine_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ingest.h"
#include "memtable.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "wal.h"

/** Longest retention period, in days: about 300 years. */
#define ENGINE_MAX_RETENTION_DAYS 110000

/** An open database directory. */
struct engine_t;

/** Settings of an engine. */
struct engine_options_t {
  /** Idle wait between background passes, in milliseconds; must be greater
  than 0. Work and scheduling add to elapsed time; writers waiting for memtable
  capacity may wake the thread sooner. */
  uint32_t m_flush_interval_ms{200};

  /** New writers wait while unretired column data is at or above this many
  bytes; 0 disables waiting. Excludes planning buffers
  and retired batches held by snapshots. */
  uint64_t m_memtable_limit_bytes{256 * 1024 * 1024};

  /** Size at which the WAL starts a new segment. */
  uint64_t m_wal_segment_bytes{WAL_DEFAULT_SEGMENT_BYTES};

  /** Days covered by one time partition, 1 to STORAGE_MAX_PARTITION_DAYS.
  Used when the directory is created; an existing one keeps its own. */
  uint32_t m_partition_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Keep this many days before the current time, at most
  ENGINE_MAX_RETENTION_DAYS; 0 keeps everything.
  Older writes are rejected, and stored partitions that hold only older
  times are dropped. A partition is dropped whole, so up to m_partition_days
  more are kept. */
  uint32_t m_retention_days{0};

  /** Sync each WAL group before acknowledging its writes. When false,
  acknowledged writes may be lost on a system crash or power loss. */
  bool m_wal_sync{false};
};

/** Counters of an engine. */
struct engine_stats_t {
  /** Background flushes that stored something. */
  uint64_t m_background_flushes{0};

  /** Background passes with a failed flush or expiry; at most one per pass. */
  uint64_t m_background_errors{0};

  /** Writes that had to wait for room in the memtable. */
  uint64_t m_throttled_writes{0};

  /** WAL segment files now. */
  uint64_t m_wal_segments{0};

  /** Partitions dropped for being past the retention period. */
  uint64_t m_expired_partitions{0};
};

/** What a flush did. */
struct engine_flush_info_t {
  /** Writes stored by this flush. */
  uint64_t m_writes{0};

  /** Rows stored by this flush. */
  uint64_t m_rows{0};

  /** F after the flush. */
  batch_seq_t m_flushed_seq{0};

  /** WAL segments deleted after the flush. */
  uint64_t m_segments_removed{0};
};

/** Open a database directory: create its WAL and storage if there are none,
otherwise load the storage and replay the WAL records after F.
@param[in]   dir      database directory; created if absent, parent must exist
@param[in]   options  settings, or NULL for the defaults
@param[out]  out      open engine, or NULL on failure
@return DB_SUCCESS, DB_LOCKED, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR,
  DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
db_status_t engine_open(const char *dir, const engine_options_t *options,
                        engine_t **out);

/** Close an engine. No call may be in progress.
@param[in]  engine  engine; may be NULL
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t engine_close(engine_t *engine);

/** Write a batch of points to a table; thread-safe. Returns once the write is
logged and visible to snapshots; m_wal_sync also makes it durable.
@param[out]  seq  sequence number of the write; set on DB_SUCCESS and
                  DB_OUTCOME_UNKNOWN, otherwise 0
@retval DB_SUCCESS logged and visible
@retval DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT, DB_LIMIT_EXCEEDED,
  DB_OUT_OF_MEMORY rejected; nothing written
@retval DB_FENCED rejected; an earlier write failed, reopen
@retval DB_ERROR rejected while the memtable was full and its last flush failed
@retval DB_CORRUPTION rejected while the memtable was full and its last flush
  detected inconsistent metadata
@retval DB_OUTCOME_UNKNOWN the write may or may not be durable; do not retry
  blindly; the engine is now fenced */
db_status_t engine_write(engine_t &engine, const char *table, size_t table_len,
                         const ingest_point_t *points, uint32_t n_points,
                         batch_seq_t &seq);

/** Store every visible, unflushed write in DuckDB in one transaction and
retire it from the memtable. Thread-safe; one flush runs at a time; writes are
not blocked.
@retval DB_SUCCESS stored, or nothing to store
@retval DB_ERROR, DB_OUT_OF_MEMORY rolled back; nothing changed; may be
  retried
@retval DB_CORRUPTION stored metadata disagrees with the open state; the engine
  is fenced
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN commit or rollback failed; the engine is fenced and
  reopening finds out what was stored */
db_status_t engine_flush(engine_t &engine, engine_flush_info_t &info);

/** Drop the stored partitions whose days have all passed the retention
period, measured back from a given time. The background thread calls this with
the current time after each flush. Thread-safe; does nothing without a
retention period.
@retval DB_SUCCESS dropped, or nothing to drop
@retval DB_ERROR rolled back; nothing changed
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN commit or rollback failed; the engine is fenced */
db_status_t engine_expire(engine_t &engine, int64_t now_us, uint64_t &removed);

/** Drop an ordinary table. Managed time-series tables are rejected without
flushing or changing their data or schemas. Thread-safe.
@retval DB_SUCCESS dropped and durable
@retval DB_INVALID_ARGUMENT invalid name, or a managed time-series table
@retval DB_NOT_FOUND no such table
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_ERROR DuckDB refused the drop
@retval DB_OUT_OF_MEMORY allocation failed
@retval DB_CORRUPTION stored metadata failed validation
@retval DB_OUTCOME_UNKNOWN the commit failed; the engine is fenced */
db_status_t engine_drop_table(engine_t &engine, const char *table,
                              size_t table_len);

/** Execute one CREATE TABLE or DROP TABLE for an ordinary table in main.
CREATE TABLE AS SELECT sees the same consistent snapshot as a query. Managed
time-series tables and internal objects cannot be changed. Other DDL waits;
writes and flushes may continue until commit. Existing query snapshots remain
valid.
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_NOT_FOUND, DB_ERROR,
  DB_OUT_OF_MEMORY, DB_CORRUPTION, DB_FENCED or DB_OUTCOME_UNKNOWN */
db_status_t engine_exec_ddl(engine_t &engine, const std::string &sql);

/** Copy the newest schema of every table, flushed or not, by name.
Thread-safe. */
void engine_list_schemas(engine_t &engine, std::vector<schema_t> &tables);

/** A query being read chunk by chunk. */
struct engine_cursor_t;

/** Start a SELECT, or EXPLAIN of it, over one consistent snapshot, for
reading its rows chunk by chunk. The statement is planned here and runs during
the fetches. Thread-safe; each cursor is used by one thread at a time. Until
the cursor is closed it holds a DuckDB connection, a read transaction and a
memtable snapshot: its selected batches stay alive after flushing; unrelated
batches may be reclaimed independently. Close every cursor before
engine_close().
@param[out]  out  open cursor, or NULL on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT for a refused statement or result
  type, DB_ERROR for a SQL error, or DB_OUT_OF_MEMORY */
db_status_t engine_cursor_open(engine_t &engine, const std::string &sql,
                               bool explain, engine_cursor_t **out);

/** Read the column metadata of a cursor.
@return the result columns of a cursor */
const std::vector<query_column_t> &engine_cursor_columns(
    const engine_cursor_t &cursor);

/** Read the next rows of a cursor, at most QUERY_CHUNK_ROWS.
@param[out]  rows  the rows; empty once every row was read
@return as query_cursor_fetch() */
db_status_t engine_cursor_fetch(engine_cursor_t &cursor, query_rows_t &rows);

/** Request interruption of an active cursor from any thread while it is
open. The running or next fetch observes the request; after it returns
DB_INTERRUPTED, every later fetch does too. Normal end is reached when
engine_cursor_fetch() returns DB_SUCCESS with empty rows; interrupting after
that leaves later fetches successful and empty. Do not call concurrently with
engine_cursor_close(). */
void engine_cursor_interrupt(engine_cursor_t &cursor);

/** Stop a query and free its cursor.
@param[in]  cursor  cursor; may be NULL */
void engine_cursor_close(engine_cursor_t *cursor);

/** Show DuckDB's plan for a SELECT, as a cursor would run it.
@param[out]  plan  plan text; empty on failure
@return DB_SUCCESS or the error from planning or fetching the EXPLAIN result */
db_status_t engine_explain(engine_t &engine, const std::string &sql,
                           std::string &plan);

/** Read what fenced the engine.
@return the first engine/WAL failure, or success if neither is fenced */
db_status_t engine_fence_status(engine_t &engine);

/** Read the diagnostics of the latest failed background pass, labeled by task.
Both failures are retained; a repeated fence does not replace them.
@return the latest failed pass, or success before any background failure */
db_status_t engine_background_status(engine_t &engine);

/** Read the engine counters. */
void engine_get_stats(engine_t &engine, engine_stats_t &stats);

/** Read the memtable counters. */
void engine_get_memtable_stats(engine_t &engine, memtable_stats_t &stats);

#ifdef PT_DEBUG
/** Borrow the memtable for snapshot invariant tests. Debug builds only.
@return the engine's memtable */
memtable_t &engine_dbg_memtable(engine_t &engine);

/** Borrow storage for internal checks and failure injection. Debug builds
only; callers must exclude concurrent flushes and expiry.
@return the engine's storage */
storage_t &engine_dbg_storage(engine_t &engine);

/** Wake the background flusher after preparing a failure. Debug builds only;
used by tests. */
void engine_dbg_wake_flusher(engine_t &engine);

/** Make the next WAL sync fail. Debug builds only; used by tests. */
void engine_dbg_fail_next_wal_sync(engine_t &engine);

/** Make the next storage flush fail before it commits. Debug builds only;
used by tests. */
void engine_dbg_fail_next_flush(engine_t &engine);

/** Enable or disable persistent flush failures before commit. Debug builds
only; serializes with the flusher. */
void engine_dbg_set_flush_failure(engine_t &engine, bool fail);

/** Set a test callback just before DDL execution. Debug builds only.
@param[in]  callback  called without write, flush or snapshot locks; NULL clears
@param[in]  context   borrowed until the callback finishes */
void engine_dbg_set_ddl_callback(engine_t &engine, void (*callback)(void *),
                                 void *context);

/** Set a test callback after pinning the memtable, before fixing stored data.
Call only while no cursor is opening. Debug builds only.
@param[in]  callback  called with snapshot_mutex shared; NULL clears
@param[in]  context   borrowed until the callback finishes */
void engine_dbg_set_snapshot_callback(engine_t &engine,
                                      void (*callback)(void *), void *context);
#endif

#endif
