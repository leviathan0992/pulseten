/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine.h
 *    Coordinates writes, WAL, memtables and DuckDB storage.
 *
 * Writes plan and encode in parallel, then queue in WAL order under
 * write_mutex, revalidating schema changes as needed. They become visible
 * after WAL group commit; m_wal_sync also makes them durable before return.
 *
 * Flush syncs the WAL, stores visible writes in DuckDB, advances the stored
 * sequence F, and retires memtable batches without taking write_mutex.
 * Recovery replays only WAL records after F. The background thread runs
 * flush and retention expiry; memtable pressure can wake it sooner.
 *
 * Queries combine stored data at F with visible memtable writes through S.
 * snapshot_mutex coordinates snapshot creation with expiry and ordinary DDL
 * commit. Ordinary DDL cannot drop managed time-series tables; retention
 * expiry drops their expired partitions while preserving schemas.
 *
 * Lock order: flush_mutex, snapshot_mutex, write_mutex, tables.m_mutex,
 * then WAL and memtable mutexes. flusher_mutex is a leaf.
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
  uint64_t m_wal_segment_size_bytes{WAL_DEFAULT_SEGMENT_BYTES};

  /** Days covered by one time partition, 1 to STORAGE_MAX_PARTITION_DAYS.
  Used when the directory is created; an existing one keeps its own. */
  uint32_t m_partition_duration_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Keep this many days before the current time, at most
  ENGINE_MAX_RETENTION_DAYS; 0 keeps everything.
  Older writes are rejected, and stored partitions that hold only older
  times are dropped. A partition is dropped whole, so up to m_partition_duration_days
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
  uint64_t m_wal_segment_count{0};

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
db_status_t engine_write(engine_t &engine, const char *table_name,
                         size_t table_name_length,
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

/** Execute one CREATE TABLE or DROP TABLE for an ordinary table in main.
CREATE TABLE AS SELECT sees the same consistent snapshot as a query. Managed
time-series tables and internal objects cannot be changed. DDL execution may
run concurrently; DuckDB handles conflicts. Writes and flushes may continue
until commit. Existing query snapshots remain valid.
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_NOT_FOUND, DB_ERROR,
  DB_OUT_OF_MEMORY, DB_CORRUPTION, DB_FENCED or DB_OUTCOME_UNKNOWN */
db_status_t engine_exec_ddl(engine_t &engine, const std::string &sql);

/** Copy the newest schema of every table, flushed or not, by name.
Thread-safe. */
void engine_copy_schemas(engine_t &engine, std::vector<schema_t> &schemas);

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
#endif /* PT_DEBUG */

#endif /* engine_h */
