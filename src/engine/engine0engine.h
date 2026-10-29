/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * engine0engine.h
 *    The write path: ingest, WAL and memtable together.
 *
 * A write is planned against the newest schema, encoded, queued in the
 * WAL, and reserved in the memtable; then it waits for its group commit
 * and becomes visible. Planning and encoding run without the engine lock,
 * so concurrent writes plan in parallel. Queuing and reserving happen under
 * the lock, so the WAL order and the memtable order are the same and every
 * schema change is decided against the schema just before it. A schema
 * change planned against a version that another write has moved past is
 * planned again under the lock. Waiting for the flush happens without the
 * lock, so group commit can gather many writes.
 *
 * engine_flush() stores the visible writes in DuckDB and then retires them
 * from the memtable. It does not take the write lock, so writes go on while
 * it runs. Opening a directory loads the stored tables and F, the last
 * stored write, and replays only the WAL records after F.
 *
 * Between flush-and-expiry passes, the background thread waits up to
 * flush_interval_ms unless woken earlier by flush_bytes. Flush work and
 * scheduling add to elapsed time. Each flush also deletes WAL segments that
 * hold only stored writes. With the background thread, new writers wait
 * while unretired column data is at or above memtable_limit_bytes; 0 disables
 * waiting. This throttles writes, without bounding total process memory.
 *
 * With a retention period, writes older than it are rejected, and after
 * each background flush the stored partitions whose days have all passed
 * it are dropped whole.
 *
 * engine_drop_table() pauses new writes, waits for the queued ones to
 * become visible, flushes them all and then drops the stored table and its
 * schema. Since every write to the table is then at or before F, recovery
 * never replays one, and the WAL needs no record of the drop.
 *
 * A cursor runs SQL over a consistent snapshot: the stored data at F
 * and the memtable writes F < seq <= S. Readers pin retirement generations
 * before starting their DuckDB transaction, read its metadata, then collect
 * the matching live and retired hot batches. COMMIT holds no reader gate.
 *
 * Expiry and table-deletion transactions change stored data without moving
 * F. A table drop first flushes pending writes, which may advance F; only
 * its later deletion transaction runs under an odd drop epoch. Expiry also
 * runs under an odd epoch. A reader whose snapshot crosses either deletion
 * transaction takes its snapshot again.
 *
 * Lock order: flush_mutex, then write_mutex, then the WAL
 * and memtable mutexes. The background lock flusher_mutex is a leaf.
 *
 *-------------------------------------------------------------------------
 */

#ifndef engine0engine_h
#define engine0engine_h

#include <cstddef>
#include <string>
#include <vector>

#include "ingest0ingest.h"
#include "memtable0memtable.h"
#include "query0query.h"
#include "storage0storage.h"
#include "ut0err.h"
#include "ut0ut.h"
#include "wal0wal.h"

/** Longest retention period, in days: about 300 years. */
#define ENGINE_MAX_RETENTION_DAYS 110000

/** An open database directory. */
struct engine_t;

/** Settings of an engine. */
struct engine_options_t {
  /** Flush in a background thread. Without it, callers use engine_flush();
  dropping a table also flushes before deletion. Writers never wait for
  room when background flushing is disabled. */
  bool background_flush{true};

  /** Idle wait between background passes, in milliseconds; must be greater
  than 0 with background_flush. Work and scheduling add to elapsed time. */
  uint32_t flush_interval_ms{200};

  /** Wake the background flusher once unretired column data reaches this
  many bytes. */
  uint64_t flush_bytes{32 * 1024 * 1024};

  /** With background flushing, new writers wait while unretired column data
  is at or above this many bytes; 0 disables waiting. Excludes planning buffers
  and retired batches held by snapshots. */
  uint64_t memtable_limit_bytes{256 * 1024 * 1024};

  /** Size at which the WAL starts a new segment. */
  uint64_t wal_segment_bytes{WAL_DEFAULT_SEGMENT_BYTES};

  /** Days covered by one time partition, 1 to STORAGE_MAX_PARTITION_DAYS.
  Used when the directory is created; an existing one keeps its own. */
  uint32_t partition_days{STORAGE_DEFAULT_PARTITION_DAYS};

  /** Keep this many days before the current time, at most
  ENGINE_MAX_RETENTION_DAYS; 0 keeps everything.
  Older writes are rejected, and stored partitions that hold only older
  times are dropped. A partition is dropped whole, so up to partition_days
  more are kept. */
  uint32_t retention_days{0};
};

/** Counters of an engine. */
struct engine_stats_t {
  /** Background flushes that stored something. */
  uint64_t background_flushes{0};

  /** Background flushes or expiries that failed. */
  uint64_t background_errors{0};

  /** Writes that had to wait for room in the memtable. */
  uint64_t throttled_writes{0};

  /** WAL segment files now. */
  uint64_t wal_segments{0};

  /** Partitions dropped for being past the retention period. */
  uint64_t expired_partitions{0};
};

/** What opening a directory found. */
struct engine_recv_t {
  /** Last write stored in DuckDB (F). */
  batch_seq_t flushed_seq{0};

  /** Valid records in the WAL, stored ones included. */
  uint64_t wal_records{0};

  /** Records after F, replayed into the memtable. */
  uint64_t replayed{0};
};

/** What a flush did. */
struct engine_flush_info_t {
  /** Writes stored by this flush. */
  uint64_t writes{0};

  /** Rows stored by this flush. */
  uint64_t rows{0};

  /** F after the flush. */
  batch_seq_t flushed_seq{0};

  /** WAL segments deleted after the flush. */
  uint64_t segments_removed{0};
};

/** Open a database directory: create its WAL and storage if there are none,
otherwise load the storage and replay the WAL records after F.
@param[in]   dir      existing directory
@param[in]   options  settings, or NULL for the defaults
@param[out]  engine   open engine, or NULL on failure
@param[out]  recv     what recovery found; may be NULL
@param[out]  error    message on failure
@return DB_SUCCESS, DB_LOCKED, DB_CORRUPTION, DB_IO_ERROR, DB_ERROR,
  DB_INVALID_ARGUMENT or DB_OUT_OF_MEMORY */
dberr_t engine_open(const char *dir, const engine_options_t *options,
                    engine_t **engine, engine_recv_t *recv, std::string &error);

/** Close an engine. No call may be in progress.
@param[in]  engine  engine; may be NULL
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t engine_close(engine_t *engine);

/** Write a batch of points to a table; thread-safe. Returns once the write is
durable and visible to snapshots.
@param[in]   engine     engine
@param[in]   table      table name as given by the user
@param[in]   table_len  length of table
@param[in]   points     the points
@param[in]   n_points   number of points
@param[out]  seq        sequence number of the write; set on DB_SUCCESS and
                        DB_OUTCOME_UNKNOWN, otherwise 0
@param[out]  error      why a write was rejected
@retval DB_SUCCESS durable and visible
@retval DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT, DB_LIMIT_EXCEEDED,
  DB_OUT_OF_MEMORY rejected; nothing written
@retval DB_FENCED rejected; an earlier write failed, reopen
@retval DB_OUTCOME_UNKNOWN the write may or may not be durable; do not retry
  blindly; the engine is now fenced */
dberr_t engine_write(engine_t &engine, const char *table, size_t table_len,
                     const ingest_point_t *points, uint32_t n_points,
                     batch_seq_t &seq, std::string &error);

/** Store every visible, unflushed write in DuckDB in one transaction and
retire it from the memtable. Thread-safe; one flush runs at a time; writes are
not blocked.
@param[in]   engine  engine
@param[out]  info    what was stored
@param[out]  error   message on failure
@retval DB_SUCCESS stored, or nothing to store
@retval DB_ERROR, DB_OUT_OF_MEMORY rolled back; nothing changed; may be
  retried
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN the commit failed; the engine is fenced and
  reopening finds out what was stored */
dberr_t engine_flush(engine_t &engine, engine_flush_info_t &info,
                     std::string &error);

/** Drop the stored partitions whose days have all passed the retention
period, measured back from a given time. The background thread calls this with
the current time after each flush. Thread-safe; does nothing without a
retention period.
@param[in]   engine   engine
@param[in]   now_us   the time the retention period ends at
@param[out]  removed  partitions dropped
@param[out]  error    message on failure
@retval DB_SUCCESS dropped, or nothing to drop
@retval DB_ERROR rolled back; nothing changed
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN the commit failed; the engine is fenced */
dberr_t engine_expire(engine_t &engine, int64_t now_us, uint64_t &removed,
                      std::string &error);

/** Drop a table: every write to it, stored or not, and its schema. Writes to
every table wait while it runs; queries whose snapshot was taken before still
read the table. A later write of the name creates a new table. Thread-safe.
@param[in]   engine     engine
@param[in]   table      table name as given by the user
@param[in]   table_len  length of table
@param[out]  error      message on failure
@retval DB_SUCCESS dropped and durable
@retval DB_INVALID_ARGUMENT not a valid table name
@retval DB_NOT_FOUND no such table
@retval DB_ERROR, DB_OUT_OF_MEMORY not dropped; may be retried
@retval DB_FENCED an earlier failure fenced the engine
@retval DB_OUTCOME_UNKNOWN the commit failed; the engine is fenced */
dberr_t engine_drop_table(engine_t &engine, const char *table, size_t table_len,
                          std::string &error);

/** Copy the newest schema of every table, flushed or not, by name.
Thread-safe.
@param[in]   engine  engine
@param[out]  tables  the schemas */
void engine_list_tables(engine_t &engine, std::vector<schema_t> &tables);

/** A query being read chunk by chunk. */
struct engine_cursor_t;

/** Start a SELECT, or EXPLAIN of it, over one consistent snapshot, for
reading its rows chunk by chunk. The statement is planned here and runs during
the fetches. Thread-safe; each cursor is used by one thread at a time. Until
the cursor is closed it holds a DuckDB connection, a read transaction and a
memtable snapshot: its selected batches stay alive after flushing; unrelated
batches may be reclaimed independently. Close every cursor before
engine_close().
@param[in]   engine   engine
@param[in]   sql      a single SELECT; see query0query.h
@param[in]   explain  explain it instead of running it
@param[out]  cursor   open cursor, or NULL on failure
@param[out]  error    reason on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT for a refused statement or result
  type, DB_ERROR for a SQL error, or DB_OUT_OF_MEMORY */
dberr_t engine_cursor_open(engine_t &engine, const std::string &sql,
                           bool explain, engine_cursor_t **cursor,
                           std::string &error);

/** Read the column metadata of a cursor.
@param[in]  cursor  cursor whose result metadata is read
@return the result columns of a cursor */
const std::vector<query_column_t> &engine_cursor_columns(
    const engine_cursor_t &cursor);

/** Read the next rows of a cursor, at most QUERY_CHUNK_ROWS.
@param[in,out]  cursor  query cursor
@param[out]     rows    the rows; empty once every row was read
@param[out]     error   reason for failure
@return as query_fetch() */
dberr_t engine_cursor_fetch(engine_cursor_t &cursor, query_rows_t &rows,
                            std::string &error);

/** Request interruption of an active cursor from any thread while it is
open. The running or next fetch observes the request; after it returns
DB_INTERRUPTED, every later fetch does too. Normal end is reached when
engine_cursor_fetch() returns DB_SUCCESS with empty rows; interrupting after
that leaves later fetches successful and empty. Do not call concurrently with
engine_cursor_close().
@param[in]  cursor  cursor */
void engine_cursor_interrupt(engine_cursor_t &cursor);

/** Stop a query and free its cursor.
@param[in]  cursor  cursor; may be NULL */
void engine_cursor_close(engine_cursor_t *cursor);

/** Show DuckDB's plan for a SELECT, as a cursor would run it.
@param[in,out]  engine  database engine
@param[in]      sql     SQL statement to check or execute
@param[out]     plan    plan text
@param[out]     error   reason for failure
@return DB_SUCCESS or the error from planning or fetching the EXPLAIN result */
dberr_t engine_explain(engine_t &engine, const std::string &sql,
                       std::string &plan, std::string &error);

/** Read the engine counters.
@param[in,out]  engine  database engine
@param[out]     stats   counter snapshot */
void engine_get_stats(engine_t &engine, engine_stats_t &stats);

/** Read the memtable counters.
@param[in,out]  engine  database engine
@param[out]     stats   counter snapshot */
void engine_get_memtable_stats(engine_t &engine, memtable_stats_t &stats);

/** Borrow the engine's memtable for diagnostics.
@param[in,out]  engine  database engine
@return the memtable, for tests and diagnostics */
memtable_t *engine_get_memtable(engine_t &engine);

/** Borrow the engine's storage for diagnostics.
@param[in,out]  engine  database engine
@return the storage, for tests and diagnostics; only while no flush runs */
storage_t *engine_get_storage(engine_t &engine);

#ifdef PT_DEBUG
/** Make the next WAL flush fail. Debug builds only; used by tests.
@param[in]  engine  engine */
void engine_dbg_fail_next_flush(engine_t &engine);

/** Make the next storage flush fail before it commits. Debug builds only;
used by tests.
@param[in]  engine  engine */
void engine_dbg_fail_next_store(engine_t &engine);
#endif

#endif
