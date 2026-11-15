/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * pulseten.h
 *    The PulseTen C API.
 *
 * A database is a directory opened by one process at a time. Points are
 * written to tables in batches; a batch is logged and visible to queries
 * when pulseten_write() returns PULSETEN_OK. WAL sync is off by default;
 * enable m_wal_sync to make each successful write durable before it returns.
 * Time-series tables may be declared without columns, or inferred by writes.
 * The first accepted write defines their columns.
 * pulseten_exec() runs ordinary table SQL directly in DuckDB. Queries read
 * ordinary tables and time-series data, including unflushed writes, with
 * results returned chunk by chunk.
 *
 * Functions returning pulseten_status_t update the calling thread's message
 * from pulseten_error_message(); success clears it. Other functions leave the
 * message unchanged.
 *
 * Threads: a pulseten_db_t may be used by any number of threads at once,
 * except that pulseten_close() must not overlap any other call on it.
 * A pulseten_result_t is used by one thread at a time, except that
 * pulseten_result_interrupt() may be called from another thread.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PULSETEN_H
#define PULSETEN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/* ---- Version --------------------------------------------------------- */

#define PULSETEN_VERSION_MAJOR 0
#define PULSETEN_VERSION_MINOR 1
#define PULSETEN_VERSION_PATCH 0
#define PULSETEN_VERSION_STRING "0.1.0"

/** MAJOR * 10000 + MINOR * 100 + PATCH. */
#define PULSETEN_VERSION_NUMBER 100

/** Get the version of the linked library, which may differ from the header
the caller was compiled with.
@return the version as PULSETEN_VERSION_STRING has it, e.g. "0.1.0" */
const char *pulseten_version(void);

/* ---- Limits ---------------------------------------------------------- */

/** Points in one pulseten_write(). */
#define PULSETEN_MAX_POINTS 65536

/** Different UTC days the points of one batch may fall on. */
#define PULSETEN_MAX_BATCH_DAYS 32

/** Estimated batch data limit: 8 bytes per time and supplied tag or field,
plus string bytes and ceil(points / 8) NULL-bitmap bytes per column in the
resulting table schema, including columns absent from the batch. An estimate
above this limit is rejected with PULSETEN_LIMIT_EXCEEDED. The estimate excludes
encoded table/schema metadata and WAL framing; the encoded payload can be
larger. */
#define PULSETEN_MAX_BATCH_BYTES (8 * 1024 * 1024 - 64 * 1024)

/** Columns of a table, the time column included. */
#define PULSETEN_MAX_COLUMNS 64

/** Bytes of a table or column name. */
#define PULSETEN_MAX_NAME_BYTES 128

/** Bytes of a VARCHAR tag or field value. */
#define PULSETEN_MAX_STRING_BYTES (1024 * 1024)

/** Times accepted, in microseconds: [1900-01-01, 2200-01-01) UTC. */
#define PULSETEN_MIN_TIME_US (-2208988800000000LL)
#define PULSETEN_MAX_TIME_US 7258118400000000LL

/** Largest partition duration, in hours. */
#define PULSETEN_MAX_PARTITION_HOURS 8784

/** Largest retention period, in hours. */
#define PULSETEN_MAX_RETENTION_HOURS 2640000

/** Largest explicit DuckDB execution thread count. */
#define PULSETEN_MAX_DUCKDB_THREADS 1024

/** Most rows one pulseten_result_fetch() returns. */
#define PULSETEN_MAX_CHUNK_ROWS 2048

/* ---- Status ---------------------------------------------------------- */

typedef enum {
  /** Success. */
  PULSETEN_OK = 0,

  /** SQL error, or a failure with no more specific code. */
  PULSETEN_ERROR = 1,

  /** A write or statement was rejected; nothing changed. */
  PULSETEN_INVALID_ARGUMENT = 2,

  /** A write gives an existing column another kind or type. */
  PULSETEN_SCHEMA_CONFLICT = 3,

  /** A size or count limit was exceeded. */
  PULSETEN_LIMIT_EXCEEDED = 4,

  /** A memory allocation failed. */
  PULSETEN_OUT_OF_MEMORY = 5,

  /** An operating system I/O call failed. */
  PULSETEN_IO_ERROR = 6,

  /** Stored data failed validation. */
  PULSETEN_CORRUPTION = 7,

  /** Another handle in this process, or another process, holds the
  directory lock. */
  PULSETEN_LOCKED = 8,

  /** An earlier failure stopped a handle, or database deletion is pending.
  Reopen failed handles; finish DropDatabase for pending deletion. */
  PULSETEN_FENCED = 9,

  /** An operation may be partial or its durability unconfirmed; do not retry
  blindly. Reopen failed handles to inspect; for DropDatabase, finish or confirm
  removal before recreating the path. */
  PULSETEN_OUTCOME_UNKNOWN = 10,

  /** pulseten_result_interrupt() stopped the query. */
  PULSETEN_INTERRUPTED = 11,

  /** An operation timed out waiting for a table lock; nothing changed. */
  PULSETEN_LOCK_TIMEOUT = 12,

  /** A declared time-series table has no initialized columns yet. */
  PULSETEN_TABLE_NOT_INITIALIZED = 13,

} pulseten_status_t;

/** Describe the last call returning pulseten_status_t on the calling thread.
Functions with other return types do not update this message.
@return the message, or "" after a successful call returning pulseten_status_t;
  contents valid until the next call returning pulseten_status_t on this thread
*/
const char *pulseten_error_message(void);

/** Name a status.
@param[in]  status  status of a call
@return a short static name, e.g. "INVALID_ARGUMENT"; "UNKNOWN" for a value
  that is not a status */
const char *pulseten_status_name(pulseten_status_t status);

/* ---- Opening and closing --------------------------------------------- */

typedef struct pulseten_db_t pulseten_db_t;

/** Settings of pulseten_open(). Recompile callers when upgrading this
structure. */
typedef struct {
  /** Nonzero allows a new database to be created. Default: 1. */
  int m_create_if_missing;

  /** Idle wait between background passes, in milliseconds; must be greater
  than 0. Flush work, expiry and scheduling add to elapsed time; writers
  waiting for memtable capacity may wake the thread sooner. After a safely
  rolled-back flush OOM, retries wait a full interval. */
  uint32_t m_flush_interval_ms;

  /** Maximum wait, in milliseconds, for each table or namespace lock taken by
  a write, create, set or drop; 0 waits indefinitely. Default: 0. A timeout
  rejects the write before it enters the WAL. */
  uint32_t m_write_lock_timeout_ms;

  /** New writes wait while unflushed column data is at or above this many
  bytes; 0 disables waiting. Excludes planning buffers and retired batches
  held by snapshots; this is not a process memory limit. Default: 128 MiB. */
  uint64_t m_memtable_limit_bytes;

  /** Size at which the write-ahead log starts a new file. */
  uint64_t m_wal_file_size_bytes;

  /** Hours covered by one time partition, 1 to
  PULSETEN_MAX_PARTITION_HOURS. Used when the database is created; an
  existing one keeps its own. */
  uint32_t m_partition_duration_hours;

  /** Default retention in hours, at most PULSETEN_MAX_RETENTION_HOURS;
  0 keeps everything. Applies to tables without an override for this open.
  Older writes are rejected and queries exclude older rows at a fixed cutoff.
  Physical cleanup drops whole partitions; disk reclamation can lag by one
  partition duration plus maintenance delay. Retention may be shorter than the
  partition duration. */
  uint32_t m_retention_hours;

  /** Maximum future timestamp offset in seconds relative to the database
  clock; 0 disables the check. Default: 0. A positive value rejects a whole
  batch containing a later point before WAL admission. Applies to this open
  only, independently of retention; accepted WAL records remain recoverable
  under a stricter limit. The absolute supported timestamp range always
  applies. */
  uint32_t m_max_future_skew_seconds;

  /** Nonzero syncs each WAL group before acknowledging its writes. Default: 0,
  which writes to the operating system's page cache without waiting for a
  sync; a system crash or power loss may lose acknowledged writes or corrupt
  unsynced WAL data. Applies to this open only. File creation, rotation,
  recovery and background flushes may still sync files. */
  int m_wal_sync;

  /** DuckDB memory_limit shared by this database's query and storage
  connections. 0 uses DuckDB's default. Explicit values are bytes, at most
  2^53. Applies only to this open. The budget covers memory managed by DuckDB's
  buffer manager. */
  uint64_t m_duckdb_memory_limit_bytes;

  /** DuckDB execution threads shared by query and storage connections.
  0 keeps DuckDB's platform default; positive values are exact, at most
  PULSETEN_MAX_DUCKDB_THREADS. Applies only to this open. This does not bound
  total process threads or DuckDB's separate async threads. */
  uint32_t m_duckdb_threads;
} pulseten_options_t;

/** Fill options with the defaults.
@param[out]  options  options to fill */
void pulseten_options_init(pulseten_options_t *options);

/** Open a database and replay the valid WAL prefix. The parent directory must
exist. By default, create the database if missing. Set m_create_if_missing to
zero to require an existing database.

Recovery discards the newest WAL file from its first invalid record onward,
even if those writes were previously acknowledged. File headers and earlier
files must be valid. The recovered prefix must cover stored data before
truncation; I/O and replay failures are returned without tail repair.
@param[in]   dir      directory path
@param[in]   options  settings, or NULL for the defaults
@param[out]  out      receives the open database, or NULL on failure;
                      must not be NULL
@retval PULSETEN_OK the database is open and recovered
@retval PULSETEN_INVALID_ARGUMENT invalid path or options, or the database is
  missing and creation is disabled
@retval PULSETEN_LOCKED another handle in this process, or another process,
  holds the directory lock
@retval PULSETEN_FENCED an older version left a pending deletion
@retval PULSETEN_CORRUPTION stored data failed validation
@retval PULSETEN_IO_ERROR a directory, storage or WAL I/O call failed
@retval PULSETEN_ERROR DuckDB or another component failed to open
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_status_t pulseten_open(const char *dir,
                                const pulseten_options_t *options,
                                pulseten_db_t **out);

/** Remove the closed database currently at dir, including data, WAL and
owned spill files. Close all handles first. Unknown entries and symbolic or
hard links are rejected before deletion. Missing and empty directories are
successful cleanup.

Drop removes and syncs the main file before removing its WAL and other files.
Retry an interrupted deletion only if no new database has been created at
the path and none can be created during the retry. Drop can remove a damaged
store without opening it.
@param[in] dir database path; must not be NULL or empty
@retval PULSETEN_OK removal and parent-directory sync completed
@retval PULSETEN_LOCKED an opener/deletion holds the directory or its name
changed
@retval PULSETEN_INVALID_ARGUMENT invalid path or directory entry
@retval PULSETEN_IO_ERROR preflight or filesystem access failed
@retval PULSETEN_OUTCOME_UNKNOWN removal may be partial or not durably confirmed
*/
pulseten_status_t pulseten_drop_database(const char *dir);

/** Close a database. Destroy every result first. No other call on db may be
running or start, from any thread: closing frees db at once and does not wait.
Unflushed writes remain in the write-ahead log; closing does not force unsynced
writes to disk.
@param[in]  db  database; may be NULL */
void pulseten_close(pulseten_db_t *db);

/** Initial settings for an explicit time-series table. */
typedef struct {
  /** Partition duration in hours, at most PULSETEN_MAX_PARTITION_HOURS.
  Zero selects the database default persisted at directory creation. */
  uint32_t m_partition_duration_hours;

  /** Nonzero selects a retention override; zero inherits the Open default. */
  int m_has_retention_hours;

  /** Retention in hours; zero keeps all data. Used only with an override. */
  uint32_t m_retention_hours;
} pulseten_create_table_options_t;

/** Initialize a declaration to the durable partition duration and the Open
retention default. */
void pulseten_create_table_options_init(
    pulseten_create_table_options_t *options);

/** Persist a new time-series table without requiring column definitions.
NULL options select the defaults. No data is flushed and no physical partition
is created. The first accepted write defines the columns; until then Query and
Explain return PULSETEN_TABLE_NOT_INITIALIZED when they resolve this table.
Partition duration is fixed; retention can later change through Set.
Existing managed names and ordinary tables are rejected without changing
their settings, including names inferred by an unflushed first write. Lock
waits use m_write_lock_timeout_ms. Uncertain commit/rollback fences the
database.
@param[in] db open database; must not be NULL
@param[in] name table name, NUL-terminated; must not be NULL
@param[in] options declaration settings, or NULL for defaults
@retval PULSETEN_INVALID_ARGUMENT existing name or invalid settings
@retval PULSETEN_OUTCOME_UNKNOWN declaration may or may not be durable; reopen
*/
pulseten_status_t pulseten_create_table(
    pulseten_db_t *db, const char *name,
    const pulseten_create_table_options_t *options);

/** Durably delete an existing managed time-series table and all its data.
Works for declarations with no initialized columns. Ordinary or unknown names
are rejected. The name can then be created or inferred by a new write, with new
columns and options. Physical table ids are never reused.

Waits for this table's results and writes to finish, using the table lock
timeout. Close results before calling Drop. A preparatory global flush covers
all visible writes, including other tables; latency depends on total buffered
data. Even a failed deletion can have completed that flush. Old WAL records
cannot revive a deleted table. Requests waiting across its deletion return
INVALID_ARGUMENT rather than modifying the replacement; retry by name only if
intended. Uncertain commit/rollback fences the database until it is reopened.
@param[in] db open database; must not be NULL
@param[in] name managed table name, NUL-terminated; must not be NULL
@retval PULSETEN_INVALID_ARGUMENT invalid, unknown, ordinary or changed table
@retval PULSETEN_LOCK_TIMEOUT the table is still in use; nothing deleted
@retval PULSETEN_OUT_OF_MEMORY flush or deletion ran out of memory
@retval PULSETEN_OUTCOME_UNKNOWN deletion or preparatory flush may be durable */
pulseten_status_t pulseten_drop_table(pulseten_db_t *db, const char *name);

/** Mutable table settings, replaced as a whole by Set. */
typedef struct {
  /** Nonzero selects an override; zero inherits the current Open default. */
  int m_has_retention_hours;

  /** Retention in hours, at most PULSETEN_MAX_RETENTION_HOURS; 0 keeps all
  data. Used only when m_has_retention_hours is nonzero. */
  uint32_t m_retention_hours;
} pulseten_table_options_t;

/** Initialize table settings to inherit the Open default. */
void pulseten_table_options_init(pulseten_table_options_t *options);

/** Replace an existing time-series table's mutable settings durably.
Does not flush data or change its partition duration. NULL options inherit the
Open default. Queries hold table locks until their results are destroyed;
this table lock wait uses m_write_lock_timeout_ms. Later queries use the new
policy. Extending retention can reveal rows still on disk; physically deleted
rows cannot be restored.
@param[in] db open database; must not be NULL
@param[in] name table name, NUL-terminated; must not be NULL
@param[in] options replacement settings, or NULL to inherit
@retval PULSETEN_INVALID_ARGUMENT unknown table or invalid settings
@retval PULSETEN_LOCK_TIMEOUT table lock wait expired
@retval PULSETEN_OUTCOME_UNKNOWN commit or rollback outcome is uncertain;
  database is fenced until reopened */
pulseten_status_t pulseten_set_table_options(
    pulseten_db_t *db, const char *name,
    const pulseten_table_options_t *options);

/* ---- Writing --------------------------------------------------------- */

typedef enum {
  /** True or false. */
  PULSETEN_TYPE_BOOLEAN = 1,

  /** A signed 64-bit integer. */
  PULSETEN_TYPE_BIGINT = 2,

  /** An IEEE 754 double. Field inputs must be finite; query results may
  include NaN or infinity. */
  PULSETEN_TYPE_DOUBLE = 3,

  /** Microseconds since 1970-01-01 00:00:00 UTC; the "time" column. */
  PULSETEN_TYPE_TIMESTAMP = 4,

  /** A UTF-8 string. Tag and field inputs are limited to
  PULSETEN_MAX_STRING_BYTES bytes; query result strings have no such limit. */
  PULSETEN_TYPE_VARCHAR = 5
} pulseten_type_t;

/** A VARCHAR column with the tag kind. PulseTen creates no dedicated tag
index. Input strings need no terminating NUL. */
typedef struct {
  /** Column name: an identifier, matched case-insensitively. */
  const char *m_name;

  /** Length of name in bytes, at most PULSETEN_MAX_NAME_BYTES. */
  size_t m_name_length;

  /** The value, UTF-8. */
  const char *m_value;

  /** Length of value in bytes, at most PULSETEN_MAX_STRING_BYTES. */
  size_t m_value_length;
} pulseten_tag_t;

/** A field: a BOOLEAN, BIGINT, DOUBLE or VARCHAR column. */
typedef struct {
  /** Column name: an identifier, matched case-insensitively. */
  const char *m_name;

  /** Length of name in bytes, at most PULSETEN_MAX_NAME_BYTES. */
  size_t m_name_length;

  /** BOOLEAN, BIGINT, DOUBLE or VARCHAR. */
  pulseten_type_t m_type;

  /** The member matching type. */
  union {
    /** BOOLEAN: nonzero for true. */
    int m_boolean;

    /** BIGINT. */
    int64_t m_integer;

    /** DOUBLE; must be finite. */
    double m_double;

    /** VARCHAR. */
    struct {
      /** The string, UTF-8; needs no terminating NUL. */
      const char *m_data;

      /** Length of m_data in bytes, at most PULSETEN_MAX_STRING_BYTES. */
      size_t m_length;
    } m_string;
  } m_value;
} pulseten_field_t;

/** A point: a row of a table. Columns it does not name are NULL. */
typedef struct {
  /** Microseconds since 1970-01-01 00:00:00 UTC, in
  [PULSETEN_MIN_TIME_US, PULSETEN_MAX_TIME_US). */
  int64_t m_time_us;

  /** The tags; may be NULL when m_tag_count is 0. */
  const pulseten_tag_t *m_tags;

  /** Number of tags. */
  uint32_t m_tag_count;

  /** The fields, at least one. */
  const pulseten_field_t *m_fields;

  /** Number of fields; tags and fields together fewer than
  PULSETEN_MAX_COLUMNS. */
  uint32_t m_field_count;
} pulseten_point_t;

/** Write a batch of points to a table, creating the table and any new
columns. The batch is accepted or rejected as a whole; when this returns
PULSETEN_OK it is logged and visible to new queries. With m_wal_sync enabled,
it is also durable; the default does not wait for a WAL sync.
Open results hold shared locks on referenced tables. Adding columns waits for
those results to close. A queued schema writer also blocks later table readers
and writers. When m_write_lock_timeout_ms is nonzero, it bounds each table or
namespace lock wait of the write; a timeout rejects the batch before logging,
so it is safe to retry.
@param[in]  db        database
@param[in]  table_name  table name, NUL-terminated
@param[in]  points    the points
@param[in]  n_points  number of points, 1 to PULSETEN_MAX_POINTS
@retval PULSETEN_OK the batch is logged and visible
@retval PULSETEN_INVALID_ARGUMENT rejected; nothing written
@retval PULSETEN_SCHEMA_CONFLICT rejected; nothing written
@retval PULSETEN_LIMIT_EXCEEDED rejected; nothing written
@retval PULSETEN_LOCK_TIMEOUT table lock wait expired; nothing written
@retval PULSETEN_OUT_OF_MEMORY rejected; nothing written
@retval PULSETEN_ERROR write target lookup failed; nothing written
@retval PULSETEN_IO_ERROR WAL preparation or maintenance failed; nothing
  written; reopen
@retval PULSETEN_CORRUPTION inconsistent catalog or write metadata
@retval PULSETEN_FENCED an earlier database failure stopped writes; reopen
@retval PULSETEN_OUTCOME_UNKNOWN may or may not be durable */
pulseten_status_t pulseten_write(pulseten_db_t *db, const char *table_name,
                                 const pulseten_point_t *points,
                                 uint32_t n_points);

/* ---- Tables ---------------------------------------------------------- */

typedef enum {
  /** Column 0, "time", a TIMESTAMP. */
  PULSETEN_COLUMN_TIME = 0,

  /** A VARCHAR tag. */
  PULSETEN_COLUMN_TAG = 1,

  /** A BOOLEAN, BIGINT, DOUBLE or VARCHAR field. */
  PULSETEN_COLUMN_FIELD = 2
} pulseten_column_kind_t;

/** A column of a table. */
typedef struct {
  /** Lower-case name. */
  const char *m_name;

  /** Time, tag or field. */
  pulseten_column_kind_t m_kind;

  /** Value type. */
  pulseten_type_t m_type;
} pulseten_column_t;

/** A managed time-series table and its columns. */
typedef struct {
  /** Lower-case name. */
  const char *m_name;

  /** Nonzero after the first accepted write, even before flush. */
  int m_initialized;

  /** Fixed partition duration in hours. */
  uint32_t m_partition_duration_hours;

  /** Nonzero if retention overrides the Open default. */
  int m_has_retention_hours;

  /** Override in hours; zero keeps all data. Ignored without an override. */
  uint32_t m_retention_hours;

  /** Number of columns, "time" included; zero before initialization. */
  uint32_t m_column_count;

  /** Columns in creation order, "time" first; NULL before initialization. */
  const pulseten_column_t *m_columns;
} pulseten_table_t;

/** Metadata returned by pulseten_catalog_snapshot().
Treat all members as read-only. */
typedef struct {
  /** Number of managed time-series tables. */
  uint32_t m_table_count;

  /** Tables sorted by name. */
  const pulseten_table_t *m_tables;
} pulseten_catalog_t;

/** Return every managed time-series table and its newest columns, including
declared tables with no columns and columns whose writes are not flushed.
Each entry includes its initialization state, fixed partition duration and
retention override. The result owns its names and arrays; they remain valid
until pulseten_catalog_destroy(), even after later writes or closing the
database. Ordinary tables are not included.
@param[in]   db   database
@param[out]  out  the metadata, or NULL on failure
@retval PULSETEN_OK the metadata was returned
@retval PULSETEN_INVALID_ARGUMENT db or out is NULL
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_status_t pulseten_catalog_snapshot(pulseten_db_t *db,
                                            pulseten_catalog_t **out);

/** Free a catalog.
@param[in]  catalog  catalog; may be NULL */
void pulseten_catalog_destroy(pulseten_catalog_t *catalog);

/* ---- Querying -------------------------------------------------------- */

/** Execute SQL directly in DuckDB, discarding result rows.
Each call uses a new connection. SQL may contain multiple statements, including
a complete BEGIN/COMMIT or BEGIN/ROLLBACK transaction. An unfinished transaction
is rolled back when the call ends; session settings and temporary objects do
not carry across calls. Errors retain DuckDB's diagnostic and transaction
semantics; an error does not imply that earlier statements had no effect.
Parsed object names are checked before each statement: _pt_ objects cannot be
referenced, including for reads. The main schema and time-series names cannot
be created, replaced or modified by SQL. Calls containing namespace changes
are serialized with first time-series writes through commit or rollback.
Pure queries, DML and built-in SET/RESET/USE do not take this namespace lock.
Other control and extension statements conservatively hold it.
Database imports and whole-database copies into PulseTen storage are rejected.
PRAGMA copy_database requires two string literal arguments.
Earlier statements may already have committed when a later one is rejected.
DuckDB control statements keep their native effects: SET memory_limit changes
the shared budget, and fails with PULSETEN_OUT_OF_MEMORY if DuckDB already
holds more than the new budget; FORCE CHECKPOINT can wait for writers and delay
new transactions. Use pulseten_exec_with_cancel() for interruptible calls.
Use pulseten_query() to read time-series data, including unflushed writes.
@param[in] db database
@param[in] sql SQL text, NUL-terminated
@return PULSETEN_OK, PULSETEN_INVALID_ARGUMENT for NULL arguments or a
  protected object operation, PULSETEN_FENCED after a database failure, or a
  classified DuckDB error */
pulseten_status_t pulseten_exec(pulseten_db_t *db, const char *sql);

typedef struct pulseten_result_t pulseten_result_t;

/** Start a SELECT over one consistent snapshot of every write so far. Opening
checks and plans the statement; it runs as rows are fetched, so errors found
while running are returned by pulseten_result_fetch().
CTEs, subqueries, joins and schema-qualified ordinary tables are allowed.
Time-series tables use unqualified names or main.name. Table functions are
rejected. Until the result is destroyed it holds the snapshot, so destroy
results promptly.
@param[in]   db   database
@param[in]   sql  a single SELECT, NUL-terminated
@param[out]  out  the result, or NULL on failure
@retval PULSETEN_OK the query was prepared; fetch rows with
pulseten_result_fetch()
@retval PULSETEN_INVALID_ARGUMENT the statement is not allowed, or a result
  column has a type that cannot be returned
@retval PULSETEN_ERROR SQL error
@retval PULSETEN_IO_ERROR DuckDB reported an I/O failure
@retval PULSETEN_CORRUPTION DuckDB reported damaged data
@retval PULSETEN_FENCED an earlier failure stopped the database
@retval PULSETEN_TABLE_NOT_INITIALIZED a referenced table has no columns yet
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_status_t pulseten_query(pulseten_db_t *db, const char *sql,
                                 pulseten_result_t **out);

/** Cancellation handle for one SQL execution or query open; requests are
permanent. */
typedef struct pulseten_cancel_t pulseten_cancel_t;

/** As pulseten_exec(), allowing cancellation while waiting or executing.
Cancellation does not undo statements already committed.
@param[in] db database
@param[in] sql SQL text, NUL-terminated
@param[in] cancel optional cancellation handle, alive until this call returns
@return as pulseten_exec(), or PULSETEN_INTERRUPTED when cancelled */
pulseten_status_t pulseten_exec_with_cancel(pulseten_db_t *db, const char *sql,
                                            pulseten_cancel_t *cancel);

/** Create a cancellation handle before starting an operation.
@param[out] out handle, or NULL on failure
@return PULSETEN_OK, PULSETEN_INVALID_ARGUMENT or PULSETEN_OUT_OF_MEMORY */
pulseten_status_t pulseten_cancel_create(pulseten_cancel_t **out);

/** Cancel the SQL execution or query opening that uses this handle, from
another thread. Safe before the operation starts.
@param[in] cancel handle; may be NULL */
void pulseten_cancel_request(pulseten_cancel_t *cancel);

/** Release a cancellation handle after its operation and all requests return.
@param[in] cancel handle; may be NULL */
void pulseten_cancel_destroy(pulseten_cancel_t *cancel);

/** As pulseten_query(), allowing cancellation during opening, including table
lock waits. Use pulseten_result_interrupt() after opening succeeds.
@param[in] db database
@param[in] sql a single SELECT, NUL-terminated
@param[in] cancel handle kept alive until this call returns; NULL disables
cancellation; a handle may be used by only one query at a time
@param[out] out result, or NULL on failure
@return as pulseten_query(), or PULSETEN_INTERRUPTED when cancelled */
pulseten_status_t pulseten_query_with_cancel(pulseten_db_t *db, const char *sql,
                                             pulseten_cancel_t *cancel,
                                             pulseten_result_t **out);

/** Count the result columns.
@param[in]  result  result
@return the number of columns; 0 if result is NULL */
uint32_t pulseten_result_column_count(pulseten_result_t *result);

/** Name a result column.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_result_column_count() - 1
@return the name, valid until the result is destroyed; NULL if col is out of
  range */
const char *pulseten_result_column_name(pulseten_result_t *result,
                                        uint32_t col);

/** Get the type of a result column.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_result_column_count() - 1
@return the type its values are read as; 0 if col is out of range */
pulseten_type_t pulseten_result_column_type(pulseten_result_t *result,
                                            uint32_t col);

/** Read the next chunk of rows, replacing the previous one.
After a query failure, every later fetch returns the same code and diagnostic
with zero rows. A later interrupt does not replace an earlier failure.
@param[in]   result  result
@param[out]  n_rows  rows in the chunk, at most PULSETEN_MAX_CHUNK_ROWS; 0 after
                     the last
@retval PULSETEN_OK a chunk is available, or the result reached its end
@retval PULSETEN_ERROR the query failed while running
@retval PULSETEN_IO_ERROR DuckDB reported an I/O failure
@retval PULSETEN_CORRUPTION DuckDB reported damaged data
@retval PULSETEN_INVALID_ARGUMENT a value does not fit its column type
@retval PULSETEN_INTERRUPTED pulseten_result_interrupt() stopped the query; so
does every later fetch
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_status_t pulseten_result_fetch(pulseten_result_t *result,
                                        uint32_t *n_rows);

/** Request interruption of an active query from another thread. The running
or next fetch observes the request; after it returns PULSETEN_INTERRUPTED,
every later fetch does too. Normal end is reached when pulseten_result_fetch()
returns PULSETEN_OK with 0 rows; interrupting after that leaves later fetches
successful with 0 rows. Must not run at the same time as
pulseten_result_destroy().
@param[in]  result  result; may be NULL */
void pulseten_result_interrupt(pulseten_result_t *result);

/* The functions below read a value of the current chunk: the rows the
last pulseten_result_fetch() returned. */

/** Tell whether a value is NULL.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_result_column_count() - 1
@param[in]  row     row of the current chunk
@return nonzero if the value is NULL, or if col or row is out of range */
int pulseten_result_is_null(pulseten_result_t *result, uint32_t col,
                            uint32_t row);

/** Read a BOOLEAN value.
@param[in]  result  result
@param[in]  col     a BOOLEAN column
@param[in]  row     row of the current chunk
@return 1 for true, 0 for false; 0 if the value is NULL, col is not BOOLEAN,
  or col or row is out of range */
int pulseten_result_bool(pulseten_result_t *result, uint32_t col, uint32_t row);

/** Read a BIGINT value, or a TIMESTAMP in microseconds since 1970-01-01
00:00:00 UTC.
@param[in]  result  result
@param[in]  col     a BIGINT or TIMESTAMP column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
int64_t pulseten_result_int64(pulseten_result_t *result, uint32_t col,
                              uint32_t row);

/** Read a DOUBLE value.
@param[in]  result  result
@param[in]  col     a DOUBLE column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
double pulseten_result_double(pulseten_result_t *result, uint32_t col,
                              uint32_t row);

/** Read a VARCHAR value.
@param[in]   result  result
@param[in]   col     a VARCHAR column
@param[in]   row     row of the current chunk
@param[out]  len     length of the string in bytes; may be NULL
@return the string, NUL-terminated and valid until the next fetch or until the
  result is destroyed; NULL, with *len 0, if the value is NULL, col has
  another type, or col or row is out of range */
const char *pulseten_result_varchar(pulseten_result_t *result, uint32_t col,
                                    uint32_t row, size_t *len);

/** Stop the query and free the result.
@param[in]  result  result; may be NULL */
void pulseten_result_destroy(pulseten_result_t *result);

/** Show DuckDB's plan for a SELECT, as pulseten_query() would run it.
@param[in]   db    database
@param[in]   sql   a single SELECT, NUL-terminated
@param[out]  plan  the plan, NUL-terminated, or NULL on failure; free it with
                   pulseten_free()
@retval PULSETEN_OK the plan was produced
@retval PULSETEN_INVALID_ARGUMENT as for pulseten_query()
@retval PULSETEN_TABLE_NOT_INITIALIZED as for pulseten_query()
@retval PULSETEN_ERROR as for pulseten_query()
@retval PULSETEN_FENCED as for pulseten_query()
@retval PULSETEN_OUT_OF_MEMORY as for pulseten_query() */
pulseten_status_t pulseten_explain(pulseten_db_t *db, const char *sql,
                                   char **plan);

/** Free memory that PulseTen returned to the caller.
@param[in]  p  memory, e.g. a plan from pulseten_explain(); may be NULL */
void pulseten_free(void *p);

/* ---- Statistics ------------------------------------------------------ */

typedef struct {
  /** Sequence number of the newest write in the analytical storage. */
  uint64_t m_flushed_seq;

  /** Sequence number of the newest visible write. */
  uint64_t m_visible_seq;

  /** Bytes of unflushed writes held in memory. */
  uint64_t m_memtable_bytes;

  /** Number of write-ahead log files. */
  uint64_t m_wal_file_count;

  /** Background flushes that stored something. */
  uint64_t m_background_flushes;

  /** Background passes with a new flush, WAL cleanup or expiry failure; at most
  one per pass. Rejections after a fatal error are not new failures. */
  uint64_t m_background_errors;

  /** Writes that had to wait for memory. */
  uint64_t m_throttled_writes;

  /** Time partitions dropped for being past the retention period. */
  uint64_t m_expired_partitions;

  /** Shared DuckDB memory budget selected at open, in bytes.
  Later SQL configuration changes are not reflected here. */
  uint64_t m_duckdb_memory_limit_bytes;

  /** Effective DuckDB execution thread setting selected at open.
  Later SQL configuration changes are not reflected here. */
  uint32_t m_duckdb_threads;
} pulseten_stats_t;

/** Read the counters of a database.
@param[in]   db     database; if NULL, every counter is 0
@param[out]  stats  the counters */
void pulseten_stats(pulseten_db_t *db, pulseten_stats_t *stats);

/** Read the message of the latest failed background flush, WAL cleanup or
expiry. A failure that only reports that the database was already stopped does
not replace it.
@param[in]   db       database
@param[out]  message  the message, NUL-terminated, or NULL if none failed; free
                      it with pulseten_free()
@retval PULSETEN_OK the message, if any, was copied
@retval PULSETEN_INVALID_ARGUMENT db or message is NULL
@retval PULSETEN_OUT_OF_MEMORY the message could not be copied */
pulseten_status_t pulseten_copy_background_error(pulseten_db_t *db,
                                                 char **message);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* PULSETEN_H */
