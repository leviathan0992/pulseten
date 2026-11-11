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
 * Tables and columns are created by the writes themselves. Queries are SQL
 * SELECT statements run by DuckDB over one consistent snapshot, and their
 * rows are read chunk by chunk.
 *
 * Functions returning pulseten_status_t update the calling thread's message
 * from pulseten_errmsg(); success clears it. Other functions leave the
 * message unchanged.
 *
 * Threads: a pulseten_db_t may be used by any number of threads at once.
 * A pulseten_result_t is used by one thread at a time.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PULSETEN_H
#define PULSETEN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

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

/** Get the version of the linked library as a number.
@return MAJOR * 10000 + MINOR * 100 + PATCH, as PULSETEN_VERSION_NUMBER */
int pulseten_version_number(void);

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
#define PULSETEN_MAX_NAME_LEN 128

/** Bytes of a VARCHAR tag or field value. */
#define PULSETEN_MAX_STRING_LEN (1024 * 1024)

/** Times accepted, in microseconds: [1900-01-01, 2200-01-01) UTC. */
#define PULSETEN_MIN_TIME_US (-2208988800000000LL)
#define PULSETEN_MAX_TIME_US 7258118400000000LL

/** Largest partition_days and retention_days. */
#define PULSETEN_MAX_PARTITION_DAYS 366
#define PULSETEN_MAX_RETENTION_DAYS 110000

/** Most rows one pulseten_result_fetch() returns. */
#define PULSETEN_FETCH_ROWS 2048

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

  /** An earlier failure stopped writes and flushes; close and reopen. */
  PULSETEN_FENCED = 9,

  /** A write or flush may or may not be durable; do not retry blindly.
  Writes and flushes are stopped; close and reopen to find out. */
  PULSETEN_OUTCOME_UNKNOWN = 10,

  /** pulseten_result_interrupt() stopped the query. */
  PULSETEN_INTERRUPTED = 11,

  /** The named table does not exist. */
  PULSETEN_NOT_FOUND = 12
} pulseten_status_t;

/** Describe the last call returning pulseten_status_t on the calling thread.
Functions with other return types do not update this message.
@return the message, or "" after a successful call returning pulseten_status_t;
  contents valid until the next call returning pulseten_status_t on this thread
*/
const char *pulseten_errmsg(void);

/** Name a status.
@param[in]  status  status of a call
@return a short static name, e.g. "INVALID_ARGUMENT"; "UNKNOWN" for a value
  that is not a status */
const char *pulseten_status_name(pulseten_status_t status);

/* ---- Opening and closing --------------------------------------------- */

typedef struct pulseten_db_t pulseten_db_t;

typedef struct {
  /** Idle wait between background passes, in milliseconds; must be greater
  than 0. Flush work, expiry and scheduling add to elapsed time; writers
  waiting for memtable capacity may wake the thread sooner. */
  uint32_t m_flush_interval_ms;

  /** New writes wait while unflushed column data is at or above this many
  bytes; 0 disables waiting. Excludes planning buffers
  and retired batches held by snapshots; this is not a process memory limit. */
  uint64_t m_memtable_limit_bytes;

  /** Size at which the write-ahead log starts a new file. */
  uint64_t m_wal_segment_bytes;

  /** Days covered by one time partition, 1 to
  PULSETEN_MAX_PARTITION_DAYS. Used when the database is created; an
  existing one keeps its own. */
  uint32_t m_partition_days;

  /** Keep this many days before the current time; 0, the default, keeps
  everything. Older writes are rejected, and stored time partitions that
  hold only older data are dropped by the background thread.
  A partition is dropped whole, so up to m_partition_days more are kept.
  At most PULSETEN_MAX_RETENTION_DAYS. Applies to this open only. */
  uint32_t m_retention_days;

  /** Nonzero syncs each WAL group before acknowledging its writes. The
  default, 0, writes to the operating system's page cache without waiting
  for a sync; a system crash or power loss may lose acknowledged writes or
  corrupt unsynced WAL data.
  Applies to this open only. Segment creation, rotation, recovery and
  background flushes may still sync files. */
  int m_wal_sync;
} pulseten_options_t;

/** Fill options with the defaults.
@param[out]  options  options to fill */
void pulseten_options_init(pulseten_options_t *options);

/** Open a database directory, creating it if it does not exist; its parent
must exist. Recovers every write that was durable.
@param[in]   dir      directory path
@param[in]   options  settings, or NULL for the defaults
@param[out]  out      receives the open database, or NULL on failure;
                      must not be NULL
@retval PULSETEN_OK the database is open and recovered
@retval PULSETEN_INVALID_ARGUMENT an option is out of range
@retval PULSETEN_LOCKED another handle in this process, or another process,
  holds the directory lock
@retval PULSETEN_CORRUPTION stored data failed validation
@retval PULSETEN_IO_ERROR the directory could not be opened
@retval PULSETEN_ERROR the directory could not be opened
@retval PULSETEN_OUT_OF_MEMORY the directory could not be opened
*/
pulseten_status_t pulseten_open(const char *dir,
                                const pulseten_options_t *options,
                                pulseten_db_t **out);

/** Close a database. Destroy every result first. Unflushed writes remain
in the write-ahead log; closing does not force unsynced writes to disk.
@param[in]  db  database; may be NULL
@retval PULSETEN_OK the database is closed
@retval PULSETEN_IO_ERROR a file could not be closed; the database is closed
  anyway */
pulseten_status_t pulseten_close(pulseten_db_t *db);

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
  PULSETEN_MAX_STRING_LEN bytes; query result strings have no such limit. */
  PULSETEN_TYPE_VARCHAR = 5
} pulseten_type_t;

/** A VARCHAR column with the tag kind. PulseTen creates no dedicated tag
index. Input strings need no terminating NUL. */
typedef struct {
  /** Column name: an identifier, matched case-insensitively. */
  const char *m_name;

  /** Length of name in bytes, at most PULSETEN_MAX_NAME_LEN. */
  size_t m_name_len;

  /** The value, UTF-8. */
  const char *m_value;

  /** Length of value in bytes, at most PULSETEN_MAX_STRING_LEN. */
  size_t m_value_len;
} pulseten_tag_t;

/** A field: a BOOLEAN, BIGINT, DOUBLE or VARCHAR column. */
typedef struct {
  /** Column name: an identifier, matched case-insensitively. */
  const char *m_name;

  /** Length of name in bytes, at most PULSETEN_MAX_NAME_LEN. */
  size_t m_name_len;

  /** BOOLEAN, BIGINT, DOUBLE or VARCHAR. */
  pulseten_type_t m_type;

  /** The member matching type. */
  union {
    /** BOOLEAN: nonzero for true. */
    int m_b;

    /** BIGINT. */
    int64_t m_i;

    /** DOUBLE; must be finite. */
    double m_d;

    /** VARCHAR. */
    struct {
      /** The string, UTF-8; needs no terminating NUL. */
      const char *m_s;

      /** Length of s in bytes, at most PULSETEN_MAX_STRING_LEN. */
      size_t m_len;
    } m_str;
  } m_value;
} pulseten_field_t;

/** A point: a row of a table. Columns it does not name are NULL. */
typedef struct {
  /** Microseconds since 1970-01-01 00:00:00 UTC, in
  [PULSETEN_MIN_TIME_US, PULSETEN_MAX_TIME_US). */
  int64_t m_time_us;

  /** The tags; may be NULL when m_n_tags is 0. */
  const pulseten_tag_t *m_tags;

  /** Number of tags. */
  uint32_t m_n_tags;

  /** The fields, at least one. */
  const pulseten_field_t *m_fields;

  /** Number of fields; tags and fields together fewer than
  PULSETEN_MAX_COLUMNS. */
  uint32_t m_n_fields;
} pulseten_point_t;

/** Write a batch of points to a table, creating the table and any new
columns. The batch is accepted or rejected as a whole; when this returns
PULSETEN_OK it is logged and visible to new queries. With m_wal_sync enabled,
it is also durable; the default does not wait for a WAL sync.
@param[in]  db        database
@param[in]  table     table name, NUL-terminated
@param[in]  points    the points
@param[in]  n_points  number of points, 1 to PULSETEN_MAX_POINTS
@retval PULSETEN_OK the batch is logged and visible
@retval PULSETEN_INVALID_ARGUMENT rejected; nothing written
@retval PULSETEN_SCHEMA_CONFLICT rejected; nothing written
@retval PULSETEN_LIMIT_EXCEEDED rejected; nothing written
@retval PULSETEN_OUT_OF_MEMORY rejected; nothing written
@retval PULSETEN_ERROR the full memtable's last flush failed; nothing written
@retval PULSETEN_CORRUPTION the full memtable's last flush found bad metadata
@retval PULSETEN_FENCED rejected; reopen
@retval PULSETEN_OUTCOME_UNKNOWN may or may not be durable */
pulseten_status_t pulseten_write(pulseten_db_t *db, const char *table,
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

  /** Number of columns, "time" included. */
  uint32_t m_n_columns;

  /** Columns in the order they were added, "time" first. */
  const pulseten_column_t *m_columns;
} pulseten_table_t;

/** Metadata returned by pulseten_catalog(). Treat all members as read-only. */
typedef struct {
  /** Number of managed time-series tables. */
  uint32_t m_n_tables;

  /** Tables sorted by name. */
  const pulseten_table_t *m_tables;
} pulseten_catalog_t;

/** Return every managed time-series table and its newest columns, including
tables and columns whose writes have not been flushed. The result owns its
names and arrays; they remain valid until pulseten_catalog_destroy(), even
after later writes or closing the database. Ordinary tables are not included.
@param[in]   db   database
@param[out]  out  the metadata, or NULL on failure
@retval PULSETEN_OK the metadata was returned
@retval PULSETEN_INVALID_ARGUMENT db or out is NULL
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_status_t pulseten_catalog(pulseten_db_t *db, pulseten_catalog_t **out);

/** Free a catalog.
@param[in]  catalog  catalog; may be NULL */
void pulseten_catalog_destroy(pulseten_catalog_t *catalog);

/** Execute one CREATE TABLE or DROP TABLE for an ordinary table in main.
CREATE TABLE AS SELECT may read ordinary and time-series tables through one
consistent snapshot. Managed time-series tables and internal objects cannot
be changed. Temporary tables, foreign keys and CASCADE are not supported.
Writes and flushes may continue during DDL execution. Its commit serializes
with flushes, writes and snapshot creation; existing query snapshots remain
valid. On success the change is durable.
@param[in]  db   database
@param[in]  sql  one DDL statement, NUL-terminated
@retval PULSETEN_OK committed
@retval PULSETEN_INVALID_ARGUMENT invalid or unsupported statement
@retval PULSETEN_NOT_FOUND DROP target does not exist
@retval PULSETEN_ERROR DuckDB rejected the statement; nothing committed
@retval PULSETEN_OUT_OF_MEMORY allocation failed
@retval PULSETEN_CORRUPTION stored metadata failed validation
@retval PULSETEN_FENCED an earlier failure stopped the database
@retval PULSETEN_OUTCOME_UNKNOWN the commit failed; close and reopen */
pulseten_status_t pulseten_exec_ddl(pulseten_db_t *db, const char *sql);

/** Drop an ordinary table. Managed time-series tables are rejected without
flushing or changing their data or schemas. Equivalent to pulseten_exec_ddl()
with DROP TABLE.
@param[in]  db     database
@param[in]  table  table name, NUL-terminated
@retval PULSETEN_OK dropped and durable
@retval PULSETEN_NOT_FOUND no such table
@retval PULSETEN_INVALID_ARGUMENT invalid arguments, or a managed time-series
  table
@retval PULSETEN_FENCED an earlier failure stopped writes
@retval PULSETEN_ERROR DuckDB rejected the drop
@retval PULSETEN_OUT_OF_MEMORY allocation failed
@retval PULSETEN_CORRUPTION stored metadata failed validation
@retval PULSETEN_OUTCOME_UNKNOWN the commit failed; close and reopen */
pulseten_status_t pulseten_drop_table(pulseten_db_t *db, const char *table);

/* ---- Querying -------------------------------------------------------- */

typedef struct pulseten_result_t pulseten_result_t;

/** Start a SELECT over one consistent snapshot of every write so far. This
checks and plans the statement; it runs, and rows are produced, while they are
fetched, so errors found while running are returned by pulseten_result_fetch().
CTEs, subqueries and joins are allowed; table functions, qualified table names
and internal or system objects are rejected. Until the result is destroyed it
holds the snapshot, so destroy results promptly.
@param[in]   db   database
@param[in]   sql  a single SELECT, NUL-terminated
@param[out]  out  the result, or NULL on failure
@retval PULSETEN_OK the query was prepared; fetch rows with
pulseten_result_fetch()
@retval PULSETEN_INVALID_ARGUMENT the statement is not allowed, or a result
  column has a type that cannot be returned
@retval PULSETEN_ERROR SQL error
@retval PULSETEN_FENCED an earlier failure stopped the database
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_status_t pulseten_query(pulseten_db_t *db, const char *sql,
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
@param[in]   result  result
@param[out]  n_rows  rows in the chunk, at most PULSETEN_FETCH_ROWS; 0 after
                     the last
@retval PULSETEN_OK a chunk is available, or the result reached its end
@retval PULSETEN_ERROR the query failed while running
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
int pulseten_result_get_bool(pulseten_result_t *result, uint32_t col,
                             uint32_t row);

/** Read a BIGINT value, or a TIMESTAMP in microseconds since 1970-01-01
00:00:00 UTC.
@param[in]  result  result
@param[in]  col     a BIGINT or TIMESTAMP column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
int64_t pulseten_result_get_int64(pulseten_result_t *result, uint32_t col,
                                  uint32_t row);

/** Read a DOUBLE value.
@param[in]  result  result
@param[in]  col     a DOUBLE column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
double pulseten_result_get_double(pulseten_result_t *result, uint32_t col,
                                  uint32_t row);

/** Read a VARCHAR value.
@param[in]   result  result
@param[in]   col     a VARCHAR column
@param[in]   row     row of the current chunk
@param[out]  len     length of the string in bytes; may be NULL
@return the string, NUL-terminated and valid until the next fetch or until the
  result is destroyed; NULL, with *len 0, if the value is NULL, col has
  another type, or col or row is out of range */
const char *pulseten_result_get_varchar(pulseten_result_t *result, uint32_t col,
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
@retval PULSETEN_ERROR as for pulseten_query()
@retval PULSETEN_FENCED as for pulseten_query()
@retval PULSETEN_OUT_OF_MEMORY as for pulseten_query()
*/
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

  /** Write-ahead log files. */
  uint64_t m_wal_segments;

  /** Background flushes that stored something. */
  uint64_t m_background_flushes;

  /** Background passes with a failed flush or expiry; at most one per pass. */
  uint64_t m_background_errors;

  /** Writes that had to wait for memory. */
  uint64_t m_throttled_writes;

  /** Time partitions dropped for being past the retention period. */
  uint64_t m_expired_partitions;
} pulseten_stats_t;

/** Read the counters of a database.
@param[in]   db     database; if NULL, every counter is 0
@param[out]  stats  the counters */
void pulseten_get_stats(pulseten_db_t *db, pulseten_stats_t *stats);

/** Read the message of the latest failed background flush or expiry. A failure
that only repeats an earlier fence does not replace it.
@param[in]   db       database
@param[out]  message  the message, NUL-terminated, or NULL if none failed; free
                      it with pulseten_free()
@retval PULSETEN_OK the message, if any, was copied
@retval PULSETEN_INVALID_ARGUMENT db or message is NULL
@retval PULSETEN_OUT_OF_MEMORY the message could not be copied
*/
pulseten_status_t pulseten_copy_background_error(pulseten_db_t *db,
                                                 char **message);

#ifdef __cplusplus
}
#endif

#endif
