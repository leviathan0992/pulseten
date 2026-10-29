/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * pulseten.h
 *    The PulseTen C API.
 *
 * A database is a directory opened by one process at a time. Points are
 * written to tables in batches; a batch is durable and visible to queries
 * when pulseten_write() returns PULSETEN_OK. Tables and columns are created
 * by the writes themselves. Queries are SQL SELECT statements run by DuckDB
 * over one consistent snapshot, and their rows are read chunk by chunk.
 *
 * Functions returning pulseten_state_t update the calling thread's message
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

/** Most rows one pulseten_fetch() returns. */
#define PULSETEN_FETCH_ROWS 2048

/* ---- Status ---------------------------------------------------------- */

typedef enum {
  /** Success. */
  PULSETEN_OK = 0,

  /** SQL error, or a failure with no more specific code. */
  PULSETEN_ERROR = 1,

  /** A write or statement was rejected; nothing changed. */
  PULSETEN_INVALID_ARGUMENT = 2,

  /** A write gives an existing column another role or type. */
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

  /** pulseten_interrupt() stopped the query. */
  PULSETEN_INTERRUPTED = 11,

  /** The named table does not exist. */
  PULSETEN_NOT_FOUND = 12
} pulseten_state_t;

/** Describe the last call returning pulseten_state_t on the calling thread.
Functions with other return types do not update this message.
@return the message, or "" after a successful call returning pulseten_state_t;
  contents valid until the next call returning pulseten_state_t on this thread
*/
const char *pulseten_errmsg(void);

/** Name a status.
@param[in]  state  status
@return a short static name, e.g. "INVALID_ARGUMENT"; "UNKNOWN" for a value
  that is not a status */
const char *pulseten_state_name(pulseten_state_t state);

/* ---- Opening and closing --------------------------------------------- */

typedef struct pulseten_db_t pulseten_db_t;

typedef struct {
  /** Flush in a background thread; nonzero by default. Without it, callers
  use pulseten_flush(); dropping a table also flushes before deletion. */
  int background_flush;

  /** Idle wait between background passes, in milliseconds; must be greater
  than 0 when background_flush is enabled. Flush work, expiry and scheduling
  add to elapsed time; flush_bytes may wake the thread sooner. */
  uint32_t flush_interval_ms;

  /** Wake the background flusher when unflushed column data reaches this
  many bytes. */
  uint64_t flush_bytes;

  /** With background flushing, new writes wait while unflushed column data
  is at or above this many bytes; 0 disables waiting. Excludes planning buffers
  and retired batches held by snapshots; this is not a process memory limit. */
  uint64_t memtable_limit_bytes;

  /** Size at which the write-ahead log starts a new file. */
  uint64_t wal_segment_bytes;

  /** Days covered by one time partition, 1 to
  PULSETEN_MAX_PARTITION_DAYS. Used when the database is created; an
  existing one keeps its own. */
  uint32_t partition_days;

  /** Keep this many days before the current time; 0, the default, keeps
  everything. Older writes are rejected, and stored time partitions that
  hold only older data are dropped by the background flush, or by
  pulseten_expire(). A partition is dropped whole, so up to partition_days
  more are kept. At most PULSETEN_MAX_RETENTION_DAYS. Applies to this open
  only. */
  uint32_t retention_days;
} pulseten_options_t;

/** Fill options with the defaults.
@param[out]  options  options to fill */
void pulseten_options_init(pulseten_options_t *options);

/** Open a database directory, creating it if it does not exist; its parent
must exist. Recovers every write that was durable.
@param[in]   dir      directory path
@param[in]   options  settings, or NULL for the defaults
@param[out]  db       open database, or NULL on failure
@retval PULSETEN_OK the database is open and recovered
@retval PULSETEN_INVALID_ARGUMENT an option is out of range
@retval PULSETEN_LOCKED another handle in this process, or another process,
  holds the directory lock
@retval PULSETEN_CORRUPTION stored data failed validation
@retval PULSETEN_IO_ERROR the directory could not be opened
@retval PULSETEN_ERROR the directory could not be opened
@retval PULSETEN_OUT_OF_MEMORY the directory could not be opened
*/
pulseten_state_t pulseten_open(const char *dir,
                               const pulseten_options_t *options,
                               pulseten_db_t **db);

/** Close a database. Destroy every result first. Unflushed writes stay
durable in the write-ahead log.
@param[in]  db  database; may be NULL
@retval PULSETEN_OK the database is closed
@retval PULSETEN_IO_ERROR a file could not be closed; the database is closed
  anyway */
pulseten_state_t pulseten_close(pulseten_db_t *db);

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

/** A VARCHAR column with the tag role. PulseTen creates no dedicated tag
index. Input strings need no terminating NUL. */
typedef struct {
  /** Column name: an identifier, matched case-insensitively. */
  const char *name;

  /** Length of name in bytes, at most PULSETEN_MAX_NAME_LEN. */
  size_t name_len;

  /** The value, UTF-8. */
  const char *value;

  /** Length of value in bytes, at most PULSETEN_MAX_STRING_LEN. */
  size_t value_len;
} pulseten_tag_t;

/** A field: a BOOLEAN, BIGINT, DOUBLE or VARCHAR column. */
typedef struct {
  /** Column name: an identifier, matched case-insensitively. */
  const char *name;

  /** Length of name in bytes, at most PULSETEN_MAX_NAME_LEN. */
  size_t name_len;

  /** BOOLEAN, BIGINT, DOUBLE or VARCHAR. */
  pulseten_type_t type;

  /** The member matching type. */
  union {
    /** BOOLEAN: nonzero for true. */
    int b;

    /** BIGINT. */
    int64_t i;

    /** DOUBLE; must be finite. */
    double d;

    /** VARCHAR. */
    struct {
      /** The string, UTF-8; needs no terminating NUL. */
      const char *s;

      /** Length of s in bytes, at most PULSETEN_MAX_STRING_LEN. */
      size_t len;
    } str;
  } value;
} pulseten_field_t;

/** A point: a row of a table. Columns it does not name are NULL. */
typedef struct {
  /** Microseconds since 1970-01-01 00:00:00 UTC, in
  [PULSETEN_MIN_TIME_US, PULSETEN_MAX_TIME_US). */
  int64_t time_us;

  /** The tags; may be NULL when n_tags is 0. */
  const pulseten_tag_t *tags;

  /** Number of tags. */
  uint32_t n_tags;

  /** The fields, at least one. */
  const pulseten_field_t *fields;

  /** Number of fields; tags and fields together fewer than
  PULSETEN_MAX_COLUMNS. */
  uint32_t n_fields;
} pulseten_point_t;

/** Write a batch of points to a table, creating the table and any new
columns. The batch is accepted or rejected as a whole; when this returns
PULSETEN_OK it is durable and visible to new queries.
@param[in]   db        database
@param[in]   table     table name, NUL-terminated
@param[in]   points    the points
@param[in]   n_points  number of points, 1 to PULSETEN_MAX_POINTS
@param[out]  seq       sequence number of the write; may be NULL
@retval PULSETEN_OK the batch is durable and visible
@retval PULSETEN_INVALID_ARGUMENT rejected; nothing written
@retval PULSETEN_SCHEMA_CONFLICT rejected; nothing written
@retval PULSETEN_LIMIT_EXCEEDED rejected; nothing written
@retval PULSETEN_OUT_OF_MEMORY rejected; nothing written

@retval PULSETEN_FENCED rejected; reopen
@retval PULSETEN_OUTCOME_UNKNOWN may or may not be durable */
pulseten_state_t pulseten_write(pulseten_db_t *db, const char *table,
                                const pulseten_point_t *points,
                                uint32_t n_points, uint64_t *seq);

/** Store every unflushed write in the analytical storage now. Writes and
queries go on meanwhile. Not needed for durability.
@param[in]  db  database
@retval PULSETEN_OK stored, or nothing to store
@retval PULSETEN_ERROR nothing stored; may be retried
@retval PULSETEN_OUT_OF_MEMORY nothing stored; may be retried

@retval PULSETEN_FENCED an earlier failure stopped writes
@retval PULSETEN_OUTCOME_UNKNOWN the store may or may not be durable; writes
  are stopped */
pulseten_state_t pulseten_flush(pulseten_db_t *db);

/** Drop the stored time partitions whose days have all passed the retention
period, measured back from now. The background flush does this after each
flush; without it, call this from time to time. Does nothing without a
retention period. Unflushed writes are not affected. Queries started before it
still read the dropped partitions.
@param[in]   db       database
@param[out]  removed  partitions dropped; may be NULL
@retval PULSETEN_OK expiration completed; no eligible partitions also succeeds
@retval PULSETEN_ERROR nothing dropped; may be retried
@retval PULSETEN_FENCED an earlier failure stopped writes
@retval PULSETEN_OUTCOME_UNKNOWN the drop may or may not be durable; writes
  are stopped */
pulseten_state_t pulseten_expire(pulseten_db_t *db, uint64_t *removed);

/* ---- Tables ---------------------------------------------------------- */

typedef enum {
  /** Column 0, "time", a TIMESTAMP. */
  PULSETEN_ROLE_TIME = 0,

  /** A VARCHAR tag. */
  PULSETEN_ROLE_TAG = 1,

  /** A BOOLEAN, BIGINT, DOUBLE or VARCHAR field. */
  PULSETEN_ROLE_FIELD = 2
} pulseten_role_t;

/** A column of a table. */
typedef struct {
  /** Lower-case name. */
  const char *name;

  /** Time, tag or field. */
  pulseten_role_t role;

  /** Value type. */
  pulseten_type_t type;
} pulseten_column_t;

typedef struct pulseten_catalog_t pulseten_catalog_t;

/** Take a snapshot of every table and its newest columns, whether its writes
are flushed or not. Tables are sorted by name; columns are in the order they
were added, "time" first. Later writes do not change it.
@param[in]   db       database
@param[out]  catalog  the snapshot, or NULL on failure; destroy it with
                      pulseten_catalog_destroy()
@retval PULSETEN_OK the catalog snapshot was copied
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_state_t pulseten_catalog_get(pulseten_db_t *db,
                                      pulseten_catalog_t **catalog);

/** Count the tables of a catalog.
@param[in]  catalog  catalog
@return the number of tables; 0 if catalog is NULL */
uint32_t pulseten_catalog_table_count(const pulseten_catalog_t *catalog);

/** Name a table of a catalog.
@param[in]  catalog  catalog
@param[in]  table    table index, 0 to pulseten_catalog_table_count() - 1
@return the lower-case name, valid until the catalog is destroyed; NULL if
  table is out of range */
const char *pulseten_catalog_table_name(const pulseten_catalog_t *catalog,
                                        uint32_t table);

/** Find a table by name, matched case-insensitively.
@param[in]   catalog  catalog
@param[in]   name     table name, NUL-terminated
@param[out]  table    its index
@retval PULSETEN_OK the table was found
@retval PULSETEN_NOT_FOUND no table matched the name */
pulseten_state_t pulseten_catalog_find(const pulseten_catalog_t *catalog,
                                       const char *name, uint32_t *table);

/** Count the columns of a table of a catalog, "time" included.
@param[in]  catalog  catalog
@param[in]  table    table index
@return the number of columns; 0 if table is out of range */
uint32_t pulseten_catalog_column_count(const pulseten_catalog_t *catalog,
                                       uint32_t table);

/** Describe a column of a table of a catalog.
@param[in]  catalog  catalog
@param[in]  table    table index
@param[in]  col      column index, 0 to pulseten_catalog_column_count() - 1
@return the column, valid until the catalog is destroyed; NULL if table or col
  is out of range */
const pulseten_column_t *pulseten_catalog_column(
    const pulseten_catalog_t *catalog, uint32_t table, uint32_t col);

/** Free a catalog.
@param[in]  catalog  catalog; may be NULL */
void pulseten_catalog_destroy(pulseten_catalog_t *catalog);

/** Drop a table: its stored and unflushed rows and its columns. Its name may
then be written again, creating a new table with any columns. Writes to every
table wait while it runs, since it first flushes. Queries started before it
still read the table.
@param[in]  db     database
@param[in]  table  table name, NUL-terminated
@retval PULSETEN_OK dropped and durable
@retval PULSETEN_NOT_FOUND no such table
@retval PULSETEN_INVALID_ARGUMENT not a valid table name
@retval PULSETEN_ERROR nothing dropped; may be retried
@retval PULSETEN_FENCED an earlier failure stopped writes
@retval PULSETEN_OUTCOME_UNKNOWN the drop may or may not be durable; writes
  are stopped */
pulseten_state_t pulseten_drop_table(pulseten_db_t *db, const char *table);

/* ---- Querying -------------------------------------------------------- */

typedef struct pulseten_result_t pulseten_result_t;

/** Start a SELECT over one consistent snapshot of every write so far. This
checks and plans the statement; it runs, and rows are produced, while they are
fetched, so errors found while running are returned by pulseten_fetch(). CTEs,
subqueries and joins are allowed; table functions, qualified table names and
internal or system objects are rejected. Until the result is destroyed it
holds the snapshot, so destroy results promptly.
@param[in]   db      database
@param[in]   sql     a single SELECT, NUL-terminated
@param[out]  result  the result, or NULL on failure
@retval PULSETEN_OK the query was prepared; fetch rows with pulseten_fetch()
@retval PULSETEN_INVALID_ARGUMENT the statement is not allowed, or a result
  column has a type that cannot be returned
@retval PULSETEN_ERROR SQL error
@retval PULSETEN_FENCED an earlier failure stopped the database
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_state_t pulseten_query(pulseten_db_t *db, const char *sql,
                                pulseten_result_t **result);

/** Count the result columns.
@param[in]  result  result
@return the number of columns; 0 if result is NULL */
uint32_t pulseten_column_count(pulseten_result_t *result);

/** Name a result column.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_column_count() - 1
@return the name, valid until the result is destroyed; NULL if col is out of
  range */
const char *pulseten_column_name(pulseten_result_t *result, uint32_t col);

/** Get the type of a result column.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_column_count() - 1
@return the type its values are read as; 0 if col is out of range */
pulseten_type_t pulseten_column_type(pulseten_result_t *result, uint32_t col);

/** Read the next chunk of rows, replacing the previous one.
@param[in]   result  result
@param[out]  n_rows  rows in the chunk, at most PULSETEN_FETCH_ROWS; 0 after
                     the last
@retval PULSETEN_OK a chunk is available, or the result reached its end
@retval PULSETEN_ERROR the query failed while running
@retval PULSETEN_INVALID_ARGUMENT a value does not fit its column type
@retval PULSETEN_INTERRUPTED pulseten_interrupt() stopped the query; so does
  every later fetch
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_state_t pulseten_fetch(pulseten_result_t *result, uint32_t *n_rows);

/** Request interruption of an active query from another thread. The running
or next fetch observes the request; after it returns PULSETEN_INTERRUPTED,
every later fetch does too. Normal end is reached when pulseten_fetch()
returns PULSETEN_OK with 0 rows; interrupting after that leaves later fetches
successful with 0 rows. Must not run at the same time as
pulseten_result_destroy().
@param[in]  result  result; may be NULL */
void pulseten_interrupt(pulseten_result_t *result);

/* The functions below read a value of the current chunk: the rows the
last pulseten_fetch() returned. */

/** Tell whether a value is NULL.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_column_count() - 1
@param[in]  row     row of the current chunk
@return nonzero if the value is NULL, or if col or row is out of range */
int pulseten_is_null(pulseten_result_t *result, uint32_t col, uint32_t row);

/** Read a BOOLEAN value.
@param[in]  result  result
@param[in]  col     a BOOLEAN column
@param[in]  row     row of the current chunk
@return 1 for true, 0 for false; 0 if the value is NULL, col is not BOOLEAN,
  or col or row is out of range */
int pulseten_get_bool(pulseten_result_t *result, uint32_t col, uint32_t row);

/** Read a BIGINT value, or a TIMESTAMP in microseconds since 1970-01-01
00:00:00 UTC.
@param[in]  result  result
@param[in]  col     a BIGINT or TIMESTAMP column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
int64_t pulseten_get_int64(pulseten_result_t *result, uint32_t col,
                           uint32_t row);

/** Read a DOUBLE value.
@param[in]  result  result
@param[in]  col     a DOUBLE column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
double pulseten_get_double(pulseten_result_t *result, uint32_t col,
                           uint32_t row);

/** Read a VARCHAR value.
@param[in]   result  result
@param[in]   col     a VARCHAR column
@param[in]   row     row of the current chunk
@param[out]  len     length of the string in bytes; may be NULL
@return the string, NUL-terminated and valid until the next fetch or until the
  result is destroyed; NULL, with *len 0, if the value is NULL, col has
  another type, or col or row is out of range */
const char *pulseten_get_varchar(pulseten_result_t *result, uint32_t col,
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
pulseten_state_t pulseten_explain(pulseten_db_t *db, const char *sql,
                                  char **plan);

/** Free memory that PulseTen returned to the caller.
@param[in]  p  memory, e.g. a plan from pulseten_explain(); may be NULL */
void pulseten_free(void *p);

/* ---- Statistics ------------------------------------------------------ */

typedef struct {
  /** Sequence number of the newest write in the analytical storage. */
  uint64_t flushed_seq;

  /** Sequence number of the newest visible write. */
  uint64_t visible_seq;

  /** Bytes of unflushed writes held in memory. */
  uint64_t memtable_bytes;

  /** Write-ahead log files. */
  uint64_t wal_segments;

  /** Background flushes that stored something. */
  uint64_t background_flushes;

  /** Background flushes or expiries that failed. */
  uint64_t background_errors;

  /** Writes that had to wait for memory. */
  uint64_t throttled_writes;

  /** Time partitions dropped for being past the retention period. */
  uint64_t expired_partitions;
} pulseten_stats_t;

/** Read the counters of a database.
@param[in]   db     database; if NULL, every counter is 0
@param[out]  stats  the counters */
void pulseten_get_stats(pulseten_db_t *db, pulseten_stats_t *stats);

#ifdef __cplusplus
}
#endif

#endif
