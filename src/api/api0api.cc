/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * api0api.cc
 *    The C API of include/pulseten.h, on top of the engine.
 *
 * Each function turns its arguments into the engine's types, calls the
 * engine and turns the result back. Errors are status codes; checked
 * allocations that fail return PULSETEN_OUT_OF_MEMORY. The message of the
 * last failure is kept per thread.
 *
 *-------------------------------------------------------------------------
 */

#include "pulseten.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include <strings.h>

#include "engine0engine.h"
#include "os0file.h"
#include "ut0err.h"
#include "ut0new.h"
#include "ut0time.h"

/* pulseten_column_type() relies on these being equal. */
static_assert(PULSETEN_TYPE_BOOLEAN == (int)COL_BOOLEAN &&
                  PULSETEN_TYPE_BIGINT == (int)COL_BIGINT &&
                  PULSETEN_TYPE_DOUBLE == (int)COL_DOUBLE &&
                  PULSETEN_TYPE_TIMESTAMP == (int)COL_TIMESTAMP &&
                  PULSETEN_TYPE_VARCHAR == (int)COL_VARCHAR,
              "C types and column types differ");

/* pulseten_catalog_column() relies on these being equal. */
static_assert(PULSETEN_ROLE_TIME == (int)COL_ROLE_TIME &&
                  PULSETEN_ROLE_TAG == (int)COL_ROLE_TAG &&
                  PULSETEN_ROLE_FIELD == (int)COL_ROLE_FIELD,
              "C roles and column roles differ");

/* The limits in pulseten.h are the engine's. */
static_assert(PULSETEN_MAX_POINTS == INGEST_MAX_POINTS &&
                  PULSETEN_MAX_BATCH_DAYS == INGEST_MAX_DAYS &&
                  PULSETEN_MAX_BATCH_BYTES == INGEST_MAX_BATCH_BYTES &&
                  PULSETEN_MAX_COLUMNS == SCHEMA_MAX_COLUMNS &&
                  PULSETEN_MAX_NAME_LEN == SCHEMA_MAX_NAME_LEN &&
                  PULSETEN_MAX_STRING_LEN == INGEST_MAX_STRING &&
                  PULSETEN_MIN_TIME_US == INGEST_MIN_TIME_US &&
                  PULSETEN_MAX_TIME_US == INGEST_MAX_TIME_US &&
                  PULSETEN_MAX_PARTITION_DAYS == STORAGE_MAX_PARTITION_DAYS &&
                  PULSETEN_MAX_RETENTION_DAYS == ENGINE_MAX_RETENTION_DAYS &&
                  PULSETEN_FETCH_ROWS == QUERY_CHUNK_ROWS,
              "pulseten.h limits differ from the engine's");

static_assert(PULSETEN_VERSION_NUMBER == PULSETEN_VERSION_MAJOR * 10000 +
                                             PULSETEN_VERSION_MINOR * 100 +
                                             PULSETEN_VERSION_PATCH,
              "version number and version parts differ");

/** An open database. */
struct pulseten_db_t {
  /** The engine behind it. */
  engine_t *engine{nullptr};
};

/** A snapshot of the tables and their columns. */
struct pulseten_catalog_t {
  /** Newest schema of every table, by name. */
  std::vector<schema_t> tables;

  /** columns[t][c] describes tables[t].cols[c]; its name points into
  tables. */
  std::vector<std::vector<pulseten_column_t>> columns;
};

/** A query being read chunk by chunk. */
struct pulseten_result_t {
  /** The engine's cursor. */
  engine_cursor_t *cursor{nullptr};

  /** Result columns. */
  std::vector<query_column_t> columns;

  /** The current chunk. */
  query_rows_t rows;
};

/** Message of the last failure on this thread. */
static thread_local char api_error[2048] = {};

/** Translate an internal engine status to the public C status.
@param[in]  err  internal engine status
@return the C status of an engine error code */
static pulseten_state_t api_state(dberr_t err) {
  switch (err) {
    case DB_SUCCESS:
      return (PULSETEN_OK);
    case DB_ERROR:
      return (PULSETEN_ERROR);
    case DB_OUT_OF_MEMORY:
      return (PULSETEN_OUT_OF_MEMORY);
    case DB_IO_ERROR:
      return (PULSETEN_IO_ERROR);
    case DB_CORRUPTION:
      return (PULSETEN_CORRUPTION);
    case DB_INVALID_ARGUMENT:
      return (PULSETEN_INVALID_ARGUMENT);
    case DB_FENCED:
      return (PULSETEN_FENCED);
    case DB_LOCKED:
      return (PULSETEN_LOCKED);
    case DB_SCHEMA_CONFLICT:
      return (PULSETEN_SCHEMA_CONFLICT);
    case DB_LIMIT_EXCEEDED:
      return (PULSETEN_LIMIT_EXCEEDED);
    case DB_OUTCOME_UNKNOWN:
      return (PULSETEN_OUTCOME_UNKNOWN);
    case DB_NOT_FOUND:
      return (PULSETEN_NOT_FOUND);
    case DB_INTERRUPTED:
      return (PULSETEN_INTERRUPTED);
  }

  return (PULSETEN_ERROR);
}

/** Record the outcome of a call.
@param[in]  err      engine error code
@param[in]  message  what the engine said; may be empty
@return the C status */
static pulseten_state_t api_result(dberr_t err, const char *message) {
  api_error[0] = '\0';
  if (err == DB_SUCCESS) {
    return (PULSETEN_OK);
  }

  const char *text =
      message != nullptr && message[0] != '\0' ? message : ut_strerr(err);
  size_t len = strlen(text);
  if (len >= sizeof(api_error)) {
    len = sizeof(api_error) - 1;
  }
  memcpy(api_error, text, len);
  api_error[len] = '\0';

  return (api_state(err));
}

/** api_result() with the engine's message as a string.
@param[in]  err      internal error code
@param[in]  message  diagnostic text
@return the public C status corresponding to err; updates the calling thread's
  error message */
static pulseten_state_t api_result(dberr_t err, const std::string &message) {
  return (api_result(err, message.c_str()));
}

/** Record a rejected argument.
@param[in]  message  diagnostic text
@return PULSETEN_INVALID_ARGUMENT, with message recorded for the calling
  thread */
static pulseten_state_t api_invalid(const char *message) {
  return (api_result(DB_INVALID_ARGUMENT, message));
}

/** Get the version of the linked library, which may differ from the header
the caller was compiled with.
@return the version as PULSETEN_VERSION_STRING has it, e.g. "0.1.0" */
const char *pulseten_version(void) {
  return (PULSETEN_VERSION_STRING);
}

/** Get the version of the linked library as a number.
@return MAJOR * 10000 + MINOR * 100 + PATCH, as PULSETEN_VERSION_NUMBER */
int pulseten_version_number(void) {
  return (PULSETEN_VERSION_NUMBER);
}

/** Describe the last call returning pulseten_state_t on the calling thread.
Functions with other return types do not update this message.
@return the message, or "" after a successful call returning pulseten_state_t;
  contents valid until the next call returning pulseten_state_t on this thread
*/
const char *pulseten_errmsg(void) {
  return (api_error);
}

/** Name a status.
@param[in]  state  status
@return a short static name, e.g. "INVALID_ARGUMENT"; "UNKNOWN" for a value
  that is not a status */
const char *pulseten_state_name(pulseten_state_t state) {
  switch (state) {
    case PULSETEN_OK:
      return ("OK");
    case PULSETEN_ERROR:
      return ("ERROR");
    case PULSETEN_INVALID_ARGUMENT:
      return ("INVALID_ARGUMENT");
    case PULSETEN_SCHEMA_CONFLICT:
      return ("SCHEMA_CONFLICT");
    case PULSETEN_LIMIT_EXCEEDED:
      return ("LIMIT_EXCEEDED");
    case PULSETEN_OUT_OF_MEMORY:
      return ("OUT_OF_MEMORY");
    case PULSETEN_IO_ERROR:
      return ("IO_ERROR");
    case PULSETEN_CORRUPTION:
      return ("CORRUPTION");
    case PULSETEN_LOCKED:
      return ("LOCKED");
    case PULSETEN_FENCED:
      return ("FENCED");
    case PULSETEN_OUTCOME_UNKNOWN:
      return ("OUTCOME_UNKNOWN");
    case PULSETEN_INTERRUPTED:
      return ("INTERRUPTED");
    case PULSETEN_NOT_FOUND:
      return ("NOT_FOUND");
  }

  return ("UNKNOWN");
}

/* ---- Opening and closing --------------------------------------------- */

/** Fill options with the defaults.
@param[out]  options  options to fill */
void pulseten_options_init(pulseten_options_t *options) {
  const engine_options_t defaults;

  options->background_flush = defaults.background_flush ? 1 : 0;
  options->flush_interval_ms = defaults.flush_interval_ms;
  options->flush_bytes = defaults.flush_bytes;
  options->memtable_limit_bytes = defaults.memtable_limit_bytes;
  options->wal_segment_bytes = defaults.wal_segment_bytes;
  options->partition_days = defaults.partition_days;
  options->retention_days = defaults.retention_days;
}

/** Open a database directory, creating it if it does not exist; its parent
must exist.
@param[in]   dir      directory path
@param[in]   options  settings, or NULL for the defaults
@param[out]  out      open database, or NULL on failure
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
                               pulseten_db_t **out) {
  if (out == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  *out = nullptr;

  if (dir == nullptr || dir[0] == '\0') {
    return (api_invalid("dir must be a path"));
  }

  engine_options_t engine_options;

  if (options != nullptr) {
    engine_options.background_flush = options->background_flush != 0;
    engine_options.flush_interval_ms = options->flush_interval_ms;
    engine_options.flush_bytes = options->flush_bytes;
    engine_options.memtable_limit_bytes = options->memtable_limit_bytes;
    engine_options.wal_segment_bytes = options->wal_segment_bytes;
    engine_options.partition_days = options->partition_days;
    engine_options.retention_days = options->retention_days;
  }

  dberr_t err = os_dir_create(dir);
  if (err != DB_SUCCESS) {
    if (err != DB_IO_ERROR) {
      return (api_result(err, ""));
    }
    char message[2048];
    snprintf(message, sizeof(message), "cannot create directory %s: %s", dir,
             strerror(os_last_errno()));

    return (api_result(err, message));
  }

  pulseten_db_t *db = ut_new<pulseten_db_t>();
  if (db == nullptr) {
    return (api_result(DB_OUT_OF_MEMORY, ""));
  }

  std::string error;

  err = engine_open(dir, &engine_options, &db->engine, nullptr, error);

  if (err != DB_SUCCESS) {
    ut_delete(db);

    return (api_result(err, error));
  }

  *out = db;

  return (api_result(DB_SUCCESS, ""));
}

/** Close a database.
@param[in]  db  database; may be NULL
@retval PULSETEN_OK the database is closed
@retval PULSETEN_IO_ERROR a file could not be closed; the database is closed
  anyway */
pulseten_state_t pulseten_close(pulseten_db_t *db) {
  if (db == nullptr) {
    return (api_result(DB_SUCCESS, ""));
  }

  dberr_t err = engine_close(db->engine);
  ut_delete(db);

  return (api_result(err, ""));
}

/* ---- Writing --------------------------------------------------------- */

/** Convert the C API points, then write one engine batch; ingest checks
them. Temporary tag and field arrays borrow the input strings until the write
returns.
@param[in,out]  db        database receiving the batch
@param[in]      table     NUL-terminated table name
@param[in]      points    points to convert; NULL only if n_points is 0
@param[in]      n_points  number of points
@param[out]     seq       write sequence number, if assigned; may be NULL
@param[out]     error     validation or engine diagnostic
@return DB_SUCCESS when the batch is durable and visible; otherwise the
  validation or engine error, including DB_OUTCOME_UNKNOWN if durability is
  uncertain */
static dberr_t api_write(pulseten_db_t &db, const char *table,
                         const pulseten_point_t *points, uint32_t n_points,
                         uint64_t *seq, std::string &error) {
  /* Only what conversion needs: bounded arrays to allocate and non-NULL
  arrays to walk. Everything else is checked by ingest. */
  if (n_points > INGEST_MAX_POINTS) {
    error =
        "batch has more than " + std::to_string(INGEST_MAX_POINTS) + " points";

    return (DB_LIMIT_EXCEEDED);
  }

  size_t n_tags = 0;
  size_t n_fields = 0;
  uint64_t min_bytes = 0;

  for (uint32_t p = 0; p < n_points; ++p) {
    const pulseten_point_t &in = points[p];

    if ((in.n_tags > 0 && in.tags == nullptr) ||
        (in.n_fields > 0 && in.fields == nullptr)) {
      error = "point " + std::to_string(p) + ": tags or fields are NULL";

      return (DB_INVALID_ARGUMENT);
    }

    n_tags += in.n_tags;
    n_fields += in.n_fields;

    /* Ingest counts eight bytes per time, tag and field, before strings
    and NULL bitmaps. A batch already over that lower bound cannot fit. */
    min_bytes += 8 + 8 * ((uint64_t)in.n_tags + in.n_fields);
    if (min_bytes > INGEST_MAX_BATCH_BYTES) {
      error = "batch data exceeds " + std::to_string(INGEST_MAX_BATCH_BYTES) +
              " bytes";

      return (DB_LIMIT_EXCEEDED);
    }
  }

  std::vector<ingest_tag_t> tags(n_tags);
  std::vector<ingest_field_t> fields(n_fields);
  std::vector<ingest_point_t> converted(n_points);

  size_t t = 0;
  size_t f = 0;

  for (uint32_t p = 0; p < n_points; ++p) {
    const pulseten_point_t &in = points[p];
    ingest_point_t &out = converted[p];

    out.time_us = in.time_us;
    out.tags = in.n_tags > 0 ? tags.data() + t : nullptr;
    out.n_tags = in.n_tags;
    out.fields = in.n_fields > 0 ? fields.data() + f : nullptr;
    out.n_fields = in.n_fields;

    for (uint32_t i = 0; i < in.n_tags; ++i, ++t) {
      tags[t].name = in.tags[i].name;
      tags[t].name_len = in.tags[i].name_len;
      tags[t].value = in.tags[i].value;
      tags[t].value_len = in.tags[i].value_len;
    }

    for (uint32_t i = 0; i < in.n_fields; ++i, ++f) {
      const pulseten_field_t &src = in.fields[i];
      ingest_field_t &dst = fields[f];

      dst.name = src.name;
      dst.name_len = src.name_len;

      switch (src.type) {
        case PULSETEN_TYPE_BOOLEAN:
          dst.value.type = COL_BOOLEAN;
          dst.value.b = src.value.b != 0;
          break;

        case PULSETEN_TYPE_BIGINT:
          dst.value.type = COL_BIGINT;
          dst.value.i = src.value.i;
          break;

        case PULSETEN_TYPE_DOUBLE:
          dst.value.type = COL_DOUBLE;
          dst.value.d = src.value.d;
          break;

        case PULSETEN_TYPE_VARCHAR:
          dst.value.type = COL_VARCHAR;
          dst.value.s = src.value.str.s;
          dst.value.s_len = src.value.str.len;
          break;

        default:
          error = "a field must be BOOLEAN, BIGINT, DOUBLE or VARCHAR";

          return (DB_INVALID_ARGUMENT);
      }
    }
  }

  batch_seq_t written = 0;

  const dberr_t err = engine_write(*db.engine, table, strlen(table),
                                   converted.data(), n_points, written, error);

  if (seq != nullptr) {
    *seq = written;
  }

  return (err);
}

/** Write a batch of points to a table, creating the table and any new
columns.
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
                                uint32_t n_points, uint64_t *seq) {
  if (seq != nullptr) {
    *seq = 0;
  }

  if (db == nullptr || table == nullptr ||
      (points == nullptr && n_points > 0)) {
    return (api_invalid("db, table and points must not be NULL"));
  }

  std::string error;
  dberr_t err = api_write(*db, table, points, n_points, seq, error);

  return (api_result(err, error));
}

/** Store every unflushed write in the analytical storage now.
@param[in]  db  database
@retval PULSETEN_OK stored, or nothing to store
@retval PULSETEN_ERROR nothing stored; may be retried
@retval PULSETEN_OUT_OF_MEMORY nothing stored; may be retried

@retval PULSETEN_FENCED an earlier failure stopped writes
@retval PULSETEN_OUTCOME_UNKNOWN the store may or may not be durable; writes
  are stopped */
pulseten_state_t pulseten_flush(pulseten_db_t *db) {
  if (db == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  engine_flush_info_t info;
  std::string error;
  dberr_t err = engine_flush(*db->engine, info, error);

  return (api_result(err, error));
}

/** Drop the stored time partitions whose days have all passed the retention
period, measured back from now.
@param[in]   db       database
@param[out]  removed  partitions dropped; may be NULL
@retval PULSETEN_OK expiration completed; no eligible partitions also succeeds
@retval PULSETEN_ERROR nothing dropped; may be retried
@retval PULSETEN_FENCED an earlier failure stopped writes
@retval PULSETEN_OUTCOME_UNKNOWN the drop may or may not be durable; writes
  are stopped */
pulseten_state_t pulseten_expire(pulseten_db_t *db, uint64_t *removed) {
  if (removed != nullptr) {
    *removed = 0;
  }

  if (db == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  uint64_t dropped = 0;
  std::string error;
  dberr_t err = engine_expire(*db->engine, ut_now_us(), dropped, error);

  if (removed != nullptr) {
    *removed = dropped;
  }

  return (api_result(err, error));
}

/* ---- Tables ---------------------------------------------------------- */

/** Fill a new catalog from the engine's tables.
@param[in]   db       open database
@param[out]  catalog  new catalog to fill with owned schema copies and column
                      views
@return DB_SUCCESS after the catalog is filled */
static dberr_t api_catalog(pulseten_db_t &db, pulseten_catalog_t &catalog) {
  engine_list_tables(*db.engine, catalog.tables);

  catalog.columns.resize(catalog.tables.size());

  for (size_t t = 0; t < catalog.tables.size(); ++t) {
    const schema_t &schema = catalog.tables[t];
    std::vector<pulseten_column_t> &columns = catalog.columns[t];

    columns.resize(schema.cols.size());

    for (size_t c = 0; c < schema.cols.size(); ++c) {
      columns[c].name = schema.cols[c].name.c_str();
      columns[c].role = (pulseten_role_t)schema.cols[c].role;
      columns[c].type = (pulseten_type_t)schema.cols[c].type;
    }
  }

  return (DB_SUCCESS);
}

/** Take a snapshot of every table and its newest columns, whether its writes
are flushed or not.
@param[in]   db   database
@param[out]  out  the snapshot, or NULL on failure; destroy it with
                  pulseten_catalog_destroy()
@retval PULSETEN_OK the catalog snapshot was copied
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_state_t pulseten_catalog_get(pulseten_db_t *db,
                                      pulseten_catalog_t **out) {
  if (out == nullptr) {
    return (api_invalid("catalog must not be NULL"));
  }

  *out = nullptr;

  if (db == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  pulseten_catalog_t *catalog = ut_new<pulseten_catalog_t>();
  if (catalog == nullptr) {
    return (api_result(DB_OUT_OF_MEMORY, ""));
  }

  dberr_t err = api_catalog(*db, *catalog);

  if (err != DB_SUCCESS) {
    ut_delete(catalog);

    return (api_result(err, ""));
  }

  *out = catalog;

  return (api_result(DB_SUCCESS, ""));
}

/** Count the tables of a catalog.
@param[in]  catalog  catalog
@return the number of tables; 0 if catalog is NULL */
uint32_t pulseten_catalog_table_count(const pulseten_catalog_t *catalog) {
  return (catalog != nullptr ? (uint32_t)catalog->tables.size() : 0);
}

/** Name a table of a catalog.
@param[in]  catalog  catalog
@param[in]  table    table index, 0 to pulseten_catalog_table_count() - 1
@return the lower-case name, valid until the catalog is destroyed; NULL if
  table is out of range */
const char *pulseten_catalog_table_name(const pulseten_catalog_t *catalog,
                                        uint32_t table) {
  if (catalog == nullptr || table >= catalog->tables.size()) {
    return (nullptr);
  }

  return (catalog->tables[table].name.c_str());
}

/** Find a table by name, matched case-insensitively.
@param[in]   catalog  catalog
@param[in]   name     table name, NUL-terminated
@param[out]  table    its index
@retval PULSETEN_OK the table was found
@retval PULSETEN_NOT_FOUND no table matched the name */
pulseten_state_t pulseten_catalog_find(const pulseten_catalog_t *catalog,
                                       const char *name, uint32_t *table) {
  if (catalog == nullptr || name == nullptr || table == nullptr) {
    return (api_invalid("catalog, name and table must not be NULL"));
  }

  /* Names are stored in lower case and matched ignoring ASCII case. */
  for (size_t t = 0; t < catalog->tables.size(); ++t) {
    if (strcasecmp(catalog->tables[t].name.c_str(), name) == 0) {
      *table = (uint32_t)t;

      return (api_result(DB_SUCCESS, ""));
    }
  }

  return (api_result(DB_NOT_FOUND, "no such table"));
}

/** Count the columns of a table of a catalog, "time" included.
@param[in]  catalog  catalog
@param[in]  table    table index
@return the number of columns; 0 if table is out of range */
uint32_t pulseten_catalog_column_count(const pulseten_catalog_t *catalog,
                                       uint32_t table) {
  if (catalog == nullptr || table >= catalog->columns.size()) {
    return (0);
  }

  return ((uint32_t)catalog->columns[table].size());
}

/** Describe a column of a table of a catalog.
@param[in]  catalog  catalog
@param[in]  table    table index
@param[in]  col      column index, 0 to pulseten_catalog_column_count() - 1
@return the column, valid until the catalog is destroyed; NULL if table or col
  is out of range */
const pulseten_column_t *pulseten_catalog_column(
    const pulseten_catalog_t *catalog, uint32_t table, uint32_t col) {
  if (catalog == nullptr || table >= catalog->columns.size() ||
      col >= catalog->columns[table].size()) {
    return (nullptr);
  }

  return (&catalog->columns[table][col]);
}

/** Free a catalog.
@param[in]  catalog  catalog; may be NULL */
void pulseten_catalog_destroy(pulseten_catalog_t *catalog) {
  ut_delete(catalog);
}

/** Drop a table: its stored and unflushed rows and its columns.
@param[in]  db     database
@param[in]  table  table name, NUL-terminated
@retval PULSETEN_OK dropped and durable
@retval PULSETEN_NOT_FOUND no such table
@retval PULSETEN_INVALID_ARGUMENT not a valid table name
@retval PULSETEN_ERROR nothing dropped; may be retried
@retval PULSETEN_FENCED an earlier failure stopped writes
@retval PULSETEN_OUTCOME_UNKNOWN the drop may or may not be durable; writes
  are stopped */
pulseten_state_t pulseten_drop_table(pulseten_db_t *db, const char *table) {
  if (db == nullptr || table == nullptr) {
    return (api_invalid("db and table must not be NULL"));
  }

  std::string error;
  dberr_t err = engine_drop_table(*db->engine, table, strlen(table), error);

  return (api_result(err, error));
}

/* ---- Querying -------------------------------------------------------- */

/** Start a SELECT over one consistent snapshot of every write so far. This
checks and plans the statement; it runs, and rows are produced, while they are
fetched, so errors found while running are returned by pulseten_fetch(). CTEs,
subqueries and joins are allowed; table functions, qualified table names and
internal or system objects are rejected. Until the result is destroyed it
holds the snapshot, so destroy results promptly.
@param[in]   db   database
@param[in]   sql  a single SELECT, NUL-terminated
@param[out]  out  the result, or NULL on failure
@retval PULSETEN_OK the query was prepared; fetch rows with pulseten_fetch()
@retval PULSETEN_INVALID_ARGUMENT the statement is not allowed, or a result
  column has a type that cannot be returned
@retval PULSETEN_ERROR SQL error
@retval PULSETEN_FENCED an earlier failure stopped the database
@retval PULSETEN_OUT_OF_MEMORY allocation failed */
pulseten_state_t pulseten_query(pulseten_db_t *db, const char *sql,
                                pulseten_result_t **out) {
  if (out == nullptr) {
    return (api_invalid("result must not be NULL"));
  }

  *out = nullptr;

  if (db == nullptr || sql == nullptr) {
    return (api_invalid("db and sql must not be NULL"));
  }

  pulseten_result_t *result = ut_new<pulseten_result_t>();
  if (result == nullptr) {
    return (api_result(DB_OUT_OF_MEMORY, ""));
  }

  std::string error;
  dberr_t err =
      engine_cursor_open(*db->engine, sql, false, &result->cursor, error);

  if (err == DB_SUCCESS) {
    result->columns = engine_cursor_columns(*result->cursor);
  }

  if (err != DB_SUCCESS) {
    pulseten_result_destroy(result);

    return (api_result(err, error));
  }

  *out = result;

  return (api_result(DB_SUCCESS, ""));
}

/** Count the result columns.
@param[in]  result  result
@return the number of columns; 0 if result is NULL */
uint32_t pulseten_column_count(pulseten_result_t *result) {
  return (result != nullptr ? (uint32_t)result->columns.size() : 0);
}

/** Name a result column.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_column_count() - 1
@return the name, valid until the result is destroyed; NULL if col is out of
  range */
const char *pulseten_column_name(pulseten_result_t *result, uint32_t col) {
  if (result == nullptr || col >= result->columns.size()) {
    return (nullptr);
  }

  return (result->columns[col].name.c_str());
}

/** Get the type of a result column.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_column_count() - 1
@return the type its values are read as; 0 if col is out of range */
pulseten_type_t pulseten_column_type(pulseten_result_t *result, uint32_t col) {
  if (result == nullptr || col >= result->columns.size()) {
    return ((pulseten_type_t)0);
  }

  return ((pulseten_type_t)result->columns[col].type);
}

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
pulseten_state_t pulseten_fetch(pulseten_result_t *result, uint32_t *n_rows) {
  if (n_rows != nullptr) {
    *n_rows = 0;
  }

  if (result == nullptr || n_rows == nullptr) {
    return (api_invalid("result and n_rows must not be NULL"));
  }

  std::string error;
  dberr_t err = engine_cursor_fetch(*result->cursor, result->rows, error);

  if (err != DB_SUCCESS) {
    result->rows.clear();
  }

  *n_rows = (uint32_t)result->rows.size();

  return (api_result(err, error));
}

/** Find a non-NULL value of either accepted type in the current chunk.
@param[in]  result  result whose current chunk is read; may be NULL
@param[in]  col     zero-based result column index
@param[in]  row     zero-based row index in the current chunk
@param[in]  type    first accepted column type
@param[in]  other   second accepted column type
@return the value at (col, row) of the current chunk if it exists, is not NULL
  and the column has one of two types; else NULL */
static const query_value_t *api_value(pulseten_result_t *result, uint32_t col,
                                      uint32_t row, col_type_t type,
                                      col_type_t other) {
  if (result == nullptr || row >= result->rows.size() ||
      col >= result->columns.size()) {
    return (nullptr);
  }

  const col_type_t col_type = result->columns[col].type;
  if (col_type != type && col_type != other) {
    return (nullptr);
  }

  const query_value_t &v = result->rows[row][col];

  return (v.is_null ? nullptr : &v);
}

/** Tell whether a value is NULL.
@param[in]  result  result
@param[in]  col     column, 0 to pulseten_column_count() - 1
@param[in]  row     row of the current chunk
@return nonzero if the value is NULL, or if col or row is out of range */
int pulseten_is_null(pulseten_result_t *result, uint32_t col, uint32_t row) {
  if (result == nullptr || row >= result->rows.size() ||
      col >= result->columns.size()) {
    return (1);
  }

  return (result->rows[row][col].is_null ? 1 : 0);
}

/** Read a BOOLEAN value.
@param[in]  result  result
@param[in]  col     a BOOLEAN column
@param[in]  row     row of the current chunk
@return 1 for true, 0 for false; 0 if the value is NULL, col is not BOOLEAN,
  or col or row is out of range */
int pulseten_get_bool(pulseten_result_t *result, uint32_t col, uint32_t row) {
  const query_value_t *v =
      api_value(result, col, row, COL_BOOLEAN, COL_BOOLEAN);

  return (v != nullptr && v->b ? 1 : 0);
}

/** Read a BIGINT value, or a TIMESTAMP in microseconds since 1970-01-01
00:00:00 UTC.
@param[in]  result  result
@param[in]  col     a BIGINT or TIMESTAMP column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
int64_t pulseten_get_int64(pulseten_result_t *result, uint32_t col,
                           uint32_t row) {
  const query_value_t *v =
      api_value(result, col, row, COL_BIGINT, COL_TIMESTAMP);

  return (v != nullptr ? v->i : 0);
}

/** Read a DOUBLE value.
@param[in]  result  result
@param[in]  col     a DOUBLE column
@param[in]  row     row of the current chunk
@return the value; 0 if it is NULL, col has another type, or col or row is out
  of range */
double pulseten_get_double(pulseten_result_t *result, uint32_t col,
                           uint32_t row) {
  const query_value_t *v = api_value(result, col, row, COL_DOUBLE, COL_DOUBLE);

  return (v != nullptr ? v->d : 0);
}

/** Read a VARCHAR value.
@param[in]   result  result
@param[in]   col     a VARCHAR column
@param[in]   row     row of the current chunk
@param[out]  len     length of the string in bytes; may be NULL
@return the string, NUL-terminated and valid until the next fetch or until the
  result is destroyed; NULL, with *len 0, if the value is NULL, col has
  another type, or col or row is out of range */
const char *pulseten_get_varchar(pulseten_result_t *result, uint32_t col,
                                 uint32_t row, size_t *len) {
  const query_value_t *v =
      api_value(result, col, row, COL_VARCHAR, COL_VARCHAR);

  if (len != nullptr) {
    *len = v != nullptr ? v->s.size() : 0;
  }

  return (v != nullptr ? v->s.c_str() : nullptr);
}

/** Request interruption of an active query from another thread. The running
or next fetch observes the request; after it returns PULSETEN_INTERRUPTED,
every later fetch does too. Normal end is reached when pulseten_fetch()
returns PULSETEN_OK with 0 rows; interrupting after that leaves later fetches
successful with 0 rows. Must not run at the same time as
pulseten_result_destroy().
@param[in]  result  result; may be NULL */
void pulseten_interrupt(pulseten_result_t *result) {
  if (result != nullptr) {
    engine_cursor_interrupt(*result->cursor);
  }
}

/** Stop the query and free the result.
@param[in]  result  result; may be NULL */
void pulseten_result_destroy(pulseten_result_t *result) {
  if (result == nullptr) {
    return;
  }

  engine_cursor_close(result->cursor);
  ut_delete(result);
}

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
                                  char **plan) {
  if (plan == nullptr) {
    return (api_invalid("plan must not be NULL"));
  }

  *plan = nullptr;

  if (db == nullptr || sql == nullptr) {
    return (api_invalid("db and sql must not be NULL"));
  }

  std::string text;
  std::string error;
  dberr_t err = engine_explain(*db->engine, sql, text, error);

  if (err != DB_SUCCESS) {
    return (api_result(err, error));
  }

  char *copy = static_cast<char *>(malloc(text.size() + 1));
  if (copy == nullptr) {
    return (api_result(DB_OUT_OF_MEMORY, ""));
  }

  memcpy(copy, text.c_str(), text.size() + 1);

  *plan = copy;

  return (api_result(DB_SUCCESS, ""));
}

/** Free memory that PulseTen returned to the caller.
@param[in]  p  memory, e.g. a plan from pulseten_explain(); may be NULL */
void pulseten_free(void *p) {
  free(p);
}

/* ---- Statistics ------------------------------------------------------ */

/** Read the counters of a database.
@param[in]   db     database; if NULL, every counter is 0
@param[out]  stats  the counters */
void pulseten_get_stats(pulseten_db_t *db, pulseten_stats_t *stats) {
  memset(stats, 0, sizeof(*stats));

  if (db == nullptr) {
    return;
  }

  engine_stats_t engine_stats;
  engine_get_stats(*db->engine, engine_stats);

  memtable_stats_t memtable_stats;
  engine_get_memtable_stats(*db->engine, memtable_stats);

  stats->flushed_seq = memtable_stats.flushed_seq;
  stats->visible_seq = memtable_stats.visible_seq;
  stats->memtable_bytes = memtable_stats.live_bytes;
  stats->wal_segments = engine_stats.wal_segments;
  stats->background_flushes = engine_stats.background_flushes;
  stats->background_errors = engine_stats.background_errors;
  stats->throttled_writes = engine_stats.throttled_writes;
  stats->expired_partitions = engine_stats.expired_partitions;
}
