/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * api.cc
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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "engine.h"
#include "ingest.h"
#include "memtable.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_new.h"

/** An open database. */
struct pulseten_db_t {
  /** The engine behind it. */
  engine_t *m_engine{nullptr};
};

/** Own the schemas and arrays exposed through the C catalog. */
struct api_catalog_t : pulseten_catalog_t {
  /** Newest schema of every table, by name. */
  std::vector<schema_t> m_schemas;

  /** Column views whose names point into m_schemas. */
  std::vector<std::vector<pulseten_column_t>> m_columns;

  /** Table views whose names point into m_schemas. */
  std::vector<pulseten_table_t> m_table_views;
};

/** A query being read chunk by chunk. */
struct pulseten_result_t {
  /** The engine's cursor. */
  engine_cursor_t *m_cursor{nullptr};

  /** Result columns. */
  std::vector<query_column_t> m_columns;

  /** The current chunk. */
  query_rows_t m_rows;
};

/** Message of the last failure on this thread. */
static thread_local char api_error[2048] = {};

/** Translate an internal engine status to the public C status.
@return the C status of an engine error code */
static pulseten_status_t api_status(dberr_t err) {
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
@param[in]  status  operation result whose message is copied for this thread
@return the C status */
static pulseten_status_t api_result(const db_status_t &status) {
  api_error[0] = '\0';
  if (status.ok()) {
    return (PULSETEN_OK);
  }

  const std::string &message = status.message();
  size_t len = message.size();
  if (len >= sizeof(api_error)) {
    len = sizeof(api_error) - 1;
  }
  memcpy(api_error, message.data(), len);
  api_error[len] = '\0';

  return (api_status(status.code()));
}

/** Record a rejected argument.
@return PULSETEN_INVALID_ARGUMENT, with message recorded for the calling
  thread */
static pulseten_status_t api_invalid(const char *message) {
  return (api_result(db_status_t(DB_INVALID_ARGUMENT, message)));
}

/** Get the version of the linked library, which may differ from the header the
caller was compiled with. */
const char *pulseten_version(void) {
  return (PULSETEN_VERSION_STRING);
}

/** Get the version of the linked library as a number. */
int pulseten_version_number(void) {
  return (PULSETEN_VERSION_NUMBER);
}

/** Describe the last call returning pulseten_status_t on the calling thread. */
const char *pulseten_error_message(void) {
  return (api_error);
}

/** Name a status. */
const char *pulseten_status_name(pulseten_status_t status) {
  switch (status) {
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

/** Fill options with the defaults. */
void pulseten_options_init(pulseten_options_t *options) {
  const engine_options_t defaults;

  options->m_flush_interval_ms = defaults.m_flush_interval_ms;
  options->m_memtable_limit_bytes = defaults.m_memtable_limit_bytes;
  options->m_wal_segment_size_bytes = defaults.m_wal_segment_size_bytes;
  options->m_partition_duration_days = defaults.m_partition_duration_days;
  options->m_retention_days = defaults.m_retention_days;
  options->m_wal_sync = defaults.m_wal_sync;
}

/** Open a database directory, creating it if it does not exist; its parent must
exist. */
pulseten_status_t pulseten_open(const char *dir,
                                const pulseten_options_t *options,
                                pulseten_db_t **out) {
  if (out == nullptr) {
    return (api_invalid("out must not be NULL"));
  }

  *out = nullptr;

  if (dir == nullptr || dir[0] == '\0') {
    return (api_invalid("dir must be a path"));
  }

  engine_options_t engine_options;

  if (options != nullptr) {
    engine_options.m_flush_interval_ms = options->m_flush_interval_ms;
    engine_options.m_memtable_limit_bytes = options->m_memtable_limit_bytes;
    engine_options.m_wal_segment_size_bytes = options->m_wal_segment_size_bytes;
    engine_options.m_partition_duration_days = options->m_partition_duration_days;
    engine_options.m_retention_days = options->m_retention_days;
    engine_options.m_wal_sync = options->m_wal_sync != 0;
  }

  pulseten_db_t *db = ut_new<pulseten_db_t>();
  if (db == nullptr) {
    return (api_result(DB_OUT_OF_MEMORY));
  }

  const db_status_t err = engine_open(dir, &engine_options, &db->m_engine);
  if (!err.ok()) {
    ut_delete(db);

    return (api_result(err));
  }

  *out = db;

  return (api_result(DB_SUCCESS));
}

/** Close a database. */
pulseten_status_t pulseten_close(pulseten_db_t *db) {
  if (db == nullptr) {
    return (api_result(DB_SUCCESS));
  }

  db_status_t err = engine_close(db->m_engine);
  ut_delete(db);

  return (api_result(err));
}

/** Copy a string for the caller, who frees it with pulseten_free().
@param[out]  copy  NUL-terminated copy
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
static db_status_t api_copy_string(const std::string &text, char **copy) {
  char *out = static_cast<char *>(malloc(text.size() + 1));
  if (out == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  memcpy(out, text.c_str(), text.size() + 1);

  *copy = out;

  return (DB_SUCCESS);
}

/* ---- Writing --------------------------------------------------------- */

/** The points of a write, converted from the public types. The points refer
to the tag and field arrays, so all three live until the write returns. */
struct api_points_t {
  /** Tags of every point, one point after the other. */
  std::vector<ingest_tag_t> m_tags;

  /** Fields of every point, one point after the other. */
  std::vector<ingest_field_t> m_fields;

  /** The points. */
  std::vector<ingest_point_t> m_points;
};

/** Convert one field.
@return DB_SUCCESS, or DB_INVALID_ARGUMENT for an unknown type */
static db_status_t api_convert_field(const pulseten_field_t &src,
                                     ingest_field_t &dst) {
  dst.m_name = src.m_name;
  dst.m_name_length = src.m_name_length;

  switch (src.m_type) {
    case PULSETEN_TYPE_BOOLEAN:
      dst.m_value.m_type = col_type_t::BOOLEAN;
      dst.m_value.m_boolean = src.m_value.m_boolean != 0;

      return (DB_SUCCESS);

    case PULSETEN_TYPE_BIGINT:
      dst.m_value.m_type = col_type_t::BIGINT;
      dst.m_value.m_integer = src.m_value.m_integer;

      return (DB_SUCCESS);

    case PULSETEN_TYPE_DOUBLE:
      dst.m_value.m_type = col_type_t::DOUBLE;
      dst.m_value.m_double = src.m_value.m_double;

      return (DB_SUCCESS);

    case PULSETEN_TYPE_VARCHAR:
      dst.m_value.m_type = col_type_t::VARCHAR;
      if (src.m_value.m_string.m_data == nullptr &&
          src.m_value.m_string.m_length > 0) {
        return (db_status_t(DB_INVALID_ARGUMENT, "field value is NULL"));
      }
      if (src.m_value.m_string.m_length > INGEST_MAX_STRING) {
        return (db_status_t(DB_LIMIT_EXCEEDED, "field value is too long"));
      }
      dst.m_value.m_string.assign(
          src.m_value.m_string.m_data != nullptr ? src.m_value.m_string.m_data
                                                : "",
          src.m_value.m_string.m_length);

      return (DB_SUCCESS);

    case PULSETEN_TYPE_TIMESTAMP:
      break;
  }

  return (db_status_t(DB_INVALID_ARGUMENT,
                      "a field must be BOOLEAN, BIGINT, DOUBLE or VARCHAR"));
}

/** Check what the conversion needs, and count the tags and fields: bounded
arrays to allocate and non-NULL arrays to walk. Everything else is checked by
ingest.
@return DB_SUCCESS, DB_LIMIT_EXCEEDED or DB_INVALID_ARGUMENT */
static db_status_t api_check_points(const pulseten_point_t *points,
                                    uint32_t n_points, size_t &n_tags,
                                    size_t &n_fields) {
  if (n_points > INGEST_MAX_POINTS) {
    return (db_status_t(DB_LIMIT_EXCEEDED,
                        "batch has more than " +
                            std::to_string(INGEST_MAX_POINTS) + " points"));
  }

  n_tags = 0;
  n_fields = 0;

  uint64_t min_bytes = 0;

  for (uint32_t p = 0; p < n_points; ++p) {
    const pulseten_point_t &in = points[p];

    if ((in.m_tag_count > 0 && in.m_tags == nullptr) ||
        (in.m_field_count > 0 && in.m_fields == nullptr)) {
      return (db_status_t(
          DB_INVALID_ARGUMENT,
          "point " + std::to_string(p) + ": tags or fields are NULL"));
    }

    /* Reject impossible counts before inspecting either input array. */
    if (static_cast<uint64_t>(in.m_tag_count) + in.m_field_count >=
        SCHEMA_MAX_COLUMNS) {
      return (db_status_t(DB_LIMIT_EXCEEDED,
                          "point " + std::to_string(p) +
                              ": too many tags and fields"));
    }

    n_tags += in.m_tag_count;
    n_fields += in.m_field_count;

    /* A batch already over the lower bound ingest counts cannot fit. */
    const uint64_t n_values =
        1 + static_cast<uint64_t>(in.m_tag_count) + in.m_field_count;

    min_bytes += INGEST_VALUE_BYTES * n_values;
    if (min_bytes > INGEST_MAX_BATCH_BYTES) {
      return (db_status_t(DB_LIMIT_EXCEEDED,
                          "batch data exceeds " +
                              std::to_string(INGEST_MAX_BATCH_BYTES) +
                              " bytes"));
    }
    for (uint32_t f = 0; f < in.m_field_count; ++f) {
      const pulseten_field_t &field = in.m_fields[f];
      if (field.m_type == PULSETEN_TYPE_VARCHAR) {
        const size_t len = field.m_value.m_string.m_length;
        if (len > INGEST_MAX_STRING) {
          return (db_status_t(DB_LIMIT_EXCEEDED, "field value is too long"));
        }
        min_bytes += len;
      }
    }
    if (min_bytes > INGEST_MAX_BATCH_BYTES) {
      return (db_status_t(DB_LIMIT_EXCEEDED,
                          "batch data exceeds " +
                              std::to_string(INGEST_MAX_BATCH_BYTES) +
                              " bytes"));
    }
  }

  return (DB_SUCCESS);
}

/** Convert a batch from the public types to the ones ingest takes.
@param[in]  points  points to convert; NULL only if n_points is 0
@return DB_SUCCESS, DB_LIMIT_EXCEEDED or DB_INVALID_ARGUMENT */
static db_status_t api_convert_points(const pulseten_point_t *points,
                                      uint32_t n_points,
                                      api_points_t &converted) {
  size_t n_tags = 0;
  size_t n_fields = 0;

  db_status_t err = api_check_points(points, n_points, n_tags, n_fields);
  if (!err.ok()) {
    return (err);
  }

  converted.m_tags.resize(n_tags);
  converted.m_fields.resize(n_fields);
  converted.m_points.resize(n_points);

  ingest_tag_t *tag = converted.m_tags.data();
  ingest_field_t *field = converted.m_fields.data();

  for (uint32_t p = 0; p < n_points; ++p) {
    const pulseten_point_t &in = points[p];
    ingest_point_t &out = converted.m_points[p];

    out.m_time_us = in.m_time_us;
    out.m_tags = in.m_tag_count > 0 ? tag : nullptr;
    out.m_tag_count = in.m_tag_count;
    out.m_fields = in.m_field_count > 0 ? field : nullptr;
    out.m_field_count = in.m_field_count;

    for (uint32_t i = 0; i < in.m_tag_count; ++i, ++tag) {
      tag->m_name = in.m_tags[i].m_name;
      tag->m_name_length = in.m_tags[i].m_name_length;
      tag->m_value = in.m_tags[i].m_value;
      tag->m_value_length = in.m_tags[i].m_value_length;
    }

    for (uint32_t i = 0; i < in.m_field_count; ++i, ++field) {
      err = api_convert_field(in.m_fields[i], *field);
      if (!err.ok()) {
        return (err);
      }
    }
  }

  return (DB_SUCCESS);
}

/** Write a batch of points to a table, creating the table and any new
columns. */
pulseten_status_t pulseten_write(pulseten_db_t *db, const char *table_name,
                                 const pulseten_point_t *points,
                                 uint32_t n_points) {
  if (db == nullptr || table_name == nullptr ||
      (points == nullptr && n_points > 0)) {
    return (api_invalid("db, table and points must not be NULL"));
  }
  api_points_t converted;

  db_status_t err = api_convert_points(points, n_points, converted);
  if (!err.ok()) {
    return (api_result(err));
  }

  batch_seq_t seq = 0;
  err = engine_write(*db->m_engine, table_name, strlen(table_name),
                     converted.m_points.data(), n_points, seq);

  return (api_result(err));
}

/* ---- Tables ---------------------------------------------------------- */

/** Return table and column metadata owned by the result. */
pulseten_status_t pulseten_catalog_snapshot(pulseten_db_t *db,
                                          pulseten_catalog_t **out) {
  if (out == nullptr) {
    return (api_invalid("catalog must not be NULL"));
  }

  *out = nullptr;

  if (db == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  api_catalog_t *catalog = ut_new<api_catalog_t>();
  if (catalog == nullptr) {
    return (api_result(DB_OUT_OF_MEMORY));
  }

  engine_copy_schemas(*db->m_engine, catalog->m_schemas);
  catalog->m_columns.resize(catalog->m_schemas.size());
  catalog->m_table_views.resize(catalog->m_schemas.size());

  for (size_t t = 0; t < catalog->m_schemas.size(); ++t) {
    const schema_t &schema = catalog->m_schemas[t];
    std::vector<pulseten_column_t> &columns = catalog->m_columns[t];

    columns.resize(schema.m_cols.size());

    for (size_t c = 0; c < schema.m_cols.size(); ++c) {
      columns[c].m_name = schema.m_cols[c].m_name.c_str();
      columns[c].m_kind = static_cast<pulseten_column_kind_t>(
          static_cast<int>(schema.m_cols[c].m_kind));
      columns[c].m_type = static_cast<pulseten_type_t>(
          static_cast<int>(schema.m_cols[c].m_type));
    }

    pulseten_table_t &table = catalog->m_table_views[t];
    table.m_name = schema.m_name.c_str();
    table.m_column_count = static_cast<uint32_t>(columns.size());
    table.m_columns = columns.data();
  }

  catalog->m_table_count = static_cast<uint32_t>(catalog->m_table_views.size());
  catalog->m_tables = catalog->m_table_views.data();
  *out = catalog;

  return (api_result(DB_SUCCESS));
}

/** Free table and column metadata. */
void pulseten_catalog_destroy(pulseten_catalog_t *catalog) {
  ut_delete(static_cast<api_catalog_t *>(catalog));
}

/** Execute ordinary-table DDL without allowing changes to managed tables. */
pulseten_status_t pulseten_exec_ddl(pulseten_db_t *db, const char *sql) {
  if (db == nullptr || sql == nullptr) {
    return (api_invalid("db and sql must not be NULL"));
  }
  const db_status_t err = engine_exec_ddl(*db->m_engine, sql);

  return (api_result(err));
}

/* ---- Querying -------------------------------------------------------- */

/** Start a SELECT over one consistent snapshot of every write so far. */
pulseten_status_t pulseten_query(pulseten_db_t *db, const char *sql,
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
    return (api_result(DB_OUT_OF_MEMORY));
  }

  db_status_t err =
      engine_cursor_open(*db->m_engine, sql, false, &result->m_cursor);
  if (!err.ok()) {
    pulseten_result_destroy(result);

    return (api_result(err));
  }

  result->m_columns = engine_cursor_columns(*result->m_cursor);

  *out = result;

  return (api_result(DB_SUCCESS));
}

/** Count the result columns. */
uint32_t pulseten_result_column_count(pulseten_result_t *result) {
  return (result != nullptr ? static_cast<uint32_t>(result->m_columns.size())
                            : 0);
}

/** Name a result column. */
const char *pulseten_result_column_name(pulseten_result_t *result,
                                        uint32_t col) {
  if (result == nullptr || col >= result->m_columns.size()) {
    return (nullptr);
  }

  return (result->m_columns[col].m_name.c_str());
}

/** Get the type of a result column. */
pulseten_type_t pulseten_result_column_type(pulseten_result_t *result,
                                            uint32_t col) {
  if (result == nullptr || col >= result->m_columns.size()) {
    return (static_cast<pulseten_type_t>(0));
  }

  return (static_cast<pulseten_type_t>(
      static_cast<int>(result->m_columns[col].m_type)));
}

/** Read the next chunk of rows, replacing the previous one. */
pulseten_status_t pulseten_result_fetch(pulseten_result_t *result,
                                        uint32_t *n_rows) {
  if (n_rows != nullptr) {
    *n_rows = 0;
  }

  if (result == nullptr || n_rows == nullptr) {
    return (api_invalid("result and n_rows must not be NULL"));
  }
  db_status_t err = engine_cursor_fetch(*result->m_cursor, result->m_rows);
  if (!err.ok()) {
    result->m_rows.clear();
  }

  *n_rows = static_cast<uint32_t>(result->m_rows.size());

  return (api_result(err));
}

/** Find a non-NULL value of either accepted type in the current chunk.
@param[in]  result  result whose current chunk is read; may be NULL
@param[in]  col     zero-based result column index
@param[in]  row     zero-based row index in the current chunk
@return the value at (col, row) of the current chunk if it exists, is not NULL
  and the column has one of two types; else NULL */
static const query_value_t *api_find_value(pulseten_result_t *result,
                                           uint32_t col, uint32_t row,
                                           col_type_t type, col_type_t other) {
  if (result == nullptr || row >= result->m_rows.size() ||
      col >= result->m_columns.size()) {
    return (nullptr);
  }

  const col_type_t col_type = result->m_columns[col].m_type;
  if (col_type != type && col_type != other) {
    return (nullptr);
  }

  const query_value_t &v = result->m_rows[row][col];

  return (v.m_is_null ? nullptr : &v);
}

/** Tell whether a value is NULL. */
int pulseten_result_is_null(pulseten_result_t *result, uint32_t col,
                            uint32_t row) {
  if (result == nullptr || row >= result->m_rows.size() ||
      col >= result->m_columns.size()) {
    return (1);
  }

  return (result->m_rows[row][col].m_is_null ? 1 : 0);
}

/** Read a BOOLEAN value. */
int pulseten_read_bool(pulseten_result_t *result, uint32_t col,
                       uint32_t row) {
  const query_value_t *v = api_find_value(result, col, row, col_type_t::BOOLEAN,
                                          col_type_t::BOOLEAN);

  return (v != nullptr && v->m_b ? 1 : 0);
}

/** Read a BIGINT value, or a TIMESTAMP in microseconds since 1970-01-01
00:00:00 UTC. */
int64_t pulseten_read_int64(pulseten_result_t *result, uint32_t col,
                            uint32_t row) {
  const query_value_t *v = api_find_value(result, col, row, col_type_t::BIGINT,
                                          col_type_t::TIMESTAMP);

  return (v != nullptr ? v->m_i : 0);
}

/** Read a DOUBLE value. */
double pulseten_read_double(pulseten_result_t *result, uint32_t col,
                            uint32_t row) {
  const query_value_t *v =
      api_find_value(result, col, row, col_type_t::DOUBLE, col_type_t::DOUBLE);

  return (v != nullptr ? v->m_d : 0);
}

/** Read a VARCHAR value. */
const char *pulseten_read_varchar(pulseten_result_t *result, uint32_t col,
                                  uint32_t row, size_t *len) {
  const query_value_t *v = api_find_value(result, col, row, col_type_t::VARCHAR,
                                          col_type_t::VARCHAR);

  if (len != nullptr) {
    *len = v != nullptr ? v->m_s.size() : 0;
  }

  return (v != nullptr ? v->m_s.c_str() : nullptr);
}

/** Request interruption of an active query from another thread. */
void pulseten_result_interrupt(pulseten_result_t *result) {
  if (result != nullptr) {
    engine_cursor_interrupt(*result->m_cursor);
  }
}

/** Stop the query and free the result. */
void pulseten_result_destroy(pulseten_result_t *result) {
  if (result == nullptr) {
    return;
  }

  engine_cursor_close(result->m_cursor);
  ut_delete(result);
}

/** Show DuckDB's plan for a SELECT, as pulseten_query() would run it. */
pulseten_status_t pulseten_explain(pulseten_db_t *db, const char *sql,
                                   char **plan) {
  if (plan == nullptr) {
    return (api_invalid("plan must not be NULL"));
  }

  *plan = nullptr;

  if (db == nullptr || sql == nullptr) {
    return (api_invalid("db and sql must not be NULL"));
  }

  std::string text;
  db_status_t err = engine_explain(*db->m_engine, sql, text);
  if (!err.ok()) {
    return (api_result(err));
  }

  return (api_result(api_copy_string(text, plan)));
}

/** Read the message of the latest failed background flush or expiry. */
pulseten_status_t pulseten_copy_background_error(pulseten_db_t *db,
                                                 char **message) {
  if (message == nullptr) {
    return (api_invalid("message must not be NULL"));
  }

  *message = nullptr;

  if (db == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  const db_status_t background = engine_background_status(*db->m_engine);
  const std::string &text = background.message();

  if (text.empty()) {
    return (api_result(DB_SUCCESS));
  }

  return (api_result(api_copy_string(text, message)));
}

/** Free memory that PulseTen returned to the caller. */
void pulseten_free(void *p) {
  free(p);
}

/* ---- Statistics ------------------------------------------------------ */

/** Read the counters of a database. */
void pulseten_read_stats(pulseten_db_t *db, pulseten_stats_t *stats) {
  memset(stats, 0, sizeof(*stats));

  if (db == nullptr) {
    return;
  }

  engine_stats_t engine_stats;
  engine_get_stats(*db->m_engine, engine_stats);

  memtable_stats_t memtable_stats;
  engine_get_memtable_stats(*db->m_engine, memtable_stats);

  stats->m_flushed_seq = memtable_stats.m_flushed_seq;
  stats->m_visible_seq = memtable_stats.m_visible_seq;
  stats->m_memtable_bytes = memtable_stats.m_live_bytes;
  stats->m_wal_segment_count = engine_stats.m_wal_segment_count;
  stats->m_background_flushes = engine_stats.m_background_flushes;
  stats->m_background_errors = engine_stats.m_background_errors;
  stats->m_throttled_writes = engine_stats.m_throttled_writes;
  stats->m_expired_partitions = engine_stats.m_expired_partitions;
}
