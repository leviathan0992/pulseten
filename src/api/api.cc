/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * api.cc
 *    The C API of include/pulseten.h, on top of the database.
 *
 * Each function turns its arguments into the database's types, calls the
 * db and turns the result back. Errors are status codes; checked
 * allocations that fail return PULSETEN_OUT_OF_MEMORY. The message of the
 * last failure is kept per thread.
 *
 *-------------------------------------------------------------------------
 */

#include "pulseten.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "db.h"
#include "ingest.h"
#include "query.h"
#include "schema.h"
#include "storage.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_new.h"
#include "ut_noncopyable.h"

/** Convert the public opaque handle back to its database instance. */
static db *api_database(pulseten_db_t *handle) {
  return (reinterpret_cast<db *>(handle));
}

/** Build a self-contained C catalog whose arrays and names share one
 * allocation. */
class api_catalog final : private ut_noncopyable {
 public:
  /** Release a catalog whose ownership has not been transferred. */
  ~api_catalog() { free(m_buffer); }

  /** Copy the database metadata into independently owned C arrays and strings.
   */
  db_status_t init(db &database);

  /** Transfer the completed catalog to the caller. */
  pulseten_catalog_t *release();

 private:
  /** Reserve aligned storage, rejecting size overflow. */
  bool reserve(size_t count, size_t width, size_t alignment, size_t &offset);

  /** Copy a name and advance the position in the owned name storage. */
  static const char *copy_name(char *&position, const std::string &name);

  /** Number of tables in the completed catalog. */
  uint32_t m_table_count{0};

  /** C table views inside the owned allocation. */
  pulseten_table_t *m_tables{nullptr};

  /** Allocation transferred to pulseten_catalog_destroy() by release(). */
  void *m_buffer{nullptr};

  /** Total storage reserved for the catalog, arrays and names. */
  size_t m_bytes{sizeof(pulseten_catalog_t)};
};

/** A query being read chunk by chunk. */
class api_query_result final : private ut_noncopyable {
 public:
  /** Stop the owned database cursor before releasing row buffers. */
  ~api_query_result() { db_cursor::destroy(m_cursor); }

  /** Open the owned database cursor. */
  db_status_t open(db &database, const char *sql, query_cancel *cancel);

  /** Fetch the next chunk, clearing rows on failure. */
  db_status_t fetch(uint32_t &n_rows);

  /** Count columns fixed while opening. */
  uint32_t column_count() const {
    return (static_cast<uint32_t>(m_cursor->columns().size()));
  }

  /** Borrow a column name, or return NULL for an invalid column. */
  const char *column_name(uint32_t col) const;

  /** Read a public column type, or return zero for an invalid column. */
  pulseten_type_t column_type(uint32_t col) const;

  /** Read a non-NULL cell of either accepted type, or return NULL. */
  const query_value_t *value(uint32_t col, uint32_t row, col_type_t type,
                             col_type_t other) const;

  /** Treat an absent cell as NULL. */
  bool is_null(uint32_t col, uint32_t row) const;

  /** Interrupt the active cursor without modifying the current row buffers. */
  void interrupt() {
    m_cursor->interrupt();

    return;
  }

 private:
  /** The db's cursor. */
  db_cursor *m_cursor{nullptr};

  /** The current chunk. */
  query_rows_t m_rows;
};

/** Public opaque handle; behavior and resources belong to the internal class.
 */
struct pulseten_result_t {
  /** Internal result object; the public header exposes only the opaque handle.
   */
  api_query_result m_result;
};

/** Public cancellation handle owns its synchronized native request state. */
struct pulseten_cancel_t {
  /** State borrowed only while a query is opening. */
  query_cancel m_cancel;
};

/** Message of the last failure on this thread. */
static thread_local char api_error[2048] = {};

/** Translate an internal db status to the public C status.
@return the C status of a database error code */
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
  }

  return ("UNKNOWN");
}

/* ---- Opening and closing --------------------------------------------- */

/** Fill options with the defaults. */
void pulseten_options_init(pulseten_options_t *options) {
  const db_options_t defaults;

  options->m_flush_interval_ms = defaults.m_flush_interval_ms;
  options->m_memtable_limit_bytes = defaults.m_memtable_limit_bytes;
  options->m_wal_segment_size_bytes = defaults.m_wal_segment_size_bytes;
  options->m_partition_duration_days = defaults.m_partition_duration_days;
  options->m_retention_days = defaults.m_retention_days;
  options->m_wal_sync = defaults.m_wal_sync;
  options->m_duckdb_memory_limit_bytes = defaults.m_duckdb_memory_limit_bytes;

  return;
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

  db_options_t db_options;

  if (options != nullptr) {
    db_options.m_flush_interval_ms = options->m_flush_interval_ms;
    db_options.m_memtable_limit_bytes = options->m_memtable_limit_bytes;
    db_options.m_wal_segment_size_bytes = options->m_wal_segment_size_bytes;
    db_options.m_partition_duration_days = options->m_partition_duration_days;
    db_options.m_retention_days = options->m_retention_days;
    db_options.m_wal_sync = options->m_wal_sync != 0;
    db_options.m_duckdb_memory_limit_bytes =
        options->m_duckdb_memory_limit_bytes;
  }

  db *database = nullptr;
  if (const db_status_t err = db_open(dir, &db_options, &database); !err.ok()) {
    return (api_result(err));
  }

  *out = reinterpret_cast<pulseten_db_t *>(database);

  return (api_result(DB_SUCCESS));
}

/** Close a database. */
void pulseten_close(pulseten_db_t *database) {
  db_close(api_database(database));

  return;
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
class api_points final : private ut_noncopyable {
 public:
  /** Validate public points and build their internal array views. */
  db_status_t init(const pulseten_point_t *points, uint32_t n_points);

  /** Borrow the converted point array until this conversion object is gone. */
  const ingest_point_t *points() const { return (m_points.data()); }

 private:
  /** Tags of every point, one point after the other. */
  std::vector<ingest_tag_t> m_tags;

  /** Fields of every point, one point after the other. */
  std::vector<ingest_field_t> m_fields;

  /** The points. */
  std::vector<ingest_point_t> m_points;
};

/** Convert one C field into an ingest value.
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_LIMIT_EXCEEDED */
static db_status_t api_convert_field(const pulseten_field_t &src,
                                     ingest_field_t &dst) {
  dst.m_name = src.m_name;
  dst.m_name_length = src.m_name_length;

  switch (src.m_type) {
    case PULSETEN_TYPE_BOOLEAN: {
      /* Treat every nonzero C boolean as true. */
      dst.m_value.m_type = col_type_t::BOOLEAN;
      dst.m_value.m_boolean = src.m_value.m_boolean != 0;

      return (DB_SUCCESS);
    }
    case PULSETEN_TYPE_BIGINT: {
      dst.m_value.m_type = col_type_t::BIGINT;
      dst.m_value.m_integer = src.m_value.m_integer;

      return (DB_SUCCESS);
    }
    case PULSETEN_TYPE_DOUBLE: {
      dst.m_value.m_type = col_type_t::DOUBLE;
      dst.m_value.m_double = src.m_value.m_double;

      return (DB_SUCCESS);
    }
    case PULSETEN_TYPE_VARCHAR: {
      /* Accept NULL only for an empty string, then copy the supplied bytes. */
      dst.m_value.m_type = col_type_t::VARCHAR;
      if (src.m_value.m_string.m_data == nullptr &&
          src.m_value.m_string.m_length > 0) {
        return (db_status_t(DB_INVALID_ARGUMENT, "field value is NULL"));
      }

      if (src.m_value.m_string.m_length > INGEST_MAX_STRING) {
        return (db_status_t(DB_LIMIT_EXCEEDED, "field value is too long"));
      }

      dst.m_value.m_string.assign(src.m_value.m_string.m_data != nullptr
                                      ? src.m_value.m_string.m_data
                                      : "",
                                  src.m_value.m_string.m_length);

      return (DB_SUCCESS);
    }

    case PULSETEN_TYPE_TIMESTAMP:
      /* A point supplies its timestamp separately from its fields. */
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
      return (db_status_t(DB_LIMIT_EXCEEDED, "point " + std::to_string(p) +
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
db_status_t api_points::init(const pulseten_point_t *points,
                             uint32_t n_points) {
  size_t n_tags = 0;
  size_t n_fields = 0;

  if (const db_status_t err =
          api_check_points(points, n_points, n_tags, n_fields);
      !err.ok()) {
    return (err);
  }

  m_tags.resize(n_tags);
  m_fields.resize(n_fields);
  m_points.resize(n_points);

  ingest_tag_t *tag = m_tags.data();
  ingest_field_t *field = m_fields.data();

  for (uint32_t p = 0; p < n_points; ++p) {
    const pulseten_point_t &in = points[p];
    ingest_point_t &out = m_points[p];

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
      if (const db_status_t err = api_convert_field(in.m_fields[i], *field);
          !err.ok()) {
        return (err);
      }
    }
  }

  return (DB_SUCCESS);
}

/** Write a batch of points to a table, creating the table and any new
columns. */
pulseten_status_t pulseten_write(pulseten_db_t *database,
                                 const char *table_name,
                                 const pulseten_point_t *points,
                                 uint32_t n_points) {
  if (database == nullptr || table_name == nullptr ||
      (points == nullptr && n_points > 0)) {
    return (api_invalid("db, table and points must not be NULL"));
  }

  api_points input;
  if (const db_status_t err = input.init(points, n_points); !err.ok()) {
    return (api_result(err));
  }

  batch_seq_t seq = 0;
  const db_status_t err = api_database(database)->write(
      table_name, strlen(table_name), input.points(), n_points, seq);

  return (api_result(err));
}

/* ---- Tables ---------------------------------------------------------- */

/** Account for each C array before allocating its backing storage. */
bool api_catalog::reserve(size_t count, size_t width, size_t alignment,
                          size_t &offset) {
  const size_t maximum = std::numeric_limits<size_t>::max();
  const size_t padding = (alignment - m_bytes % alignment) % alignment;
  if (padding > maximum - m_bytes) {
    return (false);
  }

  m_bytes += padding;
  if (count > (maximum - m_bytes) / width) {
    return (false);
  }

  offset = m_bytes;
  m_bytes += count * width;

  return (true);
}

const char *api_catalog::copy_name(char *&position, const std::string &name) {
  char *copy = position;
  memcpy(copy, name.c_str(), name.size() + 1);
  position += name.size() + 1;

  return (copy);
}

/** Copy metadata into a C catalog independent of this builder and the database.
 */
db_status_t api_catalog::init(db &database) {
  std::vector<schema_t> schemas;
  database.copy_schemas(schemas);

  size_t table_offset = 0;
  if (!reserve(schemas.size(), sizeof(pulseten_table_t),
               alignof(pulseten_table_t), table_offset)) {
    return (DB_OUT_OF_MEMORY);
  }

  std::vector<size_t> column_offsets(schemas.size());
  for (size_t t = 0; t < schemas.size(); ++t) {
    if (!reserve(schemas[t].m_cols.size(), sizeof(pulseten_column_t),
                 alignof(pulseten_column_t), column_offsets[t])) {
      return (DB_OUT_OF_MEMORY);
    }
  }

  const size_t names_offset = m_bytes;
  size_t unused = 0;
  for (const schema_t &schema : schemas) {
    if (!reserve(schema.m_name.size() + 1, 1, 1, unused)) {
      return (DB_OUT_OF_MEMORY);
    }

    for (const col_def_t &column : schema.m_cols) {
      if (!reserve(column.m_name.size() + 1, 1, 1, unused)) {
        return (DB_OUT_OF_MEMORY);
      }
    }
  }

  m_buffer = malloc(m_bytes);
  if (m_buffer == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  /* Each C array starts at its required alignment. Names follow the arrays. */
  unsigned char *buffer = static_cast<unsigned char *>(m_buffer);
  m_table_count = static_cast<uint32_t>(schemas.size());
  m_tables = schemas.empty()
                 ? nullptr
                 : reinterpret_cast<pulseten_table_t *>(buffer + table_offset);
  char *name = reinterpret_cast<char *>(buffer + names_offset);

  for (size_t t = 0; t < schemas.size(); ++t) {
    const schema_t &schema = schemas[t];
    pulseten_column_t *columns =
        reinterpret_cast<pulseten_column_t *>(buffer + column_offsets[t]);

    for (size_t c = 0; c < schema.m_cols.size(); ++c) {
      columns[c].m_name = copy_name(name, schema.m_cols[c].m_name);
      columns[c].m_kind =
          static_cast<pulseten_column_kind_t>(schema.m_cols[c].m_kind);
      columns[c].m_type = static_cast<pulseten_type_t>(schema.m_cols[c].m_type);
    }

    m_tables[t].m_name = copy_name(name, schema.m_name);
    m_tables[t].m_column_count = static_cast<uint32_t>(schema.m_cols.size());
    m_tables[t].m_columns = columns;
  }

  ut_a(name == reinterpret_cast<char *>(buffer + m_bytes));

  return (DB_SUCCESS);
}

pulseten_catalog_t *api_catalog::release() {
  pulseten_catalog_t *catalog = static_cast<pulseten_catalog_t *>(m_buffer);
  catalog->m_table_count = m_table_count;
  catalog->m_tables = m_tables;
  m_buffer = nullptr;
  m_table_count = 0;
  m_tables = nullptr;

  return (catalog);
}

pulseten_status_t pulseten_catalog_snapshot(pulseten_db_t *database,
                                            pulseten_catalog_t **out) {
  if (out == nullptr) {
    return (api_invalid("catalog must not be NULL"));
  }

  *out = nullptr;

  if (database == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  api_catalog catalog;
  if (const db_status_t err = catalog.init(*api_database(database));
      !err.ok()) {
    return (api_result(err));
  }

  *out = catalog.release();

  return (api_result(DB_SUCCESS));
}

/** Free table and column metadata. */
void pulseten_catalog_destroy(pulseten_catalog_t *catalog) {
  free(catalog);

  return;
}

/* ---- Querying -------------------------------------------------------- */

/** Start a SELECT over one consistent snapshot of every write so far. */
db_status_t api_query_result::open(db &database, const char *sql,
                                   query_cancel *cancel) {
  return (database.cursor_open(sql, false, &m_cursor, cancel));
}

/** Preserve the original query API; QueryContext uses the cancellable
 * entrypoint. */
pulseten_status_t pulseten_query(pulseten_db_t *database, const char *sql,
                                 pulseten_result_t **out) {
  return (pulseten_query_with_cancel(database, sql, nullptr, out));
}

/** Allocate a request state independently of the query's connection. */
pulseten_status_t pulseten_cancel_create(pulseten_cancel_t **out) {
  if (out == nullptr) {
    return (api_invalid("cancel must not be NULL"));
  }

  *out = ut_new<pulseten_cancel_t>();
  return (api_result(*out != nullptr ? DB_SUCCESS : DB_OUT_OF_MEMORY));
}

/** Request cancellation without racing connection destruction. */
void pulseten_cancel_request(pulseten_cancel_t *cancel) {
  if (cancel != nullptr) {
    cancel->m_cancel.request();
  }

  return;
}

/** Release a handle no longer used by an opening query or request thread. */
void pulseten_cancel_destroy(pulseten_cancel_t *cancel) {
  ut_delete(cancel);

  return;
}

/** Start a SELECT with cancellation available before a result exists. */
pulseten_status_t pulseten_query_with_cancel(pulseten_db_t *database,
                                             const char *sql,
                                             pulseten_cancel_t *cancel,
                                             pulseten_result_t **out) {
  if (out == nullptr) {
    return (api_invalid("result must not be NULL"));
  }

  *out = nullptr;

  if (database == nullptr || sql == nullptr) {
    return (api_invalid("db and sql must not be NULL"));
  }

  pulseten_result_t *result = ut_new<pulseten_result_t>();
  if (result == nullptr) {
    return (api_result(DB_OUT_OF_MEMORY));
  }

  if (const db_status_t err = result->m_result.open(
          *api_database(database), sql,
          cancel != nullptr ? &cancel->m_cancel : nullptr);
      !err.ok()) {
    pulseten_result_destroy(result);

    return (api_result(err));
  }

  *out = result;

  return (api_result(DB_SUCCESS));
}

/** Count the result columns. */
uint32_t pulseten_result_column_count(pulseten_result_t *result) {
  return (result != nullptr ? result->m_result.column_count() : 0);
}

/** Name a result column. */
const char *api_query_result::column_name(uint32_t col) const {
  const std::vector<query_column_t> &columns = m_cursor->columns();
  if (col >= columns.size()) {
    return (nullptr);
  }

  return (columns[col].m_name.c_str());
}

const char *pulseten_result_column_name(pulseten_result_t *result,
                                        uint32_t col) {
  return (result != nullptr ? result->m_result.column_name(col) : nullptr);
}

/** Get the type of a result column. */
pulseten_type_t api_query_result::column_type(uint32_t col) const {
  const std::vector<query_column_t> &columns = m_cursor->columns();
  if (col >= columns.size()) {
    return (static_cast<pulseten_type_t>(0));
  }

  return (static_cast<pulseten_type_t>(columns[col].m_type));
}

pulseten_type_t pulseten_result_column_type(pulseten_result_t *result,
                                            uint32_t col) {
  return (result != nullptr ? result->m_result.column_type(col)
                            : static_cast<pulseten_type_t>(0));
}

/** Read the next chunk of rows, replacing the previous one. */
db_status_t api_query_result::fetch(uint32_t &n_rows) {
  const db_status_t err = m_cursor->fetch(m_rows);
  if (!err.ok()) {
    m_rows.clear();
  }

  n_rows = static_cast<uint32_t>(m_rows.size());
  return (err);
}

pulseten_status_t pulseten_result_fetch(pulseten_result_t *result,
                                        uint32_t *n_rows) {
  if (n_rows != nullptr) {
    *n_rows = 0;
  }

  if (result == nullptr || n_rows == nullptr) {
    return (api_invalid("result and n_rows must not be NULL"));
  }

  return (api_result(result->m_result.fetch(*n_rows)));
}

/** Find a non-NULL value of either accepted type in the current chunk.
@param[in]  col     zero-based result column index
@param[in]  row     zero-based row index in the current chunk
@return the value at (col, row) of the current chunk if it exists, is not NULL
  and the column has one of two types; else NULL */
const query_value_t *api_query_result::value(uint32_t col, uint32_t row,
                                             col_type_t type,
                                             col_type_t other) const {
  const std::vector<query_column_t> &columns = m_cursor->columns();
  if (row >= m_rows.size() || col >= columns.size()) {
    return (nullptr);
  }

  const col_type_t col_type = columns[col].m_type;
  if (col_type != type && col_type != other) {
    return (nullptr);
  }

  const query_value_t &v = m_rows[row][col];

  return (v.m_is_null ? nullptr : &v);
}

/** Tell whether a value is NULL. */
bool api_query_result::is_null(uint32_t col, uint32_t row) const {
  const std::vector<query_column_t> &columns = m_cursor->columns();
  if (row >= m_rows.size() || col >= columns.size()) {
    return (true);
  }

  return (m_rows[row][col].m_is_null);
}

int pulseten_result_is_null(pulseten_result_t *result, uint32_t col,
                            uint32_t row) {
  return (result != nullptr ? result->m_result.is_null(col, row) : true);
}

/** Read a BOOLEAN value. */
int pulseten_read_bool(pulseten_result_t *result, uint32_t col, uint32_t row) {
  const query_value_t *v =
      (result != nullptr ? result->m_result.value(col, row, col_type_t::BOOLEAN,
                                                  col_type_t::BOOLEAN)
                         : nullptr);

  return (v != nullptr && v->m_b ? 1 : 0);
}

/** Read a BIGINT value, or a TIMESTAMP in microseconds since 1970-01-01
00:00:00 UTC. */
int64_t pulseten_read_int64(pulseten_result_t *result, uint32_t col,
                            uint32_t row) {
  const query_value_t *v =
      (result != nullptr ? result->m_result.value(col, row, col_type_t::BIGINT,
                                                  col_type_t::TIMESTAMP)
                         : nullptr);

  return (v != nullptr ? v->m_i : 0);
}

/** Read a DOUBLE value. */
double pulseten_read_double(pulseten_result_t *result, uint32_t col,
                            uint32_t row) {
  const query_value_t *v =
      (result != nullptr ? result->m_result.value(col, row, col_type_t::DOUBLE,
                                                  col_type_t::DOUBLE)
                         : nullptr);

  return (v != nullptr ? v->m_d : 0);
}

/** Read a VARCHAR value. */
const char *pulseten_read_varchar(pulseten_result_t *result, uint32_t col,
                                  uint32_t row, size_t *len) {
  const query_value_t *v =
      (result != nullptr ? result->m_result.value(col, row, col_type_t::VARCHAR,
                                                  col_type_t::VARCHAR)
                         : nullptr);

  if (len != nullptr) {
    *len = v != nullptr ? v->m_s.size() : 0;
  }

  return (v != nullptr ? v->m_s.c_str() : nullptr);
}

/** Request interruption of an active query from another thread. */
void pulseten_result_interrupt(pulseten_result_t *result) {
  if (result != nullptr) {
    result->m_result.interrupt();
  }

  return;
}

/** Stop the query and free the result. */
void pulseten_result_destroy(pulseten_result_t *result) {
  if (result == nullptr) {
    return;
  }

  ut_delete(result);

  return;
}

/** Show DuckDB's plan for a SELECT, as pulseten_query() would run it. */
pulseten_status_t pulseten_explain(pulseten_db_t *database, const char *sql,
                                   char **plan) {
  if (plan == nullptr) {
    return (api_invalid("plan must not be NULL"));
  }

  *plan = nullptr;

  if (database == nullptr || sql == nullptr) {
    return (api_invalid("db and sql must not be NULL"));
  }

  std::string text;
  if (const db_status_t err = api_database(database)->explain(sql, text);
      !err.ok()) {
    return (api_result(err));
  }

  return (api_result(api_copy_string(text, plan)));
}

/** Read the message of the latest failed background flush, WAL cleanup or
 * expiry. */
pulseten_status_t pulseten_copy_background_error(pulseten_db_t *database,
                                                 char **message) {
  if (message == nullptr) {
    return (api_invalid("message must not be NULL"));
  }

  *message = nullptr;

  if (database == nullptr) {
    return (api_invalid("db must not be NULL"));
  }

  const db_status_t background = api_database(database)->background_status();
  const std::string &text = background.message();

  if (text.empty()) {
    return (api_result(DB_SUCCESS));
  }

  return (api_result(api_copy_string(text, message)));
}

/** Free memory that PulseTen returned to the caller. */
void pulseten_free(void *p) {
  free(p);

  return;
}

/* ---- Statistics ------------------------------------------------------ */

/** Read the counters of a database. */
void pulseten_read_stats(pulseten_db_t *database, pulseten_stats_t *stats) {
  memset(stats, 0, sizeof(*stats));

  if (database == nullptr) {
    return;
  }

  db_stats_t db_stats;
  api_database(database)->read_stats(db_stats);

  stats->m_flushed_seq = db_stats.m_flushed_seq;
  stats->m_visible_seq = db_stats.m_visible_seq;
  stats->m_memtable_bytes = db_stats.m_memtable_bytes;
  stats->m_wal_segment_count = db_stats.m_wal_segment_count;
  stats->m_background_flushes = db_stats.m_background_flushes;
  stats->m_background_errors = db_stats.m_background_errors;
  stats->m_throttled_writes = db_stats.m_throttled_writes;
  stats->m_expired_partitions = db_stats.m_expired_partitions;
  stats->m_duckdb_memory_limit_bytes = db_stats.m_duckdb_memory_limit_bytes;

  return;
}
