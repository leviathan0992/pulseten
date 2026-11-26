/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest.h
 *    Turn a batch of points into a write plan.
 *
 * ingest_plan() checks every point, works out which columns the batch
 * adds to the table, and stores the rows as a column batch. It only reads
 * the current schema; applying the plan is the caller's job. A batch is
 * accepted or rejected as a whole.
 *
 * Rules for a batch:
 *   - 1 to INGEST_MAX_POINTS points, all for one table.
 *   - Each point has a time in [1900-01-01, 2200-01-01) UTC, any number of
 *     tags and at least one field; a tag or field name appears at most once
 *     per point and is never "time".
 *   - Tags are VARCHAR. Fields are BOOLEAN, BIGINT, DOUBLE (finite) or
 *     VARCHAR. Strings are valid UTF-8 of at most
 *     INGEST_MAX_STRING_BYTES bytes.
 *   - A name keeps the kind and type it first had, in the table or earlier
 *     in the same batch; anything else is a schema conflict.
 *   - Columns a point does not mention are NULL for that row.
 *   - The batch spans at most INGEST_MAX_BATCH_DAYS UTC days, and the table at
 *     most SCHEMA_MAX_COLUMNS columns.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ingest_h
#define ingest_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "db_batch.h"
#include "db_schema.h"
#include "ut.h"

/** Most points in one batch. */
#define INGEST_MAX_POINTS 65536

/** Most distinct UTC days one batch may touch. */
#define INGEST_MAX_BATCH_DAYS 32

/** Longest tag or string field value, in bytes. */
#define INGEST_MAX_STRING_BYTES (1024 * 1024)

/** 1900-01-01 00:00:00 UTC in microseconds. */
#define INGEST_MIN_TIME_US (-2208988800000000LL)

/** 2200-01-01 00:00:00 UTC in microseconds, exclusive. */
#define INGEST_MAX_TIME_US 7258118400000000LL

/** Upper bound on estimated batch data: 8 bytes per time and supplied tag
or field, plus string bytes and ceil(points / 8) NULL-bitmap bytes per column
in the resulting table schema. This excludes encoded table/schema metadata
and WAL framing; the margin leaves room below the WAL record limit. */
#define INGEST_MAX_BATCH_BYTES (8 * 1024 * 1024 - 64 * 1024)

/** Bytes counted for one time, tag or field, before string bytes and NULL
bitmaps. */
#define INGEST_ESTIMATED_VALUE_BYTES 8

/** Input tag with length-delimited name and value. */
struct ingest_tag_t {
  /** Column name as given by the user. */
  const char *m_name{nullptr};

  /** Length of name in bytes. */
  size_t m_name_length{0};

  /** The value, UTF-8. */
  const char *m_value{nullptr};

  /** Length of value in bytes. */
  size_t m_value_length{0};
};

/** A field value. Only the member matching type is read. */
struct ingest_value_t {
  /** BOOLEAN, BIGINT, DOUBLE or VARCHAR. */
  data_type_t m_data_type{data_type_t::BIGINT};

  /** BOOLEAN value. */
  bool m_boolean{false};

  /** BIGINT value. */
  int64_t m_integer{0};

  /** DOUBLE value. */
  double m_double{0};

  /** VARCHAR value, UTF-8. */
  std::string m_string;
};

/** A field of a point. */
struct ingest_field_t {
  /** Column name as given by the user. */
  const char *m_name{nullptr};

  /** Length of name in bytes. */
  size_t m_name_length{0};

  /** Type and value. */
  ingest_value_t m_value;
};

/** One input point. */
struct ingest_point_t {
  /** Microseconds since 1970-01-01 00:00:00 UTC. */
  int64_t m_time_us{0};

  /** Input tags; may be NULL when m_tag_count is zero. */
  const ingest_tag_t *m_tags{nullptr};

  /** Number of tags in m_tags; may be zero. */
  uint32_t m_tag_count{0};

  /** The fields, at least one. */
  const ingest_field_t *m_fields{nullptr};

  /** Number of fields in m_fields; at least one for a valid point. */
  uint32_t m_field_count{0};
};

/** Result of ingest_plan(). */
struct ingest_plan_t {
  /** Canonical table name. */
  std::string m_table_name;

  /** True if the table does not exist yet. */
  bool m_creates_table{false};

  /** Number of columns the plan was made against; 0 for a new table. */
  uint32_t m_base_column_count{0};

  /** Columns to add, in id order, continuing after the base schema. For a
  new table this includes the time column. */
  std::vector<schema_column_t> m_new_columns;

  /** The rows, following the schema after m_new_columns are added. */
  batch_t m_batch{};
};

/** Check the input array and point count before any caller walks it. */
db_status_t ingest_validate_batch(const ingest_point_t *points,
                                  uint32_t n_points);

/** Check that point p has a time in [INGEST_MIN_TIME_US, INGEST_MAX_TIME_US).
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
db_status_t ingest_validate_time(uint32_t p, int64_t time_us);

/** Validate UTF-8 bytes.
@return true if str is well-formed UTF-8: no overlong forms, no surrogates,
  nothing above U+10FFFF */
bool ingest_validate_utf8(const char *str, size_t len);

/** Check a batch of points and build its write plan.
REQUIRES: table_name is a normalized name.
@param[in]   schema  current schema of the table, or NULL if the table does
                     not exist
@param[out]  plan    the plan; empty on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
db_status_t ingest_plan(const schema_t *schema, const std::string &table_name,
                        const ingest_point_t *points, uint32_t n_points,
                        ingest_plan_t &plan);

/** Encode a write plan as a WAL payload. */
void ingest_encode(const ingest_plan_t &plan, std::string &out);

/** Decode a WAL payload and reset the plan to empty on failure.
@return DB_SUCCESS, or DB_CORRUPTION if the payload is malformed */
db_status_t ingest_decode(const unsigned char *buf, size_t len,
                          ingest_plan_t &plan);

#endif /* ingest_h */
