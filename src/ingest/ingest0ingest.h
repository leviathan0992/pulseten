/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest0ingest.h
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
 *     VARCHAR. Strings are valid UTF-8 of at most INGEST_MAX_STRING bytes.
 *   - A name keeps the role and type it first had, in the table or earlier
 *     in the same batch; anything else is a schema conflict.
 *   - Columns a point does not mention are NULL for that row.
 *   - The batch spans at most INGEST_MAX_DAYS UTC days, and the table at
 *     most SCHEMA_MAX_COLUMNS columns.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ingest0ingest_h
#define ingest0ingest_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "batch0batch.h"
#include "schema0schema.h"
#include "ut0err.h"

/** Most points in one batch. */
#define INGEST_MAX_POINTS 65536

/** Most distinct UTC days one batch may touch. */
#define INGEST_MAX_DAYS 32

/** Longest tag or string field value, in bytes. */
#define INGEST_MAX_STRING (1024 * 1024)

/** 1900-01-01 00:00:00 UTC in microseconds. */
#define INGEST_MIN_TIME_US (-2208988800000000LL)

/** 2200-01-01 00:00:00 UTC in microseconds, exclusive. */
#define INGEST_MAX_TIME_US 7258118400000000LL

/** Upper bound on estimated batch data: 8 bytes per time and supplied tag
or field, plus string bytes and ceil(points / 8) NULL-bitmap bytes per column
in the resulting table schema. This excludes encoded table/schema metadata
and WAL framing; the margin leaves room below the WAL record limit. */
#define INGEST_MAX_BATCH_BYTES (8 * 1024 * 1024 - 64 * 1024)

/** A tag of a point. Strings are not NUL-terminated. */
struct ingest_tag_t {
  /** Column name as given by the user. */
  const char *name{nullptr};

  /** Length of name in bytes. */
  size_t name_len{0};

  /** The value, UTF-8. */
  const char *value{nullptr};

  /** Length of value in bytes. */
  size_t value_len{0};
};

/** A field value. Only the member matching type is read. */
struct ingest_value_t {
  /** BOOLEAN, BIGINT, DOUBLE or VARCHAR. */
  col_type_t type{COL_BIGINT};

  /** BOOLEAN value. */
  bool b{false};

  /** BIGINT value. */
  int64_t i{0};

  /** DOUBLE value. */
  double d{0};

  /** VARCHAR value, UTF-8. */
  const char *s{nullptr};

  /** Length of s in bytes. */
  size_t s_len{0};
};

/** A field of a point. */
struct ingest_field_t {
  /** Column name as given by the user. */
  const char *name{nullptr};

  /** Length of name in bytes. */
  size_t name_len{0};

  /** Type and value. */
  ingest_value_t value;
};

/** One input point. */
struct ingest_point_t {
  /** Microseconds since 1970-01-01 00:00:00 UTC. */
  int64_t time_us{0};

  /** The tags; NULL when there are none. */
  const ingest_tag_t *tags{nullptr};

  /** Number of tags. */
  uint32_t n_tags{0};

  /** The fields, at least one. */
  const ingest_field_t *fields{nullptr};

  /** Number of fields. */
  uint32_t n_fields{0};
};

/** Result of ingest_plan(). */
struct ingest_plan_t {
  /** Canonical table name. */
  std::string table;

  /** True if the table does not exist yet. */
  bool creates_table{false};

  /** Schema version the plan was made against, 0 for a new table. */
  uint64_t base_version{0};

  /** Columns to add, in id order, continuing after the base schema. For a
  new table this includes the time column. */
  std::vector<col_def_t> new_cols;

  /** The rows, following the schema after new_cols are added. */
  batch_t batch;

  /** Why the batch was rejected; empty on success. */
  std::string error;
};

/** Check a batch of points and build its write plan.
@param[in]   schema     current schema of the table, or NULL if the table does
                        not exist
@param[in]   table      table name as given by the user
@param[in]   table_len  length of table in bytes
@param[in]   points     the points
@param[in]   n_points   number of points
@param[out]  plan       the plan; on failure only plan.error is set
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
dberr_t ingest_plan(const schema_t *schema, const char *table, size_t table_len,
                    const ingest_point_t *points, uint32_t n_points,
                    ingest_plan_t &plan);

#endif
