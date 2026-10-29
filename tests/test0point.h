/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test0point.h
 *    Helpers for building input points and write plans in tests.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test0point_h
#define test0point_h

#include <cstring>
#include <string>

#include "ingest0ingest.h"

/** One UTC day in microseconds. */
static const int64_t DAY = 86400000000LL;

/** 2026-01-01 00:00:00 UTC. */
static const int64_t T0 = 1767225600000000LL;

/** Build an input tag from NUL-terminated strings.
@param[in]  name   NUL-terminated tag name
@param[in]  value  NUL-terminated tag value borrowed by the tag
@return a tag with NUL-terminated name and value */
static inline ingest_tag_t tag(const char *name, const char *value) {
  ingest_tag_t t;
  t.name = name;
  t.name_len = strlen(name);
  t.value = value;
  t.value_len = strlen(value);

  return (t);
}

/** Build a BIGINT input field.
@param[in]  name  NUL-terminated field name
@param[in]  v     BIGINT value
@return a BIGINT field */
static inline ingest_field_t field_int(const char *name, int64_t v) {
  ingest_field_t f;
  f.name = name;
  f.name_len = strlen(name);
  f.value.type = COL_BIGINT;
  f.value.i = v;

  return (f);
}

/** Build a DOUBLE input field.
@param[in]  name  NUL-terminated field name
@param[in]  v     DOUBLE value
@return a DOUBLE field */
static inline ingest_field_t field_double(const char *name, double v) {
  ingest_field_t f;
  f.name = name;
  f.name_len = strlen(name);
  f.value.type = COL_DOUBLE;
  f.value.d = v;

  return (f);
}

/** Build a BOOLEAN input field.
@param[in]  name  NUL-terminated field name
@param[in]  v     BOOLEAN value
@return a BOOLEAN field */
static inline ingest_field_t field_bool(const char *name, bool v) {
  ingest_field_t f;
  f.name = name;
  f.name_len = strlen(name);
  f.value.type = COL_BOOLEAN;
  f.value.b = v;

  return (f);
}

/** Build a VARCHAR input field.
@param[in]  name  NUL-terminated field name
@param[in]  v     NUL-terminated VARCHAR value borrowed by the field
@return a VARCHAR field with a NUL-terminated value */
static inline ingest_field_t field_str(const char *name, const char *v) {
  ingest_field_t f;
  f.name = name;
  f.name_len = strlen(name);
  f.value.type = COL_VARCHAR;
  f.value.s = v;
  f.value.s_len = strlen(v);

  return (f);
}

/** Build an input point borrowing its tag and field arrays.
@param[in]  time_us   UTC timestamp in microseconds since 1970-01-01
@param[in]  tags      input tags; may be NULL when n_tags is 0
@param[in]  n_tags    number of tags
@param[in]  fields    field array borrowed by the point
@param[in]  n_fields  number of fields in the array
@return a point with the given tags and fields */
static inline ingest_point_t point(int64_t time_us, const ingest_tag_t *tags,
                                   uint32_t n_tags,
                                   const ingest_field_t *fields,
                                   uint32_t n_fields) {
  ingest_point_t p;
  p.time_us = time_us;
  p.tags = tags;
  p.n_tags = n_tags;
  p.fields = fields;
  p.n_fields = n_fields;

  return (p);
}

/** Plan a write to a NUL-terminated table name against a schema.
@param[in]   schema  schema the write follows; may be NULL when creating a
                     table
@param[in]   table   logical table name
@param[in]   points  input points
@param[in]   n       number of input points
@param[out]  plan    validated plan or rejection diagnostic
@return as ingest_plan() */
static inline dberr_t plan_for(const schema_t *schema, const char *table,
                               const ingest_point_t *points, uint32_t n,
                               ingest_plan_t *plan) {
  return (ingest_plan(schema, table, strlen(table), points, n, *plan));
}

/** The schema a catalog would hold after applying a plan.
@param[in]  base  previous schema; NULL for a newly created table
@param[in]  plan  plan whose new columns are appended to the schema
@return a schema copy including the plan's newly added columns and version */
static inline schema_t schema_after(const schema_t *base,
                                    const ingest_plan_t &plan) {
  schema_t s;

  if (base != nullptr) {
    s = *base;
  }

  s.name = plan.table;
  s.version = plan.batch.schema_version;

  for (size_t i = 0; i < plan.new_cols.size(); ++i) {
    s.cols.push_back(plan.new_cols[i]);
  }

  return (s);
}

/** Read a VARCHAR row from a column batch.
@param[in]  col  VARCHAR column whose offsets and character bytes are read
@param[in]  r    zero-based row index
@return the string of row r of a VARCHAR batch column */
static inline std::string str_at(const batch_col_t &col, uint32_t r) {
  return (
      col.chars.substr(col.offsets[r], col.offsets[r + 1] - col.offsets[r]));
}

#endif
