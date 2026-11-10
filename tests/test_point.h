/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_point.h
 *    Helpers for building input points and write plans in tests.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_point_h
#define test_point_h

#include <cstdint>
#include <cstring>
#include <string>

#include "batch.h"
#include "ingest.h"
#include "schema.h"
#include "ut_err.h"

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
  t.m_name = name;
  t.m_name_len = strlen(name);
  t.m_value = value;
  t.m_value_len = strlen(value);

  return (t);
}

/** Build a BIGINT input field.
@param[in]  name  NUL-terminated field name
@return a BIGINT field */
static inline ingest_field_t field_int(const char *name, int64_t v) {
  ingest_field_t f;
  f.m_name = name;
  f.m_name_len = strlen(name);
  f.m_value.m_type = col_type_t::BIGINT;
  f.m_value.m_integer = v;

  return (f);
}

/** Build a DOUBLE input field.
@param[in]  name  NUL-terminated field name
@return a DOUBLE field */
static inline ingest_field_t field_double(const char *name, double v) {
  ingest_field_t f;
  f.m_name = name;
  f.m_name_len = strlen(name);
  f.m_value.m_type = col_type_t::DOUBLE;
  f.m_value.m_double = v;

  return (f);
}

/** Build a BOOLEAN input field.
@param[in]  name  NUL-terminated field name
@return a BOOLEAN field */
static inline ingest_field_t field_bool(const char *name, bool v) {
  ingest_field_t f;
  f.m_name = name;
  f.m_name_len = strlen(name);
  f.m_value.m_type = col_type_t::BOOLEAN;
  f.m_value.m_boolean = v;

  return (f);
}

/** Build a VARCHAR input field.
@param[in]  name  NUL-terminated field name
@param[in]  v     NUL-terminated VARCHAR value borrowed by the field
@return a VARCHAR field with a NUL-terminated value */
static inline ingest_field_t field_str(const char *name, const char *v) {
  ingest_field_t f;
  f.m_name = name;
  f.m_name_len = strlen(name);
  f.m_value.m_type = col_type_t::VARCHAR;
  f.m_value.m_string = v;
  f.m_value.m_string_len = strlen(v);

  return (f);
}

/** Build an input point borrowing its tag and field arrays.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@param[in]  tags     input tags; may be NULL when n_tags is 0
@param[in]  fields   field array borrowed by the point
@return a point with the given tags and fields */
static inline ingest_point_t point(int64_t time_us, const ingest_tag_t *tags,
                                   uint32_t n_tags,
                                   const ingest_field_t *fields,
                                   uint32_t n_fields) {
  ingest_point_t p;
  p.m_time_us = time_us;
  p.m_tags = tags;
  p.m_n_tags = n_tags;
  p.m_fields = fields;
  p.m_n_fields = n_fields;

  return (p);
}

/** Plan a write to a NUL-terminated table name against a schema.
@param[in]  schema  schema the write follows; may be NULL when creating a
                    table
@return as ingest_plan() */
static inline db_status_t plan_for(const schema_t *schema, const char *table,
                                   const ingest_point_t *points, uint32_t n,
                                   ingest_plan_t &plan) {
  return (ingest_plan(schema, table, points, n, plan));
}

/** The schema a catalog would hold after applying a plan.
@param[in]  base  previous schema; NULL for a newly created table
@return a schema copy including the plan's newly added columns and version */
static inline schema_t schema_after(const schema_t *base,
                                    const ingest_plan_t &plan) {
  schema_t s;

  if (base != nullptr) {
    s = *base;
  }

  s.m_name = plan.m_table;
  s.m_version = plan.m_batch.m_schema_version;

  for (size_t i = 0; i < plan.m_new_cols.size(); ++i) {
    s.m_cols.push_back(plan.m_new_cols[i]);
  }

  return (s);
}

/** Read a VARCHAR row from a column batch.
@param[in]  r  zero-based row index
@return the string of row r of a VARCHAR batch column */
static inline std::string str_at(const batch_col_t &col, uint32_t r) {
  return (col.m_chars.substr(col.m_offsets[r],
                             col.m_offsets[r + 1] - col.m_offsets[r]));
}

#endif
