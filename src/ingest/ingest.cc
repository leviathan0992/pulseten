/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest.cc
 *    Write planning: validation, column resolution and batch filling.
 *
 * Planning makes two passes over the points. The first checks every point
 * and resolves each tag and field to a column id, adding columns as they
 * first appear. The second fills the column batch using those ids. Nothing
 * is filled unless the whole batch is valid.
 *
 *-------------------------------------------------------------------------
 */

#include "ingest.h"

#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "batch.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"
#include "ut_time.h"

/** State while planning one batch. */
struct ingest_ctx_t {
  /** Columns of the schema after this batch: the base columns, then the
  ones this batch adds. */
  std::vector<col_def_t> m_cols;

  /** Column id of every tag and field, in input order: the tags of point 0,
  its fields, the tags of point 1, and so on. */
  std::vector<uint8_t> m_ids;

  /** Distinct UTC days of the batch. */
  std::vector<int64_t> m_days;

  /** Estimated data size of the batch in bytes. */
  uint64_t m_bytes{0};
};

/** Record why the batch is rejected.
@return an error of the given code with the formatted diagnostic */
static db_status_t ingest_reject(dberr_t code, const char *fmt, ...) {
  char buf[256];

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);

  return (db_status_t(code, buf));
}

/** Name a column kind for a diagnostic.
@return the name of a column kind, for error messages */
static const char *ingest_kind_name(col_kind_t kind) {
  switch (kind) {
    case col_kind_t::TIME:
      return ("time column");
    case col_kind_t::TAG:
      return ("tag");
    case col_kind_t::FIELD:
      return ("field");
  }

  return ("unknown kind");
}

/** Validate UTF-8 bytes without accepting overlong forms or surrogates.
@param[in]  str  UTF-8 bytes to validate
@return true if str is well-formed UTF-8: no overlong forms, no surrogates,
  nothing above U+10FFFF */
static bool ingest_utf8_is_valid(const char *str, size_t len) {
  const unsigned char *s = reinterpret_cast<const unsigned char *>(str);
  size_t i = 0;

  while (i < len) {
    const unsigned char c = s[i];

    if (c < 0x80) {
      ++i;
      continue;
    }

    /* Length of the sequence and the allowed range of its second byte. */
    size_t n;
    unsigned char lo = 0x80;
    unsigned char hi = 0xBF;

    if (c >= 0xC2 && c <= 0xDF) {
      n = 2;
    } else if (c == 0xE0) {
      n = 3;
      lo = 0xA0;
    } else if (c == 0xED) {
      n = 3;
      hi = 0x9F;
    } else if (c >= 0xE1 && c <= 0xEF) {
      n = 3;
    } else if (c == 0xF0) {
      n = 4;
      lo = 0x90;
    } else if (c == 0xF4) {
      n = 4;
      hi = 0x8F;
    } else if (c >= 0xF1 && c <= 0xF3) {
      n = 4;
    } else {
      return (false);
    }

    if (len - i < n) {
      return (false);
    }

    if (s[i + 1] < lo || s[i + 1] > hi) {
      return (false);
    }

    for (size_t k = 2; k < n; ++k) {
      if (s[i + k] < 0x80 || s[i + k] > 0xBF) {
        return (false);
      }
    }

    i += n;
  }

  return (true);
}

/** Check a string value.
@param[in]  s  string bytes; may be NULL only when len is 0
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_LIMIT_EXCEEDED */
static db_status_t ingest_check_string(uint32_t p, const char *what,
                                       const char *s, size_t len) {
  if (s == nullptr && len > 0) {
    return (
        ingest_reject(DB_INVALID_ARGUMENT, "point %u: %s is NULL", p, what));
  }

  if (len > INGEST_MAX_STRING) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: %s is longer than %d bytes", p, what,
                          INGEST_MAX_STRING));
  }

  if (!ingest_utf8_is_valid(s, len)) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: %s is not valid UTF-8", p, what));
  }

  return (DB_SUCCESS);
}

/** Find the column for a tag or field, adding it if the name is new.
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
static db_status_t ingest_resolve_column(ingest_ctx_t &ctx, ingest_plan_t &plan,
                                         uint32_t p, const char *name,
                                         size_t name_len, col_kind_t kind,
                                         col_type_t type, uint32_t &col_id) {
  std::string canonical;

  if (!schema_canonical_name(name, name_len, canonical).ok()) {
    return (ingest_reject(DB_INVALID_ARGUMENT, "point %u: invalid %s name", p,
                          ingest_kind_name(kind)));
  }

  if (canonical == SCHEMA_TIME_COLUMN) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: \"%s\" is reserved for the time column", p,
                          SCHEMA_TIME_COLUMN));
  }

  /* An existing column keeps its kind and type. */
  const col_def_t *col = schema_find_col(ctx.m_cols, canonical);

  if (col != nullptr) {
    if (col->m_kind != kind || col->m_type != type) {
      return (ingest_reject(DB_SCHEMA_CONFLICT,
                            "point %u: \"%s\" is a %s %s, not a %s %s", p,
                            canonical.c_str(), schema_type_name(col->m_type),
                            ingest_kind_name(col->m_kind),
                            schema_type_name(type), ingest_kind_name(kind)));
    }

    col_id = col->m_id;

    return (DB_SUCCESS);
  }

  /* A new column goes at the end. */
  if (ctx.m_cols.size() >= SCHEMA_MAX_COLUMNS) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: \"%s\" would exceed %d columns", p,
                          canonical.c_str(), SCHEMA_MAX_COLUMNS));
  }

  col_def_t def;
  def.m_id = static_cast<uint32_t>(ctx.m_cols.size());
  def.m_name = canonical;
  def.m_kind = kind;
  def.m_type = type;

  ctx.m_cols.push_back(def);
  plan.m_new_cols.push_back(def);

  col_id = def.m_id;

  return (DB_SUCCESS);
}

/** Remember a column id for the fill pass, rejecting a name that appears
twice in one point.
@param[in,out]  seen  columns already used by this point
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
static db_status_t ingest_mark_column(ingest_ctx_t &ctx, uint32_t p,
                                      uint32_t col_id, bool *seen) {
  if (seen[col_id]) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: \"%s\" appears more than once", p,
                          ctx.m_cols[col_id].m_name.c_str()));
  }

  seen[col_id] = true;
  ctx.m_ids.push_back(static_cast<uint8_t>(col_id));

  return (DB_SUCCESS);
}

/** Check the time of a point and count its UTC day.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_LIMIT_EXCEEDED */
static db_status_t ingest_check_time(ingest_ctx_t &ctx, uint32_t p,
                                     int64_t time_us) {
  if (time_us < INGEST_MIN_TIME_US || time_us >= INGEST_MAX_TIME_US) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: time is outside [1900, 2200)", p));
  }

  const int64_t day = ut_utc_day(time_us);

  for (size_t i = 0; i < ctx.m_days.size(); ++i) {
    if (ctx.m_days[i] == day) {
      return (DB_SUCCESS);
    }
  }

  if (ctx.m_days.size() >= INGEST_MAX_DAYS) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: batch spans more than %d UTC days", p,
                          INGEST_MAX_DAYS));
  }

  ctx.m_days.push_back(day);

  return (DB_SUCCESS);
}

/** First pass over one point: check it and resolve its columns.
@param[in]  p  zero-based input point index
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
static db_status_t ingest_check_point(ingest_ctx_t &ctx, ingest_plan_t &plan,
                                      uint32_t p, const ingest_point_t &pt) {
  db_status_t err = ingest_check_time(ctx, p, pt.m_time_us);
  if (!err.ok()) {
    return (err);
  }

  if (pt.m_n_fields == 0) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: a point needs at least one field", p));
  }

  if (pt.m_n_tags + static_cast<uint64_t>(pt.m_n_fields) >=
      SCHEMA_MAX_COLUMNS) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: more than %d tags and fields", p,
                          SCHEMA_MAX_COLUMNS - 1));
  }

  if ((pt.m_n_tags > 0 && pt.m_tags == nullptr) || pt.m_fields == nullptr) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: tags or fields are NULL", p));
  }

  /* Time column. */
  ctx.m_bytes += INGEST_VALUE_BYTES;

  bool seen[SCHEMA_MAX_COLUMNS] = {false};

  for (uint32_t t = 0; t < pt.m_n_tags; ++t) {
    const ingest_tag_t &tag = pt.m_tags[t];

    err = ingest_check_string(p, "tag value", tag.m_value, tag.m_value_len);
    if (!err.ok()) {
      return (err);
    }

    uint32_t col_id = 0;
    err = ingest_resolve_column(ctx, plan, p, tag.m_name, tag.m_name_len,
                                col_kind_t::TAG, col_type_t::VARCHAR, col_id);
    if (!err.ok()) {
      return (err);
    }

    err = ingest_mark_column(ctx, p, col_id, seen);
    if (!err.ok()) {
      return (err);
    }

    ctx.m_bytes += INGEST_VALUE_BYTES + tag.m_value_len;
  }

  for (uint32_t f = 0; f < pt.m_n_fields; ++f) {
    const ingest_field_t &field = pt.m_fields[f];
    const ingest_value_t &v = field.m_value;

    if (!schema_field_type_is_valid(v.m_type)) {
      return (ingest_reject(DB_INVALID_ARGUMENT, "point %u: invalid field type",
                            p));
    }

    if (v.m_type == col_type_t::DOUBLE && !std::isfinite(v.m_double)) {
      return (ingest_reject(DB_INVALID_ARGUMENT,
                            "point %u: DOUBLE field is NaN or infinite", p));
    }

    if (v.m_type == col_type_t::VARCHAR) {
      err = ingest_check_string(p, "field value", v.m_string, v.m_string_len);
      if (!err.ok()) {
        return (err);
      }

      ctx.m_bytes += v.m_string_len;
    }

    uint32_t col_id = 0;
    err = ingest_resolve_column(ctx, plan, p, field.m_name, field.m_name_len,
                                col_kind_t::FIELD, v.m_type, col_id);
    if (!err.ok()) {
      return (err);
    }

    err = ingest_mark_column(ctx, p, col_id, seen);
    if (!err.ok()) {
      return (err);
    }

    ctx.m_bytes += INGEST_VALUE_BYTES;
  }

  if (ctx.m_bytes > INGEST_MAX_BATCH_BYTES) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: batch data exceeds %d bytes", p,
                          INGEST_MAX_BATCH_BYTES));
  }

  return (DB_SUCCESS);
}

/** Size every column of the batch for n rows, all NULL.
@param[in]   ctx    resolved columns after the validation pass
@param[out]  batch  column buffers initialized with every row NULL */
static void ingest_init_batch(const ingest_ctx_t &ctx, uint32_t n,
                              batch_t &batch) {
  batch.m_n_rows = n;
  batch.m_cols.resize(ctx.m_cols.size());

  for (size_t i = 0; i < ctx.m_cols.size(); ++i) {
    batch_col_init(batch.m_cols[i], static_cast<uint32_t>(i),
                   ctx.m_cols[i].m_type, n);
  }
}

/** Second pass over one point: store its values in row r. */
static void ingest_fill_row(const ingest_ctx_t &ctx, const ingest_point_t &pt,
                            uint32_t r, size_t &next, batch_t &batch) {
  batch_col_t &time_col = batch.m_cols[0];
  time_col.m_ints[r] = pt.m_time_us;
  time_col.m_nulls[r] = 0;

  for (uint32_t t = 0; t < pt.m_n_tags; ++t) {
    batch_col_t &col = batch.m_cols[ctx.m_ids[next++]];

    col.m_chars.append(pt.m_tags[t].m_value, pt.m_tags[t].m_value_len);
    col.m_nulls[r] = 0;
  }

  for (uint32_t f = 0; f < pt.m_n_fields; ++f) {
    const ingest_value_t &v = pt.m_fields[f].m_value;
    batch_col_t &col = batch.m_cols[ctx.m_ids[next++]];

    switch (v.m_type) {
      case col_type_t::BOOLEAN:
        col.m_bools[r] = v.m_boolean ? 1 : 0;
        break;
      case col_type_t::BIGINT:
        col.m_ints[r] = v.m_integer;
        break;
      case col_type_t::DOUBLE:
        col.m_doubles[r] = v.m_double;
        break;
      case col_type_t::VARCHAR:
        col.m_chars.append(v.m_string, v.m_string_len);
        break;
      case col_type_t::TIMESTAMP:
        ut_error;
    }

    col.m_nulls[r] = 0;
  }

  /* Close row r of every string column, whether or not it had a value. */
  for (size_t i = 0; i < batch.m_cols.size(); ++i) {
    batch_col_t &col = batch.m_cols[i];

    if (col.m_type == col_type_t::VARCHAR) {
      col.m_offsets[r + 1] = static_cast<uint32_t>(col.m_chars.size());
    }
  }
}

/** Check and plan a batch; ingest_plan() resets the plan on failure.
@param[in]  schema  schema the write follows; may be NULL when creating a
                    table
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED; the caller resets partial output on failure */
static db_status_t ingest_plan_low(const schema_t *schema,
                                   const std::string &table,
                                   const ingest_point_t *points,
                                   uint32_t n_points, ingest_plan_t &plan) {
  ut_a(schema == nullptr || schema->m_name == table);

  plan.m_table = table;

  /* Check the batch as a whole. */
  if (points == nullptr || n_points == 0) {
    return (ingest_reject(DB_INVALID_ARGUMENT, "empty batch"));
  }

  if (n_points > INGEST_MAX_POINTS) {
    return (ingest_reject(DB_LIMIT_EXCEEDED, "batch has more than %d points",
                          INGEST_MAX_POINTS));
  }

  /* Start from the current schema, or a new table with only "time". */
  ingest_ctx_t ctx;

  if (schema != nullptr) {
    ctx.m_cols = schema->m_cols;
    plan.m_base_version = schema->m_version;
  } else {
    col_def_t time_col;
    time_col.m_id = 0;
    time_col.m_name = SCHEMA_TIME_COLUMN;
    time_col.m_kind = col_kind_t::TIME;
    time_col.m_type = col_type_t::TIMESTAMP;

    ctx.m_cols.push_back(time_col);
    plan.m_new_cols.push_back(time_col);
    plan.m_creates_table = true;
  }

  /* First pass: check every point and resolve its columns. */
  for (uint32_t p = 0; p < n_points; ++p) {
    const db_status_t err = ingest_check_point(ctx, plan, p, points[p]);
    if (!err.ok()) {
      return (err);
    }
  }

  /* The encoded batch also stores a NULL bitmap per column, including
  columns this batch does not use. */
  ctx.m_bytes += ctx.m_cols.size() * static_cast<uint64_t>((n_points + 7) / 8);

  if (ctx.m_bytes > INGEST_MAX_BATCH_BYTES) {
    return (ingest_reject(DB_LIMIT_EXCEEDED, "batch data exceeds %d bytes",
                          INGEST_MAX_BATCH_BYTES));
  }

  /* Second pass: fill the batch. */
  batch_t &batch = plan.m_batch;
  batch.m_table = plan.m_table;
  batch.m_schema_version = plan.m_base_version;

  if (!plan.m_new_cols.empty()) {
    batch.m_schema_version++;
  }

  ingest_init_batch(ctx, n_points, batch);

  size_t next = 0;
  for (uint32_t r = 0; r < n_points; ++r) {
    ingest_fill_row(ctx, points[r], r, next, batch);
  }

  ut_a(next == ctx.m_ids.size());
  ut_a(batch.m_n_rows == n_points);
  ut_a(batch.m_cols.size() == (schema != nullptr ? schema->m_cols.size() : 0) +
                                  plan.m_new_cols.size());

  return (DB_SUCCESS);
}

/** Check a batch of points and build its write plan. */
db_status_t ingest_plan(const schema_t *schema, const std::string &table,
                        const ingest_point_t *points, uint32_t n_points,
                        ingest_plan_t &plan) {
  plan = ingest_plan_t();

  const db_status_t err =
      ingest_plan_low(schema, table, points, n_points, plan);

  /* A failed plan has no partially built output; its status owns the reason. */
  if (!err.ok()) {
    plan = ingest_plan_t();
  }

  return (err);
}
