/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest0ingest.cc
 *    Write planning: validation, column resolution and batch filling.
 *
 * Planning makes two passes over the points. The first checks every point
 * and resolves each tag and field to a column id, adding columns as they
 * first appear. The second fills the column batch using those ids. Nothing
 * is filled unless the whole batch is valid.
 *
 *-------------------------------------------------------------------------
 */

#include "ingest0ingest.h"

#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <new>

#include "ut0time.h"
#include "ut0ut.h"

/** State while planning one batch. */
struct ingest_ctx_t {
  /** Columns of the schema after this batch: the base columns, then the
  ones this batch adds. */
  std::vector<col_def_t> cols;

  /** Column id of every tag and field, in input order: the tags of point 0,
  its fields, the tags of point 1, and so on. */
  std::vector<uint8_t> ids;

  /** Distinct UTC days of the batch. */
  std::vector<int64_t> days;

  /** Estimated data size of the batch in bytes. */
  uint64_t bytes{0};
};

/** Record why the batch is rejected.
@param[in,out]  plan  plan whose error is set
@param[in]      err   error code to return
@param[in]      fmt   printf-style message
@return err */
static dberr_t ingest_reject(ingest_plan_t &plan, dberr_t err, const char *fmt,
                             ...) {
  char buf[256];

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);

  plan.error = buf;

  return (err);
}

/** Name a column role for a diagnostic.
@param[in]  role  column role
@return the name of a column role, for error messages */
static const char *ingest_role_name(col_role_t role) {
  switch (role) {
    case COL_ROLE_TIME:
      return ("time column");
    case COL_ROLE_TAG:
      return ("tag");
    case COL_ROLE_FIELD:
      return ("field");
  }

  return ("unknown role");
}

/** Validate UTF-8 bytes without accepting overlong forms or surrogates.
@param[in]  str  UTF-8 bytes to validate
@param[in]  len  input length in bytes
@return true if str is well-formed UTF-8: no overlong forms, no surrogates,
  nothing above U+10FFFF */
static bool ingest_utf8_is_valid(const char *str, size_t len) {
  const unsigned char *s = (const unsigned char *)str;
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
@param[in,out]  plan  plan receiving the rejection message
@param[in]      p     input point index for the diagnostic
@param[in]      what  label of the string value, for the diagnostic
@param[in]      s     string bytes; may be NULL only when len is 0
@param[in]      len   input length in bytes
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_LIMIT_EXCEEDED */
static dberr_t ingest_check_string(ingest_plan_t &plan, uint32_t p,
                                   const char *what, const char *s,
                                   size_t len) {
  if (s == nullptr && len > 0) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT, "point %u: %s is NULL", p,
                          what));
  }

  if (len > INGEST_MAX_STRING) {
    return (ingest_reject(plan, DB_LIMIT_EXCEEDED,
                          "point %u: %s is longer than %d bytes", p, what,
                          INGEST_MAX_STRING));
  }

  if (!ingest_utf8_is_valid(s, len)) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                          "point %u: %s is not valid UTF-8", p, what));
  }

  return (DB_SUCCESS);
}

/** Find the column for a tag or field, adding it if the name is new.
@param[in,out]  ctx       planning state
@param[in,out]  plan      plan; new columns are appended to new_cols
@param[in]      p         point number, for messages
@param[in]      name      name as given
@param[in]      name_len  length of name
@param[in]      role      COL_ROLE_TAG or COL_ROLE_FIELD
@param[in]      type      value type
@param[out]     col_id    id of the column
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
static dberr_t ingest_resolve_column(ingest_ctx_t &ctx, ingest_plan_t &plan,
                                     uint32_t p, const char *name,
                                     size_t name_len, col_role_t role,
                                     col_type_t type, uint32_t &col_id) {
  std::string canonical;

  if (schema_canonical_name(name, name_len, canonical) != DB_SUCCESS) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                          "point %u: invalid %s name", p,
                          ingest_role_name(role)));
  }

  if (canonical == SCHEMA_TIME_COLUMN) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                          "point %u: \"%s\" is reserved for the time column", p,
                          SCHEMA_TIME_COLUMN));
  }

  /* An existing column keeps its role and type. */
  const col_def_t *col = schema_find_col(ctx.cols, canonical);

  if (col != nullptr) {
    if (col->role != role || col->type != type) {
      return (ingest_reject(plan, DB_SCHEMA_CONFLICT,
                            "point %u: \"%s\" is a %s %s, not a %s %s", p,
                            canonical.c_str(), schema_type_name(col->type),
                            ingest_role_name(col->role), schema_type_name(type),
                            ingest_role_name(role)));
    }

    col_id = col->id;

    return (DB_SUCCESS);
  }

  /* A new column goes at the end. */
  if (ctx.cols.size() >= SCHEMA_MAX_COLUMNS) {
    return (ingest_reject(plan, DB_LIMIT_EXCEEDED,
                          "point %u: \"%s\" would exceed %d columns", p,
                          canonical.c_str(), SCHEMA_MAX_COLUMNS));
  }

  col_def_t def;
  def.id = (uint32_t)ctx.cols.size();
  def.name = canonical;
  def.role = role;
  def.type = type;

  ctx.cols.push_back(def);
  plan.new_cols.push_back(def);

  col_id = def.id;

  return (DB_SUCCESS);
}

/** Remember a column id for the fill pass, rejecting a name that appears
twice in one point.
@param[in,out]  ctx     planning state
@param[in,out]  plan    plan receiving a duplicate-column diagnostic
@param[in]      p       input point index for the diagnostic
@param[in]      col_id  column id in the schema
@param[in,out]  seen    columns already used by this point
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
static dberr_t ingest_use_column(ingest_ctx_t &ctx, ingest_plan_t &plan,
                                 uint32_t p, uint32_t col_id, bool *seen) {
  if (seen[col_id]) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                          "point %u: \"%s\" appears more than once", p,
                          ctx.cols[col_id].name.c_str()));
  }

  seen[col_id] = true;
  ctx.ids.push_back((uint8_t)col_id);

  return (DB_SUCCESS);
}

/** Check the time of a point and count its UTC day.
@param[in,out]  ctx      distinct days and estimated batch size
@param[in,out]  plan     plan receiving a rejection message
@param[in]      p        input point index for the diagnostic
@param[in]      time_us  UTC timestamp in microseconds since 1970-01-01
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_LIMIT_EXCEEDED */
static dberr_t ingest_check_time(ingest_ctx_t &ctx, ingest_plan_t &plan,
                                 uint32_t p, int64_t time_us) {
  if (time_us < INGEST_MIN_TIME_US || time_us >= INGEST_MAX_TIME_US) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                          "point %u: time is outside [1900, 2200)", p));
  }

  const int64_t day = ut_utc_day(time_us);

  for (size_t i = 0; i < ctx.days.size(); ++i) {
    if (ctx.days[i] == day) {
      return (DB_SUCCESS);
    }
  }

  if (ctx.days.size() >= INGEST_MAX_DAYS) {
    return (ingest_reject(plan, DB_LIMIT_EXCEEDED,
                          "point %u: batch spans more than %d UTC days", p,
                          INGEST_MAX_DAYS));
  }

  ctx.days.push_back(day);

  return (DB_SUCCESS);
}

/** First pass over one point: check it and resolve its columns.
@param[in,out]  ctx   resolved columns, input ids, days and byte count
@param[in,out]  plan  planned schema additions and rejection message
@param[in]      p     zero-based input point index
@param[in]      pt    point whose time, names, roles, types and values are
                      checked
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
static dberr_t ingest_check_point(ingest_ctx_t &ctx, ingest_plan_t &plan,
                                  uint32_t p, const ingest_point_t &pt) {
  dberr_t err = ingest_check_time(ctx, plan, p, pt.time_us);
  if (err != DB_SUCCESS) {
    return (err);
  }

  if (pt.n_fields == 0) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                          "point %u: a point needs at least one field", p));
  }

  if (pt.n_tags + (uint64_t)pt.n_fields >= SCHEMA_MAX_COLUMNS) {
    return (ingest_reject(plan, DB_LIMIT_EXCEEDED,
                          "point %u: more than %d tags and fields", p,
                          SCHEMA_MAX_COLUMNS - 1));
  }

  if ((pt.n_tags > 0 && pt.tags == nullptr) || pt.fields == nullptr) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                          "point %u: tags or fields are NULL", p));
  }

  /* Time column. */
  ctx.bytes += 8;

  bool seen[SCHEMA_MAX_COLUMNS] = {false};

  for (uint32_t t = 0; t < pt.n_tags; ++t) {
    const ingest_tag_t &tag = pt.tags[t];

    err = ingest_check_string(plan, p, "tag value", tag.value, tag.value_len);
    if (err != DB_SUCCESS) {
      return (err);
    }

    uint32_t col_id = 0;
    err = ingest_resolve_column(ctx, plan, p, tag.name, tag.name_len,
                                COL_ROLE_TAG, COL_VARCHAR, col_id);
    if (err != DB_SUCCESS) {
      return (err);
    }

    err = ingest_use_column(ctx, plan, p, col_id, seen);
    if (err != DB_SUCCESS) {
      return (err);
    }

    ctx.bytes += 8 + tag.value_len;
  }

  for (uint32_t f = 0; f < pt.n_fields; ++f) {
    const ingest_field_t &field = pt.fields[f];
    const ingest_value_t &v = field.value;

    if (!schema_field_type_is_valid(v.type)) {
      return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                            "point %u: invalid field type", p));
    }

    if (v.type == COL_DOUBLE && !std::isfinite(v.d)) {
      return (ingest_reject(plan, DB_INVALID_ARGUMENT,
                            "point %u: DOUBLE field is NaN or infinite", p));
    }

    if (v.type == COL_VARCHAR) {
      err = ingest_check_string(plan, p, "field value", v.s, v.s_len);
      if (err != DB_SUCCESS) {
        return (err);
      }

      ctx.bytes += v.s_len;
    }

    uint32_t col_id = 0;
    err = ingest_resolve_column(ctx, plan, p, field.name, field.name_len,
                                COL_ROLE_FIELD, v.type, col_id);
    if (err != DB_SUCCESS) {
      return (err);
    }

    err = ingest_use_column(ctx, plan, p, col_id, seen);
    if (err != DB_SUCCESS) {
      return (err);
    }

    ctx.bytes += 8;
  }

  if (ctx.bytes > INGEST_MAX_BATCH_BYTES) {
    return (ingest_reject(plan, DB_LIMIT_EXCEEDED,
                          "point %u: batch data exceeds %d bytes", p,
                          INGEST_MAX_BATCH_BYTES));
  }

  return (DB_SUCCESS);
}

/** Size every column of the batch for n rows, all NULL.
@param[in]   ctx    resolved columns after the validation pass
@param[in]   n      number of rows to allocate
@param[out]  batch  column buffers initialized with every row NULL */
static void ingest_init_batch(const ingest_ctx_t &ctx, uint32_t n,
                              batch_t &batch) {
  batch.n_rows = n;
  batch.cols.resize(ctx.cols.size());

  for (size_t i = 0; i < ctx.cols.size(); ++i) {
    batch_col_init(batch.cols[i], (uint32_t)i, ctx.cols[i].type, n);
  }
}

/** Second pass over one point: store its values in row r.
@param[in]      ctx    resolved column ids in input order
@param[in]      pt     the point
@param[in]      r      row number
@param[in,out]  next   position in ctx.ids of the point's first value
@param[in,out]  batch  column batch receiving row r */
static void ingest_fill_row(const ingest_ctx_t &ctx, const ingest_point_t &pt,
                            uint32_t r, size_t &next, batch_t &batch) {
  batch_col_t &time_col = batch.cols[0];
  time_col.ints[r] = pt.time_us;
  time_col.nulls[r] = 0;

  for (uint32_t t = 0; t < pt.n_tags; ++t) {
    batch_col_t &col = batch.cols[ctx.ids[next++]];

    col.chars.append(pt.tags[t].value, pt.tags[t].value_len);
    col.nulls[r] = 0;
  }

  for (uint32_t f = 0; f < pt.n_fields; ++f) {
    const ingest_value_t &v = pt.fields[f].value;
    batch_col_t &col = batch.cols[ctx.ids[next++]];

    switch (v.type) {
      case COL_BOOLEAN:
        col.bools[r] = v.b ? 1 : 0;
        break;
      case COL_BIGINT:
        col.ints[r] = v.i;
        break;
      case COL_DOUBLE:
        col.doubles[r] = v.d;
        break;
      case COL_VARCHAR:
        col.chars.append(v.s, v.s_len);
        break;
      case COL_TIMESTAMP:
        ut_error;
    }

    col.nulls[r] = 0;
  }

  /* Close row r of every string column, whether or not it had a value. */
  for (size_t i = 0; i < batch.cols.size(); ++i) {
    batch_col_t &col = batch.cols[i];

    if (col.type == COL_VARCHAR) {
      col.offsets[r + 1] = (uint32_t)col.chars.size();
    }
  }
}

/** Check and plan a batch; ingest_plan() resets the plan on failure.
@param[in]   schema     schema the write follows; may be NULL when creating a
                        table
@param[in]   table      table name as given by the caller
@param[in]   table_len  length of table in bytes
@param[in]   points     input points
@param[in]   n_points   number of points in the input batch
@param[out]  plan       validated schema additions and column batch, or
                        rejection message
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED; the caller resets partial output on failure */
static dberr_t ingest_plan_low(const schema_t *schema, const char *table,
                               size_t table_len, const ingest_point_t *points,
                               uint32_t n_points, ingest_plan_t &plan) {
  /* Check the batch as a whole. */
  if (schema_table_name(table, table_len, plan.table) != DB_SUCCESS) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT, "invalid table name"));
  }

  if (schema != nullptr) {
    ut_a(schema->name == plan.table);
  }

  if (points == nullptr || n_points == 0) {
    return (ingest_reject(plan, DB_INVALID_ARGUMENT, "empty batch"));
  }

  if (n_points > INGEST_MAX_POINTS) {
    return (ingest_reject(plan, DB_LIMIT_EXCEEDED,
                          "batch has more than %d points", INGEST_MAX_POINTS));
  }

  /* Start from the current schema, or a new table with only "time". */
  ingest_ctx_t ctx;

  if (schema != nullptr) {
    ctx.cols = schema->cols;
    plan.base_version = schema->version;
  } else {
    col_def_t time_col;
    time_col.id = 0;
    time_col.name = SCHEMA_TIME_COLUMN;
    time_col.role = COL_ROLE_TIME;
    time_col.type = COL_TIMESTAMP;

    ctx.cols.push_back(time_col);
    plan.new_cols.push_back(time_col);
    plan.creates_table = true;
  }

  /* First pass: check every point and resolve its columns. */
  for (uint32_t p = 0; p < n_points; ++p) {
    const dberr_t err = ingest_check_point(ctx, plan, p, points[p]);
    if (err != DB_SUCCESS) {
      return (err);
    }
  }

  /* The encoded batch also stores a NULL bitmap per column, including
  columns this batch does not use. */
  ctx.bytes += ctx.cols.size() * (uint64_t)((n_points + 7) / 8);

  if (ctx.bytes > INGEST_MAX_BATCH_BYTES) {
    return (ingest_reject(plan, DB_LIMIT_EXCEEDED,
                          "batch data exceeds %d bytes",
                          INGEST_MAX_BATCH_BYTES));
  }

  /* Second pass: fill the batch. */
  batch_t &batch = plan.batch;
  batch.table = plan.table;
  batch.schema_version = plan.base_version;

  if (!plan.new_cols.empty()) {
    batch.schema_version++;
  }

  ingest_init_batch(ctx, n_points, batch);

  size_t next = 0;
  for (uint32_t r = 0; r < n_points; ++r) {
    ingest_fill_row(ctx, points[r], r, next, batch);
  }

  ut_a(next == ctx.ids.size());

  return (DB_SUCCESS);
}

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
                    ingest_plan_t &plan) {
  plan = ingest_plan_t();

  const dberr_t err =
      ingest_plan_low(schema, table, table_len, points, n_points, plan);

  /* On failure keep only the reason. */
  if (err != DB_SUCCESS) {
    std::string error;
    error.swap(plan.error);

    plan = ingest_plan_t();
    plan.error.swap(error);
  }

  return (err);
}
