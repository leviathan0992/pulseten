/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest.cc
 *    Write planning and WAL payload encoding and decoding.
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
#include <cstring>
#include <string>
#include <vector>

#include "db_batch.h"
#include "db_schema.h"
#include "ut.h"

/** State while planning one batch. */
class ingest_planner final : private ut_noncopyable {
 public:
  /** Plan a batch; the caller discards partial output on failure. */
  db_status_t plan(const schema_t *schema, const std::string &table_name,
                   const ingest_point_t *points, uint32_t n_points,
                   ingest_plan_t &plan);

 private:
  /** Last successful resolution at one input position. The name is borrowed
  from the input batch. */
  struct column_slot_t {
    const char *m_name{nullptr};
    size_t m_name_len{0};
    data_type_t m_data_type{data_type_t::TIMESTAMP};
    uint32_t m_id{0};
  };

  /** Resolve a column while preserving its identity. */
  db_status_t resolve_column(ingest_plan_t &plan, uint32_t p, const char *name,
                             size_t name_len, column_type_t column_type,
                             data_type_t data_type, column_slot_t &slot,
                             uint32_t &col_id);

  /** Record a column used by the current point. */
  db_status_t mark_column(uint32_t p, uint32_t col_id, bool *seen);

  /** Validate a time and record its UTC day. */
  db_status_t plan_time(uint32_t p, int64_t time_us);

  /** Validate one point and resolve its columns. */
  db_status_t plan_point(ingest_plan_t &plan, uint32_t p,
                         const ingest_point_t &pt);

  /** Allocate the resolved batch columns. */
  void init_batch(uint32_t n, batch_t &batch) const;

  /** Fill one validated row. */
  void fill_row(const ingest_point_t &pt, uint32_t r, size_t &next,
                batch_t &batch) const;

  /** Columns of the schema after this batch: the base columns, then the
  ones this batch adds. */
  std::vector<schema_column_t> m_columns;

  /** Reuse column ids when successive rows have the same layout. */
  column_slot_t m_tag_slots[SCHEMA_MAX_COLUMNS]{};
  column_slot_t m_field_slots[SCHEMA_MAX_COLUMNS]{};

  /** Column id of every tag and field, in input order: the tags of point 0,
  its fields, the tags of point 1, and so on. */
  std::vector<uint8_t> m_column_ids;

  /** Distinct UTC days of the batch. */
  std::vector<int64_t> m_days;

  /** Estimated data size of the batch in bytes. */
  uint64_t m_estimated_bytes{0};
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

/** Name a column type for a diagnostic.
@return the name of a column type, for error messages */
static const char *ingest_column_type_name(column_type_t column_type) {
  switch (column_type) {
    case column_type_t::TIME:
      return ("time column");

    case column_type_t::TAG:
      return ("tag");

    case column_type_t::FIELD:
      return ("field");
  }

  return ("unknown column type");
}

/** Validate UTF-8 bytes without accepting overlong forms or surrogates. */
bool ingest_validate_utf8(const char *str, size_t len) {
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
static db_status_t ingest_validate_string(uint32_t p, const char *what,
                                          const char *s, size_t len) {
  if (s == nullptr && len > 0) {
    return (
        ingest_reject(DB_INVALID_ARGUMENT, "point %u: %s is NULL", p, what));
  }

  if (len > INGEST_MAX_STRING_BYTES) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: %s is longer than %d bytes", p, what,
                          INGEST_MAX_STRING_BYTES));
  }

  if (!ingest_validate_utf8(s, len)) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: %s is not valid UTF-8", p, what));
  }

  return (DB_SUCCESS);
}

/** Find the column for a tag or field, adding it if the name is new.
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
db_status_t ingest_planner::resolve_column(ingest_plan_t &plan, uint32_t p,
                                           const char *name, size_t name_len,
                                           column_type_t column_type,
                                           data_type_t data_type,
                                           column_slot_t &slot,
                                           uint32_t &col_id) {
  if (slot.m_name != nullptr && name != nullptr &&
      name_len == slot.m_name_len && data_type == slot.m_data_type &&
      (name == slot.m_name || memcmp(name, slot.m_name, name_len) == 0)) {
    col_id = slot.m_id;

    return (DB_SUCCESS);
  }

  std::string normalized;

  if (!schema_normalize_name(name, name_len, normalized).ok()) {
    return (ingest_reject(DB_INVALID_ARGUMENT, "point %u: invalid %s name", p,
                          ingest_column_type_name(column_type)));
  }

  if (normalized == SCHEMA_TIME_COLUMN_NAME) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: \"%s\" is reserved for the time column", p,
                          SCHEMA_TIME_COLUMN_NAME));
  }

  /* An existing column keeps its column type and data type. */
  const schema_column_t *col = schema_find_column(m_columns, normalized);

  if (col != nullptr) {
    if (col->m_column_type != column_type || col->m_data_type != data_type) {
      return (ingest_reject(
          DB_SCHEMA_CONFLICT, "point %u: \"%s\" is a %s %s, not a %s %s", p,
          normalized.c_str(), schema_data_type_name(col->m_data_type),
          ingest_column_type_name(col->m_column_type),
          schema_data_type_name(data_type),
          ingest_column_type_name(column_type)));
    }

    col_id = col->m_id;
  } else {
    /* A new column goes at the end. */
    if (m_columns.size() >= SCHEMA_MAX_COLUMNS) {
      return (ingest_reject(DB_LIMIT_EXCEEDED,
                            "point %u: \"%s\" would exceed %d columns", p,
                            normalized.c_str(), SCHEMA_MAX_COLUMNS));
    }

    schema_column_t def;
    def.m_id = static_cast<uint32_t>(m_columns.size());
    def.m_name = normalized;
    def.m_column_type = column_type;
    def.m_data_type = data_type;

    m_columns.push_back(def);
    plan.m_new_columns.push_back(def);

    col_id = def.m_id;
  }

  slot = {name, name_len, data_type, col_id};

  return (DB_SUCCESS);
}

/** Remember a column id for the fill pass, rejecting a name that appears
twice in one point.
@param[in,out]  seen  columns already used by this point
@return DB_SUCCESS or DB_INVALID_ARGUMENT */
db_status_t ingest_planner::mark_column(uint32_t p, uint32_t col_id,
                                        bool *seen) {
  if (seen[col_id]) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: \"%s\" appears more than once", p,
                          m_columns[col_id].m_name.c_str()));
  }

  seen[col_id] = true;
  m_column_ids.push_back(static_cast<uint8_t>(col_id));

  return (DB_SUCCESS);
}

/** Check the time of a point and count its UTC day.
@param[in]  time_us  UTC timestamp in microseconds since 1970-01-01
@return DB_SUCCESS, DB_INVALID_ARGUMENT or DB_LIMIT_EXCEEDED */
db_status_t ingest_planner::plan_time(uint32_t p, int64_t time_us) {
  if (const db_status_t err = ingest_validate_time(p, time_us); !err.ok()) {
    return (err);
  }

  const int64_t day = ut_utc_day(time_us);

  for (size_t i = 0; i < m_days.size(); ++i) {
    if (m_days[i] == day) {
      return (DB_SUCCESS);
    }
  }

  if (m_days.size() >= INGEST_MAX_BATCH_DAYS) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: batch spans more than %d UTC days", p,
                          INGEST_MAX_BATCH_DAYS));
  }

  m_days.push_back(day);

  return (DB_SUCCESS);
}

/** First pass over one point: check it and resolve its columns.
@param[in]  p  zero-based input point index
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED */
db_status_t ingest_planner::plan_point(ingest_plan_t &plan, uint32_t p,
                                       const ingest_point_t &pt) {
  if (const db_status_t err = plan_time(p, pt.m_time_us); !err.ok()) {
    return (err);
  }

  if (pt.m_field_count == 0) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: a point needs at least one field", p));
  }

  if (pt.m_tag_count + static_cast<uint64_t>(pt.m_field_count) >=
      SCHEMA_MAX_COLUMNS) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: more than %d tags and fields", p,
                          SCHEMA_MAX_COLUMNS - 1));
  }

  if ((pt.m_tag_count > 0 && pt.m_tags == nullptr) || pt.m_fields == nullptr) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: tags or fields are NULL", p));
  }

  /* Time column. */
  m_estimated_bytes += INGEST_ESTIMATED_VALUE_BYTES;

  bool seen[SCHEMA_MAX_COLUMNS] = {false};

  for (uint32_t t = 0; t < pt.m_tag_count; ++t) {
    const ingest_tag_t &tag = pt.m_tags[t];

    if (const db_status_t err = ingest_validate_string(
            p, "tag value", tag.m_value, tag.m_value_length);
        !err.ok()) {
      return (err);
    }

    uint32_t col_id = 0;
    if (const db_status_t err = resolve_column(
            plan, p, tag.m_name, tag.m_name_length, column_type_t::TAG,
            data_type_t::VARCHAR, m_tag_slots[t], col_id);
        !err.ok()) {
      return (err);
    }

    if (const db_status_t err = mark_column(p, col_id, seen); !err.ok()) {
      return (err);
    }

    m_estimated_bytes += INGEST_ESTIMATED_VALUE_BYTES + tag.m_value_length;
  }

  for (uint32_t f = 0; f < pt.m_field_count; ++f) {
    const ingest_field_t &field = pt.m_fields[f];
    const ingest_value_t &v = field.m_value;

    if (!schema_validate_field_type(v.m_data_type)) {
      return (ingest_reject(DB_INVALID_ARGUMENT, "point %u: invalid field type",
                            p));
    }

    if (v.m_data_type == data_type_t::DOUBLE && !std::isfinite(v.m_double)) {
      return (ingest_reject(DB_INVALID_ARGUMENT,
                            "point %u: DOUBLE field is NaN or infinite", p));
    }

    if (v.m_data_type == data_type_t::VARCHAR) {
      if (const db_status_t err = ingest_validate_string(
              p, "field value", v.m_string.data(), v.m_string.size());
          !err.ok()) {
        return (err);
      }

      m_estimated_bytes += v.m_string.size();
    }

    uint32_t col_id = 0;
    if (const db_status_t err = resolve_column(
            plan, p, field.m_name, field.m_name_length, column_type_t::FIELD,
            v.m_data_type, m_field_slots[f], col_id);
        !err.ok()) {
      return (err);
    }

    if (const db_status_t err = mark_column(p, col_id, seen); !err.ok()) {
      return (err);
    }

    m_estimated_bytes += INGEST_ESTIMATED_VALUE_BYTES;
  }

  if (m_estimated_bytes > INGEST_MAX_BATCH_BYTES) {
    return (ingest_reject(DB_LIMIT_EXCEEDED,
                          "point %u: batch data exceeds %d bytes", p,
                          INGEST_MAX_BATCH_BYTES));
  }

  return (DB_SUCCESS);
}

/** Size every column of the batch for n rows, all NULL.
@param[out]  batch  column buffers initialized with every row NULL */
void ingest_planner::init_batch(uint32_t n, batch_t &batch) const {
  batch.m_row_count = n;
  batch.m_columns.resize(m_columns.size());

  for (size_t i = 0; i < m_columns.size(); ++i) {
    batch.m_columns[i].reset(static_cast<uint32_t>(i), m_columns[i].m_data_type,
                             n);
  }

  return;
}

/** Second pass over one point: store its values in row r. */
void ingest_planner::fill_row(const ingest_point_t &pt, uint32_t r,
                              size_t &next, batch_t &batch) const {
  batch_column_t &time_col = batch.m_columns[0];
  time_col.m_integers[r] = pt.m_time_us;
  time_col.m_nulls[r] = 0;

  for (uint32_t t = 0; t < pt.m_tag_count; ++t) {
    batch_column_t &col = batch.m_columns[m_column_ids[next++]];

    col.m_string_data.append(pt.m_tags[t].m_value, pt.m_tags[t].m_value_length);
    col.m_nulls[r] = 0;
  }

  for (uint32_t f = 0; f < pt.m_field_count; ++f) {
    const ingest_value_t &v = pt.m_fields[f].m_value;
    batch_column_t &col = batch.m_columns[m_column_ids[next++]];

    switch (v.m_data_type) {
      case data_type_t::BOOLEAN:
        col.m_booleans[r] = v.m_boolean ? 1 : 0;
        break;

      case data_type_t::BIGINT:
        col.m_integers[r] = v.m_integer;
        break;

      case data_type_t::DOUBLE:
        col.m_doubles[r] = v.m_double;
        break;

      case data_type_t::VARCHAR:
        col.m_string_data.append(v.m_string);
        break;

      case data_type_t::TIMESTAMP:
        ut_error;
    }

    col.m_nulls[r] = 0;
  }

  /* Close row r of every string column, whether or not it had a value. */
  for (size_t i = 0; i < batch.m_columns.size(); ++i) {
    batch_column_t &col = batch.m_columns[i];

    if (col.m_data_type == data_type_t::VARCHAR) {
      col.m_string_offsets[r + 1] =
          static_cast<uint32_t>(col.m_string_data.size());
    }
  }

  return;
}

/** Shared array validation for admission and standalone planning. */
db_status_t ingest_validate_batch(const ingest_point_t *points,
                                  uint32_t n_points) {
  if (points == nullptr || n_points == 0) {
    return (ingest_reject(DB_INVALID_ARGUMENT, "empty batch"));
  }

  if (n_points > INGEST_MAX_POINTS) {
    return (ingest_reject(DB_LIMIT_EXCEEDED, "batch has more than %d points",
                          INGEST_MAX_POINTS));
  }

  return (DB_SUCCESS);
}

/** One supported time range for admission and planning. */
db_status_t ingest_validate_time(uint32_t p, int64_t time_us) {
  if (time_us < INGEST_MIN_TIME_US || time_us >= INGEST_MAX_TIME_US) {
    return (ingest_reject(DB_INVALID_ARGUMENT,
                          "point %u: time is outside [1900, 2200)", p));
  }

  return (DB_SUCCESS);
}

/** Check and plan a batch; ingest_plan() resets the plan on failure.
@param[in]  schema  schema the write follows; may be NULL when creating a
                    table
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_SCHEMA_CONFLICT or
  DB_LIMIT_EXCEEDED; the caller resets partial output on failure */
db_status_t ingest_planner::plan(const schema_t *schema,
                                 const std::string &table_name,
                                 const ingest_point_t *points,
                                 uint32_t n_points, ingest_plan_t &plan) {
  ut_a(schema == nullptr || schema->m_name == table_name);

  plan.m_table_name = table_name;

  if (const db_status_t err = ingest_validate_batch(points, n_points);
      !err.ok()) {
    return (err);
  }

  /* Start from the current schema, or a new table with only "time". */

  if (schema != nullptr) {
    m_columns = schema->m_columns;
    plan.m_base_column_count = static_cast<uint32_t>(schema->m_columns.size());
  } else {
    schema_column_t time_col;
    time_col.m_id = 0;
    time_col.m_name = SCHEMA_TIME_COLUMN_NAME;
    time_col.m_column_type = column_type_t::TIME;
    time_col.m_data_type = data_type_t::TIMESTAMP;

    m_columns.push_back(time_col);
    plan.m_new_columns.push_back(time_col);
    plan.m_creates_table = true;
  }

  /* First pass: check every point and resolve its columns. */
  for (uint32_t p = 0; p < n_points; ++p) {
    if (const db_status_t err = plan_point(plan, p, points[p]); !err.ok()) {
      return (err);
    }
  }

  /* The encoded batch also stores a NULL bitmap per column, including
  columns this batch does not use. */
  m_estimated_bytes +=
      m_columns.size() * static_cast<uint64_t>((n_points + 7) / 8);

  if (m_estimated_bytes > INGEST_MAX_BATCH_BYTES) {
    return (ingest_reject(DB_LIMIT_EXCEEDED, "batch data exceeds %d bytes",
                          INGEST_MAX_BATCH_BYTES));
  }

  /* Second pass: fill the batch. */
  batch_t &batch = plan.m_batch;
  batch.m_table_name = plan.m_table_name;
  init_batch(n_points, batch);

  size_t next = 0;

  for (uint32_t r = 0; r < n_points; ++r) {
    fill_row(points[r], r, next, batch);
  }

  ut_a(next == m_column_ids.size());
  ut_a(batch.m_row_count == n_points);
  ut_a(batch.m_columns.size() ==
       (schema != nullptr ? schema->m_columns.size() : 0) +
           plan.m_new_columns.size());

  return (DB_SUCCESS);
}

/** Check a batch of points and build its write plan. */
db_status_t ingest_plan(const schema_t *schema, const std::string &table_name,
                        const ingest_point_t *points, uint32_t n_points,
                        ingest_plan_t &plan) {
  plan = ingest_plan_t();

  ingest_planner planner;
  const db_status_t err =
      planner.plan(schema, table_name, points, n_points, plan);

  /* A failed plan has no partially built output; its status owns the reason. */
  if (!err.ok()) {
    plan = ingest_plan_t();
  }

  return (err);
}

/* ========================== WAL payload =========================== */

/* WAL payload of a write: the schema change and the column batch.
 *
 * All integers are little-endian.
 *
 *   u8   payload format (1)
 *   u16  table name length, then the normalized table name
 *   u8   1 if the write creates the table, else 0
 *   u16  number of new columns, then for each:
 *          u32 id, u8 column type, u8 data type, u16 name length, name
 *   u32  number of rows
 *   u16  number of columns, then for each column, in id order:
 *          u8  data type
 *          ceil(rows / 8) bytes: bit r is 1 if row r is not NULL
 *          the non-NULL values only: BOOLEAN 1 byte, BIGINT and TIMESTAMP
 *          8 bytes, DOUBLE 8 bytes (IEEE 754 bits), VARCHAR u32 length and
 *          the bytes
 *
 * Decoding checks the structure only. Whether the schema change fits the
 * current schema is checked when the plan is applied.
 *
 */

/** Current payload format. */
constexpr uint8_t INGEST_CODEC_VERSION = 1;

/* Encoding. */

/** Append one unsigned byte to a WAL payload. */
static void ingest_put_u8(std::string &out, uint8_t v) {
  out.push_back(static_cast<char>(v));

  return;
}

/** Append a 16-bit value, little-endian. */
static void ingest_put_u16(std::string &out, uint16_t v) {
  unsigned char b[2];
  ut_write_u16(b, v);
  out.append(reinterpret_cast<const char *>(b), sizeof(b));

  return;
}

/** Append a 32-bit value, little-endian. */
static void ingest_put_u32(std::string &out, uint32_t v) {
  unsigned char b[4];
  ut_write_u32(b, v);
  out.append(reinterpret_cast<const char *>(b), sizeof(b));

  return;
}

/** Append a 64-bit value, little-endian. */
static void ingest_put_u64(std::string &out, uint64_t v) {
  unsigned char b[8];
  ut_write_u64(b, v);
  out.append(reinterpret_cast<const char *>(b), sizeof(b));

  return;
}

/** Append a name: its u16 length, then its bytes.
@param[in]  name  normalized name to encode */
static void ingest_put_name(std::string &out, const std::string &name) {
  ingest_put_u16(out, static_cast<uint16_t>(name.size()));
  out.append(name);

  return;
}

/** Append one column: type, NULL bitmap, non-NULL values.
@param[in]  col  column values and NULL flags */
static void ingest_put_column(std::string &out, const batch_column_t &col,
                              uint32_t n_rows) {
  ut_ad(col.validate(n_rows));

  ingest_put_u8(out, static_cast<uint8_t>(col.m_data_type));

  const size_t bitmap = out.size();
  out.append((n_rows + 7) / 8, '\0');

  for (uint32_t r = 0; r < n_rows; ++r) {
    if (col.m_nulls[r]) {
      continue;
    }

    out[bitmap + r / 8] |= static_cast<char>(1 << (r % 8));

    switch (col.m_data_type) {
      case data_type_t::BOOLEAN:
        ingest_put_u8(out, col.m_booleans[r]);
        break;

      case data_type_t::BIGINT:
      case data_type_t::TIMESTAMP:
        ingest_put_u64(out, static_cast<uint64_t>(col.m_integers[r]));
        break;

      case data_type_t::DOUBLE: {
        uint64_t bits;
        memcpy(&bits, &col.m_doubles[r], sizeof(bits));
        ingest_put_u64(out, bits);
        break;
      }

      case data_type_t::VARCHAR: {
        const uint32_t start = col.m_string_offsets[r];
        const uint32_t len = col.m_string_offsets[r + 1] - start;

        ingest_put_u32(out, len);
        out.append(col.m_string_data, start, len);
        break;
      }
    }
  }

  return;
}

/** Encode schema additions and batch values into a WAL payload. */
void ingest_encode(const ingest_plan_t &plan, std::string &out) {
  out.clear();

  const batch_t &batch = plan.m_batch;

  ingest_put_u8(out, INGEST_CODEC_VERSION);
  ingest_put_name(out, plan.m_table_name);
  ingest_put_u8(out, plan.m_creates_table ? 1 : 0);

  ingest_put_u16(out, static_cast<uint16_t>(plan.m_new_columns.size()));

  for (size_t i = 0; i < plan.m_new_columns.size(); ++i) {
    const schema_column_t &def = plan.m_new_columns[i];

    ingest_put_u32(out, def.m_id);
    ingest_put_u8(out, static_cast<uint8_t>(def.m_column_type));
    ingest_put_u8(out, static_cast<uint8_t>(def.m_data_type));
    ingest_put_name(out, def.m_name);
  }

  ingest_put_u32(out, batch.m_row_count);
  ingest_put_u16(out, static_cast<uint16_t>(batch.m_columns.size()));

  for (size_t i = 0; i < batch.m_columns.size(); ++i) {
    ingest_put_column(out, batch.m_columns[i], batch.m_row_count);
  }

  return;
}

/* Decoding. */

/** Read a payload in order. A read past the end invalidates the reader,
and every later read fails. */
class ingest_reader final : private ut_noncopyable {
 public:
  /** Decode a complete payload into the caller's temporary plan. */
  db_status_t decode(ingest_plan_t &plan);

  /** Read the given payload; its bytes must remain valid during decoding. */
  ingest_reader(const unsigned char *buf, size_t len)
      : m_buffer(buf), m_payload_bytes(len) {}

  /** Read bytes and advance the payload position. */
  const unsigned char *read_bytes(size_t n);

  /** Read u8 and advance the payload position. */
  uint8_t read_u8();

  /** Read u16 and advance the payload position. */
  uint16_t read_u16();

  /** Read u32 and advance the payload position. */
  uint32_t read_u32();

  /** Read u64 and advance the payload position. */
  uint64_t read_u64();

  /** Read name and advance the payload position. */
  bool read_name(std::string &out);

  /** Read a column and advance the payload position. */
  bool read_column(uint32_t col_id, uint32_t n_rows, batch_column_t &col);

  /** Check whether every byte was consumed without reading past the end. */
  bool complete() const { return (m_valid && m_offset == m_payload_bytes); }

  /** Check whether no read has crossed the payload boundary. */
  bool valid() const { return (m_valid); }

 private:
  /** The payload. */
  const unsigned char *m_buffer{nullptr};

  /** Length of buf in bytes. */
  size_t m_payload_bytes{0};

  /** Read position. */
  size_t m_offset{0};

  /** No read has run past the end. */
  bool m_valid{true};
};

/** Consume a range of bytes from a payload reader.
@return n bytes at the read position, or NULL past the end */
const unsigned char *ingest_reader::read_bytes(size_t n) {
  if (!m_valid || n > m_payload_bytes - m_offset) {
    m_valid = false;

    return (nullptr);
  }

  const unsigned char *p = m_buffer + m_offset;
  m_offset += n;

  return (p);
}

/** Read one unsigned byte from a payload.
@return the next byte, or 0 past the end */
uint8_t ingest_reader::read_u8() {
  const unsigned char *p = read_bytes(1);

  return (p != nullptr ? p[0] : 0);
}

/** Read a little-endian 16-bit value from a payload.
@return the next little-endian 16-bit value, or 0 past the end */
uint16_t ingest_reader::read_u16() {
  const unsigned char *p = read_bytes(2);

  return (p != nullptr ? ut_read_u16(p) : 0);
}

/** Read a little-endian 32-bit value from a payload.
@return the next little-endian 32-bit value, or 0 past the end */
uint32_t ingest_reader::read_u32() {
  const unsigned char *p = read_bytes(4);

  return (p != nullptr ? ut_read_u32(p) : 0);
}

/** Read a little-endian 64-bit value from a payload.
@return the next little-endian 64-bit value, or 0 past the end */
uint64_t ingest_reader::read_u64() {
  const unsigned char *p = read_bytes(8);

  return (p != nullptr ? ut_read_u64(p) : 0);
}

/** Read a name and check that it is in normalized form.
@param[out]     out  decoded normalized name, if accepted
@return true if a normalized name was read */
bool ingest_reader::read_name(std::string &out) {
  const uint16_t n = read_u16();
  const unsigned char *p = read_bytes(n);

  if (p == nullptr) {
    return (false);
  }

  std::string normalized;
  if (!schema_normalize_name(reinterpret_cast<const char *>(p), n, normalized)
           .ok()) {
    return (false);
  }

  out.assign(reinterpret_cast<const char *>(p), n);

  return (normalized == out);
}

/** Check whether a serialized type number names a supported data type.
@return true if type is the number of a data type */
static bool ingest_validate_data_type(uint8_t type) {
  return (type >= static_cast<uint8_t>(data_type_t::BOOLEAN) &&
          type <= static_cast<uint8_t>(data_type_t::VARCHAR));
}

/** Read one column.
@param[out]     col  decoded column values, offsets and NULL flags
@return true if the column was well formed */
bool ingest_reader::read_column(uint32_t col_id, uint32_t n_rows,
                                batch_column_t &col) {
  const uint8_t type = read_u8();
  if (!ingest_validate_data_type(type)) {
    return (false);
  }

  col.reset(col_id, static_cast<data_type_t>(type), n_rows);

  const size_t bitmap_len = (n_rows + 7) / 8;
  const unsigned char *bitmap = read_bytes(bitmap_len);
  if (bitmap == nullptr) {
    return (false);
  }

  /* Bits past the last row must be 0, so that every valid payload has
  exactly one encoding. */
  if (n_rows % 8 != 0 && (bitmap[bitmap_len - 1] >> (n_rows % 8)) != 0) {
    return (false);
  }

  for (uint32_t row = 0; row < n_rows; ++row) {
    if (!m_valid) {
      return (false);
    }

    const bool valid = (bitmap[row / 8] >> (row % 8)) & 1;
    col.m_nulls[row] = valid ? 0 : 1;

    if (valid) {
      switch (col.m_data_type) {
        case data_type_t::BOOLEAN: {
          const uint8_t v = read_u8();
          if (v > 1) {
            return (false);
          }

          col.m_booleans[row] = v;
          break;
        }

        case data_type_t::BIGINT:
        case data_type_t::TIMESTAMP:
          col.m_integers[row] = static_cast<int64_t>(read_u64());
          break;

        case data_type_t::DOUBLE: {
          const uint64_t bits = read_u64();
          memcpy(&col.m_doubles[row], &bits, sizeof(bits));
          break;
        }

        case data_type_t::VARCHAR: {
          const uint32_t len = read_u32();
          const unsigned char *p = read_bytes(len);
          if (p == nullptr) {
            return (false);
          }

          col.m_string_data.append(reinterpret_cast<const char *>(p), len);
          break;
        }
      }
    }

    if (col.m_data_type == data_type_t::VARCHAR) {
      col.m_string_offsets[row + 1] =
          static_cast<uint32_t>(col.m_string_data.size());
    }
  }

  return (m_valid);
}

/** Check that the header fields and new columns describe a possible schema
change.
@return true if they do */
static bool ingest_validate_plan(const ingest_plan_t &plan) {
  const size_t n_new = plan.m_new_columns.size();
  if (n_new > plan.m_batch.m_columns.size() ||
      plan.m_base_column_count != plan.m_batch.m_columns.size() - n_new) {
    return (false);
  }

  if (plan.m_creates_table) {
    if (plan.m_base_column_count != 0 || n_new == 0) {
      return (false);
    }

    const schema_column_t &time_col = plan.m_new_columns[0];
    if (time_col.m_id != 0 || time_col.m_column_type != column_type_t::TIME ||
        time_col.m_data_type != data_type_t::TIMESTAMP ||
        time_col.m_name != SCHEMA_TIME_COLUMN_NAME) {
      return (false);
    }
  } else if (plan.m_base_column_count == 0) {
    return (false);
  }

  /* New columns are consecutive; only a new table has a time column. */
  for (size_t i = 0; i < n_new; ++i) {
    const schema_column_t &def = plan.m_new_columns[i];

    if (def.m_id != plan.m_base_column_count + i) {
      return (false);
    }

    if (def.m_column_type == column_type_t::TIME) {
      if (!(plan.m_creates_table && i == 0)) {
        return (false);
      }
    } else if (def.m_column_type == column_type_t::TAG) {
      if (def.m_data_type != data_type_t::VARCHAR) {
        return (false);
      }
    } else if (!schema_validate_field_type(def.m_data_type)) {
      return (false);
    }
  }

  /* Column 0 holds non-NULL timestamps within the supported absolute range. */
  const batch_t &batch = plan.m_batch;
  const batch_column_t &time_values = batch.m_columns[0];

  if (time_values.m_data_type != data_type_t::TIMESTAMP) {
    return (false);
  }

  for (uint32_t row = 0; row < batch.m_row_count; ++row) {
    if (time_values.m_nulls[row] ||
        time_values.m_integers[row] < INGEST_MIN_TIME_US ||
        time_values.m_integers[row] >= INGEST_MAX_TIME_US) {
      return (false);
    }
  }

  return (true);
}

/** Decode a payload; ingest_decode() resets the plan on failure.
@param[out]  plan  partially or completely decoded plan; caller discards it on
                   failure
@return DB_SUCCESS or DB_CORRUPTION for an invalid payload */
db_status_t ingest_reader::decode(ingest_plan_t &plan) {
  /* Header. */
  if (read_u8() != INGEST_CODEC_VERSION) {
    return (DB_CORRUPTION);
  }

  if (!read_name(plan.m_table_name)) {
    return (DB_CORRUPTION);
  }

  const uint8_t creates = read_u8();
  if (creates > 1) {
    return (DB_CORRUPTION);
  }

  plan.m_creates_table = creates == 1;

  /* New columns. */
  const uint16_t n_new = read_u16();
  if (n_new > SCHEMA_MAX_COLUMNS) {
    return (DB_CORRUPTION);
  }

  for (uint16_t i = 0; i < n_new; ++i) {
    schema_column_t def;
    def.m_id = read_u32();

    const uint8_t column_type = read_u8();
    const uint8_t type = read_u8();

    if (column_type > static_cast<uint8_t>(column_type_t::FIELD) ||
        !ingest_validate_data_type(type)) {
      return (DB_CORRUPTION);
    }

    def.m_column_type = static_cast<column_type_t>(column_type);
    def.m_data_type = static_cast<data_type_t>(type);

    if (!read_name(def.m_name) || def.m_id >= SCHEMA_MAX_COLUMNS) {
      return (DB_CORRUPTION);
    }

    plan.m_new_columns.push_back(def);
  }

  /* Columns of the batch. */
  batch_t &batch = plan.m_batch;
  batch.m_table_name = plan.m_table_name;
  batch.m_row_count = read_u32();

  const uint16_t n_cols = read_u16();

  if (!valid() || batch.m_row_count == 0 ||
      batch.m_row_count > INGEST_MAX_POINTS || n_cols == 0 ||
      n_cols > SCHEMA_MAX_COLUMNS) {
    return (DB_CORRUPTION);
  }

  batch.m_columns.resize(n_cols);

  for (uint16_t i = 0; i < n_cols; ++i) {
    if (!read_column(i, batch.m_row_count, batch.m_columns[i])) {
      return (DB_CORRUPTION);
    }
  }

  if (n_new > n_cols) {
    return (DB_CORRUPTION);
  }

  plan.m_base_column_count = n_cols - n_new;

  /* Nothing may follow, and the parts must fit together. */
  if (!complete()) {
    return (DB_CORRUPTION);
  }

  if (!ingest_validate_plan(plan)) {
    return (DB_CORRUPTION);
  }

  return (DB_SUCCESS);
}

/** Decode a WAL payload. */
db_status_t ingest_decode(const unsigned char *buf, size_t len,
                          ingest_plan_t &plan) {
  plan = ingest_plan_t();

  const db_status_t err = ingest_reader(buf, len).decode(plan);
  if (!err.ok()) {
    plan = ingest_plan_t();
  }

  return (err);
}
