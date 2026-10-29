/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest0codec.cc
 *    Encoding and decoding of the WAL payload of a write.
 *
 *-------------------------------------------------------------------------
 */

#include "ingest0codec.h"

#include <cstring>
#include <new>

#include "ut0byte.h"

/* Encoding. */

/** Append one unsigned byte to a WAL payload.
@param[in,out]  out  payload buffer to append to
@param[in]      v    unsigned value to encode */
static void ingest_put_u8(std::string &out, uint8_t v) {
  out.push_back((char)v);
}

/** Append a 16-bit value, little-endian.
@param[in,out]  out  payload buffer to append to
@param[in]      v    unsigned value to encode */
static void ingest_put_u16(std::string &out, uint16_t v) {
  unsigned char b[2];
  ut_write_u16(b, v);
  out.append((const char *)b, sizeof(b));
}

/** Append a 32-bit value, little-endian.
@param[in,out]  out  payload buffer to append to
@param[in]      v    unsigned value to encode */
static void ingest_put_u32(std::string &out, uint32_t v) {
  unsigned char b[4];
  ut_write_u32(b, v);
  out.append((const char *)b, sizeof(b));
}

/** Append a 64-bit value, little-endian.
@param[in,out]  out  payload buffer to append to
@param[in]      v    unsigned value to encode */
static void ingest_put_u64(std::string &out, uint64_t v) {
  unsigned char b[8];
  ut_write_u64(b, v);
  out.append((const char *)b, sizeof(b));
}

/** Append a name: its u16 length, then its bytes.
@param[in,out]  out   payload buffer to append to
@param[in]      name  canonical name to encode */
static void ingest_put_name(std::string &out, const std::string &name) {
  ingest_put_u16(out, (uint16_t)name.size());
  out.append(name);
}

/** Append one column: type, NULL bitmap, non-NULL values.
@param[in,out]  out     payload buffer to append to
@param[in]      col     column values and NULL flags
@param[in]      n_rows  number of rows */
static void ingest_put_col(std::string &out, const batch_col_t &col,
                           uint32_t n_rows) {
  ingest_put_u8(out, (uint8_t)col.type);

  const size_t bitmap = out.size();
  out.append((n_rows + 7) / 8, '\0');

  for (uint32_t r = 0; r < n_rows; ++r) {
    if (col.nulls[r]) {
      continue;
    }

    out[bitmap + r / 8] |= (char)(1 << (r % 8));

    switch (col.type) {
      case COL_BOOLEAN:
        ingest_put_u8(out, col.bools[r]);
        break;

      case COL_BIGINT:
      case COL_TIMESTAMP:
        ingest_put_u64(out, (uint64_t)col.ints[r]);
        break;

      case COL_DOUBLE: {
        uint64_t bits;
        memcpy(&bits, &col.doubles[r], sizeof(bits));
        ingest_put_u64(out, bits);
        break;
      }

      case COL_VARCHAR: {
        const uint32_t start = col.offsets[r];
        const uint32_t len = col.offsets[r + 1] - start;

        ingest_put_u32(out, len);
        out.append(col.chars, start, len);
        break;
      }
    }
  }
}

/** Encode a write plan as a WAL payload.
@param[in]   plan  plan from ingest_plan()
@param[out]  out   payload bytes */
void ingest_encode(const ingest_plan_t &plan, std::string &out) {
  out.clear();

  const batch_t &batch = plan.batch;

  ingest_put_u8(out, INGEST_CODEC_VERSION);
  ingest_put_name(out, plan.table);
  ingest_put_u8(out, plan.creates_table ? 1 : 0);
  ingest_put_u64(out, plan.base_version);
  ingest_put_u64(out, batch.schema_version);

  ingest_put_u16(out, (uint16_t)plan.new_cols.size());

  for (size_t i = 0; i < plan.new_cols.size(); ++i) {
    const col_def_t &def = plan.new_cols[i];

    ingest_put_u32(out, def.id);
    ingest_put_u8(out, (uint8_t)def.role);
    ingest_put_u8(out, (uint8_t)def.type);
    ingest_put_name(out, def.name);
  }

  ingest_put_u32(out, batch.n_rows);
  ingest_put_u16(out, (uint16_t)batch.cols.size());

  for (size_t i = 0; i < batch.cols.size(); ++i) {
    ingest_put_col(out, batch.cols[i], batch.n_rows);
  }
}

/* Decoding. */

/** Reads a payload front to back. Once a read runs past the end, ok
becomes false and every later read returns 0. */
struct ingest_reader_t {
  /** The payload. */
  const unsigned char *buf{nullptr};

  /** Length of buf in bytes. */
  size_t len{0};

  /** Read position. */
  size_t at{0};

  /** No read has run past the end. */
  bool ok{true};
};

/** Consume a range of bytes from a payload reader.
@param[in,out]  r  payload reader; advances its offset or marks it invalid
@param[in]      n  number of bytes to consume
@return n bytes at the read position, or NULL past the end */
static const unsigned char *ingest_take(ingest_reader_t &r, size_t n) {
  if (!r.ok || n > r.len - r.at) {
    r.ok = false;

    return (nullptr);
  }

  const unsigned char *p = r.buf + r.at;
  r.at += n;

  return (p);
}

/** Read one unsigned byte from a payload.
@param[in,out]  r  payload reader; advances its offset or marks it invalid
@return the next byte, or 0 past the end */
static uint8_t ingest_get_u8(ingest_reader_t &r) {
  const unsigned char *p = ingest_take(r, 1);

  return (p != nullptr ? p[0] : 0);
}

/** Read a little-endian 16-bit value from a payload.
@param[in,out]  r  payload reader; advances its offset or marks it invalid
@return the next little-endian 16-bit value, or 0 past the end */
static uint16_t ingest_get_u16(ingest_reader_t &r) {
  const unsigned char *p = ingest_take(r, 2);

  return (p != nullptr ? ut_read_u16(p) : 0);
}

/** Read a little-endian 32-bit value from a payload.
@param[in,out]  r  payload reader; advances its offset or marks it invalid
@return the next little-endian 32-bit value, or 0 past the end */
static uint32_t ingest_get_u32(ingest_reader_t &r) {
  const unsigned char *p = ingest_take(r, 4);

  return (p != nullptr ? ut_read_u32(p) : 0);
}

/** Read a little-endian 64-bit value from a payload.
@param[in,out]  r  payload reader; advances its offset or marks it invalid
@return the next little-endian 64-bit value, or 0 past the end */
static uint64_t ingest_get_u64(ingest_reader_t &r) {
  const unsigned char *p = ingest_take(r, 8);

  return (p != nullptr ? ut_read_u64(p) : 0);
}

/** Read a name and check that it is in canonical form.
@param[in,out]  r    payload reader; advances its offset or marks it invalid
@param[out]     out  decoded canonical name, if accepted
@return true if a canonical name was read */
static bool ingest_get_name(ingest_reader_t &r, std::string &out) {
  const uint16_t n = ingest_get_u16(r);
  const unsigned char *p = ingest_take(r, n);

  if (p == nullptr) {
    return (false);
  }

  std::string canonical;
  if (schema_canonical_name((const char *)p, n, canonical) != DB_SUCCESS) {
    return (false);
  }

  out.assign((const char *)p, n);

  return (canonical == out);
}

/** Check whether a serialized type number names a supported column type.
@param[in]  type  column value type
@return true if type is the number of a column type */
static bool ingest_type_is_valid(uint8_t type) {
  return (type >= COL_BOOLEAN && type <= COL_VARCHAR);
}

/** Read one column.
@param[in,out]  r       payload reader; advances its offset or marks it
                        invalid
@param[in]      col_id  column id in the schema
@param[in]      n_rows  number of rows
@param[out]     col     decoded column values, offsets and NULL flags
@return true if the column was well formed */
static bool ingest_get_col(ingest_reader_t &r, uint32_t col_id, uint32_t n_rows,
                           batch_col_t &col) {
  const uint8_t type = ingest_get_u8(r);
  if (!ingest_type_is_valid(type)) {
    return (false);
  }

  batch_col_init(col, col_id, (col_type_t)type, n_rows);

  const size_t bitmap_len = (n_rows + 7) / 8;
  const unsigned char *bitmap = ingest_take(r, bitmap_len);
  if (bitmap == nullptr) {
    return (false);
  }

  /* Bits past the last row must be 0, so that every valid payload has
  exactly one encoding. */
  if (n_rows % 8 != 0 && (bitmap[bitmap_len - 1] >> (n_rows % 8)) != 0) {
    return (false);
  }

  for (uint32_t row = 0; row < n_rows && r.ok; ++row) {
    const bool valid = (bitmap[row / 8] >> (row % 8)) & 1;
    col.nulls[row] = valid ? 0 : 1;

    if (valid) {
      switch (col.type) {
        case COL_BOOLEAN: {
          const uint8_t v = ingest_get_u8(r);
          if (v > 1) {
            return (false);
          }
          col.bools[row] = v;
          break;
        }

        case COL_BIGINT:
        case COL_TIMESTAMP:
          col.ints[row] = (int64_t)ingest_get_u64(r);
          break;

        case COL_DOUBLE: {
          const uint64_t bits = ingest_get_u64(r);
          memcpy(&col.doubles[row], &bits, sizeof(bits));
          break;
        }

        case COL_VARCHAR: {
          const uint32_t len = ingest_get_u32(r);
          const unsigned char *p = ingest_take(r, len);
          if (p == nullptr) {
            return (false);
          }
          col.chars.append((const char *)p, len);
          break;
        }
      }
    }

    if (col.type == COL_VARCHAR) {
      col.offsets[row + 1] = (uint32_t)col.chars.size();
    }
  }

  return (r.ok);
}

/** Check that the header fields and new columns describe a possible schema
change.
@param[in]  plan  decoded plan to check for a possible schema change
@return true if they do */
static bool ingest_plan_is_consistent(const ingest_plan_t &plan) {
  const size_t n_new = plan.new_cols.size();
  const uint64_t expected_version = plan.base_version + (n_new > 0 ? 1 : 0);

  if (plan.batch.schema_version != expected_version) {
    return (false);
  }

  if (plan.creates_table) {
    if (plan.base_version != 0 || n_new == 0) {
      return (false);
    }

    const col_def_t &time_col = plan.new_cols[0];
    if (time_col.id != 0 || time_col.role != COL_ROLE_TIME ||
        time_col.type != COL_TIMESTAMP || time_col.name != SCHEMA_TIME_COLUMN) {
      return (false);
    }
  } else if (plan.base_version == 0) {
    return (false);
  }

  /* New columns are consecutive; only a new table has a time column. */
  for (size_t i = 0; i < n_new; ++i) {
    const col_def_t &def = plan.new_cols[i];

    if (i > 0 && def.id != plan.new_cols[i - 1].id + 1) {
      return (false);
    }

    if (def.role == COL_ROLE_TIME) {
      if (!(plan.creates_table && i == 0)) {
        return (false);
      }
    } else if (def.role == COL_ROLE_TAG) {
      if (def.type != COL_VARCHAR) {
        return (false);
      }
    } else if (!schema_field_type_is_valid(def.type)) {
      return (false);
    }
  }

  /* Column 0 of every batch is the non-NULL time column. */
  const batch_t &batch = plan.batch;
  const batch_col_t &time_values = batch.cols[0];

  if (time_values.type != COL_TIMESTAMP) {
    return (false);
  }

  for (uint32_t row = 0; row < batch.n_rows; ++row) {
    if (time_values.nulls[row]) {
      return (false);
    }
  }

  return (true);
}

/** Decode a payload; ingest_decode() resets the plan on failure.
@param[in]   buf   input bytes
@param[in]   len   input length in bytes
@param[out]  plan  partially or completely decoded plan; caller discards it on
                   failure
@return DB_SUCCESS or DB_CORRUPTION for an invalid payload */
static dberr_t ingest_decode_low(const unsigned char *buf, size_t len,
                                 ingest_plan_t &plan) {
  ingest_reader_t r;
  r.buf = buf;
  r.len = len;

  /* Header. */
  if (ingest_get_u8(r) != INGEST_CODEC_VERSION) {
    return (DB_CORRUPTION);
  }

  if (!ingest_get_name(r, plan.table)) {
    return (DB_CORRUPTION);
  }

  const uint8_t creates = ingest_get_u8(r);
  if (creates > 1) {
    return (DB_CORRUPTION);
  }
  plan.creates_table = creates == 1;

  plan.base_version = ingest_get_u64(r);
  plan.batch.schema_version = ingest_get_u64(r);

  /* New columns. */
  const uint16_t n_new = ingest_get_u16(r);
  if (n_new > SCHEMA_MAX_COLUMNS) {
    return (DB_CORRUPTION);
  }

  for (uint16_t i = 0; i < n_new; ++i) {
    col_def_t def;
    def.id = ingest_get_u32(r);

    const uint8_t role = ingest_get_u8(r);
    const uint8_t type = ingest_get_u8(r);

    if (role > COL_ROLE_FIELD || !ingest_type_is_valid(type)) {
      return (DB_CORRUPTION);
    }

    def.role = (col_role_t)role;
    def.type = (col_type_t)type;

    if (!ingest_get_name(r, def.name) || def.id >= SCHEMA_MAX_COLUMNS) {
      return (DB_CORRUPTION);
    }

    plan.new_cols.push_back(def);
  }

  /* Columns of the batch. */
  batch_t &batch = plan.batch;
  batch.table = plan.table;
  batch.n_rows = ingest_get_u32(r);

  const uint16_t n_cols = ingest_get_u16(r);

  if (!r.ok || batch.n_rows == 0 || batch.n_rows > INGEST_MAX_POINTS ||
      n_cols == 0 || n_cols > SCHEMA_MAX_COLUMNS) {
    return (DB_CORRUPTION);
  }

  batch.cols.resize(n_cols);

  for (uint16_t i = 0; i < n_cols; ++i) {
    if (!ingest_get_col(r, i, batch.n_rows, batch.cols[i])) {
      return (DB_CORRUPTION);
    }
  }

  /* Nothing may follow, and the parts must fit together. */
  if (!r.ok || r.at != r.len) {
    return (DB_CORRUPTION);
  }

  if (!ingest_plan_is_consistent(plan)) {
    return (DB_CORRUPTION);
  }

  return (DB_SUCCESS);
}

/** Decode a WAL payload.
@param[in]   buf   payload
@param[in]   len   payload length
@param[out]  plan  decoded plan
@return DB_SUCCESS, or DB_CORRUPTION if the payload is malformed */
dberr_t ingest_decode(const unsigned char *buf, size_t len,
                      ingest_plan_t &plan) {
  plan = ingest_plan_t();

  const dberr_t err = ingest_decode_low(buf, len, plan);

  if (err != DB_SUCCESS) {
    plan = ingest_plan_t();
  }

  return (err);
}
