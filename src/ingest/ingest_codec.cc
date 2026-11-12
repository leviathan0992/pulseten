/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ingest_codec.cc
 *    Encoding and decoding of the WAL payload of a write.
 *
 *-------------------------------------------------------------------------
 */

#include "ingest.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "batch.h"
#include "ingest_codec.h"
#include "schema.h"
#include "ut_byte.h"
#include "ut_err.h"
#include "ut_noncopyable.h"

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
static void ingest_put_col(std::string &out, const batch_col_t &col,
                           uint32_t n_rows) {
  ingest_put_u8(out, static_cast<uint8_t>(col.m_type));

  const size_t bitmap = out.size();
  out.append((n_rows + 7) / 8, '\0');

  for (uint32_t r = 0; r < n_rows; ++r) {
    if (col.m_nulls[r]) {
      continue;
    }

    out[bitmap + r / 8] |= static_cast<char>(1 << (r % 8));

    switch (col.m_type) {
      case col_type_t::BOOLEAN:
        ingest_put_u8(out, col.m_bools[r]);
        break;

      case col_type_t::BIGINT:
      case col_type_t::TIMESTAMP:
        ingest_put_u64(out, static_cast<uint64_t>(col.m_ints[r]));
        break;

      case col_type_t::DOUBLE: {
        uint64_t bits;
        memcpy(&bits, &col.m_doubles[r], sizeof(bits));
        ingest_put_u64(out, bits);
        break;
      }

      case col_type_t::VARCHAR: {
        const uint32_t start = col.m_offsets[r];
        const uint32_t len = col.m_offsets[r + 1] - start;

        ingest_put_u32(out, len);
        out.append(col.m_chars, start, len);
        break;
      }
    }
  }

  return;
}

/** Encode a write plan as a WAL payload. */
void ingest_encode(const ingest_plan_t &plan, std::string &out) {
  out.clear();

  const batch_t &batch = plan.m_batch;

  ingest_put_u8(out, INGEST_CODEC_VERSION);
  ingest_put_name(out, plan.m_table_name);
  ingest_put_u8(out, plan.m_creates_table ? 1 : 0);

  ingest_put_u16(out, static_cast<uint16_t>(plan.m_new_cols.size()));

  for (size_t i = 0; i < plan.m_new_cols.size(); ++i) {
    const col_def_t &def = plan.m_new_cols[i];

    ingest_put_u32(out, def.m_id);
    ingest_put_u8(out, static_cast<uint8_t>(def.m_kind));
    ingest_put_u8(out, static_cast<uint8_t>(def.m_type));
    ingest_put_name(out, def.m_name);
  }

  ingest_put_u32(out, batch.m_n_rows);
  ingest_put_u16(out, static_cast<uint16_t>(batch.m_cols.size()));

  for (size_t i = 0; i < batch.m_cols.size(); ++i) {
    ingest_put_col(out, batch.m_cols[i], batch.m_n_rows);
  }

  return;
}

/* Decoding. */

/** Reads a payload front to back. Once a read runs past the end, ok
becomes false and every later read returns 0. */
class ingest_reader final : private ut_noncopyable {
 public:
  /** Decode a complete payload into the caller's temporary plan. */
  db_status_t decode(ingest_plan_t &plan);

  /** Borrow a payload for the duration of decoding. */
  ingest_reader(const unsigned char *buf, size_t len)
      : m_buf(buf), m_len(len) {}

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
  bool read_col(uint32_t col_id, uint32_t n_rows, batch_col_t &col);

  /** Check whether every byte was consumed without reading past the end. */
  bool complete() const { return (m_ok && m_at == m_len); }

  /** Check whether no read has crossed the payload boundary. */
  bool valid() const { return (m_ok); }

 private:
  /** The payload. */
  const unsigned char *m_buf{nullptr};

  /** Length of buf in bytes. */
  size_t m_len{0};

  /** Read position. */
  size_t m_at{0};

  /** No read has run past the end. */
  bool m_ok{true};
};

/** Consume a range of bytes from a payload reader.
@return n bytes at the read position, or NULL past the end */
const unsigned char *ingest_reader::read_bytes(size_t n) {
  if (!m_ok || n > m_len - m_at) {
    m_ok = false;

    return (nullptr);
  }

  const unsigned char *p = m_buf + m_at;
  m_at += n;

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

/** Check whether a serialized type number names a supported column type.
@return true if type is the number of a column type */
static bool ingest_type_is_valid(uint8_t type) {
  return (type >= static_cast<uint8_t>(col_type_t::BOOLEAN) &&
          type <= static_cast<uint8_t>(col_type_t::VARCHAR));
}

/** Read one column.
@param[out]     col  decoded column values, offsets and NULL flags
@return true if the column was well formed */
bool ingest_reader::read_col(uint32_t col_id, uint32_t n_rows,
                             batch_col_t &col) {
  const uint8_t type = read_u8();
  if (!ingest_type_is_valid(type)) {
    return (false);
  }

  col.reset(col_id, static_cast<col_type_t>(type), n_rows);

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
    if (!m_ok) {
      return (false);
    }

    const bool valid = (bitmap[row / 8] >> (row % 8)) & 1;
    col.m_nulls[row] = valid ? 0 : 1;

    if (valid) {
      switch (col.m_type) {
        case col_type_t::BOOLEAN: {
          const uint8_t v = read_u8();
          if (v > 1) {
            return (false);
          }

          col.m_bools[row] = v;
          break;
        }

        case col_type_t::BIGINT:
        case col_type_t::TIMESTAMP:
          col.m_ints[row] = static_cast<int64_t>(read_u64());
          break;

        case col_type_t::DOUBLE: {
          const uint64_t bits = read_u64();
          memcpy(&col.m_doubles[row], &bits, sizeof(bits));
          break;
        }

        case col_type_t::VARCHAR: {
          const uint32_t len = read_u32();
          const unsigned char *p = read_bytes(len);
          if (p == nullptr) {
            return (false);
          }

          col.m_chars.append(reinterpret_cast<const char *>(p), len);
          break;
        }
      }
    }

    if (col.m_type == col_type_t::VARCHAR) {
      col.m_offsets[row + 1] = static_cast<uint32_t>(col.m_chars.size());
    }
  }

  return (m_ok);
}

/** Check that the header fields and new columns describe a possible schema
change.
@return true if they do */
static bool ingest_plan_is_consistent(const ingest_plan_t &plan) {
  const size_t n_new = plan.m_new_cols.size();
  if (n_new > plan.m_batch.m_cols.size() ||
      plan.m_base_columns != plan.m_batch.m_cols.size() - n_new) {
    return (false);
  }

  if (plan.m_creates_table) {
    if (plan.m_base_columns != 0 || n_new == 0) {
      return (false);
    }

    const col_def_t &time_col = plan.m_new_cols[0];
    if (time_col.m_id != 0 || time_col.m_kind != col_kind_t::TIME ||
        time_col.m_type != col_type_t::TIMESTAMP ||
        time_col.m_name != SCHEMA_TIME_COLUMN) {
      return (false);
    }
  } else if (plan.m_base_columns == 0) {
    return (false);
  }

  /* New columns are consecutive; only a new table has a time column. */
  for (size_t i = 0; i < n_new; ++i) {
    const col_def_t &def = plan.m_new_cols[i];

    if (def.m_id != plan.m_base_columns + i) {
      return (false);
    }

    if (def.m_kind == col_kind_t::TIME) {
      if (!(plan.m_creates_table && i == 0)) {
        return (false);
      }
    } else if (def.m_kind == col_kind_t::TAG) {
      if (def.m_type != col_type_t::VARCHAR) {
        return (false);
      }
    } else if (!schema_field_type_is_valid(def.m_type)) {
      return (false);
    }
  }

  /* Column 0 of every batch is the non-NULL time column. */
  const batch_t &batch = plan.m_batch;
  const batch_col_t &time_values = batch.m_cols[0];

  if (time_values.m_type != col_type_t::TIMESTAMP) {
    return (false);
  }

  for (uint32_t row = 0; row < batch.m_n_rows; ++row) {
    if (time_values.m_nulls[row]) {
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
    col_def_t def;
    def.m_id = read_u32();

    const uint8_t kind = read_u8();
    const uint8_t type = read_u8();

    if (kind > static_cast<uint8_t>(col_kind_t::FIELD) ||
        !ingest_type_is_valid(type)) {
      return (DB_CORRUPTION);
    }

    def.m_kind = static_cast<col_kind_t>(kind);
    def.m_type = static_cast<col_type_t>(type);

    if (!read_name(def.m_name) || def.m_id >= SCHEMA_MAX_COLUMNS) {
      return (DB_CORRUPTION);
    }

    plan.m_new_cols.push_back(def);
  }

  /* Columns of the batch. */
  batch_t &batch = plan.m_batch;
  batch.m_table_name = plan.m_table_name;
  batch.m_n_rows = read_u32();

  const uint16_t n_cols = read_u16();

  if (!valid() || batch.m_n_rows == 0 || batch.m_n_rows > INGEST_MAX_POINTS ||
      n_cols == 0 || n_cols > SCHEMA_MAX_COLUMNS) {
    return (DB_CORRUPTION);
  }

  batch.m_cols.resize(n_cols);

  for (uint16_t i = 0; i < n_cols; ++i) {
    if (!read_col(i, batch.m_n_rows, batch.m_cols[i])) {
      return (DB_CORRUPTION);
    }
  }

  if (n_new > n_cols) {
    return (DB_CORRUPTION);
  }

  plan.m_base_columns = n_cols - n_new;

  /* Nothing may follow, and the parts must fit together. */
  if (!complete()) {
    return (DB_CORRUPTION);
  }

  if (!ingest_plan_is_consistent(plan)) {
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
