/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_format.cc
 *    Encoding and validation of WAL headers and records.
 *
 *-------------------------------------------------------------------------
 */

#include "wal_format.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "ut.h"
#include "ut_byte.h"
#include "ut_crc32.h"

/** Build a WAL segment file name from its first sequence number. */
std::string wal_segment_name(batch_seq_t first_seq) {
  char buf[64];
  snprintf(buf, sizeof(buf), "wal-%020llu.pt",
           static_cast<unsigned long long>(first_seq));

  return (buf);
}

/** Recognize a segment file name. */
bool wal_segment_parse_name(const char *name, batch_seq_t &first_seq) {
  /* "wal-" + 20 digits + ".pt" */
  if (strlen(name) != 27 || strncmp(name, "wal-", 4) != 0 ||
      strcmp(name + 24, ".pt") != 0) {
    return (false);
  }

  batch_seq_t v = 0;

  for (int i = 4; i < 24; ++i) {
    if (name[i] < '0' || name[i] > '9') {
      return (false);
    }

    const batch_seq_t digit = static_cast<batch_seq_t>(name[i] - '0');

    if (v > (UINT64_MAX - digit) / 10) {
      return (false);
    }

    v = v * 10 + digit;
  }

  first_seq = v;

  return (v >= 1);
}

/** Fill a file header. */
void wal_encode_file_header(unsigned char *b, uint64_t id,
                            batch_seq_t first_seq) {
  memcpy(b, "PTWAL001", 8);
  ut_write_u32(b + 8, WAL_FORMAT_VERSION);
  ut_write_u32(b + 12, WAL_FILE_HEADER_SIZE);
  ut_write_u64(b + 16, id);
  ut_write_u64(b + 24, first_seq);
  ut_write_u32(b + 32, 0);
  ut_write_u32(b + 36, ut_crc32c(b, 36, 0));
}

/** Decode a file header. */
bool wal_decode_file_header(const unsigned char *b, uint64_t &id,
                            batch_seq_t &first_seq) {
  if (memcmp(b, "PTWAL001", 8) != 0) {
    return (false);
  }

  if (ut_read_u32(b + 8) != WAL_FORMAT_VERSION ||
      ut_read_u32(b + 12) != WAL_FILE_HEADER_SIZE || ut_read_u32(b + 32) != 0) {
    return (false);
  }

  if (ut_read_u32(b + 36) != ut_crc32c(b, 36, 0)) {
    return (false);
  }

  id = ut_read_u64(b + 16);
  first_seq = ut_read_u64(b + 24);

  return (first_seq >= 1);
}

/** Compute the CRC-32C of the record header prefix and payload.
@return CRC of a record: header bytes [0, 20) followed by the payload */
static uint32_t wal_record_crc(const unsigned char *b, size_t len) {
  const uint32_t crc = ut_crc32c(b, 20, 0);

  return (ut_crc32c(b + WAL_RECORD_HEADER_SIZE, len, crc));
}

/** Encode one record. */
void wal_encode_record(unsigned char *b, batch_seq_t seq, uint32_t group_index,
                       const unsigned char *payload, size_t len) {
  ut_write_u32(b, WAL_RECORD_MAGIC);
  ut_write_u32(b + 4, static_cast<uint32_t>(len));
  ut_write_u64(b + 8, seq);
  ut_write_u32(b + 16, group_index);
  memcpy(b + WAL_RECORD_HEADER_SIZE, payload, len);
  ut_write_u32(b + 20, wal_record_crc(b, len));
  ut_write_u32(b + 24, ut_crc32c(b, 24, 0));
}

/** Payload length announced by a record header, if it is plausible. */
bool wal_record_header_len(const unsigned char *b, size_t &len) {
  if (ut_read_u32(b) != WAL_RECORD_MAGIC) {
    return (false);
  }

  if (ut_read_u32(b + 24) != ut_crc32c(b, 24, 0)) {
    return (false);
  }

  const uint32_t n = ut_read_u32(b + 4);

  if (n == 0 || n > WAL_MAX_PAYLOAD) {
    return (false);
  }

  len = n;

  return (true);
}

/** Check a complete record. */
bool wal_record_is_valid(const unsigned char *b, size_t avail, batch_seq_t &seq,
                         uint32_t &group_index, size_t &len) {
  if (avail < WAL_RECORD_HEADER_SIZE) {
    return (false);
  }

  size_t n = 0;
  if (!wal_record_header_len(b, n)) {
    return (false);
  }
  if (n > avail - WAL_RECORD_HEADER_SIZE) {
    return (false);
  }

  /* A group cannot start before sequence number 1. */
  const batch_seq_t s = ut_read_u64(b + 8);
  const uint32_t index = ut_read_u32(b + 16);

  if (s == 0 || index >= s) {
    return (false);
  }

  if (ut_read_u32(b + 20) != wal_record_crc(b, n)) {
    return (false);
  }

  seq = s;
  group_index = index;
  len = n;

  return (true);
}
