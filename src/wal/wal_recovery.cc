/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal_recovery.cc
 *    WAL recovery scan.
 *
 *-------------------------------------------------------------------------
 */

#include "wal_recovery.h"

#include <cstdint>
#include <cstdlib>

#include "os_file.h"
#include "ut.h"
#include "wal.h"

/** Release the record buffer used while scanning a file. */
wal_file_reader::~wal_file_reader() {
  free(m_buffer);
}

/** Grow the owned record buffer to at least size bytes.
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
db_status_t wal_file_reader::reserve_buffer(size_t size) {
  if (size <= m_buffer_capacity_bytes) {
    return (DB_SUCCESS);
  }

  unsigned char *p = static_cast<unsigned char *>(realloc(m_buffer, size));
  if (p == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  m_buffer = p;
  m_buffer_capacity_bytes = size;

  return (DB_SUCCESS);
}

/** Read a complete record, checking only its framing if already stored.
@param[out]  valid  true if the requested checks passed
@return DB_SUCCESS with valid=false for an invalid or incomplete record;
  I/O and allocation errors are returned unchanged */
db_status_t wal_file_reader::read_record(uint64_t at, uint64_t avail,
                                         batch_seq_t expected, bool header_only,
                                         bool &valid, batch_seq_t &seq,
                                         size_t &len) {
  valid = false;

  /* An incomplete header ends the valid prefix. */
  if (avail < WAL_RECORD_HEADER_BYTES) {
    return (DB_SUCCESS);
  }

  /* Read the header first to learn the record length. */
  if (const db_status_t err = reserve_buffer(WAL_RECORD_HEADER_BYTES);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err =
          os_file_read(m_file, m_buffer, WAL_RECORD_HEADER_BYTES, at);
      !err.ok()) {
    return (err);
  }

  size_t n = 0;
  batch_seq_t header_seq = 0;
  uint32_t header_index = 0;
  if (!wal_decode_record_header(m_buffer, header_seq, header_index, n)) {
    return (DB_SUCCESS);
  }

  /* Trust sequence and payload length only after validating the header. */
  if (header_seq != expected) {
    return (DB_SUCCESS);
  }

  if (n > avail - WAL_RECORD_HEADER_BYTES) {
    return (DB_SUCCESS);
  }

  if (header_only) {
    seq = header_seq;
    len = n;
    valid = true;

    return (DB_SUCCESS);
  }

  /* Then read the whole record and check its CRC. */
  const size_t total = WAL_RECORD_HEADER_BYTES + n;
  ut_a(total <= avail);

  if (const db_status_t err = reserve_buffer(total); !err.ok()) {
    return (err);
  }

  if (const db_status_t err = os_file_read(m_file, m_buffer, total, at);
      !err.ok()) {
    return (err);
  }

  uint32_t index = 0;
  valid = wal_decode_record(m_buffer, total, seq, index, len);

  return (DB_SUCCESS);
}

/** Scan one file, replay records after stored_seq and locate the valid end. */
db_status_t wal_file_reader::replay(batch_seq_t first_seq, bool is_last,
                                    wal_replay_callback_t apply, void *ctx,
                                    wal_recovery_t &recovery,
                                    batch_seq_t stored_seq) {
  recovery = wal_recovery_t();

  /* Check the file header. */
  uint64_t size = 0;
  db_status_t err = os_file_size(m_file, size);
  if (!err.ok()) {
    return (err);
  }

  if (size < WAL_FILE_HEADER_BYTES) {
    return (DB_CORRUPTION);
  }

  unsigned char header[WAL_FILE_HEADER_BYTES];
  err = os_file_read(m_file, header, sizeof(header), 0);
  if (!err.ok()) {
    return (err);
  }

  batch_seq_t header_first = 0;
  if (!wal_decode_file_header(header, recovery.m_wal_id, header_first) ||
      header_first != first_seq) {
    return (DB_CORRUPTION);
  }

  recovery.m_first_seq = first_seq;
  recovery.m_last_seq = first_seq - 1;

  /* Apply records in order until the first invalid position. */
  uint64_t at = WAL_FILE_HEADER_BYTES;
  batch_seq_t expected = first_seq;

  while (at < size) {
    bool valid = false;
    batch_seq_t seq = 0;
    size_t len = 0;

    const bool header_only = expected <= stored_seq;
    err = read_record(at, size - at, expected, header_only, valid, seq, len);

    /* The valid log ends here; the tail is classified below. */
    if (!err.ok() || !valid) {
      break;
    }

    if (!header_only && apply != nullptr) {
      err = apply(ctx, seq, m_buffer + WAL_RECORD_HEADER_BYTES, len);
      if (!err.ok()) {
        break;
      }
    }

    recovery.m_records++;
    recovery.m_last_seq = seq;
    at += WAL_RECORD_HEADER_BYTES + len;
    ++expected;
  }

  /* Only records that fit the file were accepted, each after the last. */
  ut_a(at <= size);
  ut_a(expected == recovery.m_last_seq + 1);

  if (!err.ok()) {
    return (err);
  }

  /* Only the newest file may discard its invalid suffix. */
  if (at < size && !is_last) {
    return (db_status_t(
        DB_CORRUPTION,
        "invalid WAL record in a closed file at byte " + std::to_string(at)));
  }

  recovery.m_end_offset = at;
  recovery.m_tail_bytes = size - at;
  recovery.m_file_count = 1;

  return (DB_SUCCESS);
}
