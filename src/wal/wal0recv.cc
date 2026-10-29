/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * wal0recv.cc
 *    WAL recovery scan.
 *
 *-------------------------------------------------------------------------
 */

#include "wal0recv.h"

#include <cstdlib>
#include <new>

#include "ut0byte.h"
#include "wal0format.h"

/** Make buf hold at least size bytes.
@param[in,out]  buf       heap buffer, may be NULL
@param[in,out]  buf_size  current capacity
@param[in]      size      required capacity
@return DB_SUCCESS or DB_OUT_OF_MEMORY */
static dberr_t wal_recv_reserve(unsigned char **buf, size_t &buf_size,
                                size_t size) {
  if (size <= buf_size) {
    return (DB_SUCCESS);
  }

  unsigned char *p = (unsigned char *)realloc(*buf, size);
  if (p == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  *buf = p;
  buf_size = size;

  return (DB_SUCCESS);
}

/** Decide whether the bytes after the last valid record are an unfinished
last group or damage to durable data.
@param[in]  file      WAL file
@param[in]  start     first invalid position
@param[in]  size      file size
@param[in]  expected  sequence number expected at start
@return DB_SUCCESS for an unfinished group, DB_CORRUPTION for damage, or
  DB_IO_ERROR / DB_OUT_OF_MEMORY */
static dberr_t wal_recv_check_tail(os_file_t file, uint64_t start,
                                   uint64_t size, batch_seq_t expected) {
  const size_t n = (size_t)(size - start);

  unsigned char *tail = (unsigned char *)malloc(n);
  if (tail == nullptr) {
    return (DB_OUT_OF_MEMORY);
  }

  dberr_t err = os_file_read(file, tail, n, start);
  if (err != DB_SUCCESS) {
    free(tail);

    return (err);
  }

  /* Look for a valid record at every byte position after start. */
  for (size_t at = 1; at + WAL_RECORD_HEADER_SIZE <= n; ++at) {
    batch_seq_t seq = 0;
    uint32_t index = 0;
    size_t len = 0;

    if (!wal_record_is_valid(tail + at, n - at, seq, index, len)) {
      continue;
    }

    /* A group that started after the record expected at start can only
    have been written after the group holding start was flushed. */
    const batch_seq_t group_first = seq - index;

    if (group_first > expected) {
      err = DB_CORRUPTION;
      break;
    }
  }

  free(tail);

  return (err);
}

/** Read the record at offset at, if it is complete and valid.
@param[in]      file      WAL file
@param[in]      at        record offset
@param[in]      avail     bytes from at to the end of the file
@param[in,out]  buf       read buffer, grown as needed
@param[in,out]  buf_size  capacity of buf
@param[in]      expected  sequence number required at this record position
@param[out]     valid     true if a valid record was read into buf
@param[out]     seq       its sequence number
@param[out]     len       its payload length
@return DB_SUCCESS for a valid record or a physically incomplete tail;
  DB_CORRUPTION for a complete bad header or record, DB_IO_ERROR or
  DB_OUT_OF_MEMORY */
static dberr_t wal_recv_read_record(os_file_t file, uint64_t at, uint64_t avail,
                                    unsigned char **buf, size_t &buf_size,
                                    batch_seq_t expected, bool &valid,
                                    batch_seq_t &seq, size_t &len) {
  valid = false;

  if (avail < WAL_RECORD_HEADER_SIZE) {
    return (DB_SUCCESS);
  }

  /* Read the header first to learn the record length. */
  dberr_t err = wal_recv_reserve(buf, buf_size, WAL_RECORD_HEADER_SIZE);
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = os_file_read(file, *buf, WAL_RECORD_HEADER_SIZE, at);
  if (err != DB_SUCCESS) {
    return (err);
  }

  size_t n = 0;
  if (!wal_record_header_len(*buf, n)) {
    return (DB_CORRUPTION);
  }
  /* Header integrity was checked independently, including its length and
  body CRC. Only now can a missing payload be treated as a torn append. */
  const batch_seq_t header_seq = ut_read_u64(*buf + 8);
  const uint32_t header_index = ut_read_u32(*buf + 16);
  if (header_seq != expected || header_index >= header_seq) {
    return (DB_CORRUPTION);
  }
  if (n > avail - WAL_RECORD_HEADER_SIZE) {
    return (DB_SUCCESS);
  }

  /* Then read the whole record and check its CRC. */
  const size_t total = WAL_RECORD_HEADER_SIZE + n;

  err = wal_recv_reserve(buf, buf_size, total);
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = os_file_read(file, *buf, total, at);
  if (err != DB_SUCCESS) {
    return (err);
  }

  uint32_t index = 0;
  valid = wal_record_is_valid(*buf, total, seq, index, len);

  return (valid ? DB_SUCCESS : DB_CORRUPTION);
}

/** Scan one segment, call apply for each valid record and locate the end of
the valid log.
@param[in]   file       open segment file
@param[in]   first_seq  sequence number the segment must start with
@param[in]   is_last    true for the newest segment, the only one that may end
                        in an unfinished group
@param[in]   apply      callback per record, or NULL to only validate
@param[in]   ctx        passed to apply
@param[out]  recv       scan result for this segment, valid on DB_SUCCESS
@return DB_SUCCESS, DB_CORRUPTION, DB_IO_ERROR, DB_OUT_OF_MEMORY or the error
  returned by apply */
dberr_t wal_recv_scan(os_file_t file, batch_seq_t first_seq, bool is_last,
                      wal_apply_t apply, void *ctx, wal_recv_t &recv) {
  recv = wal_recv_t();

  /* Check the file header. */
  uint64_t size = 0;
  dberr_t err = os_file_get_size(file, size);
  if (err != DB_SUCCESS) {
    return (err);
  }
  if (size < WAL_FILE_HEADER_SIZE) {
    return (DB_CORRUPTION);
  }

  unsigned char header[WAL_FILE_HEADER_SIZE];
  err = os_file_read(file, header, sizeof(header), 0);
  if (err != DB_SUCCESS) {
    return (err);
  }
  batch_seq_t header_first = 0;
  if (!wal_file_header_read(header, recv.wal_id, header_first) ||
      header_first != first_seq) {
    return (DB_CORRUPTION);
  }

  recv.first_seq = first_seq;
  recv.last_seq = first_seq - 1;

  /* Apply records in order until the first invalid position. */
  unsigned char *buf = nullptr;
  size_t buf_size = 0;
  uint64_t at = WAL_FILE_HEADER_SIZE;
  batch_seq_t expected = first_seq;

  while (at < size) {
    bool valid = false;
    batch_seq_t seq = 0;
    size_t len = 0;

    err = wal_recv_read_record(file, at, size - at, &buf, buf_size, expected,
                               valid, seq, len);
    if (err != DB_SUCCESS || !valid) {
      break;
    }

    if (seq != expected) {
      /* A complete, checksummed record in the wrong place. */
      err = DB_CORRUPTION;
      break;
    }

    if (apply != nullptr) {
      err = apply(ctx, seq, buf + WAL_RECORD_HEADER_SIZE, len);
      if (err != DB_SUCCESS) {
        break;
      }
    }

    recv.records++;
    recv.last_seq = seq;
    at += WAL_RECORD_HEADER_SIZE + len;
    ++expected;
  }

  free(buf);

  /* Classify whatever follows the last valid record. Only the newest
  segment can end in an unfinished group. */
  if (err == DB_SUCCESS && at < size) {
    if (is_last) {
      err = wal_recv_check_tail(file, at, size, expected);
    } else {
      err = DB_CORRUPTION;
    }
  }

  if (err != DB_SUCCESS) {
    return (err);
  }

  recv.end_offset = at;
  recv.tail_bytes = size - at;
  recv.segments = 1;

  return (DB_SUCCESS);
}
