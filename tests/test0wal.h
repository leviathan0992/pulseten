/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test0wal.h
 *    Helpers for WAL tests.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test0wal_h
#define test0wal_h

#include "wal0wal.h"

/** Append one record and wait until it is durable.
@param[in,out]  wal      WAL
@param[in]      payload  record payload
@param[in]      len      payload length
@param[out]     seq      sequence number of the record; 0 if it was certainly
                         not written
@return as wal_append_enqueue(), then as wal_append_wait() */
static inline dberr_t append_record(wal_t &wal, const void *payload, size_t len,
                                    batch_seq_t &seq) {
  wal_writer_t w;

  dberr_t err = wal_append_enqueue(wal, payload, len, w, seq);
  if (err != DB_SUCCESS) {
    return (err);
  }

  err = wal_append_wait(wal, w);

  /* A record that was certainly not written has no sequence number. */
  if (err != DB_SUCCESS && err != DB_IO_ERROR) {
    seq = 0;
  }

  return (err);
}

#endif
