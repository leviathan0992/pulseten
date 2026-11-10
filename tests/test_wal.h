/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_wal.h
 *    Helpers for WAL tests.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_wal_h
#define test_wal_h

#include <cstddef>

#include "ut.h"
#include "ut_err.h"
#include "wal.h"

/** Append one record and wait until it is durable.
@param[out]  seq  sequence number of the record; 0 if it was certainly
                  not written
@return as wal_append_enqueue(), then as wal_append_commit() */
static inline db_status_t append_record(wal_t &wal, const void *payload,
                                        size_t len, batch_seq_t &seq) {
  wal_writer_t w;

  db_status_t err = wal_append_enqueue(wal, payload, len, w, seq);
  if (!err.ok()) {
    return (err);
  }

  err = wal_append_commit(wal, w);

  /* A record that was certainly not written has no sequence number. */
  if (!err.ok() && err.code() != DB_IO_ERROR) {
    seq = 0;
  }

  return (err);
}

#endif
