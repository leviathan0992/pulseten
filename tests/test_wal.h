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

/** Append one caller-ordered record.
The caller serializes appends and supplies its previous sequence in seq.
@param[in,out] seq previous sequence; updated to the attempted record on
success or an uncertain I/O error, otherwise reset to zero
@return the WAL I/O result */
static inline db_status_t append_record(wal &log, const void *payload,
                                        size_t len, batch_seq_t &seq) {
  wal_record_t record;
  record.m_seq = seq + 1;
  record.m_payload = static_cast<const unsigned char *>(payload);
  record.m_len = len;
  const db_status_t err = log.append(&record, 1);
  seq = err.ok() || err.code() == DB_OUTCOME_UNKNOWN ? record.m_seq : 0;

  return (err);
}

#endif /* test_wal_h */
