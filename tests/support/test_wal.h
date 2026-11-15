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
#include <string>
#include <vector>

#include "test_util.h"
#include "ut.h"
#include "wal.h"
#include "wal_recovery.h"

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
  record.m_payload_bytes = len;

  const db_status_t err = log.append(&record, 1);
  seq = err.ok() || err.code() == DB_OUTCOME_UNKNOWN ? record.m_seq : 0;

  return (err);
}

/** Records replayed by test_wal_recover(), or a failure injected into the
replay. */
struct test_wal_records_t {
  /** Payloads in sequence order; m_payloads[i] is record m_first_seq + i. */
  std::vector<std::string> m_payloads;

  /** Sequence of the first replayed record; 0 if none was replayed. A WAL
  whose oldest files were purged starts after 1. */
  batch_seq_t m_first_seq{0};

  /** Record at which the replay callback fails instead of collecting it; 0
  never fails. */
  batch_seq_t m_fail_at{0};

  /** Code returned at m_fail_at. */
  dberr_t m_failure{DB_ERROR};
};

/** Replay callback collecting into a test_wal_records_t.
@return DB_SUCCESS, the injected failure, or DB_CORRUPTION for a gap */
static inline db_status_t test_wal_collect(void *ctx, batch_seq_t seq,
                                           const unsigned char *payload,
                                           size_t len) {
  test_wal_records_t &records = *static_cast<test_wal_records_t *>(ctx);
  if (seq == records.m_fail_at) {
    return (records.m_failure);
  }

  if (records.m_payloads.empty()) {
    records.m_first_seq = seq;
  } else if (seq != records.m_first_seq + records.m_payloads.size()) {
    return (DB_CORRUPTION);
  }

  records.m_payloads.push_back(
      std::string(reinterpret_cast<const char *>(payload), len));

  return (DB_SUCCESS);
}

/** Open the WAL in dir, replay every record into records and close it again.
A handle exists exactly when the open succeeds.
@return the result of wal::open() */
static inline db_status_t test_wal_recover(const char *dir,
                                           test_wal_records_t &records,
                                           wal_recovery_t &recovery) {
  wal *log = nullptr;

  const db_status_t err =
      wal::open(dir, test_wal_collect, &records, &log, recovery);
  CHECK(err.ok() == (log != nullptr));

  if (log != nullptr) {
    CHECK(wal::destroy(log).ok());
  }

  return (err);
}

#endif /* test_wal_h */
