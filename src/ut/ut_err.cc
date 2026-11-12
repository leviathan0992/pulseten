/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_err.cc
 *    Descriptions of internal error codes.
 *
 *-------------------------------------------------------------------------
 */

#include "ut_err.h"

#include <utility>

/** Build a result with an owned diagnostic. */
db_status_t::db_status_t(dberr_t code, std::string message) : m_code(code) {
  if (code == DB_SUCCESS) {
    return;
  }

  m_message = message.empty() ? ut_strerr(code) : std::move(message);
}

/** Add context without replacing the cause. */
db_status_t db_status_t::with_context(const std::string &context) const {
  if (ok() || context.empty()) {
    return (*this);
  }

  return (db_status_t(m_code, context + ": " + m_message));
}

/** Change the outcome classification while retaining the diagnostic. */
db_status_t db_status_t::with_code(dberr_t code) const {
  return (db_status_t(code, m_message));
}

/** Describe an error code. */
const char *ut_strerr(dberr_t err) {
  /* No default label: -Wswitch reports a code missing from this list. */
  switch (err) {
    case DB_SUCCESS:
      return ("success");
    case DB_ERROR:
      return ("generic error");
    case DB_OUT_OF_MEMORY:
      return ("out of memory");
    case DB_IO_ERROR:
      return ("I/O error");
    case DB_CORRUPTION:
      return ("data corruption");
    case DB_INVALID_ARGUMENT:
      return ("invalid argument");
    case DB_FENCED:
      return ("stopped after an earlier failure; reopen required");
    case DB_LOCKED:
      return ("already open by another handle");
    case DB_SCHEMA_CONFLICT:
      return ("schema conflict");
    case DB_LIMIT_EXCEEDED:
      return ("limit exceeded");
    case DB_OUTCOME_UNKNOWN:
      return ("outcome unknown");
    case DB_INTERRUPTED:
      return ("interrupted");
  }

  return ("unknown error");
}
