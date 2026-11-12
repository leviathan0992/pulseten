/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_err.h
 *    Internal error codes.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_err_h
#define ut_err_h

#include <string>

/** Machine-readable error categories. Operations return db_status_t, which
owns the diagnostic too. Add codes only when a caller needs to distinguish
them, and update ut_strerr() in the same change. */
enum [[nodiscard]] dberr_t {
  /** Success. */
  DB_SUCCESS = 0,

  /** Failure with no more specific code. */
  DB_ERROR,

  /** Memory allocation failed. */
  DB_OUT_OF_MEMORY,

  /** An operating system I/O call failed. */
  DB_IO_ERROR,

  /** Persistent data failed validation. */
  DB_CORRUPTION,

  /** The caller passed a value outside the accepted range. */
  DB_INVALID_ARGUMENT,

  /** Rejected without side effects: an earlier fatal failure stopped the
  subsystem until it is reopened. */
  DB_FENCED,

  /** The resource is already open and locked by another handle. */
  DB_LOCKED,

  /** A write disagrees with the existing schema: same column name with a
  different kind or type. */
  DB_SCHEMA_CONFLICT,

  /** A size or count limit was exceeded. */
  DB_LIMIT_EXCEEDED,

  /** A write may or may not have become durable; it must not be retried
  blindly. Reopen the subsystem before using it again. */
  DB_OUTCOME_UNKNOWN,

  /** The caller stopped the operation. */
  DB_INTERRUPTED
};

/** Describe an error code.
@return static, non-empty description */
const char *ut_strerr(dberr_t err);

/** Result of one operation, owning its diagnostic independently of handles
and subsequent calls. Success has no message. */
struct [[nodiscard]] db_status_t {
  /** Build a successful result. */
  db_status_t() = default;

  /** Build a result from its code; success does not construct a diagnostic.
  @param[in]  code  outcome of the operation */
  db_status_t(dberr_t code) : m_code(code) {
    if (code != DB_SUCCESS) {
      m_message = ut_strerr(code);
    }
  }

  /** Build a result with an owned diagnostic.
  @param[in]  code     outcome of the operation
  @param[in]  message  diagnostic; ignored on success, empty selects the
                       code's description */
  db_status_t(dberr_t code, std::string message);

  /** Check whether the operation succeeded.
  @return true on success */
  bool ok() const { return (m_code == DB_SUCCESS); }

  /** Read the machine-readable outcome.
  @return the operation's error code */
  dberr_t code() const { return (m_code); }

  /** Borrow the diagnostic owned by this result.
  @return an empty string on success, otherwise a non-empty diagnostic */
  const std::string &message() const { return (m_message); }

  /** Add context without losing the original cause.
  @return an error prefixed by context, or success unchanged */
  db_status_t with_context(const std::string &context) const;

  /** Reclassify an outcome while preserving its diagnostic.
  @return the result with the given code */
  db_status_t with_code(dberr_t code) const;

 private:
  /** Machine-readable outcome. */
  dberr_t m_code{DB_SUCCESS};

  /** Diagnostic owned by this operation; empty on success. */
  std::string m_message;
};

#endif /* ut_err_h */
