/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0err.h
 *    Internal error codes.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut0err_h
#define ut0err_h

/** Result of an internal operation. [[nodiscard]] makes the compiler
reject a call whose error code is ignored. Add codes only when a caller
needs to tell them apart, and update ut_strerr() in the same change. */
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
  /** Rejected without side effects: an earlier I/O failure fenced the
  subsystem until it is reopened. */
  DB_FENCED,
  /** The resource is already open and locked by another handle. */
  DB_LOCKED,
  /** A write disagrees with the existing schema: same column name with a
  different role or type. */
  DB_SCHEMA_CONFLICT,
  /** A size or count limit was exceeded. */
  DB_LIMIT_EXCEEDED,
  /** A write may or may not have become durable; it must not be retried
  blindly. The subsystem is fenced until it is reopened. */
  DB_OUTCOME_UNKNOWN,
  /** The named object does not exist. */
  DB_NOT_FOUND,
  /** The caller stopped the operation. */
  DB_INTERRUPTED
};

/** Describe an error code.
@param[in]  err  error code
@return static, non-empty description */
const char *ut_strerr(dberr_t err);

#endif
