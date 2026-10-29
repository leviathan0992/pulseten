/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0err.cc
 *    Descriptions of internal error codes.
 *
 *-------------------------------------------------------------------------
 */

#include "ut0err.h"

/** Describe an error code.
@param[in]  err  error code
@return static, non-empty description */
const char *ut_strerr(dberr_t err) {
  /* No default label: -Wswitch reports a code missing from this list. */
  switch (err) {
    case DB_SUCCESS:
      return ("Success");
    case DB_ERROR:
      return ("Generic error");
    case DB_OUT_OF_MEMORY:
      return ("Out of memory");
    case DB_IO_ERROR:
      return ("I/O error");
    case DB_CORRUPTION:
      return ("Data corruption");
    case DB_INVALID_ARGUMENT:
      return ("Invalid argument");
    case DB_FENCED:
      return ("Fenced after an earlier failure");
    case DB_LOCKED:
      return ("Already open by another handle");
    case DB_SCHEMA_CONFLICT:
      return ("Schema conflict");
    case DB_LIMIT_EXCEEDED:
      return ("Limit exceeded");
    case DB_OUTCOME_UNKNOWN:
      return ("Outcome unknown");
    case DB_NOT_FOUND:
      return ("Not found");
    case DB_INTERRUPTED:
      return ("Interrupted");
  }

  return ("Unknown error");
}
