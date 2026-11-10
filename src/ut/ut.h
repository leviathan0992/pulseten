/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut.h
 *    Basic types and assertion macros.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_h
#define ut_h

#include <cstdint>

/** Sequence number of an accepted write batch, starting from 1. */
typedef uint64_t batch_seq_t;

/** Report a failed assertion and abort the process. */
[[noreturn]] void ut_dbg_assertion_failed(const char *expr, const char *file,
                                          int line);

/** Abort if expr is false. Checked in every build. */
#define ut_a(EXPR)                                        \
  do {                                                    \
    if (!(EXPR)) {                                        \
      ut_dbg_assertion_failed(#EXPR, __FILE__, __LINE__); \
    }                                                     \
  } while (0)

/** Abort on a code path that must never be reached. */
#define ut_error ut_dbg_assertion_failed("unreachable", __FILE__, __LINE__)

#ifdef PT_DEBUG
/** Abort if expr is false. Checked only in debug builds; expr must not
have side effects because release builds do not evaluate it. */
#define ut_ad(EXPR) ut_a(EXPR)
#else
#define ut_ad(EXPR) (static_cast<void>(0))
#endif

#endif
