/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0ut.h
 *    Basic types and assertion macros.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut0ut_h
#define ut0ut_h

#include <cstdint>

/** Sequence number of an accepted write batch, starting from 1. */
typedef uint64_t batch_seq_t;

/** Report a failed assertion and abort the process.
@param[in]  expr  text of the failed expression
@param[in]  file  source file of the assertion
@param[in]  line  source line of the assertion */
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
#define ut_ad(EXPR) ((void)0)
#endif

#endif
