/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut.cc
 *    Assertion failure reporting.
 *
 *-------------------------------------------------------------------------
 */

#include "ut.h"

#include <cstdio>
#include <cstdlib>

/** Report a failed assertion and abort the process. */
void ut_dbg_assertion_failed(const char *expr, const char *file, int line) {
  fprintf(stderr, "PulseTen: assertion failed: %s\nPulseTen: at %s:%d\n", expr,
          file, line);
  fflush(stderr);
  abort();
}
