/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0ut.cc
 *    Assertion failure reporting.
 *
 *-------------------------------------------------------------------------
 */

#include "ut0ut.h"

#include <cstdio>
#include <cstdlib>

/** Report a failed assertion and abort the process.
@param[in]  expr  text of the failed expression
@param[in]  file  source file of the assertion
@param[in]  line  source line of the assertion */
void ut_dbg_assertion_failed(const char *expr, const char *file, int line) {
  fprintf(stderr, "PulseTen: assertion failed: %s\nPulseTen: at %s:%d\n", expr,
          file, line);
  fflush(stderr);
  abort();
}
