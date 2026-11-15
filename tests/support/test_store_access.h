/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_store_access.h
 *    White-box storage lifecycle checks.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_store_access_h
#define test_store_access_h

#ifndef PT_TESTING
#error "test_store_access.h requires the PulseTen test library"
#endif

#include "store.h"

/** Test-only access to storage cleanup. */
class store_test_access final {
 public:
  store_test_access() = delete;

  /** Exercise idempotent cleanup without changing the production interface. */
  static void close(store &st);
};

#endif /* test_store_access_h */
