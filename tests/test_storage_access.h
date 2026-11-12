/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_storage_access.h
 *    White-box storage failure controls.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_storage_access_h
#define test_storage_access_h

#ifndef PT_TESTING
#error "test_storage_access.h requires the PulseTen test library"
#endif

#include "storage.h"

/** Storage test controls; callers exclude concurrent storage mutations. */
class storage_test_access final {
 public:
  /** This access class contains only static operations. */
  storage_test_access() = delete;

  /** Fail the next flush after writing rows and before committing. */
  static void fail_next_flush(storage &store);

  /** Enable persistent failures before a flush commits. */
  static void set_flush_failure(storage &store, bool fail);

  /** Fail the next rollback without executing it. */
  static void fail_next_rollback(storage &store);
};

#endif /* test_storage_access_h */
