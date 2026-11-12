/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_wal_access.h
 *    White-box WAL failure controls.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_wal_access_h
#define test_wal_access_h

#ifndef PT_TESTING
#error "test_wal_access.h requires the PulseTen test library"
#endif

#include "wal.h"

/** Test controls for a WAL. */
class wal_test_access final {
 public:
  /** This access class contains only static operations. */
  wal_test_access() = delete;

  /** Fail the next group sync, serialized with the writer queue. */
  static void fail_next_sync(wal &log);

  /** Fail after writing a prefix of the next group. */
  static void partial_write(wal &log);
};

#endif /* test_wal_access_h */
