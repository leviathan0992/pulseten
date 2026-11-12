/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_wal_access.cc
 *    WAL test access and failure injection.
 *
 *-------------------------------------------------------------------------
 */

#include "test_wal_access.h"

#include "ut_mutex.h"

/** Capture the requested failure through the WAL writer mutex. */
void wal_test_access::fail_next_sync(wal &log) {
  ut_lock_guard_t lock(log.m_state_mutex);
  log.m_test_fail_next_sync = true;

  return;
}

void wal_test_access::partial_write(wal &log) {
  ut_lock_guard_t lock(log.m_state_mutex);
  log.m_test_partial_write = true;
}
