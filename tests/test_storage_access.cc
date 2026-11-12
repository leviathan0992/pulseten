/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_storage_access.cc
 *    Storage test access and failure injection.
 *
 *-------------------------------------------------------------------------
 */

#include "test_storage_access.h"

#include "duckdb_database.h"

/** Inject failure after the next flush writes its rows. */
void storage_test_access::fail_next_flush(storage &store) {
  store.m_test_fail_next_flush = true;

  return;
}

/** The caller excludes concurrent storage mutations. */
void storage_test_access::set_flush_failure(storage &store, bool fail) {
  store.m_test_fail_flush = fail;

  return;
}

/** Leave the next rollback unexecuted and report an uncertain outcome. */
void storage_test_access::fail_next_rollback(storage &store) {
  store.m_duckdb->session().m_test_fail_next_rollback = true;

  return;
}
