/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_memtable.h
 *    Helpers for tests that read the memtable directly.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_memtable_h
#define test_memtable_h

#include "memtable.h"
#include "test_util.h"
#include "ut_err.h"

/** Take a snapshot of every visible, unflushed write, as a query does: pin,
then complete the pin at the current flushed boundary. */
static inline void snapshot_all(memtable &mt, memtable_snapshot_t &snapshot) {
  mt.pin_snapshot(snapshot);

  memtable_stats_t stats;
  mt.read_stats(stats);

  CHECK(mt.complete_snapshot(stats.m_flushed_seq, snapshot).ok());

  return;
}

#endif /* test_memtable_h */
