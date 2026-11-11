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
static inline void snapshot_all(memtable_t &mt, memtable_snapshot_t &snap) {
  memtable_snapshot_pin(mt, snap);

  memtable_stats_t stats;
  memtable_read_stats(mt, stats);

  CHECK(memtable_snapshot_complete(mt, stats.m_flushed_seq, snap).ok());
}

#endif /* test_memtable_h */
