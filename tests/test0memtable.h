/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test0memtable.h
 *    Helpers for tests that read the memtable directly.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test0memtable_h
#define test0memtable_h

#include "memtable0memtable.h"
#include "test0util.h"

/** Take a snapshot of every visible, unflushed write, as a query does: pin,
then complete the pin at the current flushed boundary.
@param[in,out]  mt    memtable
@param[out]     snap  snapshot; release it with memtable_snapshot_release() */
static inline void snapshot_all(memtable_t &mt, memtable_snapshot_t &snap) {
  memtable_snapshot_pin(mt, snap);

  memtable_stats_t stats;
  memtable_get_stats(mt, stats);

  CHECK(memtable_snapshot_at(mt, stats.flushed_seq, snap) == DB_SUCCESS);
}

#endif
