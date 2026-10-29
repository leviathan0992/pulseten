/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * storage0priv.h
 *    Definitions shared by the files of the storage module only.
 *
 *-------------------------------------------------------------------------
 */

#ifndef storage0priv_h
#define storage0priv_h

#include <string>
#include <utility>

#include "duck0duck.h"
#include "storage0storage.h"

/** A partition, by (table id, first day). */
typedef std::pair<uint64_t, int64_t> storage_key_t;

/** An open storage file. */
struct storage_t {
  /** The DuckDB database and its connection. */
  duck_t *duck{nullptr};

  /** State as of the last commit; changed only after a flush, an expiry
  or a table drop commits. */
  storage_state_t state;

  /** State a prepared flush will have once it commits. */
  storage_state_t pending;

  /** A flush is prepared: its transaction is open and not committed. */
  bool prepared{false};

  /** Make the next flush fail before it commits; debug builds only. */
  bool dbg_fail_next_flush{false};
};

#endif
