/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_store_access.cc
 *    White-box storage lifecycle checks.
 *
 *-------------------------------------------------------------------------
 */

#include "test_store_access.h"

/** Close storage handles to exercise subsequent failure paths. */
void store_test_access::close(store &st) {
  st.close();

  return;
}
