/*-------------------------------------------------------------------------
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * os_memory.h
 *    Physical and container memory capacity, not current free memory.
 *-------------------------------------------------------------------------
 */
#ifndef os_memory_h
#define os_memory_h

#include <cstdint>
#include <string>

#include "ut_err.h"

/** Read physical capacity, reduced by applicable Linux cgroup hard limits.
@return DB_SUCCESS or a diagnostic requiring an explicit memory budget */
db_status_t os_effective_memory(uint64_t &bytes);

/** Apply cgroup memberships and mounts read from proc to a capacity.
Walk each applicable mount from the member to its visible root. Unrestricted
limits leave bytes unchanged. Used by Linux detection and filesystem tests. */
db_status_t os_cgroup_memory(const std::string &membership,
                             const std::string &mountinfo, uint64_t &bytes);

#endif /* os_memory_h */
