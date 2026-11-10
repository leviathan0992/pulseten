/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_new-t.cc
 *    Object allocation, allocation failure and ownership.
 *
 *-------------------------------------------------------------------------
 */

#include "ut_new.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>

#include "test_util.h"

/** Local allocation controls avoid changing allocation in other modules. */
struct new_probe_t {
  /** Make the next allocations fail. */
  static inline bool m_fail_allocation{false};

  /** Objects constructed so far. */
  static inline int m_constructed{0};

  /** Objects destroyed so far. */
  static inline int m_destroyed{0};

  /** Objects whose memory was freed so far. */
  static inline int m_freed{0};

  /** Set by the constructor. */
  int m_value{17};

  /** A move-only argument the constructor took. */
  std::unique_ptr<int> m_owned;

  /** Construct a probe and record its lifetime for the allocation test. */
  new_probe_t() { ++m_constructed; }

  /** Construct a probe and record its lifetime for the allocation test.
  @param[in,out]  source  integer incremented during construction
  @param[in]      ptr     owned integer transferred into the probe */
  new_probe_t(int &source, std::unique_ptr<int> ptr) : new_probe_t() {
    m_value = ++source;
    m_owned = std::move(ptr);
  }

  /** Record destruction of the allocation probe. */
  ~new_probe_t() { ++m_destroyed; }

  /** Allocate probe storage, or return NULL when failure is injected; the
  unnamed nothrow tag selects this overload.
  @param[in]  size  bytes of probe storage to allocate
  @return allocated probe storage, or NULL when allocation failure is injected
*/
  static void *operator new(size_t size, const std::nothrow_t &) noexcept {
    return (m_fail_allocation ? nullptr : ::operator new(size, std::nothrow));
  }

  /** Record and free probe storage; the unnamed nothrow tag, when present,
  selects placement cleanup.
  @param[in]  ptr  object or allocation to release */
  static void operator delete(void *ptr) noexcept {
    ++m_freed;
    ::operator delete(ptr);
  }

  /** Record and free probe storage; the unnamed nothrow tag, when present,
  selects placement cleanup.
  @param[in]  ptr  object or allocation to release */
  static void operator delete(void *ptr, const std::nothrow_t &) noexcept {
    operator delete(ptr);
  }
};

/** Success runs the constructor and forwards references and move-only args. */
static void test_construct_and_delete() {
  new_probe_t *ptr = ut_new<new_probe_t>();
  CHECK(ptr != nullptr);
  if (ptr != nullptr) {
    CHECK(ptr->m_value == 17);
  }
  ut_delete(ptr);

  int source = 40;
  std::unique_ptr<int> owned = std::make_unique<int>(99);
  ptr = ut_new<new_probe_t>(source, std::move(owned));
  CHECK(ptr != nullptr);
  if (ptr != nullptr) {
    CHECK(source == 41 && ptr->m_value == 41);
    CHECK(owned == nullptr);
    CHECK(ptr->m_owned != nullptr && *ptr->m_owned == 99);
  }
  ut_delete(ptr);
  CHECK(new_probe_t::m_constructed == 2);
  CHECK(new_probe_t::m_destroyed == 2);
  CHECK(new_probe_t::m_freed == 2);

  ut_delete(static_cast<new_probe_t *>(nullptr));
  CHECK(new_probe_t::m_destroyed == 2);
  CHECK(new_probe_t::m_freed == 2);
}

/** Failed allocation returns NULL and never enters the constructor. */
static void test_allocation_failure() {
  new_probe_t::m_fail_allocation = true;
  CHECK(ut_new<new_probe_t>() == nullptr);
  new_probe_t::m_fail_allocation = false;
  CHECK(new_probe_t::m_constructed == 2);
  CHECK(new_probe_t::m_destroyed == 2);
  CHECK(new_probe_t::m_freed == 2);
}

/** Extended alignment is still handled by the standard new expression. */
static void test_alignment() {
  struct alignas(64) aligned_t {
    /** Checked after construction. */
    int m_value{7};
  };
  aligned_t *ptr = ut_new<aligned_t>();
  CHECK(ptr != nullptr);
  if (ptr != nullptr) {
    CHECK(reinterpret_cast<uintptr_t>(ptr) % alignof(aligned_t) == 0);
    CHECK(ptr->m_value == 7);
  }
  ut_delete(ptr);
}

/** Run every test of this file.
@return 0 on success; nonzero if a check, operation or argument validation
  fails */
int main() {
  test_construct_and_delete();
  test_allocation_failure();
  test_alignment();

  return (test_result("ut_new-t"));
}
