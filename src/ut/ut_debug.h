/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_debug.h
 *    Process-wide fault injection and synchronization for tests.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_debug_h
#define ut_debug_h

#include "ut.h"

#ifdef PT_TESTING

#include <functional>
#include <map>
#include <memory>

/** Process-wide controls. Configuration and counts are synchronized and survive
database reopens. Reset only after test operations and their workers stop. */
class ut_debug final : private ut_noncopyable {
 public:
  /** Fail the next count checks; a negative count fails until disarmed. */
  void arm(const char *name, dberr_t code, int count = 1);

  /** Disable a fault without losing its failure count. */
  void disarm(const char *name);

  /** Return an armed failure, or success if the point is inactive. */
  db_status_t check(const char *name);

  /** Read the number of injected failures at this point. */
  uint64_t failures(const char *name) const;

  /** Run callback on the next count hits; a negative count runs until cleared.
  Captured resources must remain alive until every selected callback ends. */
  void set(const char *name, std::function<void()> callback, int count = -1);

  /** Disable future callbacks; a callback already selected may still run. */
  void clear(const char *name);

  /** Select under the mutex; copy and execute the callback after releasing it.
  Each execution receives an independent copy of mutable callable state. */
  void run(const char *name);

  /** Clear faults, counts and callbacks, releasing captures outside the lock.
   */
  void reset();

 private:
  friend ut_debug &ut_debug_global();

  ut_debug() = default;

  /** One fault and its observed failures. */
  struct point_t {
    /** Error returned while armed. */
    dberr_t m_code{DB_SUCCESS};

    /** Failures remaining; negative keeps the point armed. */
    int m_remaining{0};

    /** Total failures since the last reset. */
    uint64_t m_failures{0};
  };

  /** One synchronization callback. */
  struct hook_t {
    /** Keeps the original alive while execution copies it outside the mutex. */
    std::shared_ptr<const std::function<void()>> m_callback;

    /** Hits remaining; negative keeps the callback active. */
    int m_remaining{0};
  };

  /* ===================== Test configuration ====================== */

  /** Protects this group; callback copies, execution and destruction run
  outside this mutex. */
  mutable ut_mutex_t m_mutex;

  /** Faults indexed by their source-level names. */
  std::map<std::string, point_t> m_points;

  /** Synchronization callbacks indexed by their source-level names. */
  std::map<std::string, hook_t> m_hooks;
};

/** Return the process-wide test controls. */
ut_debug &ut_debug_global();

/** Reset at each boundary; all workers must stop before the scope ends.
Nested scopes also reset controls, rather than restoring outer registrations. */
class ut_debug_scope final : private ut_noncopyable {
 public:
  ut_debug_scope() { ut_debug_global().reset(); }
  ~ut_debug_scope() { ut_debug_global().reset(); }
};

/** Return the failure armed for name, or success. */
db_status_t ut_debug_fault(const char *name);

/** Run the callback set for name, if any. */
void ut_debug_sync(const char *name);

/** Check a fault point, evaluating its name once. */
#define UT_DEBUG_FAULT(NAME) ut_debug_fault(NAME)

/** Run a synchronization point, evaluating its name once. */
#define UT_DEBUG_SYNC(NAME) ut_debug_sync(NAME)

#else

/** Return success without evaluating the test argument. */
#define UT_DEBUG_FAULT(NAME) db_status_t(DB_SUCCESS)

/** Skip synchronization without evaluating the test argument. */
#define UT_DEBUG_SYNC(NAME) static_cast<void>(0)

#endif /* PT_TESTING */

#endif /* ut_debug_h */
