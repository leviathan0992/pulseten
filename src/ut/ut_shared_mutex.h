/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_shared_mutex.h
 *    Shared mutex ownership checks in debug builds.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_shared_mutex_h
#define ut_shared_mutex_h

#include <shared_mutex>

#ifdef PT_DEBUG
#include <algorithm>
#include <atomic>
#include <vector>

#include "ut.h"
#include "ut_mutex.h"

/** A non-recursive shared mutex that records both ownership modes.
All locking must go through this wrapper; upgrades and downgrades require
releasing the current lock first. */
struct ut_shared_mutex_t {
 private:
  /** The mutex that provides synchronization. */
  std::shared_mutex m_mutex;

  /** Exclusive owner's token, or NULL. Atomic so ownership checks from
  other threads do not introduce a data race. */
  std::atomic<const void *> m_owner{nullptr};

  /** Mutexes held shared by the current thread. The inline function shares
  one thread-local list across translation units.
  @return the current thread's shared ownership records */
  static std::vector<const ut_shared_mutex_t *> &shared_locks() {
    static thread_local std::vector<const ut_shared_mutex_t *> locks;

    return (locks);
  }

 public:
  /** Check exclusive ownership without acquiring or synchronizing the mutex.
  @return true if the calling thread holds the mutex exclusively */
  bool is_owned() const {
    return (m_owner.load(std::memory_order_relaxed) == ut_mutex_thread_id());
  }

  /** Check shared ownership without acquiring or synchronizing the mutex.
  @return true if the calling thread holds the mutex shared */
  bool is_owned_shared() const {
    const std::vector<const ut_shared_mutex_t *> &locks = shared_locks();

    return (std::find(locks.begin(), locks.end(), this) != locks.end());
  }

  /** Acquire the mutex exclusively and record the owner. */
  void lock() {
    ut_ad(!is_owned());
    ut_ad(!is_owned_shared());
    m_mutex.lock();
    m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);

    return;
  }

  /** Try to acquire the mutex exclusively without waiting.
  @return true if acquired; false otherwise */
  bool try_lock() {
    ut_ad(!is_owned());
    ut_ad(!is_owned_shared());
    if (!m_mutex.try_lock()) {
      return (false);
    }

    m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);

    return (true);
  }

  /** Clear exclusive ownership before releasing the mutex. */
  void unlock() {
    ut_ad(is_owned());
    m_owner.store(nullptr, std::memory_order_relaxed);
    m_mutex.unlock();

    return;
  }

  /** Acquire the mutex shared and record the calling thread. */
  void lock_shared() {
    ut_ad(!is_owned());
    ut_ad(!is_owned_shared());
    m_mutex.lock_shared();
    shared_locks().push_back(this);

    return;
  }

  /** Try to acquire the mutex shared without waiting.
  @return true if acquired; false otherwise */
  bool try_lock_shared() {
    ut_ad(!is_owned());
    ut_ad(!is_owned_shared());
    if (!m_mutex.try_lock_shared()) {
      return (false);
    }

    shared_locks().push_back(this);

    return (true);
  }

  /** Clear this thread's shared ownership before releasing the mutex. */
  void unlock_shared() {
    ut_ad(is_owned_shared());
    std::vector<const ut_shared_mutex_t *> &locks = shared_locks();
    locks.erase(std::find(locks.begin(), locks.end(), this));
    m_mutex.unlock_shared();

    return;
  }
};
#else
/** Release builds use the standard shared mutex without ownership tracking. */
typedef std::shared_mutex ut_shared_mutex_t;
#endif /* PT_DEBUG */

#endif /* ut_shared_mutex_h */
