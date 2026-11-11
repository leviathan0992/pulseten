/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_mutex.h
 *    Mutex ownership checks in debug builds, standard locks otherwise.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_mutex_h
#define ut_mutex_h

#include <condition_variable>
#include <mutex>

#include "ut.h"

#ifdef PT_DEBUG
#include <atomic>

/** Identity shared by all mutex checks on the current thread. The function
has external inline linkage so its thread-local token is shared across files.
@return address unique to the current live thread */
inline const void *ut_mutex_thread_id() {
  static thread_local unsigned char token;

  return (&token);
}

/** A non-recursive mutex that records its owner for debug assertions.
All locking must go through this wrapper, including condition-variable waits. */
struct ut_mutex_t {
 private:
  /** The mutex that provides synchronization. */
  std::mutex m_mutex;

  /** Current owner's token, or NULL. Atomic so a failed ownership check
  from another thread does not itself introduce a data race. */
  std::atomic<const void *> m_owner{nullptr};

 public:
  /** Whether the calling thread owns this mutex. This is only a diagnostic;
  it does not acquire the mutex or synchronize access to protected data.
  @return true if the calling thread owns this mutex */
  bool is_owned() const {
    return (m_owner.load(std::memory_order_relaxed) == ut_mutex_thread_id());
  }

  /** Acquire the mutex and record the calling thread as its owner. */
  void lock() {
    ut_ad(!is_owned());
    m_mutex.lock();
    m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);
  }

  /** Try to acquire the mutex without waiting.
  @return true if acquired; false if not acquired */
  bool try_lock() {
    ut_ad(!is_owned());
    if (!m_mutex.try_lock()) {
      return (false);
    }

    m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);

    return (true);
  }

  /** Clear ownership before releasing the underlying mutex. */
  void unlock() {
    ut_ad(is_owned());
    m_owner.store(nullptr, std::memory_order_relaxed);
    m_mutex.unlock();
  }
};

/** Waits unlock and relock the wrapper, keeping its owner record accurate. */
typedef std::condition_variable_any ut_condition_variable_t;
#else
/** Release builds use the standard mutex without ownership tracking. */
typedef std::mutex ut_mutex_t;

/** Release builds use the standard condition variable. */
typedef std::condition_variable ut_condition_variable_t;
#endif /* PT_DEBUG */

/** A scoped lock that supports explicit unlocking and relocking. */
using ut_unique_lock_t = std::unique_lock<ut_mutex_t>;

/** A scoped lock held until scope exit. */
using ut_lock_guard_t = std::lock_guard<ut_mutex_t>;

#endif /* ut_mutex_h */
