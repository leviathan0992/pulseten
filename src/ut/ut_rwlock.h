/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_rwlock.h
 *    Operation-owned reader/writer lock with writer preference.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_rwlock_h
#define ut_rwlock_h

#include <chrono>
#include <cstddef>

#include "ut.h"
#include "ut_mutex.h"

/** A reader/writer lock whose ownership may pass between threads.
The caller must release each acquired mode exactly once. Waiting writers
exclude new readers; the state mutex is never held by the protected operation.
*/
struct ut_rwlock_t {
 private:
  /** Protects the lock state below. */
  ut_mutex_t m_mutex;

  /** Wakes readers and writers when their mode can be granted. */
  ut_condition_variable_t m_changed;

  /** Operations holding a shared lock. */
  size_t m_readers{0};

  /** Writers waiting for current holders to leave. */
  size_t m_waiting_writers{0};

  /** An operation holds the exclusive lock. */
  bool m_writer{false};

  /** Acquire a mode while allowed remains true. Evaluate the predicate without
  holding m_mutex; a waiting writer stays registered across predicate checks. */
  template <typename Predicate>
  bool acquire_while(bool exclusive, Predicate allowed) {
    ut_unique_lock_t lock(m_mutex);
    if (exclusive) {
      ++m_waiting_writers;
    }

    for (;;) {
      lock.unlock();
      const bool proceed = allowed();
      lock.lock();
      if (!proceed) {
        if (exclusive) {
          --m_waiting_writers;
          m_changed.notify_all();
        }

        return (false);
      }

      const bool available = exclusive ? !m_writer && m_readers == 0
                                       : !m_writer && m_waiting_writers == 0;
      if (available) {
        if (exclusive) {
          --m_waiting_writers;
          m_writer = true;
        } else {
          ++m_readers;
        }

        return (true);
      }

      /* The owner can cancel even when no lock holder changes state. */
      m_changed.wait_for(lock, std::chrono::milliseconds(10));
    }
  }

 public:
  /** Acquire the lock exclusively. */
  void lock() {
    ut_unique_lock_t lock(m_mutex);
    ++m_waiting_writers;
    while (m_writer || m_readers != 0) {
      m_changed.wait(lock);
    }

    --m_waiting_writers;
    m_writer = true;

    return;
  }

  /** Acquire X while allowed remains true; false means no mode was acquired. */
  template <typename Predicate>
  bool lock_while(Predicate allowed) {
    return (acquire_while(true, allowed));
  }

  /** Acquire S while allowed remains true; false means no mode was acquired. */
  template <typename Predicate>
  bool lock_shared_while(Predicate allowed) {
    return (acquire_while(false, allowed));
  }

  /** Try to acquire the lock exclusively.
  @return true if acquired */
  bool try_lock() {
    ut_lock_guard_t lock(m_mutex);
    if (m_writer || m_readers != 0 || m_waiting_writers != 0) {
      return (false);
    }

    m_writer = true;

    return (true);
  }

  /** Release an exclusive acquisition, possibly from a different thread. */
  void unlock() {
    ut_lock_guard_t lock(m_mutex);
    ut_a(m_writer);
    m_writer = false;
    m_changed.notify_all();

    return;
  }

  /** Acquire the lock shared. */
  void lock_shared() {
    ut_unique_lock_t lock(m_mutex);
    while (m_writer || m_waiting_writers != 0) {
      m_changed.wait(lock);
    }

    ++m_readers;

    return;
  }

  /** Try to acquire the lock shared without bypassing a waiting writer.
  @return true if acquired */
  bool try_lock_shared() {
    ut_lock_guard_t lock(m_mutex);
    if (m_writer || m_waiting_writers != 0) {
      return (false);
    }

    ++m_readers;

    return (true);
  }

  /** Release a shared acquisition, possibly from a different thread. */
  void unlock_shared() {
    ut_lock_guard_t lock(m_mutex);
    ut_a(m_readers != 0);
    --m_readers;
    if (m_readers == 0) {
      m_changed.notify_all();
    }

    return;
  }
};

#endif /* ut_rwlock_h */
