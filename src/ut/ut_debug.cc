/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_debug.cc
 *    Process-wide fault injection and synchronization for tests.
 *
 *-------------------------------------------------------------------------
 */

#include "ut_debug.h"

/** Arm or rearm one failure point. */
void ut_debug::arm(const char *name, dberr_t code, int count) {
  ut_a(code != DB_SUCCESS);

  ut_lock_guard_t lock(m_mutex);
  point_t &point = m_points[name];
  point.m_code = code;
  point.m_remaining = count;

  return;
}

/** Leave the failure count of a disarmed point readable. */
void ut_debug::disarm(const char *name) {
  ut_lock_guard_t lock(m_mutex);
  const auto found = m_points.find(name);
  if (found != m_points.end()) {
    found->second.m_remaining = 0;
  }

  return;
}

/** Fail while the point has failures left. */
db_status_t ut_debug::check(const char *name) {
  ut_lock_guard_t lock(m_mutex);
  const auto found = m_points.find(name);
  if (found == m_points.end() || found->second.m_remaining == 0) {
    return (DB_SUCCESS);
  }

  point_t &point = found->second;
  if (point.m_remaining > 0) {
    --point.m_remaining;
  }

  ++point.m_failures;

  return (db_status_t(point.m_code, std::string("injected ") + name));
}

/** Read how often a point has failed. */
uint64_t ut_debug::failures(const char *name) const {
  ut_lock_guard_t lock(m_mutex);
  const auto found = m_points.find(name);

  return (found == m_points.end() ? 0 : found->second.m_failures);
}

/** Install or replace one hook. */
void ut_debug::set(const char *name, std::function<void()> callback,
                   int count) {
  auto next =
      std::make_shared<const std::function<void()>>(std::move(callback));
  std::shared_ptr<const std::function<void()>> previous;

  /* Release old captures after unlocking; their destructors may reenter. */
  {
    ut_lock_guard_t lock(m_mutex);
    hook_t &hook = m_hooks[name];
    previous = std::move(hook.m_callback);
    hook.m_callback = std::move(next);
    hook.m_remaining = count;
  }

  return;
}

/** Remove one hook. */
void ut_debug::clear(const char *name) {
  std::shared_ptr<const std::function<void()>> previous;

  /* Remove the registration without destroying its callable under the lock. */
  {
    ut_lock_guard_t lock(m_mutex);
    const auto found = m_hooks.find(name);
    if (found == m_hooks.end()) {
      return;
    }

    previous = std::move(found->second.m_callback);
    m_hooks.erase(found);
  }

  return;
}

/** Retain the selected action, then copy and invoke it without the mutex. */
void ut_debug::run(const char *name) {
  std::shared_ptr<const std::function<void()>> selected;

  {
    ut_lock_guard_t lock(m_mutex);
    const auto found = m_hooks.find(name);
    if (found == m_hooks.end() || found->second.m_remaining == 0) {
      return;
    }

    hook_t &hook = found->second;
    selected = hook.m_callback;
    if (hook.m_remaining > 0) {
      --hook.m_remaining;
    }
  }

  /* Copy construction may reenter; mutable state still belongs to this run. */
  std::function<void()> callback = *selected;
  callback();

  return;
}

/** Discard registrations, including those made by released captures. */
void ut_debug::reset() {
  std::map<std::string, hook_t> hooks;

  for (;;) {
    {
      ut_lock_guard_t lock(m_mutex);
      m_points.clear();
      hooks.swap(m_hooks);
    }

    if (hooks.empty()) {
      return;
    }

    /* Capture destructors may register controls, so drain them unlocked. */
    hooks.clear();
  }
}

/** Construct the controls on first use, before any test worker starts. */
ut_debug &ut_debug_global() {
  static ut_debug debug;

  return (debug);
}

/** Check the process-wide fault registration. */
db_status_t ut_debug_fault(const char *name) {
  return (ut_debug_global().check(name));
}

/** Run the process-wide synchronization registration. */
void ut_debug_sync(const char *name) {
  ut_debug_global().run(name);

  return;
}
