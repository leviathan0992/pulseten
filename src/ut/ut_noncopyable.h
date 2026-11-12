/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_noncopyable.h
 *    Stable-address objects that cannot be copied or moved.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_noncopyable_h
#define ut_noncopyable_h

/** Disable copying and moving without introducing virtual dispatch. */
class ut_noncopyable {
 protected:
  /** Construct the base of a stable-address object. */
  ut_noncopyable() = default;

  /** Destroy the base through its concrete owner. */
  ~ut_noncopyable() = default;

 public:
  /** Copying would duplicate resource ownership. */
  ut_noncopyable(const ut_noncopyable &) = delete;

  /** Copy assignment would replace resource ownership. */
  ut_noncopyable &operator=(const ut_noncopyable &) = delete;

  /** Moving would invalidate pointers held by operations. */
  ut_noncopyable(ut_noncopyable &&) = delete;

  /** Move assignment would invalidate pointers held by operations. */
  ut_noncopyable &operator=(ut_noncopyable &&) = delete;
};

#endif /* ut_noncopyable_h */
