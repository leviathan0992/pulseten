/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut0new.h
 *    Single-object allocation and destruction.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut0new_h
#define ut0new_h

#include <new>
#include <type_traits>
#include <utility>

/** Allocate and construct one object. Allocation failure returns NULL.
Without arguments, default-initialize as new T does, without
value-initializing scalar members.
@tparam T single-object type to construct
@tparam Args constructor argument types
@param[in]  args  arguments forwarded to the constructor
@return constructed object, or NULL if allocation fails */
template <typename T, typename... Args>
[[nodiscard]] T *ut_new(Args &&...args) {
  static_assert(!std::is_array_v<T>, "ut_new allocates a single object");

  if constexpr (sizeof...(Args) == 0) {
    return (new (std::nothrow) T);
  } else {
    return (new (std::nothrow) T(std::forward<Args>(args)...));
  }
}

/** Destroy and free one object. A NULL pointer is allowed. The object must
have been allocated by ut_new() or ordinary single-object new.
@tparam T complete single-object type to destroy
@param[in]  ptr  object to destroy, or NULL */
template <typename T>
void ut_delete(T *ptr) {
  static_assert(sizeof(T) > 0, "ut_delete requires a complete type");
  static_assert(!std::is_array_v<T>, "ut_delete destroys a single object");
  delete ptr;
}

#endif
