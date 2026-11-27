/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut.h
 *    Common types, diagnostics, memory, encoding, time and synchronization.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_h
#define ut_h

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

/* ==================== Basic types and assertions ==================== */

/** Sequence number of an accepted write batch, starting from 1. */
typedef uint64_t batch_seq_t;

/** Report a failed assertion and abort the process. */
[[noreturn]] void ut_assertion_failed(const char *expr, const char *file,
                                      int line);

/** Abort if expr is false. Checked in every build. */
#define ut_a(EXPR)                                    \
  do {                                                \
    if (!(EXPR)) {                                    \
      ut_assertion_failed(#EXPR, __FILE__, __LINE__); \
    }                                                 \
  } while (0)

/** Abort on a code path that must never be reached. */
#define ut_error ut_assertion_failed("unreachable", __FILE__, __LINE__)

#ifdef PT_DEBUG
/** Abort if expr is false. Checked only in debug builds; expr must not
have side effects because release builds do not evaluate it. */
#define ut_ad(EXPR) ut_a(EXPR)
#else
#define ut_ad(EXPR) (static_cast<void>(0))
#endif /* PT_DEBUG */

/* ========================= Object ownership ========================= */

/** Disable copying and moving without introducing virtual dispatch. */
class ut_noncopyable {
 protected:
  ut_noncopyable() = default;

  ~ut_noncopyable() = default;

 public:
  ut_noncopyable(const ut_noncopyable &) = delete;

  ut_noncopyable &operator=(const ut_noncopyable &) = delete;

  ut_noncopyable(ut_noncopyable &&) = delete;

  ut_noncopyable &operator=(ut_noncopyable &&) = delete;
};

/* ======================== Object allocation ========================= */

/** Allocate and construct one object. Allocation failure returns NULL.
Without arguments, default-initialize as new T does, without
value-initializing scalar members.
@tparam T single-object type to construct
@tparam Args constructor argument types
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

  return;
}

/* =========================== Error status =========================== */

/** Machine-readable error categories. Operations return db_status_t, which
owns the diagnostic too. Add codes only when a caller needs to distinguish
them, and update ut_strerr() in the same change. */
enum [[nodiscard]] dberr_t {
  /** Success. */
  DB_SUCCESS = 0,

  /** Failure with no more specific code. */
  DB_ERROR,

  /** Memory allocation failed. */
  DB_OUT_OF_MEMORY,

  /** An operating system I/O call failed. */
  DB_IO_ERROR,

  /** Persistent data failed validation. */
  DB_CORRUPTION,

  /** The caller passed a value outside the accepted range. */
  DB_INVALID_ARGUMENT,

  /** Rejected without side effects: an earlier fatal failure stopped the
  subsystem until it is reopened. */
  DB_FENCED,

  /** The resource is already open and locked by another handle. */
  DB_LOCKED,

  /** A write disagrees with the existing schema: same column name with a
  different kind or type. */
  DB_SCHEMA_CONFLICT,

  /** A size or count limit was exceeded. */
  DB_LIMIT_EXCEEDED,

  /** A write may or may not have become durable; it must not be retried
  blindly. Reopen the subsystem before using it again. */
  DB_OUTCOME_UNKNOWN,

  /** The caller stopped the operation. */
  DB_INTERRUPTED,

  /** A write could not acquire its table lock in time; nothing written. */
  DB_LOCK_TIMEOUT,

  /** A declared time-series table has not accepted its first schema yet. */
  DB_TABLE_NOT_INITIALIZED
};

/** Describe an error code.
@return static, non-empty description */
const char *ut_strerr(dberr_t err);

/** Result of one operation, owning its diagnostic independently of handles
and subsequent calls. Success has no message. */
struct [[nodiscard]] db_status_t {
  /** Build a successful result. */
  db_status_t() = default;

  /** Build a result from its code; success does not construct a diagnostic.
  @param[in]  code  outcome of the operation */
  db_status_t(dberr_t code);

  /** Build a result with an owned diagnostic.
  @param[in]  code     outcome of the operation
  @param[in]  message  diagnostic; ignored on success, empty selects the
                       code's description */
  db_status_t(dberr_t code, std::string message);

  /** Check whether the operation succeeded.
  @return true on success */
  bool ok() const { return (m_code == DB_SUCCESS); }

  /** Read the machine-readable outcome.
  @return the operation's error code */
  dberr_t code() const { return (m_code); }

  /** Return the diagnostic stored in this result.
  @return an empty string on success, otherwise a non-empty diagnostic */
  const std::string &message() const { return (m_message); }

  /** Add context without losing the original cause.
  @return an error prefixed by context, or success unchanged */
  db_status_t with_context(const std::string &context) const;

  /** Reclassify an outcome while preserving its diagnostic.
  @return the result with the given code */
  db_status_t with_code(dberr_t code) const;

 private:
  /** Machine-readable outcome. */
  dberr_t m_code{DB_SUCCESS};

  /** Diagnostic owned by this operation; empty on success. */
  std::string m_message;
};

/* ========================== Byte encoding =========================== */

/** Store a 16-bit value little-endian. */
void ut_write_u16(unsigned char *b, uint16_t v);

/** Store a 32-bit value little-endian. */
void ut_write_u32(unsigned char *b, uint32_t v);

/** Store a 64-bit value little-endian. */
void ut_write_u64(unsigned char *b, uint64_t v);

/** Load a little-endian 16-bit value.
@return the value */
uint16_t ut_read_u16(const unsigned char *b);

/** Load a little-endian 32-bit value.
@return the value */
uint32_t ut_read_u32(const unsigned char *b);

/** Load a little-endian 64-bit value.
@return the value */
uint64_t ut_read_u64(const unsigned char *b);

/* ============================ Checksums ============================= */

/** Compute CRC-32C. Calls can be chained: ut_crc32c(b, nb, ut_crc32c(a, na,
0)) equals the CRC of a followed by b.
@param[in]  crc  result of a previous call, or 0 to start
@return CRC-32C */
uint32_t ut_crc32c(const void *buf, size_t len, uint32_t crc);

/* =============================== Time =============================== */

/** One second in microseconds. */
#define UT_SECOND_US 1000000LL

/** One hour in microseconds. */
#define UT_HOUR_US 3600000000LL

/** Microseconds in one UTC day. */
#define UT_DAY_US 86400000000LL

/** Convert a microsecond timestamp to hours since 1970-01-01 UTC, rounding
toward the past. */
int64_t ut_utc_hour(int64_t time_us);

/** Return the UTC day containing time_us, rounding toward the past. */
int64_t ut_utc_day(int64_t time_us);

/** Read the current UTC time in microseconds since 1970-01-01.
@return the current wall-clock time in microseconds since 1970-01-01 00:00:00
  UTC */
int64_t ut_now_us();

/** Format a UTC day as a civil date.
@param[in]  day  UTC day number relative to 1970-01-01
@return the date of a UTC day as "YYYY-MM-DD" (proleptic Gregorian) */
std::string ut_day_to_date(int64_t day);

/** Format a microsecond UTC timestamp as a SQL TIMESTAMP literal. */
std::string ut_timestamp_literal(int64_t time_us);

/* ============================= Mutexes ============================== */

#ifdef PT_DEBUG

/** Identity shared by all mutex checks on the current thread. The token
is shared across translation units.
@return address unique to the current live thread */
const void *ut_mutex_thread_id();

/** A non-recursive mutex that records its owner for debug assertions.
All locking must go through this wrapper. */
struct ut_mutex_t {
 private:
  /* ===================== Native synchronization ===================== */

  /** Synchronizes caller-owned state; no following field group is protected. */
  std::mutex m_mutex;

  /* ================== Atomic ownership diagnostics ================== */

  /** Current owner's token, or NULL. Atomic so a failed ownership check
  from another thread does not itself introduce a data race. */
  std::atomic<const void *> m_owner{nullptr};

 public:
  /** Check whether the calling thread owns this mutex. This is a diagnostic;
  it does not acquire the mutex or synchronize access to protected data.
  @return true if the calling thread owns this mutex */
  bool is_owned() const;

  /** Acquire the mutex and record the calling thread as its owner. */
  void lock();

  /** Try to acquire the mutex without waiting.
  @return true if acquired; false if not acquired */
  bool try_lock();

  /** Clear ownership before releasing the underlying mutex. */
  void unlock();
};

#else
/** Release builds use the standard mutex without ownership tracking. */
typedef std::mutex ut_mutex_t;

#endif /* PT_DEBUG */

/** A scoped lock that supports explicit unlocking and relocking. */
using ut_unique_lock_t = std::unique_lock<ut_mutex_t>;

/** A scoped lock held until scope exit. */
using ut_lock_guard_t = std::lock_guard<ut_mutex_t>;

/* ========================== Shared mutexes ========================== */

#ifdef PT_DEBUG

/** A non-recursive shared mutex that records both ownership modes.
All locking must go through this wrapper; upgrades and downgrades require
releasing the current lock first. */
struct ut_shared_mutex_t {
 private:
  /* ===================== Native synchronization ===================== */

  /** Synchronizes caller-owned state; no following field group is protected. */
  std::shared_mutex m_mutex;

  /* ================== Atomic ownership diagnostics ================== */

  /** Exclusive owner's token, or NULL. Atomic so ownership checks from
  other threads do not introduce a data race. */
  std::atomic<const void *> m_owner{nullptr};

  /** Mutexes held shared by the current thread. One thread-local list
  is shared across translation units.
  @return the current thread's shared ownership records */
  static std::vector<const ut_shared_mutex_t *> &shared_locks();

 public:
  /** Check exclusive ownership without acquiring or synchronizing the mutex.
  @return true if the calling thread holds the mutex exclusively */
  bool is_owned() const;

  /** Check shared ownership without acquiring or synchronizing the mutex.
  @return true if the calling thread holds the mutex shared */
  bool is_owned_shared() const;

  /** Acquire the mutex exclusively and record the owner. */
  void lock();

  /** Try to acquire the mutex exclusively without waiting.
  @return true if acquired; false otherwise */
  bool try_lock();

  /** Clear exclusive ownership before releasing the mutex. */
  void unlock();

  /** Acquire the mutex shared and record the calling thread. */
  void lock_shared();

  /** Try to acquire the mutex shared without waiting.
  @return true if acquired; false otherwise */
  bool try_lock_shared();

  /** Clear this thread's shared ownership before releasing the mutex. */
  void unlock_shared();
};
#else
/** Release builds use the standard shared mutex without ownership tracking. */
typedef std::shared_mutex ut_shared_mutex_t;
#endif /* PT_DEBUG */

/* ============================== Events ============================== */

/** A notification retained until reset, shared by any number of waiters.
Reset before checking the business condition, then pass its returned count to
wait. A later reset cannot hide an intervening set from that waiter.
Business state needs its own synchronization. Never wait while holding a lock
needed by the notifying thread. All users must finish before destruction. */
class ut_event_t final : private ut_noncopyable {
 public:
  /** Signal all waiters and retain the notification for later arrivals. */
  void set();

  /** Clear the notification and return the count for a subsequent wait. */
  uint64_t reset();

  /** Wait for a retained or intervening signal. Pass a reset count to observe
  signals since that reset; 0 observes only the current state and later signals.
  Returning does not consume or reset the event. */
  void wait(uint64_t signal_count = 0);

  /** Like wait(), with a timeout. Return false only when the timeout expires
  without observing a signal. Spurious wakeups do not restart the timeout. */
  bool wait_for(std::chrono::nanoseconds timeout, uint64_t signal_count = 0);

 private:
  /* ========================== Event state =========================== */

  /** Protects this group; never held while accessing business state. */
  std::mutex m_mutex;

  /** Native waiting is confined to this implementation. */
  std::condition_variable m_condition;

  /** A signal remains visible until reset. */
  bool m_signaled{false};

  /** Increments on each transition to signaled; 0 is reserved by wait(). */
  uint64_t m_signal_count{1};
};

/* ======================= Reader/writer locks ======================== */

/** A reader/writer lock whose ownership may pass between threads.
The caller must release each acquired mode exactly once. Waiting writers
exclude new readers; the state mutex is never held by the protected operation.
*/
class ut_rwlock final : private ut_noncopyable {
 public:
  /** Acquire the lock exclusively. */
  void lock();

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
  bool try_lock();

  /** Release an exclusive acquisition, possibly from a different thread. */
  void unlock();

  /** Acquire the lock shared. */
  void lock_shared();

  /** Try to acquire the lock shared without bypassing a waiting writer.
  @return true if acquired */
  bool try_lock_shared();

  /** Release a shared acquisition, possibly from a different thread. */
  void unlock_shared();

 private:
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
          m_state_changed.set();
        }

        return (false);
      }

      const bool available = exclusive
                                 ? !m_writer_active && m_readers == 0
                                 : !m_writer_active && m_waiting_writers == 0;

      if (available) {
        if (exclusive) {
          --m_waiting_writers;
          m_writer_active = true;
        } else {
          ++m_readers;
        }

        return (true);
      }

      /* The owner can cancel even when no lock holder changes state. */
      const uint64_t signal_count = m_state_changed.reset();
      lock.unlock();
      m_state_changed.wait_for(std::chrono::milliseconds(10), signal_count);
      lock.lock();
    }
  }

  /* ====================== Lock ownership state ====================== */

  /** Protects this group; released before callbacks and event waits. */
  ut_mutex_t m_mutex;

  /** Operations holding a shared lock. */
  size_t m_readers{0};

  /** Writers waiting for current holders to leave. */
  size_t m_waiting_writers{0};

  /** An operation holds the exclusive lock. */
  bool m_writer_active{false};

  /* ============== Internally synchronized notification ============== */

  /** Wakes readers and writers when their mode can be granted. */
  ut_event_t m_state_changed;
};

#endif /* ut_h */
