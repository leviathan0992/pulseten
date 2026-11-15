/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut.cc
 *    Common diagnostics, encoding, time and synchronization.
 *
 *-------------------------------------------------------------------------
 */

#include "ut.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

/* ============================ Assertions ============================ */

/** Print the failed assertion and source location, then abort. */
void ut_assertion_failed(const char *expr, const char *file, int line) {
  fprintf(stderr, "PulseTen: assertion failed: %s\nPulseTen: at %s:%d\n", expr,
          file, line);
  fflush(stderr);
  abort();
}

/* =========================== Error status =========================== */

/** Initialize an error with the default diagnostic for its code. */
db_status_t::db_status_t(dberr_t code) : m_code(code) {
  if (code != DB_SUCCESS) {
    m_message = ut_strerr(code);
  }
}

/** Initialize an error with an owned diagnostic, or the code's name. */
db_status_t::db_status_t(dberr_t code, std::string message) : m_code(code) {
  if (code == DB_SUCCESS) {
    return;
  }

  m_message = message.empty() ? ut_strerr(code) : std::move(message);
}

/** Add context without replacing the cause. */
db_status_t db_status_t::with_context(const std::string &context) const {
  if (ok() || context.empty()) {
    return (*this);
  }

  return (db_status_t(m_code, context + ": " + m_message));
}

/** Change the outcome classification while retaining the diagnostic. */
db_status_t db_status_t::with_code(dberr_t code) const {
  return (db_status_t(code, m_message));
}

/** Return the diagnostic associated with an internal error code. */
const char *ut_strerr(dberr_t err) {
  /* No default label: -Wswitch reports a code missing from this list. */
  switch (err) {
    case DB_SUCCESS:
      return ("success");

    case DB_ERROR:
      return ("generic error");

    case DB_OUT_OF_MEMORY:
      return ("out of memory");

    case DB_IO_ERROR:
      return ("I/O error");

    case DB_CORRUPTION:
      return ("data corruption");

    case DB_INVALID_ARGUMENT:
      return ("invalid argument");

    case DB_FENCED:
      return ("stopped after an earlier failure; reopen required");

    case DB_LOCKED:
      return ("already open by another handle");

    case DB_SCHEMA_CONFLICT:
      return ("schema conflict");

    case DB_LIMIT_EXCEEDED:
      return ("limit exceeded");

    case DB_OUTCOME_UNKNOWN:
      return ("outcome unknown");

    case DB_INTERRUPTED:
      return ("interrupted");

    case DB_TABLE_NOT_INITIALIZED:
      return ("table schema is not initialized");

    case DB_LOCK_TIMEOUT:
      return ("table lock wait timed out");
  }

  return ("unknown error");
}

/* ========================== Byte encoding =========================== */

/** Store a 16-bit value in little-endian order. */
void ut_write_u16(unsigned char *b, uint16_t v) {
  b[0] = static_cast<unsigned char>(v);
  b[1] = static_cast<unsigned char>(v >> 8);

  return;
}

/** Store a 32-bit value in little-endian order. */
void ut_write_u32(unsigned char *b, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    b[i] = static_cast<unsigned char>(v >> (8 * i));
  }

  return;
}

/** Store a 64-bit value in little-endian order. */
void ut_write_u64(unsigned char *b, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    b[i] = static_cast<unsigned char>(v >> (8 * i));
  }

  return;
}

/** Read a little-endian 16-bit value. */
uint16_t ut_read_u16(const unsigned char *b) {
  return (static_cast<uint16_t>(b[0] | (b[1] << 8)));
}

/** Read a little-endian 32-bit value. */
uint32_t ut_read_u32(const unsigned char *b) {
  uint32_t v = 0;

  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(b[i]) << (8 * i);
  }

  return (v);
}

/** Read a little-endian 64-bit value. */
uint64_t ut_read_u64(const unsigned char *b) {
  uint64_t v = 0;

  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(b[i]) << (8 * i);
  }

  return (v);
}

/* ============================ Checksums ============================= */

/** Reflected CRC-32C polynomial. */
static const uint32_t UT_CRC32C_POLYNOMIAL = 0x82F63B78;

/** Lookup table, initialized when CRC-32C is first used. */
struct crc32c_table_t {
  /** CRC-32C of each byte value. */
  uint32_t m_values[256]{};

  /** Build the lookup table for the reflected CRC-32C polynomial. */
  crc32c_table_t() {
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;

      for (int k = 0; k < 8; ++k) {
        if (c & 1) {
          c = (c >> 1) ^ UT_CRC32C_POLYNOMIAL;
        } else {
          c = c >> 1;
        }
      }

      m_values[i] = c;
    }
  }
};

/** Compute CRC-32C. */
uint32_t ut_crc32c(const void *buf, size_t len, uint32_t crc) {
  /* Safe even when a different translation unit calls us before main().
  The local static is initialized once, including concurrent first use. */
  static const crc32c_table_t crc32c_table;

  const unsigned char *p = static_cast<const unsigned char *>(buf);
  uint32_t c = ~crc;

  for (size_t i = 0; i < len; ++i) {
    c = crc32c_table.m_values[(c ^ p[i]) & 0xFF] ^ (c >> 8);
  }

  return (~c);
}

/* =============================== Time =============================== */

/** Return the UTC hour containing a microsecond timestamp. */
int64_t ut_utc_hour(int64_t time_us) {
  int64_t hour = time_us / UT_HOUR_US;
  if (time_us % UT_HOUR_US < 0) {
    --hour;
  }

  return (hour);
}

/** Convert microseconds to a UTC day, rounding toward the earlier day. */
int64_t ut_utc_day(int64_t time_us) {
  int64_t day = time_us / UT_DAY_US;

  if (time_us % UT_DAY_US < 0) {
    --day;
  }

  return (day);
}

/** Format a UTC day as a calendar date. */
std::string ut_day_to_date(int64_t day) {
  /* Days to civil date, after Howard Hinnant's civil_from_days: count in
  400-year eras starting on 0000-03-01. */
  const int64_t z = day + 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const int64_t doe = z - era * 146097;
  const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const int64_t mp = (5 * doy + 2) / 153;

  const int64_t d = doy - (153 * mp + 2) / 5 + 1;
  const int64_t m = mp < 10 ? mp + 3 : mp - 9;
  const int64_t y = yoe + era * 400 + (m <= 2 ? 1 : 0);

  /* Room for any three 64-bit values, so the compiler can prove no
  truncation. */
  char buf[64];
  snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld", static_cast<long long>(y),
           static_cast<long long>(m), static_cast<long long>(d));

  return (buf);
}

/** Use one literal format for partition constraints and query cutoffs. */
std::string ut_timestamp_literal(int64_t time_us) {
  int64_t within = time_us % UT_DAY_US;
  if (within < 0) {
    within += UT_DAY_US;
  }

  const int64_t seconds = within / 1000000;
  char clock[40];
  snprintf(clock, sizeof(clock), " %02lld:%02lld:%02lld.%06lld'",
           static_cast<long long>(seconds / 3600),
           static_cast<long long>(seconds / 60 % 60),
           static_cast<long long>(seconds % 60),
           static_cast<long long>(within % 1000000));

  return ("TIMESTAMP '" + ut_day_to_date(ut_utc_day(time_us)) + clock);
}

/** Read the current Unix time in microseconds. */
int64_t ut_now_us() {
  const std::chrono::system_clock::duration d =
      std::chrono::system_clock::now().time_since_epoch();

  return (std::chrono::duration_cast<std::chrono::microseconds>(d).count());
}

#ifdef PT_DEBUG

/* ========================= Mutex ownership ========================== */

/** Check whether the current thread owns this mutex. */
bool ut_mutex_t::is_owned() const {
  return (m_owner.load(std::memory_order_relaxed) == ut_mutex_thread_id());
}

/** Acquire the mutex and record its owning thread. */
void ut_mutex_t::lock() {
  ut_ad(!is_owned());

  m_mutex.lock();
  m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);

  return;
}

/** Try to acquire the mutex and record ownership on success. */
bool ut_mutex_t::try_lock() {
  ut_ad(!is_owned());

  if (!m_mutex.try_lock()) {
    return (false);
  }

  m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);

  return (true);
}

/** Clear the ownership record before releasing the mutex. */
void ut_mutex_t::unlock() {
  ut_ad(is_owned());

  m_owner.store(nullptr, std::memory_order_relaxed);
  m_mutex.unlock();

  return;
}

/** Return a stable identity unique to the current thread. */
const void *ut_mutex_thread_id() {
  static thread_local unsigned char token;

  return (&token);
}

/* ====================== Shared mutex ownership ====================== */

/** Check whether the current thread holds the exclusive lock. */
bool ut_shared_mutex_t::is_owned() const {
  return (m_owner.load(std::memory_order_relaxed) == ut_mutex_thread_id());
}

/** Check whether this mutex is in the current thread's shared-lock set. */
bool ut_shared_mutex_t::is_owned_shared() const {
  const std::vector<const ut_shared_mutex_t *> &locks = shared_locks();

  return (std::find(locks.begin(), locks.end(), this) != locks.end());
}

/** Acquire exclusive access and record its owning thread. */
void ut_shared_mutex_t::lock() {
  ut_ad(!is_owned());
  ut_ad(!is_owned_shared());

  m_mutex.lock();
  m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);

  return;
}

/** Try to acquire exclusive access and record ownership on success. */
bool ut_shared_mutex_t::try_lock() {
  ut_ad(!is_owned());
  ut_ad(!is_owned_shared());

  if (!m_mutex.try_lock()) {
    return (false);
  }

  m_owner.store(ut_mutex_thread_id(), std::memory_order_relaxed);

  return (true);
}

/** Clear the exclusive owner before releasing the mutex. */
void ut_shared_mutex_t::unlock() {
  ut_ad(is_owned());

  m_owner.store(nullptr, std::memory_order_relaxed);
  m_mutex.unlock();

  return;
}

/** Acquire shared access and register it for the current thread. */
void ut_shared_mutex_t::lock_shared() {
  ut_ad(!is_owned());
  ut_ad(!is_owned_shared());

  m_mutex.lock_shared();
  shared_locks().push_back(this);

  return;
}

/** Try to acquire shared access and register it on success. */
bool ut_shared_mutex_t::try_lock_shared() {
  ut_ad(!is_owned());
  ut_ad(!is_owned_shared());

  if (!m_mutex.try_lock_shared()) {
    return (false);
  }

  shared_locks().push_back(this);

  return (true);
}

/** Remove the shared ownership record before releasing the mutex. */
void ut_shared_mutex_t::unlock_shared() {
  ut_ad(is_owned_shared());

  std::vector<const ut_shared_mutex_t *> &locks = shared_locks();
  locks.erase(std::find(locks.begin(), locks.end(), this));
  m_mutex.unlock_shared();

  return;
}

/** Return the shared mutexes held by the current thread. */
std::vector<const ut_shared_mutex_t *> &ut_shared_mutex_t::shared_locks() {
  static thread_local std::vector<const ut_shared_mutex_t *> locks;

  return (locks);
}

#endif /* PT_DEBUG */

/* ============================== Events ============================== */

/** Signal the event and wake all waiters on a new signal. */
void ut_event_t::set() {
  std::lock_guard<std::mutex> lock(m_mutex);
  if (!m_signaled) {
    m_signaled = true;
    ++m_signal_count;
    m_condition.notify_all();
  }
}

/** Clear the event and return its signal count for a subsequent wait. */
uint64_t ut_event_t::reset() {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_signaled = false;

  return (m_signal_count);
}

/** Wait until the event is set or its signal count changes. */
void ut_event_t::wait(uint64_t signal_count) {
  std::unique_lock<std::mutex> lock(m_mutex);
  if (signal_count == 0) {
    signal_count = m_signal_count;
  }

  m_condition.wait(
      lock, [&] { return (m_signaled || m_signal_count != signal_count); });
}

/** Wait for a signal or a signal-count change, bounded by a timeout. */
bool ut_event_t::wait_for(std::chrono::nanoseconds timeout,
                          uint64_t signal_count) {
  std::unique_lock<std::mutex> lock(m_mutex);
  if (signal_count == 0) {
    signal_count = m_signal_count;
  }

  return (m_condition.wait_for(lock, timeout, [&] {
    return (m_signaled || m_signal_count != signal_count);
  }));
}

/* ======================= Reader/writer locks ======================== */

/** Queue an exclusive writer until all active holders leave. */
void ut_rwlock::lock() {
  ut_unique_lock_t lock(m_mutex);
  ++m_waiting_writers;

  while (m_writer_active || m_readers != 0) {
    const uint64_t signal_count = m_state_changed.reset();
    lock.unlock();
    m_state_changed.wait(signal_count);
    lock.lock();
  }

  --m_waiting_writers;
  m_writer_active = true;

  return;
}

/** Try exclusive access without passing an already waiting writer. */
bool ut_rwlock::try_lock() {
  ut_lock_guard_t lock(m_mutex);
  if (m_writer_active || m_readers != 0 || m_waiting_writers != 0) {
    return (false);
  }

  m_writer_active = true;

  return (true);
}

/** Release exclusive access and wake contenders. */
void ut_rwlock::unlock() {
  ut_lock_guard_t lock(m_mutex);
  ut_a(m_writer_active);

  m_writer_active = false;
  m_state_changed.set();

  return;
}

/** Wait for shared access while giving queued writers priority. */
void ut_rwlock::lock_shared() {
  ut_unique_lock_t lock(m_mutex);

  while (m_writer_active || m_waiting_writers != 0) {
    const uint64_t signal_count = m_state_changed.reset();
    lock.unlock();
    m_state_changed.wait(signal_count);
    lock.lock();
  }

  ++m_readers;

  return;
}

/** Try shared access only when no writer is active or waiting. */
bool ut_rwlock::try_lock_shared() {
  ut_lock_guard_t lock(m_mutex);
  if (m_writer_active || m_waiting_writers != 0) {
    return (false);
  }

  ++m_readers;

  return (true);
}

/** Release shared access and wake contenders when the last reader leaves. */
void ut_rwlock::unlock_shared() {
  ut_lock_guard_t lock(m_mutex);
  ut_a(m_readers != 0);

  --m_readers;
  if (m_readers == 0) {
    m_state_changed.set();
  }

  return;
}
