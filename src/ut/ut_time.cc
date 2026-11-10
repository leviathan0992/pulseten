/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_time.cc
 *    UTC day arithmetic.
 *
 *-------------------------------------------------------------------------
 */

#include "ut_time.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>

/** Convert a timestamp to its UTC day, rounding toward the earlier day. */
int64_t ut_utc_day(int64_t time_us) {
  int64_t day = time_us / UT_DAY_US;

  if (time_us % UT_DAY_US < 0) {
    --day;
  }

  return (day);
}

/** Format a UTC day as a civil date. */
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

/** Read the current UTC time in microseconds since 1970-01-01. */
int64_t ut_now_us() {
  const std::chrono::system_clock::duration d =
      std::chrono::system_clock::now().time_since_epoch();

  return (std::chrono::duration_cast<std::chrono::microseconds>(d).count());
}
