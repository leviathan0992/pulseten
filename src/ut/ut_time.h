/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * ut_time.h
 *    UTC day arithmetic on microsecond timestamps.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ut_time_h
#define ut_time_h

#include <cstdint>
#include <string>

/** One UTC day in microseconds. */
#define UT_DAY_US 86400000000LL

/** Convert a timestamp to its UTC day, rounding toward the earlier day.
@param[in]  time_us  UTC timestamp in microseconds; negative times round
                     toward the earlier day
@return UTC day number of a time, day 0 being 1970-01-01; times before 1970
  round toward the earlier day */
int64_t ut_utc_day(int64_t time_us);

/** Read the current UTC time in microseconds since 1970-01-01.
@return the current wall-clock time in microseconds since 1970-01-01 00:00:00
  UTC */
int64_t ut_now_us();

/** Format a UTC day as a civil date.
@param[in]  day  UTC day number relative to 1970-01-01
@return the date of a UTC day as "YYYY-MM-DD" (proleptic Gregorian) */
std::string ut_day_to_date(int64_t day);

#endif
