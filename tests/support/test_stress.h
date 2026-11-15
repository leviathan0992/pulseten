/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_stress.h
 *    Settings of seeded stress tests: the seed and the number of steps, each
 *    overridable from the environment, and the command that repeats a run.
 *
 * PT_STRESS_SEED replaces the default seed; PT_STRESS_STEPS replaces the
 * default number of steps. Sanitizer builds run a tenth of the default steps
 * unless PT_STRESS_STEPS is set. The same seed and steps give the same run.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_stress_h
#define test_stress_h

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

/** Sanitizers slow a run by 5 to 15 times. */
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define TEST_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define TEST_SANITIZED 1
#endif
#endif

/** Seed and length of one stress run. */
struct test_stress_t {
  /** Seed of every random choice. */
  uint64_t m_seed{0};

  /** Steps to run. */
  uint64_t m_steps{0};
};

/** Read one unsigned environment variable.
@param[out]  value  the parsed value; unchanged if absent or invalid
@return true if the variable was set to a number */
static inline bool test_stress_env(const char *name, uint64_t &value) {
  const char *text = getenv(name);
  if (text == nullptr || *text == '\0') {
    return (false);
  }

  char *end = nullptr;
  const unsigned long long parsed = strtoull(text, &end, 10);

  if (*end != '\0') {
    fprintf(stderr, "ignoring %s=%s: not a number\n", name, text);

    return (false);
  }

  value = parsed;

  return (true);
}

/** Choose the seed and steps of a run from the defaults and environment. */
static inline test_stress_t test_stress_settings(uint64_t seed,
                                                 uint64_t steps) {
  test_stress_t settings;
  settings.m_seed = seed;
  settings.m_steps = steps;

  test_stress_env("PT_STRESS_SEED", settings.m_seed);

  if (!test_stress_env("PT_STRESS_STEPS", settings.m_steps)) {
#ifdef TEST_SANITIZED
    settings.m_steps = settings.m_steps / 10 > 0 ? settings.m_steps / 10 : 1;
#endif /* TEST_SANITIZED */
  }

  return (settings);
}

/** Print the command that repeats a failed run of one case. */
static inline void test_stress_repeat(const char *program,
                                      const char *case_name,
                                      const test_stress_t &settings) {
  fprintf(stderr,
          "repeat with: PT_STRESS_SEED=%" PRIu64 " PT_STRESS_STEPS=%" PRIu64
          " %s %s\n",
          settings.m_seed, settings.m_steps, program, case_name);

  return;
}

#endif /* test_stress_h */
