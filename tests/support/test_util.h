/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_util.h
 *    Helpers shared by test programs.
 *
 * Each test program is a single source file that includes this header
 * once. It lists its cases in a test_case_t table and returns test_main()
 * from main(), so any case can be run alone by name:
 *
 *    wal-t                  run every case
 *    wal-t rotation purge   run the named cases
 *    wal-t --list           print the case names
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_util_h
#define test_util_h

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#ifdef PT_TESTING
#include "ut_debug.h"
#endif

/** Number of failed checks in this test program. */
static int test_failures = 0;

/* Functions are inline so that programs using only some of them compile
without unused-function warnings. */

/** Record a failed check without stopping the test.
@return ok */
static inline bool test_check(bool ok, const char *what, const char *file,
                              int line) {
  if (!ok) {
    fprintf(stderr, "FAIL %s:%d: %s\n", file, line, what);
    ++test_failures;
  }

  return (ok);
}

/** Record a failed check and continue. */
#define CHECK(EXPR) \
  static_cast<void>(test_check((EXPR), #EXPR, __FILE__, __LINE__))

/** True for integer types that std::cmp_equal accepts: not bool or a
character type. */
template <typename T>
inline constexpr bool test_is_integer_v =
    std::is_integral_v<T> && !std::is_same_v<T, bool> &&
    !std::is_same_v<T, char> && !std::is_same_v<T, char8_t> &&
    !std::is_same_v<T, char16_t> && !std::is_same_v<T, char32_t> &&
    !std::is_same_v<T, wchar_t>;

/** True for values compared and printed as text. */
template <typename T>
inline constexpr bool test_is_text_v =
    std::is_convertible_v<const T &, std::string_view>;

/** Compare by value: integers across signedness, text by content. */
template <typename A, typename B>
static inline bool test_equal(const A &a, const B &b) {
  if constexpr (test_is_integer_v<A> && test_is_integer_v<B>) {
    return (std::cmp_equal(a, b));
  } else if constexpr (test_is_text_v<A> && test_is_text_v<B>) {
    return (std::string_view(a) == std::string_view(b));
  } else {
    return (a == b);
  }
}

/** Format a checked value for a failure message. */
template <typename T>
static inline std::string test_format(const T &value) {
  if constexpr (std::is_same_v<T, bool>) {
    return (value ? "true" : "false");
  } else if constexpr (std::is_enum_v<T>) {
    return (std::to_string(
        static_cast<long long>(static_cast<std::underlying_type_t<T>>(value))));
  } else if constexpr (std::is_integral_v<T>) {
    return (std::to_string(value));
  } else if constexpr (std::is_floating_point_v<T>) {
    char text[32];
    snprintf(text, sizeof(text), "%.17g", static_cast<double>(value));

    return (text);
  } else if constexpr (test_is_text_v<T>) {
    return ("\"" + std::string(std::string_view(value)) + "\"");
  } else {
    return ("<unprintable>");
  }
}

/** Record a failed equality with both values.
@return whether the values are equal */
template <typename A, typename B>
static inline bool test_check_eq(const A &a, const B &b, const char *a_text,
                                 const char *b_text, const char *file,
                                 int line) {
  const bool ok = test_equal(a, b);
  if (!ok) {
    fprintf(stderr, "FAIL %s:%d: %s == %s\n  left:  %s\n  right: %s\n", file,
            line, a_text, b_text, test_format(a).c_str(),
            test_format(b).c_str());
    ++test_failures;
  }

  return (ok);
}

/** Record a failed equality, printing both values, and continue. Each side is
evaluated once. */
#define CHECK_EQ(A, B) \
  static_cast<void>(test_check_eq((A), (B), #A, #B, __FILE__, __LINE__))

/** Record a failed precondition and leave the current void function, so the
rest of the test never runs on a missing object. */
#define REQUIRE(EXPR)                                     \
  do {                                                    \
    if (!test_check((EXPR), #EXPR, __FILE__, __LINE__)) { \
      return;                                             \
    }                                                     \
  } while (0)

/** Parse a positive decimal count without accepting overflow or trailing text.
@param[out]  value  parsed count; unchanged on failure
@return true if text is a positive decimal integer fitting uint64_t */
static inline bool test_parse_positive_count(const char *text,
                                             uint64_t &value) {
  if (text == nullptr || text[0] < '0' || text[0] > '9') {
    return (false);
  }

  char *end = nullptr;
  errno = 0;

  const unsigned long long parsed = strtoull(text, &end, 10);
  if (errno == ERANGE || *end != '\0' || parsed == 0) {
    return (false);
  }

  value = static_cast<uint64_t>(parsed);

  return (true);
}

/** Report the outcome.
@return exit status for main() */
static inline int test_result(const char *name) {
  if (test_failures != 0) {
    fprintf(stderr, "%s: %d check(s) failed\n", name, test_failures);

    return (1);
  }

  printf("%s: all checks passed\n", name);

  return (0);
}

/** One named case in a test program. */
struct test_case_t {
  /** Name that selects the case on the command line. */
  const char *m_name;

  /** The case body. */
  void (*m_run)();
};

/** Find a case by name.
@return the case, or NULL if no case has that name */
static inline const test_case_t *test_find_case(const test_case_t *cases,
                                                size_t n_cases,
                                                const char *name) {
  for (size_t i = 0; i < n_cases; ++i) {
    if (strcmp(cases[i].m_name, name) == 0) {
      return (&cases[i]);
    }
  }

  return (nullptr);
}

/** Run one case and print its time, marking it if any check failed. */
static inline void test_run_case(const test_case_t &test) {
  const int failures = test_failures;
  const auto start = std::chrono::steady_clock::now();

  {
#ifdef PT_TESTING
    ut_debug_scope debug_scope;
#endif
    test.m_run();
  }

  const std::chrono::duration<double, std::milli> elapsed =
      std::chrono::steady_clock::now() - start;
  printf("  %-44s %9.1f ms%s\n", test.m_name, elapsed.count(),
         test_failures != failures ? "  FAILED" : "");
  fflush(stdout);

  return;
}

/** Run the cases named in argv, or every case when none is named. "--list"
prints the case names instead. Names are checked before any case runs.
@return exit status for main(): 0 if every check passed, 1 if a check failed,
2 for a duplicate case or an unknown name */
static inline int test_main_low(const char *program, int argc, char **argv,
                                const test_case_t *cases, size_t n_cases) {
  for (size_t i = 0; i < n_cases; ++i) {
    if (test_find_case(cases, i, cases[i].m_name) != nullptr) {
      fprintf(stderr, "%s: duplicate case %s\n", program, cases[i].m_name);

      return (2);
    }
  }

  if (argc == 2 && strcmp(argv[1], "--list") == 0) {
    for (size_t i = 0; i < n_cases; ++i) {
      printf("%s\n", cases[i].m_name);
    }

    return (0);
  }

  for (int i = 1; i < argc; ++i) {
    if (test_find_case(cases, n_cases, argv[i]) == nullptr) {
      fprintf(stderr, "%s: unknown case %s; --list prints the names\n", program,
              argv[i]);

      return (2);
    }
  }

  if (argc <= 1) {
    for (size_t i = 0; i < n_cases; ++i) {
      test_run_case(cases[i]);
    }
  } else {
    for (int i = 1; i < argc; ++i) {
      test_run_case(*test_find_case(cases, n_cases, argv[i]));
    }
  }

  return (test_result(program));
}

/** Run a test program's case table; see test_main_low().
@return exit status for main() */
template <size_t N>
static inline int test_main(const char *program, int argc, char **argv,
                            const test_case_t (&cases)[N]) {
  return (test_main_low(program, argc, argv, cases, N));
}

#endif /* test_util_h */
