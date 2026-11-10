/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_util.h
 *    Helpers shared by test programs.
 *
 * Each test program is a single source file that includes this header
 * once, calls CHECK() and returns test_result() from main().
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_util_h
#define test_util_h

#include <dirent.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

/** Number of failed checks in this test program. */
static int test_failures = 0;

/* Functions are inline so that programs using only some of them compile
without unused-function warnings. */

/** Record a failed check without stopping the test. */
static inline void test_check(bool ok, const char *what, const char *file,
                              int line) {
  if (!ok) {
    fprintf(stderr, "FAIL %s:%d: %s\n", file, line, what);
    ++test_failures;
  }
}

#define CHECK(EXPR) test_check((EXPR), #EXPR, __FILE__, __LINE__)

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

/** Remove a directory and the files in it. */
static inline void test_remove_dir(const char *path) {
  DIR *dir = opendir(path);

  if (dir != nullptr) {
    struct dirent *entry;

    while ((entry = readdir(dir)) != nullptr) {
      if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
        unlink((std::string(path) + "/" + entry->d_name).c_str());
      }
    }

    closedir(dir);
  }

  rmdir(path);
}

/** A scratch directory under $TMPDIR (or /tmp), removed together with the
files in it. */
struct test_dir_t {
  /** The directory's path. */
  char m_path[1024]{};

  /** Create a fresh scratch directory for one test. */
  test_dir_t() {
    const char *base = getenv("TMPDIR");
    if (base == nullptr || base[0] == '\0') {
      base = "/tmp";
    }
    snprintf(m_path, sizeof(m_path), "%s/pt-test-XXXXXX", base);
    if (mkdtemp(m_path) == nullptr) {
      perror("mkdtemp");
      exit(2);
    }
  }

  /** Remove the scratch directory and its files. */
  ~test_dir_t() { test_remove_dir(m_path); }

  /** Build a file path within this scratch directory.
  @param[in]  name  file name within this scratch directory
  @return path of a file in this directory */
  std::string file(const char *name) const {
    return (std::string(m_path) + "/" + name);
  }
};

#endif
