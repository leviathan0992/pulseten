/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test0util.h
 *    Helpers shared by test programs.
 *
 * Each test program is a single source file that includes this header
 * once, calls CHECK() and returns test_result() from main().
 *
 *-------------------------------------------------------------------------
 */

#ifndef test0util_h
#define test0util_h

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

/** Number of failed checks in this test program. */
static int test_failures = 0;

/** Message of calls whose message a test does not check; one per thread,
since threads of a test call the engine at the same time. */
static thread_local std::string test_error;

/* Functions are inline so that programs using only some of them compile
without unused-function warnings. */

/** Record a failed check without stopping the test.
@param[in]  ok    check result
@param[in]  what  text of the checked expression
@param[in]  file  source file of the check
@param[in]  line  source line of the check */
static inline void test_check(bool ok, const char *what, const char *file,
                              int line) {
  if (!ok) {
    fprintf(stderr, "FAIL %s:%d: %s\n", file, line, what);
    ++test_failures;
  }
}

#define CHECK(EXPR) test_check((EXPR), #EXPR, __FILE__, __LINE__)

/** Report the outcome.
@param[in]  name  test program name
@return exit status for main() */
static inline int test_result(const char *name) {
  if (test_failures != 0) {
    fprintf(stderr, "%s: %d check(s) failed\n", name, test_failures);

    return (1);
  }
  printf("%s: all checks passed\n", name);

  return (0);
}

/** Remove a directory and the files in it.
@param[in]  path  file or directory path */
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
  char path[1024]{};

  /** Create a fresh scratch directory for one test. */
  test_dir_t() {
    const char *base = getenv("TMPDIR");
    if (base == nullptr || base[0] == '\0') {
      base = "/tmp";
    }
    snprintf(path, sizeof(path), "%s/pt-test-XXXXXX", base);
    if (mkdtemp(path) == nullptr) {
      perror("mkdtemp");
      exit(2);
    }
  }

  /** Remove the scratch directory and its files. */
  ~test_dir_t() { test_remove_dir(path); }

  /** Build a file path within this scratch directory.
  @param[in]  name  file name within this scratch directory
  @return path of a file in this directory */
  std::string file(const char *name) const {
    return (std::string(path) + "/" + name);
  }
};

#endif
