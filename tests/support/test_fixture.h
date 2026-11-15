/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * test_fixture.h
 *    The environment of one test case: a scratch directory removed when the
 *    case ends.
 *
 * This header depends on no PulseTen module, so tests of every layer can
 * use it. Database options for tests are in test_db.h.
 *
 *-------------------------------------------------------------------------
 */

#ifndef test_fixture_h
#define test_fixture_h

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <dirent.h>
#include <unistd.h>

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

  return;
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

#endif /* test_fixture_h */
