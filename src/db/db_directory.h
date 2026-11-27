/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_directory.h
 *    Directory ownership, lifecycle locking and resumable database removal.
 *
 *-------------------------------------------------------------------------
 */

#ifndef db_directory_h
#define db_directory_h

#include <string>
#include <vector>

#include <sys/stat.h>

#include "os_file.h"
#include "ut.h"
#include "ut_debug.h"

#define DB_DIRECTORY_LOCK "LOCK"

/** Hold the directory lock until all database resources close.
Only the directory inode is locked: Drop removes LOCK, so a lock on LOCK
could be taken again on a new file. Inode rechecks prevent an opener with a
stale directory descriptor from operating on a replacement at the same path.
LOCK marks a PulseTen directory; Open creates it and Drop removes it last. */
class db_directory final : private ut_noncopyable {
 public:
  ~db_directory() { close(); }

  /** Lock and check the directory before opening storage. */
  db_status_t open(const char *path, bool create_if_missing,
                   std::string &absolute, bool &initialize_catalog);

  /** Release descriptors after storage and WAL have closed. */
  void close();

  /** Remove a closed database. Unknown entries and links fail preflight.
  A failed removal can be retried while the path remains free of replacements.
  A missing or empty directory is successful cleanup. */
  static db_status_t drop(const char *path);

 private:
  /** Inode captured during preflight; spill children have no subdirectories. */
  struct entry_t {
    std::string m_name;
    struct stat m_stat{};
    std::vector<entry_t> m_children;
  };

  /** Lock the directory inode, then check LOCK if present or requested. */
  db_status_t lock(const char *path, bool create_lock,
                   std::string *absolute = nullptr);

  /** Check, or create, LOCK while holding the directory lock. */
  db_status_t check_lock_file(bool create_lock);

  /** Check the directory and determine whether a new catalog may be created. */
  db_status_t prepare_open(bool create_if_missing, bool &initialize_catalog);

  /** Check that the path name still denotes the locked directory inode.
  @return DB_SUCCESS, DB_LOCKED if it was replaced, or DB_IO_ERROR */
  db_status_t same_directory() const;

  /** List every entry, failing on any name or file it does not own. */
  db_status_t manifest(std::vector<entry_t> &entries) const;

  /** List the open spill directory of entry; the caller closes it. */
  db_status_t manifest_spill(os_file_t spill, entry_t &entry) const;

  /** Remove storage before its recovery files, then LOCK and the directory. */
  db_status_t remove_database();

  /** Remove one preflight entry, recursing into the spill directory. */
  db_status_t remove_entry(os_file_t parent, const entry_t &entry);

  /** Empty the open spill directory of entry; the caller closes it. */
  db_status_t remove_children(os_file_t directory, const entry_t &entry,
                              const struct stat &now);

  /** Remove the locked directory and make its removal durable. */
  db_status_t remove_directory();

  /** Parent of the database directory; path names resolve against it. */
  os_file_t m_parent{OS_FILE_CLOSED};

  /** Locked database directory inode. */
  os_file_t m_directory{OS_FILE_CLOSED};

  /** The directory has a valid LOCK file. */
  bool m_has_lock_file{false};

  /** Final path component of the directory under m_parent. */
  std::string m_name;

  /** The path has no directory. Drop only syncs the parent if it exists. */
  bool m_missing{false};
};

#endif /* db_directory_h */
