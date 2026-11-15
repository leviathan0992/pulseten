/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * db_directory.cc
 *    Descriptor-relative database ownership and deletion.
 *
 *-------------------------------------------------------------------------
 */

#include "db_directory.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "store.h"
#include "wal.h"

/** Preserve errno before formatting diagnostics. */
static db_status_t db_directory_error(int error, const std::string &action) {
  return (db_status_t(
      DB_IO_ERROR,
      action + ": " +
          std::error_code(error, std::generic_category()).message()));
}

/** Sync an open directory, naming the database in the diagnostic. */
static db_status_t db_directory_sync(os_file_t fd) {
  return (os_dir_sync_open(fd).with_context("cannot sync database directory"));
}

/** Inspect entries without following links. */
static db_status_t db_directory_stat(int parent, const std::string &name,
                                     struct stat &out) {
  if (fstatat(parent, name.c_str(), &out, AT_SYMLINK_NOFOLLOW) != 0) {
    return (db_directory_error(errno,
                               "cannot inspect database entry '" + name + "'"));
  }

  return (DB_SUCCESS);
}

/** Only singly-linked regular files can be owned removal candidates. */
static bool db_directory_regular(const struct stat &info) {
  return (S_ISREG(info.st_mode) && info.st_nlink == 1);
}

/** List entries in name order, so preflight and recheck compare alike. */
static db_status_t db_directory_list(os_file_t fd,
                                     std::vector<std::string> &names) {
  if (const db_status_t err = os_dir_list_open(fd, names); !err.ok()) {
    return (err.with_context("cannot list database directory"));
  }

  std::sort(names.begin(), names.end());

  return (DB_SUCCESS);
}

/** Numeric components emitted by the pinned DuckDB temporary-file manager. */
static bool db_directory_decimal(const std::string &value) {
  return (!value.empty() && value.size() <= 20 &&
          (value.size() == 1 || value[0] != '0') &&
          std::all_of(value.begin(), value.end(),
                      [](char c) { return c >= '0' && c <= '9'; }));
}

/** Recognize owner markers, pooled storage and large-block spill files.
Names are duckdb_temp_<pid>_<instance>_ followed by "owner",
"block-<id>.block" or "storage_<size>-<index>.tmp". */
static bool db_directory_spill_name(const std::string &name) {
  const std::string prefix = "duckdb_temp_";
  if (!name.starts_with(prefix)) {
    return (false);
  }

  const size_t pid = name.find('_', prefix.size());
  const size_t instance =
      pid == std::string::npos ? pid : name.find('_', pid + 1);
  if (instance == std::string::npos ||
      !db_directory_decimal(name.substr(prefix.size(), pid - prefix.size())) ||
      !db_directory_decimal(name.substr(pid + 1, instance - pid - 1))) {
    return (false);
  }

  const std::string suffix = name.substr(instance + 1);
  if (suffix == "owner") {
    return (true);
  }

  const std::string block = "block-", block_end = ".block";
  if (suffix.starts_with(block) && suffix.ends_with(block_end)) {
    return (db_directory_decimal(suffix.substr(
        block.size(), suffix.size() - block.size() - block_end.size())));
  }

  const std::string storage = "storage_", storage_end = ".tmp";
  if (!suffix.starts_with(storage) || !suffix.ends_with(storage_end)) {
    return (false);
  }

  const size_t dash = suffix.find('-', storage.size());
  if (dash == std::string::npos ||
      !db_directory_decimal(suffix.substr(
          dash + 1, suffix.size() - dash - 1 - storage_end.size()))) {
    return (false);
  }

  const std::string size = suffix.substr(storage.size(), dash - storage.size());

  return (size == "DEFAULT" || size == "S32K" || size == "S64K" ||
          size == "S96K" || size == "S128K" || size == "S160K" ||
          size == "S192K" || size == "S224K");
}

/** Files left by previous versions of the directory lifecycle. */
static constexpr const char *db_legacy_identity = "IDENTITY";
static constexpr const char *db_legacy_initializing = "INITIALIZING";
static constexpr const char *db_legacy_deleting = "DELETING";
static constexpr const char *db_legacy_identity_temp = "IDENTITY.tmp";

/** Validate the whole pinned file set, never a broad filename prefix. */
static bool db_directory_owned_name(const std::string &name) {
  batch_seq_t first = 0;

  return (name == DB_DIRECTORY_LOCK || name == db_legacy_identity ||
          name == db_legacy_initializing || name == db_legacy_deleting ||
          name == db_legacy_identity_temp || name == STORE_FILE_NAME ||
          name == std::string(STORE_FILE_NAME) + ".wal" ||
          wal_parse_file_name(name.c_str(), first) ||
          wal_parse_incomplete_file_name(name, first));
}

/** Verify the name still denotes the locked inode before path-based work. */
db_status_t db_directory::same_directory() const {
  struct stat opened{}, named{};
  if (fstat(m_directory, &opened) != 0) {
    return (db_directory_error(errno, "cannot inspect locked directory"));
  }

  if (fstatat(m_parent, m_name.c_str(), &named, AT_SYMLINK_NOFOLLOW) != 0 ||
      !S_ISDIR(named.st_mode) || opened.st_dev != named.st_dev ||
      opened.st_ino != named.st_ino || opened.st_nlink == 0) {
    return (db_status_t(DB_LOCKED, "database directory changed while locking"));
  }

  return (DB_SUCCESS);
}

/** Lock the directory inode before LOCK, rejecting stale descriptors. */
db_status_t db_directory::lock(const char *path, bool create_lock,
                               std::string *absolute) {
  if (path == nullptr || path[0] == '\0') {
    return (db_status_t(DB_INVALID_ARGUMENT, "dir must be a path"));
  }

  std::string input(path);
  while (input.size() > 1 && input.back() == '/') {
    input.pop_back();
  }

  if (input == "/") {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "filesystem root is not a database directory"));
  }

  /* Work relative to the parent, so the name can be rechecked after locking. */
  size_t slash = input.find_last_of('/');
  std::string parent = slash == std::string::npos
                           ? "."
                           : input.substr(0, slash == 0 ? 1 : slash);
  m_name = slash == std::string::npos ? input : input.substr(slash + 1);
  const bool dot = m_name == "." || m_name == "..";
  char *resolved = realpath((dot ? input : parent).c_str(), nullptr);

  if (resolved == nullptr) {
    if (!create_lock && errno == ENOENT) {
      m_missing = true;

      return (DB_SUCCESS);
    }

    return (errno == ENOMEM
                ? DB_OUT_OF_MEMORY
                : db_directory_error(errno, "cannot resolve database path"));
  }

  parent = resolved;
  free(resolved);

  if (dot) {
    if (parent == "/") {
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "filesystem root is not a database directory"));
    }

    slash = parent.find_last_of('/');
    m_name = parent.substr(slash + 1);
    parent.resize(slash == 0 ? 1 : slash);
  }

  m_parent = os_file_openat(AT_FDCWD, parent.c_str(), O_RDONLY | O_DIRECTORY);
  if (m_parent == OS_FILE_CLOSED) {
    if (!create_lock && errno == ENOENT) {
      m_missing = true;

      return (DB_SUCCESS);
    }

    return (db_directory_error(errno, "cannot open database parent"));
  }

  struct stat initial{};
  if (fstatat(m_parent, m_name.c_str(), &initial, AT_SYMLINK_NOFOLLOW) != 0) {
    if (!create_lock && errno == ENOENT) {
      m_missing = true;

      return (DB_SUCCESS);
    }

    return (db_directory_error(errno, "cannot inspect database directory"));
  }

  if (!S_ISDIR(initial.st_mode)) {
    return (
        db_status_t(DB_INVALID_ARGUMENT,
                    "database path must be a directory, not a link or file"));
  }

  m_directory =
      os_file_openat(m_parent, m_name.c_str(), O_RDONLY | O_DIRECTORY);
  if (m_directory == OS_FILE_CLOSED) {
    if (!create_lock && errno == ENOENT) {
      m_missing = true;

      return (DB_SUCCESS);
    }

    return (db_directory_error(errno, "cannot open database directory"));
  }

  UT_DEBUG_SYNC("directory.before_lock");

  if (const db_status_t err = os_file_try_lock(m_directory); !err.ok()) {
    return (err);
  }

  if (const db_status_t err = same_directory(); !err.ok()) {
    return (err);
  }

  struct stat locked{};
  if (fstat(m_directory, &locked) != 0) {
    return (db_directory_error(errno, "cannot inspect locked directory"));
  }

  if (locked.st_dev != initial.st_dev || locked.st_ino != initial.st_ino) {
    return (db_status_t(DB_LOCKED,
                        "database directory was replaced while locking"));
  }

  if (absolute != nullptr) {
    *absolute = (parent == "/" ? parent : parent + "/") + m_name;
  }

  return (lock_file(create_lock));
}

/** Take the file lock while holding the directory inode lock. */
db_status_t db_directory::lock_file(bool create_lock) {
  m_lock = os_file_openat(m_directory, DB_DIRECTORY_LOCK,
                          O_RDWR | (create_lock ? O_CREAT : 0));
  if (m_lock == OS_FILE_CLOSED) {
    if (!create_lock && errno == ENOENT) {
      return (DB_SUCCESS);
    }

    return (db_directory_error(errno, "cannot open database LOCK"));
  }

  struct stat info{};
  if (fstat(m_lock, &info) != 0) {
    return (db_directory_error(errno, "cannot inspect database LOCK"));
  }

  if (!db_directory_regular(info) || info.st_size != 0) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "database LOCK is not an owned regular file"));
  }

  return (os_file_try_lock(m_lock));
}

/** An existing-only open must find storage before creating a missing LOCK. */
db_status_t db_directory::open(const char *path, bool create_if_missing,
                               std::string &absolute,
                               bool &initialize_catalog) {
  if (const db_status_t err = lock(path, create_if_missing, &absolute);
      !err.ok()) {
    return (err);
  }

  if (const db_status_t err =
          prepare_open(create_if_missing, initialize_catalog);
      !err.ok()) {
    return (err);
  }

  return (m_lock == OS_FILE_CLOSED ? lock_file(true) : DB_SUCCESS);
}

/** Allow catalog creation only before the database has a published WAL. */
db_status_t db_directory::prepare_open(bool create_if_missing,
                                       bool &initialize_catalog) {
  initialize_catalog = false;

  std::vector<std::string> names;
  if (!m_missing) {
    if (const db_status_t err = db_directory_list(m_directory, names);
        !err.ok()) {
      return (err);
    }
  }

  if (std::binary_search(names.begin(), names.end(), db_legacy_deleting)) {
    return (db_status_t(
        DB_FENCED, "database deletion is in progress; finish DropDatabase"));
  }

  const bool has_store =
      std::binary_search(names.begin(), names.end(), STORE_FILE_NAME);
  const std::string duckdb_wal = std::string(STORE_FILE_NAME) + ".wal";
  const std::string duckdb_spill = std::string(STORE_FILE_NAME) + ".tmp";

  if (has_store) {
    struct stat storage{};
    if (const db_status_t err =
            db_directory_stat(m_directory, STORE_FILE_NAME, storage);
        !err.ok()) {
      return (err);
    }

    if (!db_directory_regular(storage) || storage.st_size == 0) {
      return (db_status_t(DB_CORRUPTION, "required storage file is invalid"));
    }
  }

  for (const std::string &name : names) {
    /* LOCK and leftovers of an older first Open carry no database state. */
    if (name == DB_DIRECTORY_LOCK || name == db_legacy_initializing ||
        name == db_legacy_identity_temp) {
      continue;
    }

    if (has_store && (name == STORE_FILE_NAME || name == duckdb_wal ||
                      name == duckdb_spill)) {
      continue;
    }

    /* A store with recovery files is a published database. */
    if (has_store) {
      return (DB_SUCCESS);
    }

    /* Recovery files without their store mean data was lost. */
    const dberr_t code = db_directory_owned_name(name) || name == duckdb_spill
                             ? DB_CORRUPTION
                             : DB_INVALID_ARGUMENT;

    return (db_status_t(code, "required storage file is missing"));
  }

  if (!has_store && !create_if_missing) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "database does not exist (create_if_missing is off)"));
  }

  /* An interrupted first Open can leave an empty DuckDB catalog. */
  initialize_catalog = create_if_missing;

  return (DB_SUCCESS);
}

/** List the open spill directory of entry; the caller closes it. */
db_status_t db_directory::manifest_spill(os_file_t spill,
                                         entry_t &entry) const {
  struct stat info{}, root{};
  if (fstat(spill, &info) != 0 || fstat(m_directory, &root) != 0) {
    return (db_directory_error(errno, "cannot inspect spill directory"));
  }

  if (info.st_dev != root.st_dev || info.st_dev != entry.m_stat.st_dev ||
      info.st_ino != entry.m_stat.st_ino) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "spill directory changed or crosses a filesystem"));
  }

  std::vector<std::string> children;
  if (const db_status_t err = db_directory_list(spill, children); !err.ok()) {
    return (err);
  }

  for (const std::string &child : children) {
    entry_t item;
    item.m_name = child;
    if (const db_status_t err = db_directory_stat(spill, child, item.m_stat);
        !err.ok()) {
      return (err);
    }

    if (!db_directory_spill_name(child) || !db_directory_regular(item.m_stat)) {
      return (
          db_status_t(DB_INVALID_ARGUMENT, "unowned spill entry: " + child));
    }

    entry.m_children.push_back(std::move(item));
  }

  return (DB_SUCCESS);
}

/** Validate the whole tree, spill children included, before any deletion. */
db_status_t db_directory::manifest(std::vector<entry_t> &entries) const {
  entries.clear();

  std::vector<std::string> names;
  if (const db_status_t err = db_directory_list(m_directory, names);
      !err.ok()) {
    return (err);
  }

  for (const std::string &name : names) {
    entry_t entry;
    entry.m_name = name;
    if (const db_status_t err =
            db_directory_stat(m_directory, name, entry.m_stat);
        !err.ok()) {
      return (err);
    }

    if (name == std::string(STORE_FILE_NAME) + ".tmp") {
      if (!S_ISDIR(entry.m_stat.st_mode)) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "spill path is not an owned directory"));
      }

      os_file_t spill =
          os_file_openat(m_directory, name.c_str(), O_RDONLY | O_DIRECTORY);
      if (spill == OS_FILE_CLOSED) {
        return (db_directory_error(errno, "cannot open spill directory"));
      }

      const db_status_t listed = manifest_spill(spill, entry);
      static_cast<void>(os_file_close(spill));

      if (!listed.ok()) {
        return (listed);
      }
    } else if (!db_directory_owned_name(name) ||
               !db_directory_regular(entry.m_stat)) {
      return (
          db_status_t(DB_INVALID_ARGUMENT, "unowned database entry: " + name));
    }

    entries.push_back(std::move(entry));
  }

  return (DB_SUCCESS);
}

/** Empty the open spill directory of entry; the caller closes it.
@param[in]  now  current inode of the directory name, rechecked on the open
                 descriptor */
db_status_t db_directory::remove_children(os_file_t directory,
                                          const entry_t &entry,
                                          const struct stat &now) {
  struct stat info{};
  if (fstat(directory, &info) != 0) {
    return (db_directory_error(errno, "cannot inspect spill directory"));
  }

  if (info.st_dev != now.st_dev || info.st_ino != now.st_ino) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "spill directory changed during deletion"));
  }

  for (const entry_t &item : entry.m_children) {
    if (const db_status_t err = remove_entry(directory, item); !err.ok()) {
      return (err);
    }
  }

  return (db_directory_sync(directory));
}

/** Recheck the preflight inode right before each descriptor-relative unlink. */
db_status_t db_directory::remove_entry(os_file_t parent, const entry_t &entry) {
  struct stat now{};
  if (const db_status_t err = db_directory_stat(parent, entry.m_name, now);
      !err.ok()) {
    return (err);
  }

  if (now.st_dev != entry.m_stat.st_dev || now.st_ino != entry.m_stat.st_ino ||
      (!S_ISDIR(now.st_mode) && !db_directory_regular(now))) {
    return (
        db_status_t(DB_INVALID_ARGUMENT,
                    "database entry changed during deletion: " + entry.m_name));
  }

  const bool directory = S_ISDIR(entry.m_stat.st_mode);
  if (directory) {
    os_file_t child =
        os_file_openat(parent, entry.m_name.c_str(), O_RDONLY | O_DIRECTORY);
    if (child == OS_FILE_CLOSED) {
      return (db_directory_error(errno, "cannot reopen spill directory"));
    }

    const db_status_t emptied = remove_children(child, entry, now);
    static_cast<void>(os_file_close(child));

    if (!emptied.ok()) {
      return (emptied);
    }
  }

  if (const db_status_t err = UT_DEBUG_FAULT("directory.drop.file");
      !err.ok()) {
    return (err);
  }

  if (unlinkat(parent, entry.m_name.c_str(), directory ? AT_REMOVEDIR : 0) !=
      0) {
    return (db_directory_error(
        errno, "cannot remove database entry '" + entry.m_name + "'"));
  }

  UT_DEBUG_SYNC("directory.drop.file_removed");

  return (UT_DEBUG_FAULT("directory.drop.after_file"));
}

/** Remove the name only while it denotes our locked inode, then sync parent. */
db_status_t db_directory::remove_directory() {
  if (!m_missing) {
    if (const db_status_t err = same_directory(); !err.ok()) {
      return (err);
    }

    if (const db_status_t err = UT_DEBUG_FAULT("directory.drop.remove_dir");
        !err.ok()) {
      return (err);
    }

    if (unlinkat(m_parent, m_name.c_str(), AT_REMOVEDIR) != 0) {
      return (db_directory_error(errno, "cannot remove database directory"));
    }
  }

  if (m_parent == OS_FILE_CLOSED) {
    return (DB_SUCCESS);
  }

  db_status_t synced = UT_DEBUG_FAULT("directory.drop.parent_sync");
  if (synced.ok()) {
    synced = db_directory_sync(m_parent);
  }

  return (synced.ok() || m_missing ? synced
                                   : synced.with_code(DB_OUTCOME_UNKNOWN));
}

/** Remove the main file durably before discarding its recovery files. */
db_status_t db_directory::remove_database() {
  if (m_missing) {
    return (remove_directory());
  }

  UT_DEBUG_SYNC("directory.drop.before_remove");
  if (const db_status_t err = same_directory(); !err.ok()) {
    return (err);
  }

  std::vector<entry_t> entries;
  if (const db_status_t err = manifest(entries); !err.ok()) {
    return (err);
  }

  if (entries.empty()) {
    return (remove_directory());
  }

  /* Every Open creates LOCK first and Drop removes it last, so a nonempty
  PulseTen directory always has one. Refuse to delete anything without it. */
  if (m_lock == OS_FILE_CLOSED) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "directory is not a PulseTen database: LOCK is "
                        "missing"));
  }

  const auto incomplete = [](const db_status_t &err) {
    return (err.with_code(DB_OUTCOME_UNKNOWN)
                .with_context("database removal incomplete or not durably "
                              "confirmed. Retry only if the path has not been "
                              "reused and cannot be reused during the retry"));
  };

  if (const db_status_t err = UT_DEBUG_FAULT("directory.drop.begin");
      !err.ok()) {
    return (err);
  }

  const auto storage = std::find_if(
      entries.begin(), entries.end(),
      [](const entry_t &entry) { return (entry.m_name == STORE_FILE_NAME); });

  if (storage != entries.end()) {
    if (const db_status_t err = remove_entry(m_directory, *storage);
        !err.ok()) {
      return (incomplete(err));
    }
  }

  /* A retry may see an earlier unlink that has not reached disk yet. */
  if (const db_status_t err = UT_DEBUG_FAULT("directory.drop.storage_sync");
      !err.ok()) {
    return (incomplete(err));
  }

  if (const db_status_t err = db_directory_sync(m_directory); !err.ok()) {
    return (incomplete(err));
  }

  UT_DEBUG_SYNC("directory.drop.storage_removed");
  for (const entry_t &entry : entries) {
    if (entry.m_name == STORE_FILE_NAME || entry.m_name == DB_DIRECTORY_LOCK) {
      continue;
    }

    if (const db_status_t err = remove_entry(m_directory, entry); !err.ok()) {
      return (incomplete(err));
    }
  }

  if (const db_status_t err = UT_DEBUG_FAULT("directory.drop.data_sync");
      !err.ok()) {
    return (incomplete(err));
  }

  if (const db_status_t err = db_directory_sync(m_directory); !err.ok()) {
    return (incomplete(err));
  }

  UT_DEBUG_SYNC("directory.drop.before_finish");
  if (const db_status_t err = manifest(entries); !err.ok()) {
    return (incomplete(err));
  }

  for (const entry_t &entry : entries) {
    if (entry.m_name != DB_DIRECTORY_LOCK) {
      return (incomplete(db_status_t(
          DB_INVALID_ARGUMENT, "database contents changed during deletion")));
    }
  }

  if (const db_status_t err = UT_DEBUG_FAULT("directory.drop.finish");
      !err.ok()) {
    return (incomplete(err));
  }

  if (unlinkat(m_directory, DB_DIRECTORY_LOCK, 0) != 0 && errno != ENOENT) {
    return (
        incomplete(db_directory_error(errno, "cannot remove database LOCK")));
  }

  UT_DEBUG_SYNC("directory.drop.empty");
  const db_status_t removed = remove_directory();

  return (removed.ok() ? removed : incomplete(removed));
}

/** Descriptor ownership supplies one cleanup boundary for all failures. */
db_status_t db_directory::drop(const char *path) {
  db_directory directory;
  if (const db_status_t err = directory.lock(path, false); !err.ok()) {
    return (err);
  }

  return (directory.remove_database());
}

/** Release every descriptor; safe to call more than once. */
void db_directory::close() {
  static_cast<void>(os_file_close(m_lock));
  static_cast<void>(os_file_close(m_directory));
  static_cast<void>(os_file_close(m_parent));
}
