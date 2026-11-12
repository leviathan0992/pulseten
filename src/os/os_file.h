/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * os_file.h
 *    Thin POSIX file layer.
 *
 * Fallible operations return db_status_t. EINTR handling is operation-specific;
 * os_file_close() never retries close(). The last recorded operating-system
 * errno is kept per thread for diagnostics. File I/O bypasses user-space
 * buffering; successful writes become durable through os_file_sync().
 *
 *-------------------------------------------------------------------------
 */

#ifndef os_file_h
#define os_file_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ut_err.h"

/** Open file descriptor. */
typedef int os_file_t;

/** Value of an os_file_t that is not open. */
#define OS_FILE_CLOSED (-1)

/** How os_file_open() treats the path. */
enum os_file_create_t {
  /** Open an existing file for reading and writing. */
  OS_FILE_OPEN,

  /** Create a new file; fail if the path exists. */
  OS_FILE_CREATE,

  /** Open the file, creating it if it does not exist. */
  OS_FILE_OPEN_OR_CREATE
};

/** Open or create a file.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_file_open(const char *path, os_file_create_t mode,
                         os_file_t &file);

/** Close a file without retrying close(), even on EINTR. The descriptor is
released even when close reports an error.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_file_close(os_file_t &file);

/** Write exactly n bytes at offset.
@return DB_SUCCESS or DB_IO_ERROR; on error a prefix may have been written */
db_status_t os_file_write(os_file_t file, const void *buf, size_t n,
                          uint64_t offset);

/** Read exactly n bytes at offset.
@return DB_SUCCESS, or DB_IO_ERROR on failure or end of file */
db_status_t os_file_read(os_file_t file, void *buf, size_t n, uint64_t offset);

/** Make written data and the file size durable. Uses F_FULLFSYNC on macOS,
because fsync there does not flush the drive cache, and fdatasync on Linux.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_file_sync(os_file_t file);

/** Get the current size of a file.
@param[out]  size  size in bytes, or 0 on failure
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_file_get_size(os_file_t file, uint64_t &size);

/** Cut a file to the given size.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_file_truncate(os_file_t file, uint64_t size);

/** Take an exclusive advisory lock on an open file without waiting. The lock
belongs to this open file and is released when it is closed; a second open of
the same file, in this or another process, cannot take it.
@return DB_SUCCESS, DB_LOCKED if held elsewhere, or DB_IO_ERROR */
db_status_t os_file_lock(os_file_t file);

/** Remove a file; a missing file is not an error.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_file_delete_if_exists(const char *path);

/** Give a file a second name that must not exist yet, then remove the old
name. Unlike rename(), this never replaces an existing file.
@param[in]  to  new path; must not exist
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_file_rename_noreplace(const char *from, const char *to);

/** List the names of the entries of a directory, without "." and "..".
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_dir_list(const char *path, std::vector<std::string> &names);

/** Create a directory and make its entry durable by syncing the parent. An
existing directory also syncs its parent, so retrying a failed sync is safe.
The parent must exist.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_dir_create(const char *path);

/** Make directory entries (created, renamed or removed files) durable.
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_dir_sync(const char *path);

/** Resolve an existing directory to an absolute path, following symlinks.
The returned path keeps later file operations independent of chdir().
@param[out]  resolved  absolute path; empty on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_OUT_OF_MEMORY or DB_IO_ERROR */
db_status_t os_dir_resolve(const char *path, std::string &resolved);

/** Generate a random nonzero 64-bit UID.
@param[out]  uid  generated UID, 0 on failure
@return DB_SUCCESS or DB_IO_ERROR */
db_status_t os_generate_uid(uint64_t &uid);

#endif /* os_file_h */
