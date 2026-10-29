/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * os0file.h
 *    Thin POSIX file layer.
 *
 * Fallible operations return dberr_t. EINTR handling is operation-specific;
 * os_file_close() never retries close(). The last recorded operating-system
 * errno is kept per thread for diagnostics.
 *
 *-------------------------------------------------------------------------
 */

#ifndef os0file_h
#define os0file_h

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ut0err.h"

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
@param[in]   path  file path
@param[in]   mode  OS_FILE_OPEN, OS_FILE_CREATE or OS_FILE_OPEN_OR_CREATE
@param[out]  file  open descriptor, or OS_FILE_CLOSED on failure
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_open(const char *path, os_file_create_t mode, os_file_t &file);

/** Close a file without retrying close(), even on EINTR. The descriptor is
released even when close reports an error.
@param[in,out]  file  descriptor; set to OS_FILE_CLOSED
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_close(os_file_t &file);

/** Write exactly n bytes at offset.
@param[in]  file    descriptor
@param[in]  buf     data
@param[in]  n       number of bytes
@param[in]  offset  file offset
@return DB_SUCCESS or DB_IO_ERROR; on error a prefix may have been written */
dberr_t os_file_write(os_file_t file, const void *buf, size_t n,
                      uint64_t offset);

/** Read exactly n bytes at offset.
@param[in]   file    descriptor
@param[out]  buf     destination
@param[in]   n       number of bytes
@param[in]   offset  file offset
@return DB_SUCCESS, or DB_IO_ERROR on failure or end of file */
dberr_t os_file_read(os_file_t file, void *buf, size_t n, uint64_t offset);

/** Make written data and the file size durable. Uses F_FULLFSYNC on macOS,
because fsync there does not flush the drive cache, and fdatasync on Linux.
@param[in]  file  descriptor
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_flush(os_file_t file);

/** Get the current size of a file.
@param[in]   file  descriptor
@param[out]  size  size in bytes
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_get_size(os_file_t file, uint64_t &size);

/** Cut a file to the given size.
@param[in]  file  descriptor
@param[in]  size  new size in bytes
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_truncate(os_file_t file, uint64_t size);

/** Take an exclusive advisory lock on an open file without waiting. The lock
belongs to this open file and is released when it is closed; a second open of
the same file, in this or another process, cannot take it.
@param[in]  file  descriptor
@return DB_SUCCESS, DB_LOCKED if held elsewhere, or DB_IO_ERROR */
dberr_t os_file_lock(os_file_t file);

/** Remove a file; a missing file is not an error.
@param[in]  path  file path
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_delete_if_exists(const char *path);

/** Give a file a second name that must not exist yet, then remove the old
name. Unlike rename(), this never replaces an existing file.
@param[in]  from  current path
@param[in]  to    new path; must not exist
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_rename_noreplace(const char *from, const char *to);

/** List the names of the entries of a directory, without "." and "..".
@param[in]   path   directory path
@param[out]  names  entry names
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_dir_list(const char *path, std::vector<std::string> &names);

/** Create a directory and make its entry durable by syncing the parent. An
existing directory also syncs its parent, so retrying a failed sync is safe.
The parent must exist.
@param[in]  path  directory path
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_dir_create(const char *path);

/** Make directory entries (created, renamed or removed files) durable.
@param[in]  path  directory path
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_dir_flush(const char *path);

/** Resolve an existing directory to an absolute path, following symlinks.
The returned path keeps later file operations independent of chdir().
@param[in]   path      existing directory path
@param[out]  resolved  absolute path; empty on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_OUT_OF_MEMORY or DB_IO_ERROR */
dberr_t os_dir_resolve(const char *path, std::string &resolved);

/** Generate a random nonzero 64-bit persistent identity.
@param[out]  id  identity, 0 on failure
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_random_id(uint64_t &id);

/** Read the last recorded operating-system error for this thread.
@return errno of the most recent failed os_* call on this thread */
int os_last_errno();

#endif
