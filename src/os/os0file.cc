/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * os0file.cc
 *    POSIX implementation of the file layer.
 *
 *-------------------------------------------------------------------------
 */

#include "os0file.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <new>

/** errno of the most recent failed call on this thread. */
static thread_local int os_errno = 0;

/** Record errno of a failed call.
@return DB_IO_ERROR */
static dberr_t os_fail() {
  os_errno = errno;

  return (DB_IO_ERROR);
}

/** fsync() retried on EINTR.
@param[in]  fd  descriptor
@return 0, or -1 with errno set */
static int os_fsync(int fd) {
  int ret;

  do {
    ret = fsync(fd);
  } while (ret != 0 && errno == EINTR);

  return (ret);
}

/** Generate a random nonzero 64-bit persistent identity.
@param[out]  id  identity, 0 on failure
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_random_id(uint64_t &id) {
  id = 0;
  while (id == 0) {
    int ret;
    do {
      ret = getentropy(&id, sizeof(id));
    } while (ret != 0 && errno == EINTR);
    if (ret != 0) {
      id = 0;

      return (os_fail());
    }
  }

  return (DB_SUCCESS);
}

/** Read the last recorded operating-system error for this thread.
@return errno of the most recent failed os_* call on this thread */
int os_last_errno() {
  return (os_errno);
}

/** Open or create a file.
@param[in]   path  file path
@param[in]   mode  OS_FILE_OPEN, OS_FILE_CREATE or OS_FILE_OPEN_OR_CREATE
@param[out]  file  open descriptor, or OS_FILE_CLOSED on failure
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_open(const char *path, os_file_create_t mode, os_file_t &file) {
  file = OS_FILE_CLOSED;

  int flags = O_RDWR | O_CLOEXEC;
  if (mode == OS_FILE_CREATE) {
    flags |= O_CREAT | O_EXCL;
  } else if (mode == OS_FILE_OPEN_OR_CREATE) {
    flags |= O_CREAT;
  }

  int fd;
  do {
    fd = open(path, flags, 0644);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return (os_fail());
  }

  file = fd;

  return (DB_SUCCESS);
}

/** Close a file.
@param[in,out]  file  descriptor; set to OS_FILE_CLOSED
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_close(os_file_t &file) {
  if (file == OS_FILE_CLOSED) {
    return (DB_SUCCESS);
  }

  /* Never retry close(): on EINTR the descriptor is already released. */
  const int ret = close(file);
  file = OS_FILE_CLOSED;

  return (ret == 0 ? DB_SUCCESS : os_fail());
}

/** Write exactly n bytes at offset.
@param[in]  file    descriptor
@param[in]  buf     data
@param[in]  n       number of bytes
@param[in]  offset  file offset
@return DB_SUCCESS or DB_IO_ERROR; on error a prefix may have been written */
dberr_t os_file_write(os_file_t file, const void *buf, size_t n,
                      uint64_t offset) {
  const char *p = static_cast<const char *>(buf);

  while (n > 0) {
    const ssize_t ret = pwrite(file, p, n, static_cast<off_t>(offset));

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }

      return (os_fail());
    }

    if (ret == 0) {
      errno = EIO;

      return (os_fail());
    }

    p += ret;
    n -= static_cast<size_t>(ret);
    offset += static_cast<uint64_t>(ret);
  }

  return (DB_SUCCESS);
}

/** Read exactly n bytes at offset.
@param[in]   file    descriptor
@param[out]  buf     destination
@param[in]   n       number of bytes
@param[in]   offset  file offset
@return DB_SUCCESS, or DB_IO_ERROR on failure or end of file */
dberr_t os_file_read(os_file_t file, void *buf, size_t n, uint64_t offset) {
  char *p = static_cast<char *>(buf);

  while (n > 0) {
    const ssize_t ret = pread(file, p, n, static_cast<off_t>(offset));

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }

      return (os_fail());
    }

    if (ret == 0) {
      /* End of file before n bytes. */
      errno = 0;

      return (os_fail());
    }

    p += ret;
    n -= static_cast<size_t>(ret);
    offset += static_cast<uint64_t>(ret);
  }

  return (DB_SUCCESS);
}

/** Make written data and the file size durable.
@param[in]  file  descriptor
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_flush(os_file_t file) {
  int ret;

#if defined(__APPLE__)
  do {
    ret = fcntl(file, F_FULLFSYNC);
  } while (ret != 0 && errno == EINTR);

  if (ret == 0) {
    return (DB_SUCCESS);
  }

  /* Some file systems do not support F_FULLFSYNC; fall back to fsync. */
  if (errno != ENOTSUP && errno != EINVAL) {
    return (os_fail());
  }

  ret = os_fsync(file);

#elif defined(__linux__)
  do {
    ret = fdatasync(file);
  } while (ret != 0 && errno == EINTR);

#else
  ret = os_fsync(file);
#endif

  return (ret == 0 ? DB_SUCCESS : os_fail());
}

/** Get the current size of a file.
@param[in]   file  descriptor
@param[out]  size  size in bytes
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_get_size(os_file_t file, uint64_t &size) {
  struct stat st;

  if (fstat(file, &st) != 0) {
    return (os_fail());
  }

  size = static_cast<uint64_t>(st.st_size);

  return (DB_SUCCESS);
}

/** Cut a file to the given size.
@param[in]  file  descriptor
@param[in]  size  new size in bytes
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_truncate(os_file_t file, uint64_t size) {
  int ret;

  do {
    ret = ftruncate(file, (off_t)size);
  } while (ret != 0 && errno == EINTR);

  return (ret == 0 ? DB_SUCCESS : os_fail());
}

/** Take an exclusive advisory lock on an open file without waiting.
@param[in]  file  descriptor
@return DB_SUCCESS, DB_LOCKED if held elsewhere, or DB_IO_ERROR */
dberr_t os_file_lock(os_file_t file) {
  int ret;

  do {
    ret = flock(file, LOCK_EX | LOCK_NB);
  } while (ret != 0 && errno == EINTR);

  if (ret == 0) {
    return (DB_SUCCESS);
  }

  if (errno == EWOULDBLOCK) {
    os_errno = errno;

    return (DB_LOCKED);
  }

  return (os_fail());
}

/** Remove a file; a missing file is not an error.
@param[in]  path  file path
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_delete_if_exists(const char *path) {
  if (unlink(path) != 0 && errno != ENOENT) {
    return (os_fail());
  }

  return (DB_SUCCESS);
}

/** Give a file a second name that must not exist yet, then remove the old
name.
@param[in]  from  current path
@param[in]  to    new path; must not exist
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_file_rename_noreplace(const char *from, const char *to) {
  /* link() fails with EEXIST instead of replacing the target. */
  if (link(from, to) != 0) {
    return (os_fail());
  }

  if (unlink(from) != 0) {
    return (os_fail());
  }

  return (DB_SUCCESS);
}

/** List the names of the entries of a directory, without "." and "..".
@param[in]   path   directory path
@param[out]  names  entry names
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_dir_list(const char *path, std::vector<std::string> &names) {
  names.clear();

  DIR *dir = opendir(path);
  if (dir == nullptr) {
    return (os_fail());
  }

  dberr_t err = DB_SUCCESS;

  for (;;) {
    errno = 0;
    struct dirent *entry = readdir(dir);

    if (entry == nullptr) {
      if (errno != 0) {
        err = os_fail();
      }
      break;
    }

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }

    names.push_back(entry->d_name);
  }

  closedir(dir);

  return (err);
}

/** Create a directory and make its entry durable by syncing the parent.
@param[in]  path  directory path
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_dir_create(const char *path) {
  /* Prepare the parent path before creating anything. Ignore trailing
  slashes when finding the directory entry's containing directory. */
  std::string parent = path;
  while (parent.size() > 1 && parent.back() == '/') {
    parent.pop_back();
  }
  const size_t slash = parent.find_last_of('/');
  if (slash == std::string::npos) {
    parent = ".";
  } else {
    parent.resize(slash == 0 ? 1 : slash);
  }

  if (mkdir(path, 0755) != 0) {
    if (errno != EEXIST) {
      return (os_fail());
    }

    /* Something exists there; it must be a directory. */
    struct stat st;
    if (stat(path, &st) != 0) {
      return (os_fail());
    }

    if (!S_ISDIR(st.st_mode)) {
      errno = ENOTDIR;

      return (os_fail());
    }
  }

  /* Sync even if it already existed: a previous create may have failed
  here, leaving a directory whose entry was not made durable. */
  return (os_dir_flush(parent.c_str()));
}

/** Resolve a directory before storing paths for later file operations.
@param[in]   path      existing directory path
@param[out]  resolved  absolute path; empty on failure
@return DB_SUCCESS, DB_INVALID_ARGUMENT, DB_OUT_OF_MEMORY or DB_IO_ERROR */
dberr_t os_dir_resolve(const char *path, std::string &resolved) {
  resolved.clear();
  if (path == nullptr || path[0] == '\0') {
    return (DB_INVALID_ARGUMENT);
  }

  char *absolute;
  do {
    absolute = realpath(path, nullptr);
  } while (absolute == nullptr && errno == EINTR);
  if (absolute == nullptr) {
    return (errno == ENOMEM ? DB_OUT_OF_MEMORY : os_fail());
  }

  resolved = absolute;
  free(absolute);

  return (DB_SUCCESS);
}

/** Make directory entries (created, renamed or removed files) durable.
@param[in]  path  directory path
@return DB_SUCCESS or DB_IO_ERROR */
dberr_t os_dir_flush(const char *path) {
  int fd;
  do {
    fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return (os_fail());
  }

  dberr_t err = DB_SUCCESS;
  if (os_fsync(fd) != 0) {
    err = os_fail();
  }

  os_file_t dir = fd;
  const dberr_t close_err = os_file_close(dir);

  if (err == DB_SUCCESS) {
    err = close_err;
  }

  return (err);
}
