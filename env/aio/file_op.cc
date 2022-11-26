/*
 *   Copyright (c) 2011-2019, Meituan Dianping. All Rights Reserved.
 *   Author xingyong
 *   xingyong@meituan.com
 */

#include "file_op.h"
#include "io_context.h"

#include <errno.h>
#include <stddef.h>
#include <assert.h>

namespace TAIO{

ssize_t submit_pread(int fd, void *buf, size_t count, off_t offset, IOTask *iotask, IOContext *ioctx) {
  int64_t ret = -1;
  iotask->PrepPread(fd, buf, count, offset);
  assert(iotask != nullptr);
  assert(ioctx != nullptr);
  ret = ioctx->Submit(iotask);

  return ret;
}

ssize_t submit_pwrite(int fd, const void *buf, size_t count, off_t offset, IOTask *iotask, IOContext *ioctx) {
  int64_t ret = -1;
  iotask->PrepPwrite(fd, buf, count, offset);
  assert(iotask != nullptr);
  assert(ioctx != nullptr);
  ret = ioctx->Submit(iotask);

  return ret;
}

ssize_t co_preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset, IOTask *iotask, IOContext *ioctx) {
  int64_t ret = -1;
  iotask->PrepPreadv(fd, iov, iovcnt, offset);
  assert(iotask != nullptr);
  assert(ioctx != nullptr);

  if (ioctx->Submit(iotask) > 0) {
    iotask->Wait();

    if (iotask->ErrorCode() != 0) {
      errno = iotask->ErrorCode();
    } else {
      ret = iotask->FinishedIOSize();
    }
  }
  return ret;
}

ssize_t co_pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset, IOTask *iotask, IOContext *ioctx) {
  int64_t ret = -1;
  iotask->PrepPwritev(fd, iov, iovcnt, offset);
  assert(iotask != nullptr);
  assert(ioctx != nullptr);

  if (ioctx->Submit(iotask) > 0) {
    iotask->Wait();

    if (iotask->ErrorCode() != 0) {
      errno = iotask->ErrorCode();
    } else {
      ret = iotask->FinishedIOSize();
    }
  }
  return ret;
}

int co_fsync(int fd, IOTask *iotask, IOContext *ioctx) {
  int ret = -1;
  iotask->PrepFsync(fd);
  assert(iotask != nullptr);
  assert(ioctx != nullptr);

  if (ioctx->Submit(iotask) > 0) {
    iotask->Wait();

    if (iotask->ErrorCode() != 0) {
      errno = iotask->ErrorCode();
    } else {
      ret = 0;
    }
  }
  return ret;
}

// int co_fdatasync(int fd) {
//   int ret = -1;
//   IOTask *iotask = GetCurrentIOTask();
//   iotask->PrepFdsync(fd);
//   IOContext *ioctx = Processer::GetCurrentIOContext();
//   BLD_ASSERT(ioctx != nullptr);

//   if (ioctx->Submit(iotask) > 0) {
//     iotask->Wait();

//     if (iotask->ErrorCode() != 0) {
//       errno = iotask->ErrorCode();
//     } else {
//       ret = 0;
//     }
//   }
//   return ret;
// }

}