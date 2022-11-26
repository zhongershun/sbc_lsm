/*
 *   Copyright (c) 2011-2019, Meituan Dianping. All Rights Reserved.
 *   Author xingyong
 *   xingyong@meituan.com
 */
#pragma once
#include <iostream>
#include <thread>
#include <atomic>
#include <env/aio/io_context.h>
#include <sys/uio.h> /* for iovec definitions */
#include <unistd.h>
// #include "liburing.h"
#include <libaio.h>

namespace TAIO{

typedef void io_callback_t(struct iocb *, int64_t, int64_t);

// IO operatoins in coroutine environment. Interface definitions are kept same
// as their corresponding posix interfaces. See manunal pages for help.
// NOTE: O_DIRECT is required for the file descriptor.
//
extern ssize_t submit_pread(int fd, void *buf, size_t count, off_t offset,
                            IOTask *iotask, IOContext *ioctx);

extern ssize_t submit_pwrite(int fd, const void *buf, size_t count,
                             off_t offset, IOTask *iotask, IOContext *ioctx);

extern ssize_t co_preadv(int fd, const struct ::iovec *iov, int iovcnt,
                         off_t offset, IOTask *iotask, IOContext *ioctx);

extern ssize_t co_pwritev(int fd, const struct ::iovec *iov, int iovcnt,
                          off_t offset, IOTask *iotask, IOContext *ioctx);

// In kernel 3.10, the file operations for IOCB_CMD_FSYNC and IOCB_CMD_FDSYNC
// are not implemented. If co_fsync() and co_fdatasync() are called, the
// returned value is -1 and errno is -EINVAL.
//
extern int co_fsync(int fd, IOTask *iotask, IOContext *ioctx);



}
