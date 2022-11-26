/*
 *   Copyright (c) 2011-2019, Meituan Dianping. All Rights Reserved.
 *   Author xingyong
 *   xingyong@meituan.com
 */
#pragma once

#include <fcntl.h>            /* O_RDWR */
#include <inttypes.h>         /* uint64_t */
#include <linux/aio_abi.h>    /* for AIO types and constants */
#include <stdio.h>            /* for perror() */
#include <string.h>           /* memset() */
#include <sys/syscall.h>      /* for __NR_* definitions */
#include <unistd.h>           /* for syscall() */
#include <thread>
#include <vector>

namespace TAIO{

typedef void io_callback_t(struct iocb *, int64_t, int64_t);

class IOContext;
class IOTask {
 public:
  IOTask() {
    Reset();
    // mutex_.set_name(__FUNCTION__);
    // mutex_.lock();
  }

  ~IOTask() {
    // mutex_.unlock();
  }

  void Reset() {
    memset(&iocb_, 0, sizeof(iocb_));
    iocb_.aio_data = (uint64_t)OnIODone;
    finished_io_size_ = 0;
    err_code_ = 0;
    done_ = false;
  }

  int64_t FinishedIOSize() const {
    return finished_io_size_;
  }

  int64_t ErrorCode() const {
    return err_code_;
  }

  void Wait() {
    // mutex_.lock();
  }

  void Finish() {
    // mutex_.unlock();
    done_ = true;
  }

  static void OnIODone(struct iocb *iocb, int64_t res, int64_t res2) {
    IOTask *iotask = (IOTask *)iocb;

    if (res2 != 0) {
      iotask->err_code_ = res2;
    } else if (res < 0) {
      iotask->err_code_ = -res;
    } else {
      iotask->finished_io_size_ = res;
    }

    iotask->Finish();
  }

  bool IsDone() const {
    return done_;
  }

  void PrepPread(int fd, const void *buf, size_t count, long long offset) {
    Reset();
    iocb_.aio_fildes = fd;
    iocb_.aio_lio_opcode = IOCB_CMD_PREAD;
    iocb_.aio_buf = (uint64_t)buf;
    iocb_.aio_nbytes = count;
    iocb_.aio_offset = offset;
  }

  void PrepPwrite(int fd, const void *buf, size_t count, long long offset) {
    Reset();
    iocb_.aio_fildes = fd;
    iocb_.aio_lio_opcode = IOCB_CMD_PWRITE;
    iocb_.aio_buf = (uint64_t)buf;
    iocb_.aio_nbytes = count;
    iocb_.aio_offset = offset;
  }

  void PrepPreadv(int fd, const struct ::iovec *iov, int iovcnt, long long offset) {
    Reset();
    iocb_.aio_fildes = fd;
    iocb_.aio_lio_opcode = IOCB_CMD_PREADV;
    iocb_.aio_buf = (uint64_t)iov;
    iocb_.aio_nbytes = iovcnt;
    iocb_.aio_offset = offset;
  }

  void PrepPwritev(int fd, const struct ::iovec *iov, int iovcnt, long long offset) {
    Reset();
    iocb_.aio_fildes = fd;
    iocb_.aio_lio_opcode = IOCB_CMD_PWRITEV;
    iocb_.aio_buf = (uint64_t)iov;
    iocb_.aio_nbytes = iovcnt;
    iocb_.aio_offset = offset;
  }

  void PrepFsync(int fd) {
    Reset();
    iocb_.aio_fildes = fd;
    iocb_.aio_lio_opcode = IOCB_CMD_FSYNC;
  }

  void PrepFdsync(int fd) {
    Reset();
    iocb_.aio_fildes = fd;
    iocb_.aio_lio_opcode = IOCB_CMD_FDSYNC;
  }

 private:
  friend class IOContext;
  struct iocb iocb_;
  // CoMutex mutex_;
  int64_t finished_io_size_;
  int64_t err_code_;
  bool done_;
};

class IOContext {
 public:
  IOContext() : ioctx_(0), io_event_cnt_(0) {}

  ~IOContext() { }

  // nr： 并发的io 事件个数
  int Setup(int nr) {
    return syscall(__NR_io_setup, nr, &ioctx_);
  }

  int Submit(IOTask *task) {
    struct iocb *iocb = &task->iocb_;
    int ret = syscall(__NR_io_submit, ioctx_, 1, &iocb);
    io_event_cnt_++;
    return ret;
  }

  int Cancel(IOTask *task, struct io_event *result) {
    return syscall(__NR_io_cancel, ioctx_, task->iocb_, result);
  }

  // 返回第min_nr个io事件到max_nr个io事件的状态
  int GetEvents(struct io_event *events, int min_nr, int max_nr, struct timespec *timeout) {
    int ret = syscall(__NR_io_getevents, ioctx_, min_nr, max_nr, events, timeout);
    if(ret > 0){
      io_event_cnt_-=ret;
// #ifndef NEDBUG
//       std::cout << io_event_cnt_ << "\n";
// #endif
    }
    return ret;
  }

  int GetIOCtxNum() const {
    return io_event_cnt_;
  }

  int Destroy() {
    return syscall(__NR_io_destroy, ioctx_);
  }

 private:
  aio_context_t ioctx_;
  std::atomic<int> io_event_cnt_ = 0;
};

class IOProcesser {
public:
  IOProcesser(std::vector<TAIO::IOContext*> *ioctx_list, int io_depth = 100):io_depth_(io_depth), stop_(false), 
    ioctx_list_(ioctx_list){
    // ioctx_->Setup(io_depth_);
    for (size_t i = 0; i < ioctx_list_->size(); i++) {
      (*ioctx_list_)[i]->Setup(io_depth_);
    }
    
    aio_callback_thread_ = std::thread(&IOProcesser::ProcessIO, this);
  }

  IOProcesser() = delete;

  ~IOProcesser() {
    stop_ = true;
    if (aio_callback_thread_.joinable()) {
      aio_callback_thread_.join();
    }

    if (ioctx_list_ != NULL) {
      for (size_t i = 0; i < ioctx_list_->size(); i++) {
        if ((*ioctx_list_)[i]->Destroy() != 0) {
          std::cout << "failed to destroy io context\n";
        }
        // std::cout << "Destroy io context" << i << "\n";
        (*ioctx_list_)[i]->~IOContext();
        delete (*ioctx_list_)[i];
        (*ioctx_list_)[i] = NULL;
      }
    }
  }

 private:
  void ProcessIO() {
    int cpu_core = 14;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_core, &cpuset);
    int state = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    struct io_event events[64];
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100000000;  // this thread will be blocked at most 100 milliseconds
    memset(events, 0, sizeof(events));

    while (!stop_) {
      for (size_t i = 0; i < ioctx_list_->size(); i++) {
        int r = (*ioctx_list_)[i]->GetEvents(events, 0, 64, &ts);

        if (r < 0) {
          // if (errno != EINTR) {
          //   std::cout << "failed to get events\n";
          // }
        } else if (r > 0) {
          for (struct io_event *e = &events[0]; e < &events[r]; ++e) {
            ((io_callback_t *)(e->data))((struct iocb *)e->obj, e->res, e->res2);
          }
        }
      }
    }
  }

  int io_depth_;
  bool stop_;
  std::vector<TAIO::IOContext *> *ioctx_list_;
  std::thread aio_callback_thread_;
};

}

