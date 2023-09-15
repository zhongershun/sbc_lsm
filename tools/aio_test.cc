
#include <fcntl.h>            /* O_RDWR */
#include <inttypes.h>         /* uint64_t */
#include <linux/aio_abi.h>    /* for AIO types and constants */
#include <stdio.h>            /* for perror() */
#include <string.h>           /* memset() */
#include <sys/syscall.h>      /* for __NR_* definitions */
#include <unistd.h>           /* for syscall() */
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <errno.h>
#include <stddef.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include <random>
#include <fstream>
#include <queue>
#include <utility>


#include "util/heap.h"
#include "gtest/gtest.h"


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

  int SubmitTasks(IOTask **task, int task_num) {
    struct iocb *cbs[1024];
    for (int i = 0; i < task_num; i++) {
      cbs[i] = &task[i]->iocb_;
    }
    int ret = syscall(__NR_io_submit, ioctx_, task_num, cbs);

    if (ret != task_num) {
       if (ret < 0) perror("io_submit");
       else fprintf(stderr, "io_submit only submitted %d\n", ret);
    }
    io_event_cnt_ += ret;
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

ssize_t submit_pwrites(IOTask **iotasks, int task_num, IOContext *ioctx) {
  int64_t ret = -1;
  assert(iotask != nullptr);
  assert(ioctx != nullptr);
  ret = ioctx->SubmitTasks(iotasks, task_num);

  return ret;
}

ssize_t preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset, IOTask *iotask, IOContext *ioctx) {
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

ssize_t pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset, IOTask *iotask, IOContext *ioctx) {
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

int fsync(int fd, IOTask *iotask, IOContext *ioctx) {
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

}

namespace {

static inline int64_t GetUnixTimeUs() {
  struct timeval tp;
  gettimeofday(&tp, nullptr);
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

}


static int kAlignSize = 512;
static int kIoSize = 4096;
static int kIoCount = 4096;

void AsyncWriteSingle(int64_t i=1) {
  std::string file_ = "aio_test_file_single";
  int fd_ = -1;
  char *write_buf_ = nullptr;
  char *read_buf_ = nullptr;

  fd_ = open(file_.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
  assert(fd_ > 0);

  if (kIoSize < kAlignSize) {
    kIoSize = kAlignSize;
  }

  if(posix_memalign((void **)&write_buf_, kAlignSize, kIoSize * kIoCount) != 0) {
    std::cout << "Alloc mem failed!\n";
    abort();
  }


  struct io_event events[64];
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 100000000;  // this thread will be blocked at most 100 milliseconds
  memset(events, 0, sizeof(events));

  TAIO::IOTask *iotask = new TAIO::IOTask();
  TAIO::IOContext *ioctx = new TAIO::IOContext();
  int64_t offset = i * kIoSize;
  char *wbuf = &write_buf_[offset];

  ioctx->Setup(64);
    
  auto write_start = GetUnixTimeUs();
  int ret = submit_pwrite(fd_, wbuf, kIoSize, offset, iotask, ioctx);

  while (true) {
    ret = ioctx->GetEvents(events, 1, 1, &ts);

    if (ret < 0) {
      printf("io_getevents error: %d\n", ret);
      break;
    }
    if (ret > 0) {
      // res2 代表写入了多少数据
      ((TAIO::io_callback_t *)(events[0].data))((struct iocb *)events[0].obj, events[0].res, events[0].res2);
      printf("result, res2: %lld, res: %lld, task IO size: %ld\n", 
        events[0].res2, events[0].res, iotask->FinishedIOSize());
      ret = events[0].res;
      break;
    }
  }
  std::cout << "Write:" << kIoSize << " Bytes, " << GetUnixTimeUs() - write_start << " us\n";
  assert(ret == kIoSize);
  ioctx->Destroy();

  free(write_buf_);
  close(fd_);
  delete iotask;
  delete ioctx;
}

// 一个一个发IO请求
void AsyncWriteMultiSyscall(int io_num=1) {
  std::string file_ = "aio_test_file_single";
  int fd_ = -1;
  char *write_buf_ = nullptr;
  char *read_buf_ = nullptr;

  io_num = std::min(io_num, 64);

  fd_ = open(file_.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
  assert(fd_ > 0);

  if (kIoSize < kAlignSize) {
    kIoSize = kAlignSize;
  }

  if(posix_memalign((void **)&write_buf_, kAlignSize, kIoSize * kIoCount) != 0) {
    std::cout << "Alloc write buf mem failed!\n";
    abort();
  }
  if(posix_memalign((void **)&read_buf_, kAlignSize, kIoSize * kIoCount) != 0) {
    std::cout << "Alloc read buf mem failed!\n";
    abort();
  }

  for (int i = 0; i < kIoSize * kIoCount; ++i) {
    (write_buf_)[i] = ('a' + i%26);
  }

  struct io_event events[64];
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 100000000;  // this thread will be blocked at most 100 milliseconds
  memset(events, 0, sizeof(events));

  TAIO::IOTask *iotasks[1024];
  TAIO::IOContext *ioctx = new TAIO::IOContext();

  
  char *wbuf = write_buf_;

  ioctx->Setup(100);
  
  auto write_start = GetUnixTimeUs();
  int ret = 0;
  for (int i = 0; i < io_num; i++) {
    int64_t offset = i * kIoSize;
    iotasks[i] = new TAIO::IOTask();
    ret = submit_pwrite(fd_, wbuf+offset, kIoSize, offset, iotasks[i], ioctx);
  }
  auto submit_finished = GetUnixTimeUs();
  while (true) {
    ret = ioctx->GetEvents(events, 0, 64, &ts);

    if (ret < 0) {
      printf("io_getevents error: %d\n", ret);
      break;
    }
    if (ret > 0) {
      for (struct io_event *e = &events[0]; e < &events[ret]; ++e) {
        ((TAIO::io_callback_t *)(e->data))((struct iocb *)e->obj, e->res, e->res2);
      }
    }
    int io_finished = 0;
    for (int i = 0; i < io_num; i++)
    {
      if (iotasks[i]->IsDone())
      {
        io_finished++;
      }
    }
    if(io_finished == io_num){
      break;
    }
    
  }
  auto write_finished = GetUnixTimeUs();

  std::cout << "Write:" << kIoSize * io_num << " Bytes, " << write_finished - write_start << " us, Syscall dur: " <<  submit_finished - write_start << "us\n";


  TAIO::IOTask *iotask = new TAIO::IOTask();
  char *rbuf = &read_buf_[0];
  ret = submit_pread(fd_, rbuf, kIoSize*io_num, 0, iotask, ioctx);
  auto read_start = GetUnixTimeUs();
  while (true) {
    ret = ioctx->GetEvents(events, 1, 1, &ts);

    if (ret < 0) {
      printf("io_getevents error: %d\n", ret);
      break;
    }
    if (ret > 0) {
      // res2 代表写入了多少数据
      ((TAIO::io_callback_t *)(events[0].data))((struct iocb *)events[0].obj, events[0].res, events[0].res2);
      printf("Read finished, res2: %lld, res: %lld, task IO size: %ld\n", 
        events[0].res2, events[0].res, iotask->FinishedIOSize());
      ret = events[0].res;
      break;
    }
  }
  std::cout << "Read:" << ret << " Bytes, " << GetUnixTimeUs() - read_start << " us\n";

  for (int i = 0; i < ret; i++)
  {
    if(wbuf[i] != rbuf[i]) {
      std::cout << i << ", " << wbuf[i] << ", " << rbuf[i] << "\n";
      break;
    }
  }

  ASSERT_EQ(ret, io_num*kIoSize);
  ASSERT_EQ(memcmp(wbuf, rbuf, ret), 0);
  

  ioctx->Destroy();
  free(write_buf_);
  close(fd_);
}


// 一次把所有的IO请求发完
void AsyncWriteMultiSyscallBatch(int io_num=1) {
  std::string file_ = "aio_test_file_single";
  int fd_ = -1;
  char *write_buf_ = nullptr;
  char *read_buf_ = nullptr;

  io_num = std::min(io_num, 64);

  system(("rm -rf " + file_).c_str());

  fd_ = open(file_.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
  assert(fd_ > 0);

  if (kIoSize < kAlignSize) {
    kIoSize = kAlignSize;
  }

  if(posix_memalign((void **)&write_buf_, kAlignSize, kIoSize * kIoCount) != 0) {
    std::cout << "Alloc write buf mem failed!\n";
    abort();
  }
  if(posix_memalign((void **)&read_buf_, kAlignSize, kIoSize * kIoCount) != 0) {
    std::cout << "Alloc read buf mem failed!\n";
    abort();
  }

  for (int i = 0; i < kIoSize * kIoCount; ++i) {
    (write_buf_)[i] = ('a' + i%26);
  }

  struct io_event events[64];
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 100000000;  // this thread will be blocked at most 100 milliseconds
  memset(events, 0, sizeof(events));

  TAIO::IOTask *iotasks[1024];
  TAIO::IOContext *ioctx = new TAIO::IOContext();

  char *wbuf = write_buf_;

  ioctx->Setup(100);
  
  auto write_start = GetUnixTimeUs();
  int ret = 0;
  for (int i = 0; i < io_num; i++) {
    int64_t offset = i * kIoSize;
    iotasks[i] = new TAIO::IOTask();
    iotasks[i]->PrepPwrite(fd_, wbuf+offset, kIoSize, offset);
    // ret = submit_pwrites(fd_, wbuf+offset, kIoSize, offset, iotasks[i], ioctx);
  }
  ret = TAIO::submit_pwrites(iotasks, io_num, ioctx);
  auto submit_finished = GetUnixTimeUs();
  while (true) {
    ret = ioctx->GetEvents(events, 0, 64, &ts);

    if (ret < 0) {
      printf("io_getevents error: %d\n", ret);
      break;
    }
    if (ret > 0) {
      for (struct io_event *e = &events[0]; e < &events[ret]; ++e) {
        ((TAIO::io_callback_t *)(e->data))((struct iocb *)e->obj, e->res, e->res2);
      }
    }
    int io_finished = 0;
    for (int i = 0; i < io_num; i++)
    {
      if (iotasks[i]->IsDone())
      {
        io_finished++;
      }
    }
    if(io_finished == io_num){
      break;
    }
    
  }
  auto write_finished = GetUnixTimeUs();

  std::cout << "Write:" << kIoSize * io_num << " Bytes, " << write_finished - write_start << " us, Syscall dur: " <<  submit_finished - write_start << "us\n";


  TAIO::IOTask *iotask = new TAIO::IOTask();
  char *rbuf = &read_buf_[0];
  ret = submit_pread(fd_, rbuf, kIoSize*io_num, 0, iotask, ioctx);
  auto read_start = GetUnixTimeUs();
  while (true) {
    ret = ioctx->GetEvents(events, 1, 1, &ts);

    if (ret < 0) {
      printf("io_getevents error: %d\n", ret);
      break;
    }
    if (ret > 0) {
      // res2 代表写入了多少数据
      ((TAIO::io_callback_t *)(events[0].data))((struct iocb *)events[0].obj, events[0].res, events[0].res2);
      printf("Read finished, res2: %lld, res: %lld, task IO size: %ld\n", 
        events[0].res2, events[0].res, iotask->FinishedIOSize());
      ret = events[0].res;
      break;
    }
  }
  std::cout << "Read:" << ret << " Bytes, " << GetUnixTimeUs() - read_start << " us\n";

  for (int i = 0; i < ret; i++)
  {
    if(wbuf[i] != rbuf[i]) {
      std::cout << i << ", " << wbuf[i] << ", " << rbuf[i] << "\n";
      break;
    }
  }

  ASSERT_EQ(ret, io_num*kIoSize);
  ASSERT_EQ(memcmp(wbuf, rbuf, ret), 0);
  

  ioctx->Destroy();
  free(write_buf_);
  close(fd_);
}


void AsyncWriteMultiSyscallMultiFileBatch(int io_num=1, int file_num = 1) {
  std::vector<std::string> file_list;
  std::vector<int> fd_list;
  std::vector<int64_t> offset_list;

  char *write_buf_ = nullptr;
  char *read_buf_ = nullptr;

  io_num = std::min(io_num, 64);

  for (int i = 0; i < file_num; i++) {
    file_list.emplace_back("aio_test_file_" + std::to_string(i));
    offset_list.emplace_back(0);
    system(("rm -rf " + file_list.back()).c_str());

    auto fd_ = open(file_list.back().c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
    fd_list.emplace_back(fd_);
    assert(fd_ > 0);
  }

  if (kIoSize < kAlignSize) {
    kIoSize = kAlignSize;
  }

  if(posix_memalign((void **)&write_buf_, kAlignSize, kIoSize * kIoCount) != 0) {
    std::cout << "Alloc write buf mem failed!\n";
    abort();
  }
  if(posix_memalign((void **)&read_buf_, kAlignSize, kIoSize * kIoCount) != 0) {
    std::cout << "Alloc read buf mem failed!\n";
    abort();
  }

  for (int i = 0; i < kIoSize * kIoCount; ++i) {
    (write_buf_)[i] = ('a' + i%26);
  }

  struct io_event events[64];
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 100000000;  // this thread will be blocked at most 100 milliseconds
  memset(events, 0, sizeof(events));

  TAIO::IOTask *iotasks[1024];
  TAIO::IOContext *ioctx = new TAIO::IOContext();

  char *wbuf = write_buf_;

  ioctx->Setup(100);
  
  auto write_start = GetUnixTimeUs();
  int ret = 0;
  for (int i = 0; i < io_num; i++) {
    int64_t offset = i * kIoSize;
    iotasks[i] = new TAIO::IOTask();
    iotasks[i]->PrepPwrite(fd_list[i % file_num], wbuf+offset, kIoSize, offset_list[i % file_num]);
    offset_list[i % file_num] += kIoSize;
    // ret = submit_pwrites(fd_, wbuf+offset, kIoSize, offset, iotasks[i], ioctx);
  }
  ret = TAIO::submit_pwrites(iotasks, io_num, ioctx);
  auto submit_finished = GetUnixTimeUs();
  while (true) {
    ret = ioctx->GetEvents(events, 0, 64, &ts);

    if (ret < 0) {
      printf("io_getevents error: %d\n", ret);
      break;
    }
    if (ret > 0) {
      for (struct io_event *e = &events[0]; e < &events[ret]; ++e) {
        ((TAIO::io_callback_t *)(e->data))((struct iocb *)e->obj, e->res, e->res2);
      }
    }
    int io_finished = 0;
    for (int i = 0; i < io_num; i++)
    {
      if (iotasks[i]->IsDone())
      {
        io_finished++;
      }
    }
    if(io_finished == io_num){
      break;
    }
    
  }
  auto write_finished = GetUnixTimeUs();

  std::cout << "Write:" << kIoSize * io_num << " Bytes, " << write_finished - write_start << " us, Syscall dur: " <<  submit_finished - write_start << "us\n";

  TAIO::IOTask *iotask = new TAIO::IOTask();

  uint64_t read_size = 0;
  char *rbuf = &read_buf_[0];
  int off_read = 0;
  for (int i = 0; i < file_num; i++) {
    ret = submit_pread(fd_list[i], rbuf+off_read, offset_list[i], 0, iotask, ioctx);
    off_read += offset_list[i];
  }
  auto read_start = GetUnixTimeUs();
  while (true) {
    ret = ioctx->GetEvents(events, 0, 64, &ts);

    if (ret < 0) {
      printf("io_getevents error: %d\n", ret);
      break;
    }
    // if (ret > 0) {
    //   // res2 代表写入了多少数据
    //   ((TAIO::io_callback_t *)(events[0].data))((struct iocb *)events[0].obj, events[0].res, events[0].res2);
    //   printf("Read finished, res2: %lld, res: %lld, task IO size: %ld\n", 
    //     events[0].res2, events[0].res, iotask->FinishedIOSize());
    //   ret = events[0].res;
    //   break;
    // }
    if (ret > 0) {
      for (struct io_event *e = &events[0]; e < &events[ret]; ++e) {
        ((TAIO::io_callback_t *)(e->data))((struct iocb *)e->obj, e->res, e->res2);
      }
    }
    int io_finished = 0;
    size_t read_size_ = 0;
    for (int i = 0; i < io_num; i++) {
      if (iotasks[i]->IsDone()) {
        read_size_ += iotasks[i]->FinishedIOSize();
        io_finished++;
      }
    }
    if(io_finished == io_num){
      read_size = read_size_;
      break;
    }
  }
  std::cout << "Read:" << read_size << " Bytes, " << GetUnixTimeUs() - read_start << " us\n";

  for (int i = 0; i < ret; i++)
  {
    if(wbuf[i] != rbuf[i]) {
      std::cout << i << ", " << wbuf[i] << ", " << rbuf[i] << "\n";
      break;
    }
  }

  // ASSERT_EQ(ret, io_num*kIoSize);
  ASSERT_EQ(memcmp(wbuf, rbuf, ret), 0);
  

  ioctx->Destroy();
  free(write_buf_);
  for (auto &&fd : fd_list) {
    close(fd);
  }
}

namespace ROCKSDB_NAMESPACE {

void TestFastSort(size_t data_size, size_t value_size) {
  struct Node
  {
    Node() = default;
    Node(std::string &key, std::string &value) {
      k = key;
      v = value;
    }
    std::string k;
    std::string v;
    bool operator<(const Node& other) const {
        return k < other.k;
    }
  };

  auto WriteVectorToFile = [](const std::vector<Node>& data, const std::string& filename)
  {
      system(("rm -rf " + filename).c_str());
      std::ofstream file(filename, std::ios::binary);
      if (!file)
      {
          std::cerr << "Failed to open file for writing: " << filename << std::endl;
          return false;
      }

      for (const Node& node : data)
      {
          // 写入每个 Node 的数据
          file << node.k << " " << node.v << "\n";
          // file.write(node.k.c_str(), node.k.size() + 1); // 写入 key
          // file.write(node.v.c_str(), node.v.size() + 1); // 写入 value
      }

      file.close();
      return true;
  };

  auto ReadVectorFromFile = [](std::vector<Node>& data, const std::string& filename)
  {
    std::ifstream file(filename, std::ios::binary);
    if (!file)
    {
        std::cerr << "Failed to open file for reading: " << filename << std::endl;
        return false;
    }

    data.clear();

    while (!file.eof())
    {
        Node node;
        char buffer[1024]; // 假设 key 和 value 的最大长度为 1024 字符

        // 读取 key
        file >> node.k >> node.v;
        // file.getline(buffer, sizeof(buffer));
        // node.k = buffer;

        // // 读取 value
        // file.getline(buffer, sizeof(buffer));
        // node.v = buffer;

        if (!node.k.empty() && !node.v.empty())
        {
            data.push_back(node);
        }
    }

    file.close();
    return true;
  };
  
  // size_t value_size = 1<<10;
  size_t key_num = data_size / (10+value_size);
  std::string file_name = "atest_file.log";
  // std::vector<std::vector<Node>> kv_list(20);

  std::vector<Node> kv_list;

  std::default_random_engine gen_key;
  std::uniform_int_distribution<size_t> key_gen(0, key_num);

  char value[4096] = {'a'};
  memset(value, 'a', value_size);
  std::string value_temp(value);
  std::cout << "Value size: " << value_temp.size() << "\n";
  char buf[10];
  for (size_t i = 0; i < key_num; i++)
  {
    // std::string key = std::to_string(key_gen(gen_key));
    snprintf(buf, sizeof(buf), "%09ld", key_gen(gen_key));
    std::string key(buf);
    kv_list.emplace_back(Node(key, value_temp));
    // kv_list[i & kv_list.size()].emplace_back(Node(key, value_temp));
  }

  WriteVectorToFile(kv_list, file_name);

  kv_list.clear();

  auto start = GetUnixTimeUs();

  ReadVectorFromFile(kv_list, file_name);

  auto sort_start = GetUnixTimeUs();

  std::sort(kv_list.begin(), kv_list.end());

  auto sort_end = GetUnixTimeUs();

  WriteVectorToFile(kv_list, file_name + "_sorted");

  auto end = GetUnixTimeUs();

  std::cout << "Key count: " << key_num << "\n"
    << " Read dur: " << sort_start - start << " us\n"
    << " Sort dur: " << sort_end - sort_start << " us\n"
    << " Write dur: " << end - sort_end << " us\n\n";
}


void TestMergeSort(size_t data_size, size_t value_size, size_t file_num) {
  struct Node
  {
    Node() = default;
    Node(std::string &key, std::string &value) {
      k = key;
      v = value;
    }
    std::string k;
    std::string v;
    bool operator<(const Node& other) const {
        return k < other.k;
    }
  };

  auto WriteVectorToFile = [](const std::vector<Node>& data, const std::string& filename)
  {
      system(("rm -rf " + filename).c_str());
      std::ofstream file(filename, std::ios::binary);
      if (!file)
      {
          std::cerr << "Failed to open file for writing: " << filename << std::endl;
          return false;
      }

      for (const Node& node : data)
      {
          // 写入每个 Node 的数据
          file << node.k << " " << node.v << "\n";
          // file.write(node.k.c_str(), node.k.size() + 1); // 写入 key
          // file.write(node.v.c_str(), node.v.size() + 1); // 写入 value
      }

      file.close();
      return true;
  };


  auto WriteVectorToFileRef = [](const std::vector<Node*>& data, const std::string& filename)
  {
      system(("rm -rf " + filename).c_str());
      std::ofstream file(filename, std::ios::binary);
      if (!file)
      {
          std::cerr << "Failed to open file for writing: " << filename << std::endl;
          return false;
      }

      for (const Node* node : data)
      {
          // 写入每个 Node 的数据
          file << node->k << " " << node->v << "\n";
          // file.write(node.k.c_str(), node.k.size() + 1); // 写入 key
          // file.write(node.v.c_str(), node.v.size() + 1); // 写入 value
      }

      file.close();
      return true;
  };


  auto ReadVectorFromFile = [](std::vector<Node>& data, const std::string& filename)
  {
    std::ifstream file(filename, std::ios::binary);
    if (!file)
    {
        std::cerr << "Failed to open file for reading: " << filename << std::endl;
        return false;
    }

    data.clear();

    while (!file.eof())
    {
        Node node;
        char buffer[1024]; // 假设 key 和 value 的最大长度为 1024 字符

        // 读取 key
        file >> node.k >> node.v;
        // file.getline(buffer, sizeof(buffer));
        // node.k = buffer;

        // // 读取 value
        // file.getline(buffer, sizeof(buffer));
        // node.v = buffer;

        if (!node.k.empty() && !node.v.empty())
        {
            data.push_back(node);
        }
    }

    file.close();
    return true;
  };

  class MinNodeComparator {
  public:
    MinNodeComparator() = default;
    bool operator()(Node* a, Node* b) const {
      return a->k > b->k;
    }

  };
  
  // size_t value_size = 1<<10;
  size_t key_num = data_size / (10+value_size);
  std::string file_name = "atest_file_log_";
  std::vector<std::vector<Node>> kv_list(file_num);
  std::vector<Node*> kv_list_ans;

  BinaryHeap<Node*, MinNodeComparator> heap;
  // std::priority_queue<Node*, MinNodeComparator> ref;

  std::default_random_engine gen_key;
  std::uniform_int_distribution<size_t> key_gen(0, key_num);

  char value[4096] = {'a'};
  memset(value, 'a', value_size);
  std::string value_temp(value);
  std::cout << "Value size: " << value_temp.size() << "\n";
  char buf[10];
  for (size_t i = 0; i < key_num; i++)
  {
    // std::string key = std::to_string(key_gen(gen_key));
    snprintf(buf, sizeof(buf), "%09ld", i);
    std::string key(buf);
    kv_list[i % file_num].emplace_back(Node(key, value_temp));
  }

  for (size_t i = 0; i < file_num; i++) {
    WriteVectorToFile(kv_list[i], file_name + std::to_string(i));
  }

  kv_list.clear();
  kv_list.resize(file_num);

  auto start = GetUnixTimeUs();

  for (size_t i = 0; i < file_num; i++) {
    ReadVectorFromFile(kv_list[i], file_name + std::to_string(i));
  }

  auto sort_start = GetUnixTimeUs();

  std::vector<std::vector<Node>::iterator> iter_list;
  for (auto &&i : kv_list) {
    iter_list.emplace_back(i.begin());
  }

  bool finish = false;
  while (!finish) {
    finish = true;
    for (size_t i = 0; i < file_num; i++) {
      if(iter_list[i] != kv_list[i].end()) {
        heap.push(&(*iter_list[i]));
        finish = false;
        // std::cout << "Push " << iter_list[i]->k << "\n";
        iter_list[i]++;
      }
    }
    while (heap.size()) {
      kv_list_ans.emplace_back(heap.top());
      // std::cout << "Pop " << heap.top()->k << "\n";
      heap.pop();
      finish = false;
    }
  }
  
  auto sort_end = GetUnixTimeUs();

  WriteVectorToFileRef(kv_list_ans, file_name + "ans");
  

  auto end = GetUnixTimeUs();

  std::cout << "Key count: " << key_num << "\n"
    << " Read dur: " << sort_start - start << " us\n"
    << " Sort dur: " << sort_end - sort_start << " us\n"
    << " Write dur: " << end - sort_end << " us\n\n";
}


void TestMergeSortPriority(size_t data_size, size_t value_size, size_t file_num) {
  struct Node
  {
    Node() = default;
    Node(std::string &key, std::string &value) {
      k = key;
      v = value;
    }
    std::string k;
    std::string v;
    bool operator<(const Node& other) const {
        return k > other.k;
    }
  };

  auto WriteVectorToFile = [](const std::vector<Node>& data, const std::string& filename)
  {
      system(("rm -rf " + filename).c_str());
      std::ofstream file(filename, std::ios::binary);
      if (!file)
      {
          std::cerr << "Failed to open file for writing: " << filename << std::endl;
          return false;
      }

      for (const Node& node : data)
      {
          // 写入每个 Node 的数据
          file << node.k << " " << node.v << "\n";
          // file.write(node.k.c_str(), node.k.size() + 1); // 写入 key
          // file.write(node.v.c_str(), node.v.size() + 1); // 写入 value
      }

      file.close();
      return true;
  };


  auto WriteVectorToFileRef = [](const std::vector<Node*>& data, const std::string& filename)
  {
      system(("rm -rf " + filename).c_str());
      std::ofstream file(filename, std::ios::binary);
      if (!file)
      {
          std::cerr << "Failed to open file for writing: " << filename << std::endl;
          return false;
      }

      for (const Node* node : data)
      {
          // 写入每个 Node 的数据
          file << node->k << " " << node->v << "\n";
          // file.write(node.k.c_str(), node.k.size() + 1); // 写入 key
          // file.write(node.v.c_str(), node.v.size() + 1); // 写入 value
      }

      file.close();
      return true;
  };


  auto ReadVectorFromFile = [](std::vector<Node>& data, const std::string& filename)
  {
    std::ifstream file(filename, std::ios::binary);
    if (!file)
    {
        std::cerr << "Failed to open file for reading: " << filename << std::endl;
        return false;
    }

    data.clear();

    while (!file.eof())
    {
        Node node;
        char buffer[1024]; // 假设 key 和 value 的最大长度为 1024 字符

        // 读取 key
        file >> node.k >> node.v;
        // file.getline(buffer, sizeof(buffer));
        // node.k = buffer;

        // // 读取 value
        // file.getline(buffer, sizeof(buffer));
        // node.v = buffer;

        if (!node.k.empty() && !node.v.empty())
        {
            data.push_back(node);
        }
    }

    file.close();
    return true;
  };
  
  // size_t value_size = 1<<10;
  size_t key_num = data_size / (10+value_size);
  std::string file_name = "atest_file_log_";
  std::vector<std::vector<Node>> kv_list(file_num);
  std::vector<Node*> kv_list_ans;

  std::priority_queue<Node*> heap;

  std::default_random_engine gen_key;
  std::uniform_int_distribution<size_t> key_gen(0, key_num);

  char value[4096] = {'a'};
  memset(value, 'a', value_size);
  std::string value_temp(value);
  std::cout << "Value size: " << value_temp.size() << "\n";
  char buf[10];
  for (size_t i = 0; i < key_num; i++) {
    // std::string key = std::to_string(key_gen(gen_key));
    snprintf(buf, sizeof(buf), "%09ld", i);
    std::string key(buf);
    kv_list[i % file_num].emplace_back(Node(key, value_temp));
  }

  for (size_t i = 0; i < file_num; i++) {
    WriteVectorToFile(kv_list[i], file_name + std::to_string(i));
  }

  kv_list.clear();
  kv_list.resize(file_num);

  auto start = GetUnixTimeUs();

  for (size_t i = 0; i < file_num; i++) {
    ReadVectorFromFile(kv_list[i], file_name + std::to_string(i));
  }

  auto sort_start = GetUnixTimeUs();

  std::vector<std::vector<Node>::iterator> iter_list;
  for (auto &&i : kv_list) {
    iter_list.emplace_back(i.begin());
  }

  bool finish = false;
  while (!finish) {
    finish = true;
    for (size_t i = 0; i < file_num; i++) {
      if(iter_list[i] != kv_list[i].end()) {
        heap.push(&(*iter_list[i]));
        finish = false;
        // std::cout << "Push " << iter_list[i]->k << "\n";
        iter_list[i]++;
      }
    }
    while (heap.size()) {       
      kv_list_ans.emplace_back(heap.top());
      std::cout << "Pop " << heap.top()->k << "\n";
      heap.pop();
      finish = false;
    }
  }
  
  auto sort_end = GetUnixTimeUs();

  WriteVectorToFileRef(kv_list_ans, file_name + "ans");
  

  auto end = GetUnixTimeUs();

  std::cout << "Key count: " << key_num << "\n"
    << " Read dur: " << sort_start - start << " us\n"
    << " Sort dur: " << sort_end - sort_start << " us\n"
    << " Write dur: " << end - sort_end << " us\n\n";
}


void TestVectorQueue() {
  struct KVPair {
    Slice k;
    Slice v;
  };

  std::vector<KVPair> queue_vector;
  std::queue<KVPair> queue;

  queue_vector.reserve(1024);

  size_t push_num = 1000;

  auto start = GetUnixTimeUs();

  for (size_t i = 0; i < push_num; i++)
  {
    Slice a, b;
    queue_vector.push_back({a, b});
  }

  auto mid = GetUnixTimeUs();

  for (size_t i = 0; i < push_num; i++)
  {
    Slice a, b;
    queue.push({a, b});
  }

  auto end = GetUnixTimeUs();


  std::cout << "Key count: " << push_num << "\n"
    << " Vector dur: " << mid - start << " us\n"
    << " Queue dur: " << end - mid << " us\n";

}

}

int main(int argc, char **argv) {
  for (int i = 0; i < argc; i++)
  {
    std::cout << argv[i] << "\n";
  }
  // int io_num = 10;
  // if(argc > 1) {
  //   io_num = std::stoi(argv[1]);
  // }
  
  // for (size_t v_size = 64; v_size <= 4096; v_size<<=2)
  // {
  //   ROCKSDB_NAMESPACE::TestMergeSort(1ll << 30, v_size, 20);
  // }

  ROCKSDB_NAMESPACE::TestVectorQueue();
  
  return 0;
}