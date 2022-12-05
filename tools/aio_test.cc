
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


int main(int argc, char **argv) {
  for (int i = 0; i < argc; i++)
  {
    std::cout << argv[i] << "\n";
  }
  int io_num = 10;
  if(argc > 1) {
    io_num = std::stoi(argv[1]);
  }
  
  AsyncWriteMultiSyscallBatch(io_num);
  return 0;
}