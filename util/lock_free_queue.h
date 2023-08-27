
#pragma once

#include <atomic>
#include <string>
#include <iostream>
#include <list>
#include <stdio.h>

#include "rocksdb/status.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

#define GCC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if GCC_VERSION > 40704
#define ATOMIC_LOAD(x) __atomic_load_n((x), __ATOMIC_SEQ_CST)
#define ATOMIC_STORE(x, v) __atomic_store_n((x), (v), __ATOMIC_SEQ_CST)
#else
#define __COMPILER_BARRIER() asm volatile("" ::: "memory")
#define ATOMIC_LOAD(x) ({__COMPILER_BARRIER(); *(x);})
#define ATOMIC_STORE(x, v) ({__COMPILER_BARRIER(); *(x) = v; __sync_synchronize(); })
#endif

#define MEM_BARRIER() __sync_synchronize()
#define PAUSE() asm("pause\n")
#define ATOMIC_FAA(val, addv) __sync_fetch_and_add((val), (addv))
#define ATOMIC_AAF(val, addv) __sync_add_and_fetch((val), (addv))
#define ATOMIC_SET(val, newv) __sync_lock_test_and_set((val), (newv))
#define ATOMIC_VCAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
#define ATOMIC_BCAS(val, cmpv, newv) __sync_bool_compare_and_swap((val), (cmpv), (newv))


template <class T>
class LockfreeQueue {
  struct Node {
    T *volatile v;
  };
 public:
  LockfreeQueue(size_t size):data_(nullptr), consumer_(0), producer_(0), size_(0) {
    data_ = (Node*)malloc(size * sizeof(Node));
    memset(data_, 0, size * sizeof(Node));
    consumer_ = 0;
    producer_ = 0;
    size_ = size;
  }
  ~LockfreeQueue();
  void destroy();
  Status push(T *ptr);
  T *pop();
  int64_t get_free() const;
  int64_t get_length() const;
  int64_t get_capacity() const;
 private:
  Node *data_;
  volatile uint64_t consumer_;
  volatile uint64_t producer_;
  volatile int64_t size_;
};

template <class T>
LockfreeQueue<T>::~LockfreeQueue() {
  destroy();
}

template <class T>
void LockfreeQueue<T>::destroy() {
  if (nullptr != data_) {
    free(data_);
    data_ = nullptr;
  }

  size_ = 0;
  producer_ = 0;
  consumer_ = 0;
}

template <class T>
Status LockfreeQueue<T>::push(T *ptr) {
  Status ret = Status::OK();

  if (nullptr == ptr) {
    ret = Status::Corruption("Input nullptr");
  } else {
    volatile uint64_t oldv = producer_;
    volatile uint64_t cmpv = oldv;
    ret = Status::Corruption("Queue full");

    while (oldv < (consumer_ + size_)) {
      if (cmpv != (oldv = ATOMIC_VCAS(&producer_, cmpv, oldv + 1))) {
        cmpv = oldv;
      } else {
        ret = Status::OK();
        break;
      }
    }

    if (Status::OK() == ret) {
      int64_t index = oldv % size_;
      T *old = NULL;

      while (old != ATOMIC_VCAS(&(data_[index].v), old, ptr)) {
        PAUSE();
      }
    }
  }

  return ret;
}

template <class T>
T *LockfreeQueue<T>::pop() {
  T *volatile ret = nullptr;

  {
    volatile uint64_t oldv = consumer_;
    volatile uint64_t cmpv = oldv;
    int64_t index = 0;
    bool bret = false;

    while (oldv < producer_) {
      index = oldv % size_;

      if (nullptr == ATOMIC_LOAD(&(data_[index].v))) {
        PAUSE();
        oldv = ATOMIC_LOAD(&consumer_);
        cmpv = oldv;
      } else if (cmpv != (oldv = ATOMIC_VCAS(&consumer_, cmpv, oldv + 1))) {
        cmpv = oldv;
      } else {
        bret = true;
        break;
      }
    }

    if (bret) {
      while (nullptr == (ret = (T * volatile) ATOMIC_SET(&(data_[index].v), (T * volatile)nullptr))) {
        PAUSE();
      }
    }
  }

  return ret;
}

template <class T>
int64_t LockfreeQueue<T>::get_free() const {
  return get_capacity() - get_length();
}

template <class T>
int64_t LockfreeQueue<T>::get_length() const {
  return producer_ - consumer_;
}

template <class T>
int64_t LockfreeQueue<T>::get_capacity() const {
  return size_;
}


}