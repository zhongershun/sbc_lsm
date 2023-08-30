
#include<iostream>
#include<vector>
#include<string>
#include<thread>
#include<unistd.h>

#include "util/random.h"
#include "util/lock_free_queue.h"

namespace ROCKSDB_NAMESPACE {

struct Node{
  Node(uint64_t i):i_(i){}
  uint64_t i_;
};

void Insert(LockFreeQueue<Node> *queue, std::atomic<uint64_t> *sum, int idx, int num) {
  Random rnd(idx);
  uint64_t sum_t = 0;
  for(int i = 0; i < num; i++) {
    auto k = rnd.Uniform(10000);
    *sum += k;
    sum_t += k;
    auto node = new Node(k);
    auto s = queue->push(node);
    if(!s) {
      std::cout << "Push failed: " << idx << ", " << k << "\n";
      abort();
    }
  }
  // std::cout << "Sum " << idx << " : " << sum_t << "\n";
}

void CheckSum(LockFreeQueue<Node> &queue, std::atomic<uint64_t> &sum_ans) {
  uint64_t sum = 0;
  Node *t = nullptr;
  while ((queue.pop(t))) {
    sum += t->i_;
    delete t;
  }
  if(sum != sum_ans) {
    std::cout << "Sum: " << sum << ", Sum ans: " << sum_ans << "\n";
    abort();
  }
}

void TestLockFreeBasic() {
  uint64_t thread_num = 100;
  uint64_t thread_op = 1000;
  std::atomic<uint64_t> sum = 0;

  std::vector<std::thread> threads;
  rocksdb::LockFreeQueue<rocksdb::Node> queue(1024*1024);
  for (uint64_t i = 0; i < thread_num; i++) {
    threads.emplace_back(rocksdb::Insert, &queue, &sum, i, thread_op);
  }

  for (auto &&t : threads) {
    t.join();
  }

  if(queue.get_length() != (thread_num * thread_op)) {
    std::cout << "Queue length: " << queue.get_length() << ", Item num: " << (thread_num * thread_op) << "\n";
    abort();
  }
  rocksdb::CheckSum(queue, sum);
  std::cout << "Lock free queue test passed.\n";
}

void Inc(std::atomic<uint64_t> *sum, size_t op_cnt){
  for (size_t i = 0; i < op_cnt; i++) {
    uint64_t t = sum->fetch_add(1);
    std::cout << t << " ";
  }
}

void TestFetchAdd() {
  uint64_t thread_op = 10;
  uint64_t thread_num = 10;
  std::atomic<uint64_t> sum = 0;
  std::vector<std::thread> threads;
  for (uint64_t i = 0; i < thread_num; i++) {
    threads.emplace_back(rocksdb::Inc, &sum,thread_op);
  }

  for (auto &&t : threads) {
    t.join();
  }
}

}

int main()
{
  rocksdb::TestLockFreeBasic();
  return 0;
}