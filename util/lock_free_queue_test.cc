
#include<iostream>
#include<vector>
#include<string>
#include<thread>
#include<unistd.h>

#include "util/random.h"
#include "util/lock_free_queue.h"

namespace ROCKSDB_NAMESPACE {

struct Node{
  Node(int64_t i):i_(i){}
  int64_t i_;
};

void Insert(LockfreeQueue<Node> *queue, std::atomic<int64_t> *sum, int idx, int num) {
  Random rnd(idx);
  int64_t sum_t = 0;
  for(int i = 0; i < num; i++) {
    auto k = rnd.Uniform(10000);
    *sum += k;
    sum_t += k;
    auto node = new Node(k);
    auto s = queue->push(node);
    if(s != Status::OK()) {
      std::cout << "Push failed: " << idx << ", " << k << "\n";
      abort();
    }
  }
  // std::cout << "Sum " << idx << " : " << sum_t << "\n";
}

void CheckSum(LockfreeQueue<Node> &queue, std::atomic<int64_t> &sum_ans) {
  int64_t sum = 0;
  Node *t = nullptr;
  while ((t = queue.pop()) != nullptr) {
    sum += t->i_;
    delete t;
  }
  if(sum != sum_ans) {
    std::cout << "Sum: " << sum << ", Sum ans: " << sum_ans << "\n";
    abort();
  }
}

}


int main()
{
  int64_t queue_len = 100005;
  int64_t thread_num = 100;
  int64_t thread_op = 1000;
  std::atomic<int64_t> sum = 0;

  std::vector<std::thread> threads;
  rocksdb::LockfreeQueue<rocksdb::Node> queue(queue_len);
  for (int64_t i = 0; i < thread_num; i++) {
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

  return 0;
}