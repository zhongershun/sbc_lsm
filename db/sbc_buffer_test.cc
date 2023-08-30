#include<iostream>
#include<vector>
#include<string>
#include<thread>
#include<unistd.h>

#include "util/random.h"
#include "db/sbc_buffer.h"

namespace ROCKSDB_NAMESPACE {

void Push(SBCKeyValueBuffer *sbc_key_value_buffer, std::atomic<int64_t> *sum, size_t op_cnt, int idx) {
  std::string value = "12345";
  ParsedInternalKey ikey;
  Random rnd(idx);
  for(size_t i=0;i<op_cnt;i++) {
    auto t = rnd.Uniform(10000);
    sum->fetch_add(t);
    auto s = sbc_key_value_buffer->AddKeyValue(std::to_string(t), value, ikey);
    std::cout << "Push: " << t << "\n";
  }
}

void Pop(SBCKeyValueBuffer *sbc_key_value_buffer, std::atomic<int64_t> *sum, size_t op_cnt) {
  while(op_cnt) {
    KeyValueNode *kv_node = sbc_key_value_buffer->PopKeyValue();
    if(kv_node) {
      auto k = std::string(kv_node->key, kv_node->key_size); 
      std::cout << "Pop: " << k << "\n";
      sum->fetch_add(std::stoll(k));
      op_cnt--;
    }
  }
}

void TestConcurrentPushPop() {
  int64_t thread_num = 100;
  int64_t thread_op = 100;
  std::atomic<int64_t> sum = 0;
  std::atomic<int64_t> sum_ans = 0;

  SBCKeyValueBuffer kv_buffer(thread_op);
  std::vector<std::thread> threads;
  // for (int64_t i = 0; i < thread_num; i++) {
  //   threads.emplace_back(rocksdb::Insert, &queue, &sum, i, thread_op);
  // }
  threads.emplace_back(Push, &kv_buffer, &sum, thread_op, 0);
  // for (auto &&t : threads) {
  //   t.join();
  // }

  // threads.clear();
  threads.emplace_back(Pop, &kv_buffer, &sum_ans, thread_op);

  for (auto &&t : threads) {
    t.join();
  }
  if(sum != sum_ans) {
    std::cout << "Sum: " << sum << ", Sum ans: " << sum_ans << "\n";
    abort();
  }
  std::cout << "Lock free queue test passed.\n";

}

}

int main() {
  ROCKSDB_NAMESPACE::TestConcurrentPushPop();
  return 0;
}