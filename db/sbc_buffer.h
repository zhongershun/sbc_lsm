#pragma once

#include <cinttypes>
#include <vector>

#include "rocksdb/db.h"
#include "util/lock_free_queue.h"
#include "memory/arena.h"
#include "db/dbformat.h"
// #include "db/compaction/compaction_job.h"

namespace ROCKSDB_NAMESPACE
{
class CompactionJob;

struct KeyValueNode {
  char *key;
  char *value;
  ParsedInternalKey ikey;
  size_t key_size;
  size_t value_size;
  Status input_status;
};

class SBCKeyValueBuffer {
 public:

  SBCKeyValueBuffer(LockFreeQueue<KeyValueNode> *queue):queue_(queue), from_sbc_buffer_(true) {}

  SBCKeyValueBuffer(size_t size):from_sbc_buffer_(false){
    queue_ = new LockFreeQueue<KeyValueNode>(size);

  }

  ~SBCKeyValueBuffer() {
    if(!from_sbc_buffer_) {
      delete queue_;
    }
  }

  Status AddKeyValue(const Slice &key, const Slice &value, const ParsedInternalKey &ikey) {
    size_t alloc_size = key.size() + value.size() + sizeof(KeyValueNode);
    auto addr = allocator_.Allocate(alloc_size);
    if(addr == nullptr) {
      return Status::MemoryLimit("AddKeyValue alloc memory failed");
    }

    char* key_addr = addr;
    char* value_addr = addr + key.size();
    KeyValueNode* kv_node = new (addr + key.size() + value.size()) KeyValueNode;

    memcpy(key_addr, key.data(), key.size());
    memcpy(value_addr, value.data(), value.size());

    kv_node->key = key_addr;
    kv_node->value = value_addr;

    kv_node->ikey.sequence = ikey.sequence;
    kv_node->ikey.type = ikey.type;
    kv_node->ikey.user_key = Slice(key_addr, key.size() - kNumInternalBytes);

    kv_node->key_size = key.size();
    kv_node->value_size = value.size();
    kv_node->input_status = Status::OK();

    auto s = queue_->push(kv_node);
    if(s) {
      return Status::OK();
    } else {
      return Status::NoSpace("Key value queue full");
    }
  }

  // NOTE: 手动释放Node内存
  KeyValueNode* PopKeyValue() {
    KeyValueNode *t;
    if (queue_->pop(t)) {
      return t;
    } else {
      return nullptr;
    }
    
  }

  size_t size() {
    return queue_->get_length();
  }

 private:
  LockFreeQueue<KeyValueNode> *queue_;
  Arena allocator_;
  bool from_sbc_buffer_;
};


class SBCBuffer
{
public:
  // SBCBuffer() = default;
  explicit SBCBuffer(uint64_t buffer_size, int worker_num = 1):left_size_(buffer_size), queue_(1024*1024) {
  }
  ~SBCBuffer() {

  }

  SBCBuffer(SBCBuffer&& job) = delete;
  SBCBuffer(const SBCBuffer& job) = delete;
  SBCBuffer& operator=(const SBCBuffer& job) = delete;

  SBCKeyValueBuffer* GetSBCKeyValueBuffer() {
    return new SBCKeyValueBuffer(&queue_);
  }

private:
  uint64_t left_size_;
  std::vector<CompactionJob*> sbc_job_list_;
  LockFreeQueue<KeyValueNode> queue_;
};


} // namespace ROCKSDB_NAMESPACE
