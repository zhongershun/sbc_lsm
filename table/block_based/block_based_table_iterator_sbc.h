//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_based_table_reader_impl.h"
#include "table/block_based/block_prefetcher.h"
#include "table/block_based/reader_common.h"

#include <queue>

// #define DISP_OFF

namespace ROCKSDB_NAMESPACE {
// NOTE: kForward only
// Iterates over the contents of BlockBasedTable.

constexpr size_t kKVBufferSize = 1024;
constexpr size_t kKeyBufferSize = 32 * kKVBufferSize;

class KVQueue {
public:
  KVQueue():start_(0), end_(0){}

  bool empty() const {
    return start_ == end_;
  }

  size_t size() const {
    return end_ - start_;
  }

  inline const std::pair<Slice, Slice>& front() const {
    if(start_ < end_)
      return kv_queue_[start_];
    return kv_queue_[kKVBufferSize];
  }

  inline bool push(std::pair<rocksdb::Slice, rocksdb::Slice> &&kv) {
    if(end_ < kKVBufferSize) {
      kv_queue_[end_] = kv;
      end_++;
      return true;
    }
    return false;
  }

  inline void pop() {
    start_++;
    if(start_ >= end_) {
      start_ = end_ = 0;
    }
  }

private:
  std::pair<Slice, Slice> kv_queue_[kKVBufferSize+1];
  size_t start_;
  size_t end_;
};

class BlockBasedTableIteratorSBC : public InternalIteratorBase<Slice> {
  // compaction_readahead_size: its value will only be used if for_compaction =
  // true
  // @param read_options Must outlive this iterator.
 public:
  BlockBasedTableIteratorSBC(
      const BlockBasedTable* table, const ReadOptions& read_options,
      const InternalKeyComparator& icomp,
      std::unique_ptr<InternalIteratorBase<IndexValue>>&& index_iter,
      bool check_filter, bool need_upper_bound_check,
      const SliceTransform* prefix_extractor, TableReaderCaller caller,
      size_t compaction_readahead_size = 0, bool allow_unprepared_value = false)
      : index_iter_(std::move(index_iter)),
        table_(table),
        read_options_(read_options),
        icomp_(icomp),
        user_comparator_(icomp.user_comparator()),
        pinned_iters_mgr_(nullptr),
        prefix_extractor_(prefix_extractor),
        lookup_context_(caller),
        block_prefetcher_(
            compaction_readahead_size,
            table_->get_rep()->table_options.initial_auto_readahead_size),
        allow_unprepared_value_(allow_unprepared_value),
        block_iter_points_to_real_block_(false),
        check_filter_(check_filter),
        need_upper_bound_check_(need_upper_bound_check),
        async_read_in_progress_(false),
        kv_queue_(),
        queue_size_(kKVBufferSize),
        block_start_offset_(0) {
          key_buf_.reserve(kKeyBufferSize + 100);
          scan_len = read_options_.scan_len;
          aligned_buf_.reserve(10);
        }

  ~BlockBasedTableIteratorSBC() {
    for (auto &&i : aligned_buf_) {
      i.reset();
    }
  }

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void Next() final override;
  bool NextAndGetResult(IterateResult* result) override;

  void SeekForPrev(const Slice& target) override {
    abort();
  }
  void SeekToLast() override {
    abort();
  }
  void Prev() override {
    abort();
  }
  bool FromCompSST() const override {
    return from_comp_sst_;
  }
  void SetFromCompSST(bool from_comp_sst) override {
    from_comp_sst_ = from_comp_sst;
  }
  bool Valid() const override {
    return !is_out_of_bound_ &&
           (kv_queue_.size() > 0 || is_at_first_key_from_index_ ||
            (block_iter_points_to_real_block_ && block_iter_.Valid()));
  }
  Slice key() const override {
    assert(Valid());
    if (!kv_queue_.empty()) {
      if(kv_queue_.front().first.data_[0] != 'u') {
        // std::cout << "KVQueue size: " << kv_queue_.size()  << " Key " << kv_queue_.front().first.data() << "\n";
      }
      return kv_queue_.front().first;
    } else {
      return block_iter_.key();
    }
  }
  Slice user_key() const override {
    assert(Valid());
    if (is_at_first_key_from_index_) {
      return ExtractUserKey(index_iter_->value().first_internal_key);
    } else {
      return ExtractUserKey(key());
    }
  }
  bool PrepareValue() override {
    assert(Valid());

    return !is_at_first_key_from_index_;
  }
  Slice value() const override {
    // PrepareValue() must have been called.
    assert(!is_at_first_key_from_index_);
    assert(Valid());

    if (!kv_queue_.empty()) {
      return kv_queue_.front().second;
    } else {
      return block_iter_.value();
    }
  }
  Status status() const override {
    // Prefix index set status to NotFound when the prefix does not exist
    if (!index_iter_->status().ok() && !index_iter_->status().IsNotFound()) {
      return index_iter_->status();
    } else if (block_iter_points_to_real_block_) {
      return block_iter_.status();
    } else if (async_read_in_progress_) {
      return Status::TryAgain();
    } else {
      return Status::OK();
    }
  }

  inline IterBoundCheck UpperBoundCheckResult() override {
    if (is_out_of_bound_) {
      return IterBoundCheck::kOutOfBound;
    } else if (block_upper_bound_check_ ==
               BlockUpperBound::kUpperBoundBeyondCurBlock) {
      assert(!is_out_of_bound_);
      return IterBoundCheck::kInbound;
    } else {
      return IterBoundCheck::kUnknown;
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  bool IsKeyPinned() const override {
    // Our key comes either from block_iter_'s current key
    // or index_iter_'s current *value*.
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           ((is_at_first_key_from_index_ && index_iter_->IsValuePinned()) ||
            (block_iter_points_to_real_block_ && block_iter_.IsKeyPinned()));
  }
  bool IsValuePinned() const override {
    assert(!is_at_first_key_from_index_);
    assert(Valid());

    // BlockIter::IsValuePinned() is always true. No need to check
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           block_iter_points_to_real_block_;
  }

  void ResetDataIter() {
    if (block_iter_points_to_real_block_) {
      if (pinned_iters_mgr_ != nullptr && pinned_iters_mgr_->PinningEnabled()) {
        block_iter_.DelegateCleanupsTo(pinned_iters_mgr_);
      }
      block_iter_.Invalidate(Status::OK());
      // delete block_;
      block_iter_points_to_real_block_ = false;
      // std::cout << "ResetBlock: " << index_iter_->value().handle.offset() << "\n\n";
    }
    block_upper_bound_check_ = BlockUpperBound::kUnknown;
  }

  void SavePrevIndexValue() {
    if (block_iter_points_to_real_block_) {
      // Reseek. If they end up with the same data block, we shouldn't re-fetch
      // the same data block.
      prev_block_offset_ = index_iter_->value().handle.offset();
    }
  }

  void GetReadaheadState(ReadaheadFileInfo* readahead_file_info) override {
    if (block_prefetcher_.prefetch_buffer() != nullptr &&
        read_options_.adaptive_readahead) {
      block_prefetcher_.prefetch_buffer()->GetReadaheadState(
          &(readahead_file_info->data_block_readahead_info));
      if (index_iter_) {
        index_iter_->GetReadaheadState(readahead_file_info);
      }
    }
  }

  void SetReadaheadState(ReadaheadFileInfo* readahead_file_info) override {
    if (read_options_.adaptive_readahead) {
      block_prefetcher_.SetReadaheadState(
          &(readahead_file_info->data_block_readahead_info));
      if (index_iter_) {
        index_iter_->SetReadaheadState(readahead_file_info);
      }
    }
  }

  void LoadDataFromFile() {
    IOOptions opts;
    char scratch;
    table_->get_rep()->file->PrepareIOOptions(read_options_, opts);

    block_start_offset_ = index_iter_->value().handle.offset();
    
    size_t n = table_->get_rep()->last_key_block_offset 
      + table_->get_rep()->last_key_offset_in_block + 5000 
      - block_start_offset_;
    if(read_options_.readahead_size) {
      n = read_options_.readahead_size;
    }

    aligned_buf_.emplace_back(rocksdb::AlignedBuf());
    table_->get_rep()->file->Read(opts, block_start_offset_, n, &data_block_buffer_, 
      &scratch, &aligned_buf_.back(), read_options_.rate_limiter_priority);
    
    scratch_ = data_block_buffer_.data_;
    left_ = data_block_buffer_.size();
#ifdef DISP_OFF
    std::cout << "LoadFile from: " << block_start_offset_ << " size: " 
      << n << " Scan len: " << scan_len << "\n";
#endif
  }

  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter_;

 private:
  enum class IterDirection {
    kForward,
    kBackward,
  };
  // This enum indicates whether the upper bound falls into current block
  // or beyond.
  //   +-------------+
  //   |  cur block  |       <-- (1)
  //   +-------------+
  //                         <-- (2)
  //  --- <boundary key> ---
  //                         <-- (3)
  //   +-------------+
  //   |  next block |       <-- (4)
  //        ......
  //
  // When the block is smaller than <boundary key>, kUpperBoundInCurBlock
  // is the value to use. The examples are (1) or (2) in the graph. It means
  // all keys in the next block or beyond will be out of bound. Keys within
  // the current block may or may not be out of bound.
  // When the block is larger or equal to <boundary key>,
  // kUpperBoundBeyondCurBlock is to be used. The examples are (3) and (4)
  // in the graph. It means that all keys in the current block is within the
  // upper bound and keys in the next block may or may not be within the uppder
  // bound.
  // If the boundary key hasn't been checked against the upper bound,
  // kUnknown can be used.
  enum class BlockUpperBound {
    kUpperBoundInCurBlock,
    kUpperBoundBeyondCurBlock,
    kUnknown,
  };

  const BlockBasedTable* table_;
  const ReadOptions& read_options_;
  const InternalKeyComparator& icomp_;
  UserComparatorWrapper user_comparator_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  DataBlockIter block_iter_;
  const SliceTransform* prefix_extractor_;
  uint64_t prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;

  BlockPrefetcher block_prefetcher_;

  const bool allow_unprepared_value_;
  // True if block_iter_ is initialized and points to the same block
  // as index iterator.
  bool block_iter_points_to_real_block_;
  // See InternalIteratorBase::IsOutOfBound().
  bool is_out_of_bound_ = false;
  // How current data block's boundary key with the next block is compared with
  // iterate upper bound.
  BlockUpperBound block_upper_bound_check_ = BlockUpperBound::kUnknown;
  // True if we're standing at the first key of a block, and we haven't loaded
  // that block yet. A call to PrepareValue() will trigger loading the block.
  bool is_at_first_key_from_index_ = false;
  bool check_filter_;
  // TODO(Zhongyi): pick a better name
  bool need_upper_bound_check_;

  bool async_read_in_progress_;

  bool from_comp_sst_ = false;

  KVQueue kv_queue_;
  size_t queue_size_;
  std::string key_buf_;
  Slice data_block_buffer_;
  size_t block_start_offset_;  // 这是读到aligned_buf_里第一个block在文件里的偏移地址
  const char* scratch_ = nullptr; // 指向datablock Buffer的指针，block在文件里的真实偏移 - block_start_offset_才是在buffer里的偏移
  std::vector<AlignedBuf> aligned_buf_;   // 所有读到内存里的data block的数据，
  Block* block_ = nullptr;
  size_t scan_len = INT64_MAX;
  size_t left_ = 0;

  void LoadKVFromBlock();

  void FillKVQueue();

  // If `target` is null, seek to first.
  void SeekImpl(const Slice* target, bool async_prefetch);

  void InitDataBlock();
  void AsyncInitDataBlock(bool is_first_pass);
  void FindKeyForward();
  void FindBlockForward();
  void CheckOutOfBound();

  // Check if data block is fully within iterate_upper_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update data_block_within_upper_bound_ accordingly.
  void CheckDataBlockWithinUpperBound();

  bool CheckPrefixMayMatch(const Slice& ikey, IterDirection direction) {
    if (need_upper_bound_check_ && direction == IterDirection::kBackward) {
      // Upper bound check isn't sufficient for backward direction to
      // guarantee the same result as total order, so disable prefix
      // check.
      return true;
    }
    if (check_filter_ && !table_->PrefixRangeMayMatch(
                             ikey, read_options_, prefix_extractor_,
                             need_upper_bound_check_, &lookup_context_)) {
      // TODO remember the iterator is invalidated because of prefix
      // match. This can avoid the upper level file iterator to falsely
      // believe the position is the end of the SST file and move to
      // the first key of the next file.
      ResetDataIter();
      return false;
    }
    return true;
  }
};
}  // namespace ROCKSDB_NAMESPACE
