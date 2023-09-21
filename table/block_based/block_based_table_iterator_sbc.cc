//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/block_based_table_iterator_sbc.h"
#include <iostream>
#include "block_based_table_iterator_sbc.h"

namespace ROCKSDB_NAMESPACE {

void BlockBasedTableIteratorSBC::SeekToFirst() { 
  Slice first_key = Slice(table_->get_rep()->first_key);
#if 0
  std::cout << "Seek(" << table_->get_rep()->sst_number_for_tracing() << ", "
    << table_->get_rep()->level_for_tracing() << ", "<< first_key.ToString() << ")\n";
#endif
  SeekImpl(&first_key, false); 
}

void BlockBasedTableIteratorSBC::Seek(const Slice& target) {
  if(user_comparator_.Compare(ExtractUserKey(target),
     table_->get_rep()->first_key) < 0) {
    Slice first_key = Slice(table_->get_rep()->first_key);
    SeekImpl(&first_key, true); 
  } else if(user_comparator_.Compare(ExtractUserKey(target),
     table_->get_rep()->last_key) > 0) {
    Slice last_key = Slice(table_->get_rep()->last_key);
    SeekImpl(&last_key, true);
    Next();
  } else {
    SeekImpl(&target, true);
  }
}

void BlockBasedTableIteratorSBC::SeekImpl(const Slice* target,
                                       bool async_prefetch) {
  bool is_first_pass = true;
  if (async_read_in_progress_) {
    AsyncInitDataBlock(false);
    is_first_pass = false;
  }

  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  if (target && !CheckPrefixMayMatch(*target, IterDirection::kForward)) {
    ResetDataIter();
    return;
  }

  bool need_seek_index = true;

  if (need_seek_index) {
    if (target) {
      index_iter_->Seek(*target);
    } else {
      index_iter_->SeekToFirst();
    }

    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  IndexValue v = index_iter_->value();
  const bool same_block = block_iter_points_to_real_block_ &&
                          v.handle.offset() == prev_block_offset_;
  
  // 一次读一堆数据上来
  IOOptions opts;
  char scratch;
  table_->get_rep()->file->PrepareIOOptions(read_options_, opts);
  block_start_offset_ = table_->get_rep()->first_key_start_block_offset;
  size_t n = table_->get_rep()->last_key_block_offset + table_->get_rep()->last_key_offset_in_block + 1024 - table_->get_rep()->first_key_start_block_offset;
  table_->get_rep()->file->Read(opts, block_start_offset_, n, &data_block_, &scratch, &aligned_buf_, read_options_.rate_limiter_priority);
  scratch_ = data_block_.data_;

  {
    // Need to use the data block.
    if (!same_block) {
      if (read_options_.async_io && async_prefetch) {
        if (is_first_pass) {
          AsyncInitDataBlock(is_first_pass);
        }
        if (async_read_in_progress_) {
          // Status::TryAgain indicates asynchronous request for retrieval of
          // data blocks has been submitted. So it should return at this point
          // and Seek should be called again to retrieve the requested block and
          // execute the remaining code.
          return;
        }
      } else {
        InitDataBlock();
      }
    } else {
      // When the user does a reseek, the iterate_upper_bound might have
      // changed. CheckDataBlockWithinUpperBound() needs to be called
      // explicitly if the reseek ends up in the same data block.
      // If the reseek ends up in a different block, InitDataBlock() will do
      // the iterator upper bound check.
      CheckDataBlockWithinUpperBound();
    }

    if (target) {
      block_iter_.Seek(*target);
    } else {
      block_iter_.SeekToFirst();
    }
    FindKeyForward();
    key_buf_.clear();
    LoadKVFromBlock();
  }

  CheckOutOfBound();

  if (target) {
    assert(!Valid() || icomp_.Compare(*target, key()) <= 0);
  }
}


void BlockBasedTableIteratorSBC::Next() {
  // assert(block_iter_points_to_real_block_);
  // block_iter_.Next();
  // FindKeyForward();
  // CheckOutOfBound();

  kv_queue_.pop();
  if(kv_queue_.empty()) {
    FillKVQueue();
  }
}

// 这会让block_iter_.Valid() = false
inline void BlockBasedTableIteratorSBC::LoadKVFromBlock() {
  if(block_iter_.Valid()) {
    Slice key = block_iter_.key();
    auto pos = key_buf_.size();
    key_buf_.append(key.data(), key.size());
    kv_queue_.push(std::make_pair(Slice(key_buf_.c_str() + pos, key.size()), block_iter_.value()));
    block_iter_.Next();
    block_iter_.PrefetchNextKey();
  } else {
    FindKeyForward();
  }
  CheckOutOfBound();
}

void BlockBasedTableIteratorSBC::FillKVQueue() {
  key_buf_.clear();
  while (block_iter_points_to_real_block_ && kv_queue_.size() < queue_size_) {
    LoadKVFromBlock();
  }
}

bool BlockBasedTableIteratorSBC::NextAndGetResult(IterateResult* result) {
  Next();
  bool is_valid = Valid();
  if (is_valid) {
    result->key = key();
    result->bound_check_result = UpperBoundCheckResult();
    result->value_prepared = !is_at_first_key_from_index_;
  } else {
#if false
  std::cout << "Iter finished (" << table_->get_rep()->sst_number_for_tracing() << ", "
    << table_->get_rep()->level_for_tracing() << ")\n";
  int t;
#endif
  }
  return is_valid;
}

void BlockBasedTableIteratorSBC::InitDataBlock() {
  BlockHandle data_block_handle = index_iter_->value().handle;
  if (!block_iter_points_to_real_block_ ||
      data_block_handle.offset() != prev_block_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetDataIter();
    }
    auto* rep = table_->get_rep();
    prefetch_64(scratch_ + data_block_handle.offset() - block_start_offset_ + data_block_handle.size() - sizeof(uint32_t));

    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    // Prefetch additional data for range scans (iterators).
    // Implicit auto readahead:
    //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
    // Explicit user requested readahead:
    //   Enabled from the very first IO when ReadOptions.readahead_size is set.
    block_prefetcher_.PrefetchIfNeeded(
        rep, data_block_handle, read_options_.readahead_size, is_for_compaction,
        /*no_sequential_checking=*/false, read_options_.rate_limiter_priority);
    Status s;
    // if(index_iter_->value().handle.offset() == 28449737) {
    //   system("pause");
    // }
    table_->NewDataBlockIteratorFromBuffer<DataBlockIter>(
        read_options_, data_block_handle, &block_iter_, BlockType::kData,
        /*get_context=*/nullptr, &lookup_context_,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s, 
        scratch_ + data_block_handle.offset() - block_start_offset_,
        block_);
    // The last data block
    if(data_block_handle.offset() == rep->last_key_block_offset) {
      block_iter_.UpdateEndOffset(rep->last_key_offset_in_block);
    }
    block_iter_points_to_real_block_ = true;
    // std::cout << "InitBlock: " << index_iter_->value().handle.offset() << " "
    //   << rep->last_key_block_offset << " " << rep->last_key_offset_in_block
    //   << "\n";
    
    prefetch_64(scratch_ + data_block_handle.offset() - block_start_offset_);
    CheckDataBlockWithinUpperBound();
  }
}

void BlockBasedTableIteratorSBC::AsyncInitDataBlock(bool is_first_pass) {
  BlockHandle data_block_handle = index_iter_->value().handle;
  bool is_for_compaction =
      lookup_context_.caller == TableReaderCaller::kCompaction;
  if (is_first_pass) {
    if (!block_iter_points_to_real_block_ ||
        data_block_handle.offset() != prev_block_offset_ ||
        // if previous attempt of reading the block missed cache, try again
        block_iter_.status().IsIncomplete()) {
      if (block_iter_points_to_real_block_) {
        ResetDataIter();
      }
      auto* rep = table_->get_rep();
      // Prefetch additional data for range scans (iterators).
      // Implicit auto readahead:
      //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
      // Explicit user requested readahead:
      //   Enabled from the very first IO when ReadOptions.readahead_size is
      //   set.
      // In case of async_io with Implicit readahead, block_prefetcher_ will
      // always the create the prefetch buffer by setting no_sequential_checking
      // = true.
      block_prefetcher_.PrefetchIfNeeded(
          rep, data_block_handle, read_options_.readahead_size,
          is_for_compaction, /*no_sequential_checking=*/read_options_.async_io,
          read_options_.rate_limiter_priority);

      Status s;
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          /*get_context=*/nullptr, &lookup_context_,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/true, s);

      if (s.IsTryAgain()) {
        async_read_in_progress_ = true;
        return;
      }
    }
  } else {
    // Second pass will call the Poll to get the data block which has been
    // requested asynchronously.
    Status s;
    table_->NewDataBlockIterator<DataBlockIter>(
        read_options_, data_block_handle, &block_iter_, BlockType::kData,
        /*get_context=*/nullptr, &lookup_context_,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s);
  }
  block_iter_points_to_real_block_ = true;
  CheckDataBlockWithinUpperBound();
  async_read_in_progress_ = false;
}

void BlockBasedTableIteratorSBC::FindKeyForward() {
  // This method's code is kept short to make it likely to be inlined.

  assert(!is_out_of_bound_);
  assert(block_iter_points_to_real_block_);

  if (!block_iter_.Valid()) {
    // This is the only call site of FindBlockForward(), but it's extracted into
    // a separate method to keep FindKeyForward() short and likely to be
    // inlined. When transitioning to a different block, we call
    // FindBlockForward(), which is much longer and is probably not inlined.
    FindBlockForward();
  } else {
    // This is the fast path that avoids a function call.
  }
}

void BlockBasedTableIteratorSBC::FindBlockForward() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    // Whether next data block is out of upper bound, if there is one.
    bool next_block_is_out_of_bound =
        read_options_.iterate_upper_bound != nullptr &&
        block_iter_points_to_real_block_ &&
        block_upper_bound_check_ == BlockUpperBound::kUpperBoundInCurBlock;
    assert(!next_block_is_out_of_bound ||
           user_comparator_.CompareWithoutTimestamp(
               *read_options_.iterate_upper_bound, /*a_has_ts=*/false,
               index_iter_->user_key(), /*b_has_ts=*/true) <= 0);
    ResetDataIter();
    if(index_iter_->value().handle.offset() >= table_->get_rep()->last_key_block_offset) {
      next_block_is_out_of_bound = true;
    }
    index_iter_->Next();
    if (next_block_is_out_of_bound) {
      // The next block is out of bound. No need to read it.
      TEST_SYNC_POINT_CALLBACK("BlockBasedTableIteratorSBC:out_of_bound", nullptr);
      // We need to make sure this is not the last data block before setting
      // is_out_of_bound_, since the index key for the last data block can be
      // larger than smallest key of the next file on the same level.
      if (index_iter_->Valid()&&
        !(index_iter_->value().handle.offset() >= table_->get_rep()->last_key_block_offset)) {
        is_out_of_bound_ = true;
      }
      return;
    }

    if (!index_iter_->Valid()) {
      return;
    }

    InitDataBlock();
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

void BlockBasedTableIteratorSBC::CheckOutOfBound() {
  if (read_options_.iterate_upper_bound != nullptr &&
      block_upper_bound_check_ != BlockUpperBound::kUpperBoundBeyondCurBlock &&
      Valid()) {
    is_out_of_bound_ =
        user_comparator_.CompareWithoutTimestamp(
            *read_options_.iterate_upper_bound, /*a_has_ts=*/false, user_key(),
            /*b_has_ts=*/true) <= 0;
  }
}

void BlockBasedTableIteratorSBC::CheckDataBlockWithinUpperBound() {
  if (read_options_.iterate_upper_bound != nullptr &&
      block_iter_points_to_real_block_) {
    block_upper_bound_check_ = (user_comparator_.CompareWithoutTimestamp(
                                    *read_options_.iterate_upper_bound,
                                    /*a_has_ts=*/false, index_iter_->user_key(),
                                    /*b_has_ts=*/true) > 0)
                                   ? BlockUpperBound::kUpperBoundBeyondCurBlock
                                   : BlockUpperBound::kUpperBoundInCurBlock;
  }
}
}  // namespace ROCKSDB_NAMESPACE
