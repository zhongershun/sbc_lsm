#pragma once

#include <stack>

// #include "db/blob/blob_garbage_meter.h"
// #include "db/compaction/compaction.h"
// #include "db/compaction/compaction_iterator.h"
// #include "db/internal_stats.h"
// #include "db/output_validator.h"


namespace ROCKSDB_NAMESPACE {

// class SBCBuffer {

// public:
//     SBCBuffer(/* args */);
//     ~SBCBuffer();

//     Status AddToOutPut(const IteratorWrapper& sbc_iter,
//                       const CompactionFileOpenFunc& open_file_func,
//                       const CompactionFileCloseFunc& close_file_func);

// private:
//     std::stack<std::unique_ptr<rocksdb::TableBuilder>>builder_list_;
// };

// SBCBuffer::SBCBuffer(/* args */) {
// }

// SBCBuffer::~SBCBuffer() {
// }

// Status SBCBuffer::AddToOutPut(const IteratorWrapper& sbc_iter,
//                       const CompactionFileOpenFunc& open_file_func,
//                       const CompactionFileCloseFunc& close_file_func) {
//   Status s;
//   const Slice& key = sbc_iter.key();
//   return s;
// }

}  // namespace ROCKSDB_NAMESPACE