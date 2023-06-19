//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/block_based_table_reader.h"

#include <cmath>
#include <memory>
#include <string>

#include "cache/cache_reservation_manager.h"
#include "db/db_test_util.h"
#include "db/table_properties_collector.h"
#include "file/file_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/partitioned_index_iterator.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

#define VALUE_SIZE 1024
#define BLOCK_SIZE 4096

class BlockBasedTableReaderBaseTest : public testing::Test {
 protected:
  // Prepare key-value pairs to occupy multiple blocks.
  // Each value is 1024B, every 16 pairs constitute 1 block.
  // If mixed_with_human_readable_string_value == true,
  // then adjacent blocks contain values with different compression
  // complexity: human readable strings are easier to compress than random
  // strings.
  static std::map<std::string, std::string> GenerateKVMap(
      int num_block = 10,
      bool mixed_with_human_readable_string_value = false) {
    std::map<std::string, std::string> kv;

    Random rnd(101);
    uint32_t key = 0;
    int block_key_num = BLOCK_SIZE / VALUE_SIZE;
    for (int block = 0; block < num_block; block++) {
      for (int i = 0; i < block_key_num; i++) {
        char k[9] = {0};
        // Internal key is constructed directly from this key,
        // and internal key size is required to be >= 8 bytes,
        // so use %08u as the format string.
        sprintf(k, "%08u", key);
        std::string v;
        if (mixed_with_human_readable_string_value) {
          v = (block % 2) ? rnd.HumanReadableString(VALUE_SIZE)
                          : rnd.RandomString(VALUE_SIZE);
        } else {
          v = rnd.RandomString(VALUE_SIZE);
        }
        kv[std::string(k)] = v;
        key++;
      }
    }
    return kv;
  }

  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    test_dir_ = test::PerThreadDBPath("block_based_table_reader_test");
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
    ConfigureTableFactory();
  }

  virtual void ConfigureTableFactory() = 0;

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  // Creates a table with the specificied key value pairs (kv).
  void CreateTable(const std::string& table_name,
                   const CompressionType& compression_type,
                   const std::map<std::string, std::string>& kv) {
    std::unique_ptr<WritableFileWriter> writer;
    NewFileWriter(table_name, &writer);

    // Create table builder.
    ImmutableOptions ioptions(options_);
    InternalKeyComparator comparator(options_.comparator);
    ColumnFamilyOptions cf_options;
    MutableCFOptions moptions(cf_options);
    IntTblPropCollectorFactories factories;
    std::unique_ptr<TableBuilder> table_builder(
        options_.table_factory->NewTableBuilder(
            TableBuilderOptions(ioptions, moptions, comparator, &factories,
                                compression_type, CompressionOptions(),
                                0 /* column_family_id */,
                                kDefaultColumnFamilyName, -1 /* level */),
            writer.get()));

    // Build table.
    for (auto it = kv.begin(); it != kv.end(); it++) {
      std::string k = ToInternalKey(it->first);
      std::string v = it->second;
      table_builder->Add(k, v);
    }
    ASSERT_OK(table_builder->Finish());
  }

  void NewBlockBasedTableReader(const FileOptions& foptions,
                                const ImmutableOptions& ioptions,
                                const InternalKeyComparator& comparator,
                                const std::string& table_name,
                                std::unique_ptr<BlockBasedTable>* table,
                                bool prefetch_index_and_filter_in_cache = true,
                                Status* status = nullptr) {
    const MutableCFOptions moptions(options_);
    TableReaderOptions table_reader_options = TableReaderOptions(
        ioptions, moptions.prefix_extractor, EnvOptions(), comparator);

    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file);

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(Path(table_name), &file_size));

    std::unique_ptr<TableReader> general_table;
    Status s = options_.table_factory->NewTableReader(
        ReadOptions(), table_reader_options, std::move(file), file_size,
        &general_table, prefetch_index_and_filter_in_cache);

    if (s.ok()) {
      table->reset(reinterpret_cast<BlockBasedTable*>(general_table.release()));
    }

    if (status) {
      *status = s;
    }
  }

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  std::string test_dir_;
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  Options options_;

  std::string ToInternalKey(const std::string& key) {
    InternalKey internal_key(key, 0, ValueType::kTypeValue);
    return internal_key.Encode().ToString();
  }

  std::string ToUserKey(const std::string& key) {
    assert(key.size() >= kNumInternalBytes);
    return Slice(key.c_str(), key.size() - kNumInternalBytes).ToString();
  }

 private:
  void WriteToFile(const std::string& content, const std::string& filename) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(filename), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  void NewFileWriter(const std::string& filename,
                     std::unique_ptr<WritableFileWriter>* writer) {
    std::string path = Path(filename);
    EnvOptions env_options;
    FileOptions foptions;
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(fs_->NewWritableFile(path, foptions, &file, nullptr));
    writer->reset(new WritableFileWriter(std::move(file), path, env_options));
  }

  void NewFileReader(const std::string& filename, const FileOptions& opt,
                     std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string path = Path(filename);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(path, opt, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), path,
                                             env_->GetSystemClock().get()));
  }
};

class BlockBasedTableReaderTest : public BlockBasedTableReaderBaseTest, public testing::WithParamInterface<std::tuple<CompressionType, bool, BlockBasedTableOptions::IndexType, bool>> {
 protected:
  void SetUp() override {
    compression_type_ = std::get<0>(GetParam());
    use_direct_reads_ = std::get<1>(GetParam());
    ioptions = ImmutableOptions(options);
    comparator = InternalKeyComparator(options.comparator);
    BlockBasedTableReaderBaseTest::SetUp();
  }

  void ConfigureTableFactory() override {
    BlockBasedTableOptions opts;
    opts.index_type = std::get<2>(GetParam());
    opts.no_block_cache = std::get<3>(GetParam());
    options_.table_factory.reset(
        static_cast<BlockBasedTableFactory*>(NewBlockBasedTableFactory(opts)));
  }

  void CreateAndOpenTable() {
    kv = BlockBasedTableReaderBaseTest::GenerateKVMap(
          5 /* num_block */,
          true /* mixed_with_human_readable_string_value */);

    // Prepare keys, values, and statuses for MultiGet.
    {
      auto it = kv.begin();
      for (size_t i = 0; i < kv.size(); i++) {
        keys.emplace_back(it->first);
        values.emplace_back();
        statuses.emplace_back();
        std::advance(it, 1);
      }
    }

    foptions.use_direct_reads = use_direct_reads_;
    table_name =
        "BlockBasedTableReaderTest" + CompressionTypeToString(compression_type_);
    CreateTable(table_name, compression_type_, kv);


    NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &table);

    table->DisplayKeyRange();
  }

  std::map<std::string, std::string> kv;
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> keys;
  autovector<PinnableSlice, MultiGetContext::MAX_BATCH_SIZE> values;
  autovector<Status, MultiGetContext::MAX_BATCH_SIZE> statuses;
  CompressionType compression_type_;
  std::string table_name;
  Options options;
  ImmutableOptions ioptions;
  FileOptions foptions;
  InternalKeyComparator comparator;
  std::unique_ptr<BlockBasedTable> table;
  bool use_direct_reads_;
};


TEST_P(BlockBasedTableReaderTest, Get) {
  CreateAndOpenTable();

  // Prepare MultiGetContext.
  autovector<GetContext, MultiGetContext::MAX_BATCH_SIZE> get_context;
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    get_context.emplace_back(BytewiseComparator(), nullptr, nullptr, nullptr,
                             GetContext::kNotFound, keys[i], &values[i],
                             nullptr, nullptr, nullptr, nullptr,
                             true /* do_merge */, nullptr, nullptr, nullptr,
                             nullptr, nullptr, nullptr);
    key_context.emplace_back(nullptr, keys[i], &values[i], nullptr,
                             &statuses.back());
    key_context.back().get_context = &get_context.back();
  }
  for (auto& key_ctx : key_context) {
    sorted_keys.emplace_back(&key_ctx);
  }

  PerfContext* perf_ctx = get_perf_context();
  perf_ctx->Reset();

  for (size_t i=0; i<keys.size(); i++)
  {
    table->Get(ReadOptions(), ToInternalKey(keys[i].ToString()), key_context[i].get_context, nullptr, false);
  }
  
  ASSERT_GE(perf_ctx->block_read_count - perf_ctx->index_block_read_count -
                perf_ctx->filter_block_read_count -
                perf_ctx->compression_dict_block_read_count, 1);
  ASSERT_GE(perf_ctx->block_read_byte, 1);

  for (const Status& status : statuses) {
    ASSERT_OK(status);
  }

  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(values[i].ToString(), kv[keys[i].ToString()]);
    // std::cout << keys[i].ToString() << "\n";
  }

  size_t start_key = 6;
  size_t last_key = 13;
  table->UpdateKeyRange(ToInternalKey(keys[start_key].ToString()), 4168, 2081, 
                        ToInternalKey(keys[last_key].ToString()), 12505, 2081);

  for (size_t i = 0; i < keys.size(); i++) {
    PinnableSlice value;
    GetContext g_ctx(BytewiseComparator(), nullptr, nullptr, nullptr,
                             GetContext::kNotFound, keys[i], &value,
                             nullptr, nullptr, nullptr, nullptr,
                             true /* do_merge */, nullptr, nullptr, nullptr,
                             nullptr, nullptr, nullptr);

    Status s = table->Get(ReadOptions(), ToInternalKey(keys[i].ToString()), &g_ctx, nullptr, false);
    if(i < start_key || i > last_key){
      std::cout << keys[i].ToString() << "\n";
      ASSERT_EQ(g_ctx.State(), GetContext::GetState::kNotFound);
    } else {
      ASSERT_EQ(g_ctx.State(), GetContext::GetState::kFound);
      ASSERT_EQ(values[i].ToString(), kv[keys[i].ToString()]);
    }
  }
}

TEST_P(BlockBasedTableReaderTest, Scan) {
  CreateAndOpenTable();

  table->DisplayKeyRange();


  // Prepare MultiGetContext.
  autovector<GetContext, MultiGetContext::MAX_BATCH_SIZE> get_context;
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    get_context.emplace_back(BytewiseComparator(), nullptr, nullptr, nullptr,
                             GetContext::kNotFound, keys[i], &values[i],
                             nullptr, nullptr, nullptr, nullptr,
                             true /* do_merge */, nullptr, nullptr, nullptr,
                             nullptr, nullptr, nullptr);
    key_context.emplace_back(nullptr, keys[i], &values[i], nullptr,
                             &statuses.back());
    key_context.back().get_context = &get_context.back();
  }
  for (auto& key_ctx : key_context) {
    sorted_keys.emplace_back(&key_ctx);
  }

  Arena* arena = new Arena();
  auto iter = table->NewIterator(ReadOptions(), nullptr, arena, 
    false, kUserIterator);
  
  for (iter->SeekToFirst(); iter->Valid(); iter->Next())
  {
    auto k = ToUserKey(iter->key().ToString());
    std::cout << k << " " << k.size() << "\n";
    ASSERT_EQ(iter->value().ToString(), kv[k]);
  }
}


TEST_P(BlockBasedTableReaderTest, SeekToLast) {
  CreateAndOpenTable();

  Arena* arena = new Arena();
  auto iter = table->NewIterator(ReadOptions(), nullptr, arena, 
    false, kUserIterator);
  
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), kv[keys[keys.size()-1].ToString()]);
  std::cout << "Last key: " << iter->key().ToString() << "\n";

  table->UpdateKeyRange(ToInternalKey(keys[6].ToString()), 4168, 2081, 
                        ToInternalKey(keys[13].ToString()), 12505, 2081);

  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  std::cout << "Last key: " << iter->key().ToString() << "\n";
  ASSERT_EQ(iter->value().ToString(), kv[keys[13].ToString()]);

  iter->Next();
  ASSERT_FALSE(iter->Valid());

  iter->Seek(ToInternalKey(keys[0].ToString()));
  ASSERT_TRUE(iter->Valid());

  iter->Seek(ToInternalKey(keys[15].ToString()));
  ASSERT_FALSE(iter->Valid());
}


TEST_P(BlockBasedTableReaderTest, SeekToFirst) {
  CreateAndOpenTable();

  Arena* arena = new Arena();
  auto iter = table->NewIterator(ReadOptions(), nullptr, arena, 
    false, kUserIterator);
  
  iter->SeekToFirst();
  std::cout << "First key: " << iter->key().ToString() << "\n";
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), kv[keys[0].ToString()]);
  

  table->UpdateKeyRange(ToInternalKey(keys[6].ToString()), 4168, 2081, 
                        ToInternalKey(keys[13].ToString()), 12505, 2081);

  iter->SeekToFirst();
  std::cout << "First key: " << iter->key().ToString() << "\n";
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), kv[keys[6].ToString()]);
  
}


TEST_P(BlockBasedTableReaderTest, WriteNewKeyRangeBlock) {
  CreateAndOpenTable();
  table->UpdateKeyRange(ToInternalKey(keys[6].ToString()), 4168, 2081, 
                        ToInternalKey(keys[13].ToString()), 12505, 2081);
  ASSERT_OK(table->WriteKeyRangeBlock());
  
  std::unique_ptr<BlockBasedTable> new_table;
  NewBlockBasedTableReader(foptions, ioptions, comparator, table_name, &new_table);
  new_table->DisplayKeyRange();

  Arena* arena = new Arena();
  auto iter = new_table->NewIterator(ReadOptions(), nullptr, arena, 
    false, kUserIterator);
  iter->SeekToFirst();
  std::cout << "First key: " << iter->key().ToString() << " " << iter->key().size() << "\n";
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), kv[keys[6].ToString()]);

  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  std::cout << "Last key: " << iter->key().ToString() << " " << iter->key().size() << "\n";
  ASSERT_EQ(iter->value().ToString(), kv[keys[13].ToString()]);

}


// Param 1: compression type
// Param 2: whether to use direct reads
// Param 3: Block Based Table Index type
// Param 4: BBTO no_block_cache option
INSTANTIATE_TEST_CASE_P(
    Get, BlockBasedTableReaderTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCompressions()), ::testing::Bool(),
        ::testing::Values(BlockBasedTableOptions::IndexType::kBinarySearch),
        ::testing::Values(false)));

// INSTANTIATE_TEST_CASE_P(
//     Scan, BlockBasedTableReaderTest,
//     ::testing::Combine(
//         ::testing::ValuesIn(GetSupportedCompressions()), ::testing::Bool(),
//         ::testing::Values(BlockBasedTableOptions::IndexType::kBinarySearch),
//         ::testing::Values(false)));

// INSTANTIATE_TEST_CASE_P(
//     SeekToLast, BlockBasedTableReaderTest,
//     ::testing::Combine(
//         ::testing::Values(kNoCompression), ::testing::Values(true),
//         ::testing::Values(BlockBasedTableOptions::IndexType::kBinarySearch),
//         ::testing::Values(false)));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
