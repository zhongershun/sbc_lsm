//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The rocksdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/experimental.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/convenience.h"
#include "rocksdb/db.h"
#include "util/concurrent_task_limiter_impl.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"
#include "rocksdb/iostats_context.h"
#include "util/rate_limiter.h"
#include "util/string_util.h"
// #include "util/cpu_info.h"
// #include "util/io_info.h"
#include "monitoring/histogram.h"
#include "utilities/distribution_generator.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"

#include <gflags/gflags.h>
#include "sys/time.h"
#include <iostream>
#include <fstream>
#include "unordered_map"
#include "thread"
#include "atomic"
#include "pthread.h"
#include "time.h"
#include "unistd.h"
#include "util/random.h"

DEFINE_int32(value_size, 1024, "");
DEFINE_bool(use_sync, false, "");
DEFINE_bool(bind_core, true, "");
DEFINE_uint64(data_size, 1ll<<30, "");
DEFINE_int32(write_rate, 100, "");
DEFINE_int32(read_rate, 100, "");
DEFINE_int32(scan_rate, 100, "");
DEFINE_int32(core_num, 4, "");
DEFINE_int32(client_num, 10, "");
DEFINE_int64(read_count, 100, "");
DEFINE_int32(workloads, 2, ""); 
DEFINE_int32(num_levels, 3, "");
DEFINE_int32(disk_type, 1, "0 SSD, 1 NVMe");
DEFINE_uint64(cache_size, 0, "");
DEFINE_bool(create_new_db, false, "");
DEFINE_int32(distribution, 0, "0: uniform, 1: zipfian");
DEFINE_int32(shortcut_cache, 0, "");
DEFINE_int32(read_num, 1000000, "");
DEFINE_bool(disableWAL, false, "");
DEFINE_bool(disable_auto_compactions, true, "");


#define UNUSED(v) ((void)(v))

namespace rocksdb {


namespace {

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers,
  kRMW,
  kInsert,
  kScan,
  kTailRead,
  kTailReadCPU,
  kTailReadIO
};

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string FilesPerLevel(DB *db_, int cf) {
  auto NumTableFilesAtLevel = [&](int level, int cf_) {
    std::string property;
    if (cf_ == 0) {
      // default cfd
      (db_->GetProperty(
          "rocksdb.num-files-at-level" + NumberToString(level), &property));
    } else {
      // (db_->GetProperty(
      //     handles_[cf_], "rocksdb.num-files-at-level" + NumberToString(level),
      //     &property));
    }
    return atoi(property.c_str());
  };
  int num_levels = db_->NumberLevels();
      // (cf == 0) ? db_->NumberLevels() : db_->NumberLevels(handles_[1]);
  std::string result;
  size_t last_non_zero_offset = 0;
  for (int level = 0; level < num_levels; level++) {
    int f = NumTableFilesAtLevel(level, cf);
    char buf[100];
    snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
    result += buf;
    if (f > 0) {
      last_non_zero_offset = result.size();
    }
  }
  result.resize(last_non_zero_offset);
  return result;
}

}  // anonymous namespace


void InsertData(Options options_ins, std::string DBPath, size_t key_num, 
    std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>, std::hash<unsigned char>> &hist_) {
  DB* db = nullptr;
  std::cout << "Create a new DB start!\n";
  system((std::string("rm -rf ")+DBPath).c_str());
  DB::Open(options_ins, DBPath, &db);
  auto loadData = [&](size_t begin, size_t end){
    Random rnd(begin);
    char buf[100];
    std::string value_temp;
    std::default_random_engine gen_key;
    std::uniform_int_distribution<size_t> key_gen(0, key_num);
    for (size_t i = begin; i < end; i++) {
      value_temp = rnd.RandomString(FLAGS_value_size);
      auto start_ = std::chrono::system_clock::now();
      auto key = key_gen(gen_key);
      snprintf(buf, sizeof(buf), "key%09ld", key);
      auto s = db->Put(WriteOptions(), Slice(buf, 12), value_temp);
      assert(s.ok());
#ifndef NDEBUG
        std::string ret_value;
        s = db->Get(ReadOptions(), Slice(buf, 12), &ret_value);
        assert(s.ok());
        // std::cout << ret_value << " " << value_temp<<"\n";  
        assert(ret_value == value_temp);
#endif
      auto end_ = std::chrono::system_clock::now();
      hist_[kInsert]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
    }
  };
  std::vector<std::thread> insert_clients;
  for (size_t i = 0; i < 1; i++) {
    insert_clients.push_back(std::thread(loadData, 0, key_num));
  }
  for (auto&& c : insert_clients) {
    c.join();
  }
  db->Flush(FlushOptions());
  // db->WaitForCompact(1);
  delete db;
  std::cout << "Create a new DB finished!\n";
}



void TestScan() {
  std::string DBPath = "rocksdb_bench_SBC_1GB_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = 1ll << 30;
  size_t value_size = FLAGS_value_size;
  size_t key_num = data_size / (value_size+12ll);
  FLAGS_read_count = 1;
  if(FLAGS_disk_type == 0){
    DBPath = "/zyn/SSD/test_RocksDB/" + DBPath;
  }

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << " \n ValueSize: " << FLAGS_value_size
    << "\n KeyNum: " << key_num
    << "\n BindCore: " << FLAGS_bind_core 
    << "\n Cache size: " << BytesToHumanString(FLAGS_cache_size) 
    << "\n Distribution: " << FLAGS_distribution
    << "\n Core num: " << FLAGS_core_num
    << "\n";
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  auto hist_insert = std::make_shared<HistogramImpl>();  
  hist_.insert({kInsert, std::move(hist_insert)});

  
  // 判断数据库能不能打开，不能打开就要重新插数据
  DB *db_tmp = nullptr;
  Options opt_tmp;
  Status s_tmp = DB::Open(opt_tmp, DBPath, &db_tmp);
  delete db_tmp;

  // 如果数据库打不开或者强制重建数据库，才会重新插数据
  if(FLAGS_create_new_db || s_tmp != Status::OK()){
    Options options_ins;
    options_ins.create_if_missing = true;  
    InsertData(options_ins, DBPath, key_num, hist_);
  }

  DB* db = nullptr;
  Options options;
  options.use_direct_reads = true;
  options.disable_auto_compactions = true;

  if(FLAGS_cache_size > 0) {
    std::shared_ptr<Cache> cache = NewLRUCache(FLAGS_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  auto s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  // 绑定CPU核心
  for (int i = 0; i < FLAGS_core_num; i++) {
    CPU_SET(i, &cpuset);
  }
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  unsigned long seed_;
  Random rnd(301);
  std::default_random_engine gen;
  std::default_random_engine gen_key;
  std::uniform_int_distribution<size_t> key_gen_uniform(0, key_num);

  UniformGenerator scan_len_uniform(10, 1000);
  Env *env = Env::Default();
  SystemClock *clock = env->GetSystemClock().get();
  uint64_t prev_cpu_micros;
  uint64_t now_cpu_micros;

  uint64_t prev_read_nanos = 0;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  uint64_t prev_cpu_write_nanos = 0;
  uint64_t prev_cpu_read_nanos = 0;

  uint64_t now_read_nanos = 0;
  uint64_t now_write_nanos = 0;
  uint64_t now_fsync_nanos = 0;
  uint64_t now_range_sync_nanos = 0;
  uint64_t now_prepare_write_nanos = 0;
  uint64_t now_cpu_write_nanos = 0;
  uint64_t now_cpu_read_nanos = 0;

  uint64_t read_io = 0;
  uint64_t write_io = 0;
  uint64_t cpu_dur = 0;
  uint64_t dur = 0;

  if (true) {
    SetPerfLevel(PerfLevel::kEnableTimeExceptForMutex);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }

  for(int64_t i=0;i<FLAGS_read_count;i++) {
    auto iter = db->NewIterator(ReadOptions());
    auto start_ = std::chrono::system_clock::now();
    prev_read_nanos = IOSTATS(read_nanos);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
    prev_cpu_micros = clock->CPUMicros();
    iter->SeekToFirst();
    while(iter->Valid()) {
      iter->Next();
    }
    now_cpu_micros = clock->CPUMicros();
    now_read_nanos = IOSTATS(read_nanos);
    now_write_nanos = IOSTATS(write_nanos);
    now_fsync_nanos = IOSTATS(fsync_nanos);
    now_range_sync_nanos = IOSTATS(range_sync_nanos);
    now_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    now_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    now_cpu_read_nanos = IOSTATS(cpu_read_nanos);
    auto end_ = std::chrono::system_clock::now();

    read_io += (now_read_nanos + now_cpu_read_nanos) - (prev_read_nanos + prev_cpu_read_nanos);
    write_io += (now_write_nanos + now_fsync_nanos + now_range_sync_nanos + now_prepare_write_nanos + now_cpu_write_nanos) - 
      (prev_write_nanos + prev_fsync_nanos + prev_range_sync_nanos + prev_prepare_write_nanos + prev_cpu_write_nanos);
    cpu_dur += now_cpu_micros - prev_cpu_micros;
    dur += std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count();
  }
  std::cout << "Scan count: " << FLAGS_read_count 
            << ", Read IO: " << read_io / 1000 / FLAGS_read_count
            << " us, Write IO: " << write_io / 1000 / FLAGS_read_count
            << " us, CPU: " << cpu_dur / FLAGS_read_count
            << " us, Duration: " << dur / FLAGS_read_count
            << "us\n";
}

void TestSBC() {
  std::string DBPath = "rocksdb_bench_SBC_1GB_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = 1ll << 30;
  size_t value_size = FLAGS_value_size;
  size_t key_num = data_size / (value_size+12ll);
  FLAGS_read_count = 1;
  if(FLAGS_disk_type == 0){
    DBPath = "/zyn/SSD/test_RocksDB/" + DBPath;
  }
  system(("rm -rf " + DBPath).c_str());
  system(("cp -rf rocksdb_bench_SBC_1GB_raw_1024 " + DBPath).c_str());

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << " MB\n ValueSize: " << FLAGS_value_size
    << "\n KeyNum: " << key_num
    << "\n BindCore: " << FLAGS_bind_core 
    << "\n Cache size: " << BytesToHumanString(FLAGS_cache_size) 
    << "\n Distribution: " << FLAGS_distribution
    << "\n Core num: " << FLAGS_core_num
    << "\n";
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  auto hist_insert = std::make_shared<HistogramImpl>();  
  hist_.insert({kInsert, std::move(hist_insert)});

  
  // 判断数据库能不能打开，不能打开就要重新插数据
  DB *db_tmp = nullptr;
  Options opt_tmp;
  Status s_tmp = DB::Open(opt_tmp, DBPath, &db_tmp);
  delete db_tmp;

  // 如果数据库打不开或者强制重建数据库，才会重新插数据
  if(FLAGS_create_new_db || s_tmp != Status::OK()){
    Options options_ins;
    options_ins.create_if_missing = true;  
    InsertData(options_ins, DBPath, key_num, hist_);
  }

  DB* db = nullptr;
  Options options;
  options.use_direct_reads = true;
  options.disable_auto_compactions = true;

  if(FLAGS_cache_size > 0) {
    std::shared_ptr<Cache> cache = NewLRUCache(FLAGS_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  auto s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  // 绑定CPU核心
  for (int i = 0; i < FLAGS_core_num; i++) {
    CPU_SET(i, &cpuset);
  }
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  auto iter = db->NewIterator(ReadOptions());
  std::cout << "Scan1 create iter\n";
  auto start_ = std::chrono::system_clock::now();
  iter->SeekToFirst();
  while(iter->Valid()) {
    iter->Next();
  }
  auto end_ = std::chrono::system_clock::now();
  delete iter;
  delete db;
  std::cout << "Scan1 duration: " 
    << std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count() << "\n";


  // ------------------------- CompactRange ---------------------------
  s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";
  db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  delete db;

  // ------------------------- SBC ---------------------------
  s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";
  std::string key_start = "key";
  std::string key_end = "key9";
  start_ = std::chrono::system_clock::now();
  iter = db->NewSBCIterator(ReadOptions(), key_start, key_end);
  iter->Seek(key_start);
  for(;iter->Valid() && options.comparator->Compare(iter->key(), Slice(key_end)) < 0;iter->SBCNext()) {
    iter->SBCNext();
  }
  end_ = std::chrono::system_clock::now();

  db->FinishSBC(iter);
  std::cout << "SBC finished table num: " << FilesPerLevel(db, 0) 
    << "\nDuration: " << std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count() 
    << "\n";
  delete db;

  // ----------------- 把数据从头到尾scan一遍 -----------------------
  s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";
  iter = db->NewIterator(ReadOptions());
    
  start_ = std::chrono::system_clock::now();
  iter->SeekToFirst();
  while(iter->Valid()) {
    iter->Next();
  }
  end_ = std::chrono::system_clock::now();
  std::cout << "Scan2 duration: " 
    << std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count() << "\n";
  delete iter;
  delete db;
}

}  // namespace ROCKSDB_NAMESPACE


int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_workloads == 1) {
    std::cout << FLAGS_workloads <<"TestScan()\n";
    rocksdb::TestScan();
  } else if(FLAGS_workloads == 2) {
    std::cout << FLAGS_workloads <<"TestSBC()\n";
    rocksdb::TestSBC();
  } else {
    std::cout << "Error workload: " << FLAGS_workloads <<" workload\n";
  }
  
  // ::testing::InitGoogleTest(&argc, argv);
  // ROCKSDB_NAMESPACE::MyTest(value_size, sub_compactions, bind_core, data_size);
  // return RUN_ALL_TESTS();
  return 0;
}
