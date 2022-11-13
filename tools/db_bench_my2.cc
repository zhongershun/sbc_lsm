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
// #include "test_util/sync_point.h"
#include "util/concurrent_task_limiter_impl.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"
#include "rocksdb/iostats_context.h"
#include "util/rate_limiter.h"
#include "util/string_util.h"
#include "util/cpu_info.h"
#include "monitoring/histogram.h"
#include "utilities/distribution_generator.h"

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

DEFINE_int32(value_size, 1024, "");
DEFINE_bool(use_sync, false, "");
DEFINE_bool(bind_core, true, "");
DEFINE_uint64(data_size, 10ll<<30, "");
DEFINE_int32(write_rate, 0, "");
DEFINE_int32(read_rate, 0, "");
DEFINE_int32(core_num, 2, "");
DEFINE_int32(client_num, 10, "");
DEFINE_int64(read_count, 10000, "");
DEFINE_int32(workloads, 5, "0: TestBasic, 1: TestReadBasic, 2: bull, 3: TestLatency"); // 0: subcompaction, 1: throughput, 2: get duration
DEFINE_int32(num_levels, 3, "");
DEFINE_int32(disk_type, 1, "0 SSD, 1 NVMe, ");
DEFINE_uint64(cache_size, 0, "");
DEFINE_bool(create_new_db, false, "");
DEFINE_int32(distribution, 0, "0: uniform, 1: zipfian");
DEFINE_int32(shortcut_cache, 0, "");
DEFINE_int32(read_num, 1000000, "");


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
  kInsert
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


// std::string FilesPerLevel(DB *db_, ColumnFamilyHandle* handle_) {
//   auto NumTableFilesAtLevel = [&](int level, ColumnFamilyHandle* handle_) {
//     std::string property;
//     db_->GetProperty(
//       handle_, "rocksdb.num-files-at-level" + NumberToString(level),
//       &property);
//     return atoi(property.c_str());
//   };
//   int num_levels = db_->NumberLevels(handle_);
//   std::string result;
//   size_t last_non_zero_offset = 0;
//   for (int level = 0; level < num_levels; level++) {
//     int f = NumTableFilesAtLevel(level, handle_);
//     char buf[100];
//     snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
//     result += buf;
//     if (f > 0) {
//       last_non_zero_offset = result.size();
//     }
//   }
//   result.resize(last_non_zero_offset);
//   return result;
// }


// int TableNumAtLevel(DB *db_, ColumnFamilyHandle* column_family, int level) {
//   std::string property;

//   db_->GetProperty(column_family, "rocksdb.num-files-at-level" + NumberToString(level),
//         &property);
  
//   return atoi(property.c_str());
// }

}  // anonymous namespace

// 测试在只读场景下IO和CPU的利用率
void TestReadOnly() {
  // cpu_set_t cpuset;
  // CPU_ZERO(&cpuset);
  // // 绑定到0-4核心
  // for (int i = 0; i < 4; i++) {
  //   CPU_SET(i, &cpuset);
  // }
  // int state = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  Options options_ins;
  options_ins.create_if_missing = true;  
  DB* db = nullptr;

  std::string DBPath = "./rocksdb_bench_my_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = FLAGS_data_size;
  size_t value_size = FLAGS_value_size;
  size_t client_num = FLAGS_client_num;
  size_t key_num = data_size / (value_size+22ll);
  size_t key_per_client = key_num / client_num; 

  if(FLAGS_disk_type == 0) {
    DBPath = "/test/rocksdb_bench_my_" + std::to_string(FLAGS_value_size);
  }

#ifndef NDEBUG
  DBPath += "_DBG";
  FLAGS_create_new_db = true;
  data_size = 100ul << 20;
  key_num = data_size / (value_size+22ll);
  key_per_client = key_num / client_num;
#endif

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << " MB\n ValueSize: " << FLAGS_value_size
    << "\n KeyNum: " << key_num
    << "\n BindCore: " << FLAGS_bind_core 
    << "\n Cache size: " << BytesToHumanString(FLAGS_cache_size) 
    << "\n Distribution: " << FLAGS_distribution
    << "\n Client num: " << client_num
    << "\n";

  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  auto hist_write = std::make_shared<HistogramImpl>();
  auto hist_read = std::make_shared<HistogramImpl>();
  auto hist_insert = std::make_shared<HistogramImpl>();
  hist_.insert({kWrite, std::move(hist_write)});
  hist_.insert({kRead, std::move(hist_read)});
  hist_.insert({kInsert, std::move(hist_insert)});
  DB *db_tmp = nullptr;
  Options opt_tmp;
  Status s_tmp = DB::Open(opt_tmp, DBPath, &db_tmp);
  delete db_tmp;
  
  // 如果数据库打不开或者强制重建数据库，才会重新插数据
  if(FLAGS_create_new_db || s_tmp != Status::OK()){
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

  Options options;
  options.disable_auto_compactions = true;
  options.use_direct_reads = true;

  if(FLAGS_cache_size > 0) {
    std::shared_ptr<Cache> cache = NewLRUCache(FLAGS_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }
  

  auto s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";

  std::atomic<int64_t> op_count_;
  size_t op_count_list[100];

  auto ReadWrite = [&](size_t min, size_t max, int idx, int read, int write){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    // 绑定到0-4核心
    for (int i = 0; i < 4; i++) {
      CPU_SET(i, &cpuset);
    }
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    unsigned long seed_;
    Random rnd(301);
    std::default_random_engine gen;
    std::uniform_int_distribution<int> op_gen(0, write+read-1); // 闭区间

    std::default_random_engine gen_key;


    std::uniform_int_distribution<size_t> key_gen_uniform(min, max);
    ZipfianGenerator key_gen_zipfian(min, max);

    gen.seed(read+write);
    size_t w_count = 0;
    size_t r_count = 0;
    char buf[100];
    
    while(op_count_>0) {
      int op = op_gen(gen);
      size_t key;
      if(FLAGS_distribution == 0) {
        key = key_gen_uniform(gen_key);
      } else if(FLAGS_distribution == 1) {
        key = key_gen_zipfian.Next_hash();
      } else {
        abort();
      }
      snprintf(buf, sizeof(buf), "key%09ld", key);

      if(op<write) {
        auto value_t = rnd.RandomString(FLAGS_value_size);
        auto start_ = std::chrono::system_clock::now();
        db->Put(WriteOptions(), Slice(buf, 12), value_t);
        auto end_ = std::chrono::system_clock::now();
        w_count++;
        hist_[kWrite]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
      } else {
        std::string value;
        auto start_ = std::chrono::system_clock::now();
        db->Get(ReadOptions(), Slice(buf, 12), &value);
        auto end_ = std::chrono::system_clock::now();
        hist_[kRead]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
        r_count++;
      }
      op_count_list[idx]++;
      op_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    std::cout<<"Thread: " << idx <<", Write:Read (" << w_count<<", "<<r_count<<")\n";
  };
  
  op_count_ = FLAGS_read_count;
  size_t op_sum = op_count_;
  std::vector<std::thread> client_threads;
  std::vector<std::string> cpu_set;
  CPUStat::CPU_OCCUPY cpu_stat1[20];
  CPUStat::CPU_OCCUPY cpu_stat2[20];
  cpu_set.push_back("cpu0 ");
  cpu_set.push_back("cpu1 ");
  cpu_set.push_back("cpu2 ");
  cpu_set.push_back("cpu3 ");

  for (size_t i = 0; i < cpu_set.size(); i++) {
    CPUStat::get_cpuoccupy((CPUStat::CPU_OCCUPY *)&cpu_stat1[i], cpu_set[i].c_str());
  }
  // Workload start
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < client_num; i++) {
    client_threads.emplace_back(std::thread(ReadWrite, 0, key_num, i, 100, 0));
  }
  // Statistic CPU
  std::thread cpu_rec = std::thread(CPUStat::GetCPUStatMs, cpu_set);

  // Wait workload finished
  for (size_t i = 0; i < client_num; i++) {
    client_threads[i].join();
  }
  CPUStat::run = false;
  cpu_rec.join();

  // Workload end
  auto end = std::chrono::system_clock::now();
  auto duration =  std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
  for (size_t i = 0; i < cpu_set.size(); i++) {
    CPUStat::get_cpuoccupy((CPUStat::CPU_OCCUPY *)&cpu_stat2[i], cpu_set[i].c_str());
    CPUStat::cal_cpuoccupy(&cpu_stat1[i], &cpu_stat2[i]);
  }
  
  std::cout << "Throughput: [" << op_sum << ", " << duration / 1000 << "s, " << op_sum*1.0 / (duration) << " Kop/s] \n";
  std::cout << "Read: " << hist_[kRead]->ToString() << "\n";

}

void TestReadWrite() {
  // cpu_set_t cpuset;
  // CPU_ZERO(&cpuset);
  // // 绑定到0-4核心
  // for (int i = 0; i < 4; i++) {
  //   CPU_SET(i, &cpuset);
  // }
  // int state = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  Options options_ins;
  options_ins.create_if_missing = true;  
  DB* db = nullptr;

  std::string DBPath = "./rocksdb_bench_my_rw_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = FLAGS_data_size;
  size_t value_size = FLAGS_value_size;
  size_t client_num = FLAGS_client_num;
  size_t key_num = data_size / (value_size+22ll);
  size_t key_per_client = key_num / client_num; 

  if(FLAGS_disk_type == 0) {
    DBPath = "/test/rocksdb_bench_my_rw_" + std::to_string(FLAGS_value_size);
  }

#ifndef NDEBUG
  DBPath += "_DBG";
  FLAGS_create_new_db = true;
  data_size = 100ul << 20;
  key_num = data_size / (value_size+22ll);
  key_per_client = key_num / client_num;
#endif

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << " MB\n ValueSize: " << FLAGS_value_size
    << "\n KeyNum: " << key_num
    << "\n BindCore: " << FLAGS_bind_core 
    << "\n Cache size: " << BytesToHumanString(FLAGS_cache_size) 
    << "\n Distribution: " << FLAGS_distribution
    << "\n Client num: " << client_num
    << "\n";

  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  auto hist_write = std::make_shared<HistogramImpl>();
  auto hist_read = std::make_shared<HistogramImpl>();
  auto hist_insert = std::make_shared<HistogramImpl>();
  hist_.insert({kWrite, std::move(hist_write)});
  hist_.insert({kRead, std::move(hist_read)});
  hist_.insert({kInsert, std::move(hist_insert)});
  DB *db_tmp = nullptr;
  Options opt_tmp;
  Status s_tmp = DB::Open(opt_tmp, DBPath, &db_tmp);
  delete db_tmp;
  
  // 如果数据库打不开或者强制重建数据库，才会重新插数据
  if(FLAGS_create_new_db || s_tmp != Status::OK()){
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

  Options options;
  options.disable_auto_compactions = true;
  options.use_direct_reads = true;

  if(FLAGS_cache_size > 0) {
    std::shared_ptr<Cache> cache = NewLRUCache(FLAGS_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }
  

  auto s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";

  std::atomic<int64_t> op_count_;
  size_t op_count_list[100];

  auto ReadWrite = [&](size_t min, size_t max, int idx, int read, int write){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    // 绑定到0-4核心
    for (int i = 0; i < 4; i++) {
      CPU_SET(i, &cpuset);
    }
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    unsigned long seed_;
    Random rnd(301);
    std::default_random_engine gen;
    std::uniform_int_distribution<int> op_gen(0, write+read-1); // 闭区间

    std::default_random_engine gen_key;


    std::uniform_int_distribution<size_t> key_gen_uniform(min, max);
    ZipfianGenerator key_gen_zipfian(min, max);

    gen.seed(read+write);
    size_t w_count = 0;
    size_t r_count = 0;
    char buf[100];
    
    while(op_count_>0) {
      int op = op_gen(gen);
      size_t key;
      if(FLAGS_distribution == 0) {
        key = key_gen_uniform(gen_key);
      } else if(FLAGS_distribution == 1) {
        key = key_gen_zipfian.Next_hash();
      } else {
        abort();
      }
      snprintf(buf, sizeof(buf), "key%09ld", key);

      if(op<write) {
        auto value_t = rnd.RandomString(FLAGS_value_size);
        auto start_ = std::chrono::system_clock::now();
        db->Put(WriteOptions(), Slice(buf, 12), value_t);
        auto end_ = std::chrono::system_clock::now();
        w_count++;
        hist_[kWrite]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
      } else {
        std::string value;
        auto start_ = std::chrono::system_clock::now();
        db->Get(ReadOptions(), Slice(buf, 12), &value);
        auto end_ = std::chrono::system_clock::now();
        hist_[kRead]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
        r_count++;
      }
      op_count_list[idx]++;
      op_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    std::cout<<"Thread: " << idx <<", Write:Read (" << w_count<<", "<<r_count<<")\n";
  };
  
  op_count_ = FLAGS_read_count;
  size_t op_sum = op_count_;
  std::vector<std::thread> client_threads;
  std::vector<std::string> cpu_set;
  CPUStat::CPU_OCCUPY cpu_stat1[20];
  CPUStat::CPU_OCCUPY cpu_stat2[20];
  cpu_set.push_back("cpu0 ");
  cpu_set.push_back("cpu1 ");
  cpu_set.push_back("cpu2 ");
  cpu_set.push_back("cpu3 ");

  for (size_t i = 0; i < cpu_set.size(); i++) {
    CPUStat::get_cpuoccupy((CPUStat::CPU_OCCUPY *)&cpu_stat1[i], cpu_set[i].c_str());
  }
  // Workload start
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < client_num; i++) {
    client_threads.emplace_back(std::thread(ReadWrite, 0, key_num, i, FLAGS_read_rate, FLAGS_write_rate));
  }
  // Statistic CPU
  std::thread cpu_rec = std::thread(CPUStat::GetCPUStatMs, cpu_set);

  // Wait workload finished
  for (size_t i = 0; i < client_num; i++) {
    client_threads[i].join();
  }
  CPUStat::run = false;
  cpu_rec.join();

  // Workload end
  auto end = std::chrono::system_clock::now();
  auto duration =  std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
  for (size_t i = 0; i < cpu_set.size(); i++) {
    CPUStat::get_cpuoccupy((CPUStat::CPU_OCCUPY *)&cpu_stat2[i], cpu_set[i].c_str());
    CPUStat::cal_cpuoccupy(&cpu_stat1[i], &cpu_stat2[i]);
  }
  
  std::cout << "Throughput: [" << op_sum << ", " << duration / 1000 << "s, " << op_sum*1.0 / (duration) << " Kop/s] \n";
  std::cout << "Read: " << hist_[kRead]->ToString() << "\n";
  std::cout << "Write: " << hist_[kWrite]->ToString() << "\n";

}

}  // namespace ROCKSDB_NAMESPACE


int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if(FLAGS_workloads == 6){
    std::cout << FLAGS_workloads <<"TestReadWrite\n";
    rocksdb::TestReadWrite();
  } else if(FLAGS_workloads == 5) {
    rocksdb::TestReadOnly();
  } else {
    std::cout << "Error workload: " << FLAGS_workloads <<" workload\n";
  }
  
  // ::testing::InitGoogleTest(&argc, argv);
  // ROCKSDB_NAMESPACE::MyTest(value_size, sub_compactions, bind_core, data_size);
  // return RUN_ALL_TESTS();
  return 0;
}
