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
#include "util/cpu_info.h"
#include "util/io_info.h"
#include "util/random.h"
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
#include <filesystem>

DEFINE_int32(value_size, 1024, "");
DEFINE_bool(use_sync, false, "");
DEFINE_bool(bind_core, false, "");
DEFINE_uint64(data_size, 1024ll<<20, "");
DEFINE_int32(write_rate, 99, "");
DEFINE_int32(read_rate, 0, "");
DEFINE_int32(scan_rate, 1, "");
DEFINE_int32(core_num, 4, "");
DEFINE_int32(client_num, 10, "");
DEFINE_int32(client_num_read, 0, "");
DEFINE_int32(client_num_write, 4, "");
DEFINE_int32(client_num_scan, 1, "");
DEFINE_int64(op_count, 10000, "");
DEFINE_int32(workloads, 2, ""); 
DEFINE_int32(disk_type, 1, "0 SSD, 1 NVMe");
DEFINE_uint64(cache_size, 0, "");
DEFINE_bool(create_new_db, false, "");
DEFINE_int32(distribution, 0, "0: uniform, 1: zipfian");
DEFINE_int32(shortcut_cache, 0, "");
DEFINE_int32(read_num, 1000000, "");
DEFINE_bool(disableWAL, false, "");
DEFINE_bool(disable_auto_compactions, false, "");
DEFINE_bool(enable_sbc, true, "");
DEFINE_int32(run_time, 400, "Unit: second");
DEFINE_int32(interval, 1000, "Unit: millisecond");
DEFINE_int32(level_multiplier, 2, "");
DEFINE_bool(level_compaction_dynamic_level_bytes, false, "");
DEFINE_uint64(key_range, 100ll<<20, "");
DEFINE_int32(l0_stalling_limit, 20, "");


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

auto GetTimeNow() {
  return std::chrono::system_clock::now();
}

int64_t GetTimeIntervalMsFrom(std::chrono::_V2::system_clock::time_point start) {
  auto end = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
}

int64_t GetTimeIntervalMs(std::chrono::_V2::system_clock::time_point start,
  std::chrono::_V2::system_clock::time_point end) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
}

}  // anonymous namespace


void InsertDataImpl(Options options_ins, std::string DBPath, size_t key_num, 
    std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
    std::hash<unsigned char>> *hist_, std::vector<int> *tps = nullptr, size_t range = 0) {
  DB* db = nullptr;
  std::cout << "Create a new DB start!\n";
  system((std::string("rm -rf ")+DBPath).c_str());
  DB::Open(options_ins, DBPath, &db);
  auto last_time = std::chrono::system_clock::now();
  uint64_t op_last = 0;
  uint64_t op_count;
  auto loadData = [&](size_t begin, size_t end){
    Random rnd(begin);
    char buf[100];
    std::string value_temp;
    std::default_random_engine gen_key;
    auto key_range = range == 0 ? key_num : range; 
    std::uniform_int_distribution<size_t> key_gen(0, key_range);
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
      op_count++;
      if(tps != nullptr && GetTimeIntervalMs(last_time, end_) > FLAGS_interval) {
        last_time = end_;
        tps->emplace_back(op_count - op_last);
        op_last = op_count;
        // std::cout << tps->back() << "\n";
      }
      if(hist_){
        (*hist_)[kInsert]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
      }
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
  static_cast<DBImpl*>(db)->WaitForCompact(1);
  delete db;
  std::cout << "Create a new DB finished!\n";
}


void InsertOnly() {
  Options options_ins;
  options_ins.create_if_missing = true;  
  options_ins.level0_slowdown_writes_trigger = FLAGS_l0_stalling_limit;
  // options_ins.max_bytes_for_level_multiplier = FLAGS_level_multiplier;
  options_ins.level_compaction_dynamic_level_bytes = FLAGS_level_compaction_dynamic_level_bytes;
  // options_ins.max_bytes_for_level_base = 512ll << 20;
  DB* db = nullptr;

  uint64_t data_size = FLAGS_data_size;
  size_t value_size = FLAGS_value_size;
  size_t client_num = FLAGS_client_num;
  size_t key_num = data_size / (value_size+22ll);
  std::string DBPath = "./sbc_bench_mix_" + 
    BytesToHumanStringConnect(data_size) +"_" + std::to_string(FLAGS_value_size);

  if(FLAGS_disk_type == 0) {
    DBPath = "/test/sbc_bench_mix_" + 
      BytesToHumanStringConnect(data_size) +"_" + std::to_string(FLAGS_value_size);
  }

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << "\n ValueSize: " << FLAGS_value_size
    << "\n KeyNum: " << key_num
    << "\n BindCore: " << FLAGS_bind_core 
    << "\n Cache size: " << BytesToHumanString(FLAGS_cache_size) 
    << "\n Distribution: " << FLAGS_distribution
    << "\n Client num: " << client_num
    << "\n Core num: " << FLAGS_core_num
    << "\n Key range: " << FLAGS_key_range
    << "\n Level-0 slowdown trigger: " << FLAGS_l0_stalling_limit
    << "\n";
  
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  auto hist_insert = std::make_shared<HistogramImpl>();
  hist_.insert({kInsert, std::move(hist_insert)});
  std::vector<int> tps;
  InsertDataImpl(options_ins, DBPath, key_num, &hist_, &tps, FLAGS_key_range);
  std::ofstream optput_tps("tps.txt");
  if (optput_tps.is_open()) {
      for (const auto& element : tps) {
          optput_tps << element << "\n";
      }
      optput_tps.close();
      std::cout << "Write tps.txt" << std::endl;
  } else {
      std::cout << "Unable to open tps.txt" << std::endl;
  }
}


void TestMixWorkload() {
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

  std::string DBPath = "./rocksdb_bench_my_mix_10GB_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = FLAGS_data_size;
  size_t value_size = FLAGS_value_size;
  size_t client_num = FLAGS_client_num;
  size_t key_num = data_size / (value_size+22ll);
  // size_t key_per_client = key_num / client_num; 

  if(FLAGS_write_rate != 0) {
    DBPath = "./rocksdb_bench_my_write_10GB_" + std::to_string(FLAGS_value_size);
    // system(("rm -rf " + DBPath).c_str());
    // system(("cp -rf rocksdb_bench_SBC_1GB_raw_1024 " + DBPath).c_str());
  }

  if(FLAGS_disk_type == 0) {
    DBPath = "/test/rocksdb_bench_my_mix_" + std::to_string(FLAGS_value_size);
  }

#ifndef NDEBUG
  DBPath += "_DBG";
  FLAGS_create_new_db = true;
  data_size = 150ul << 20;
  key_num = data_size / (value_size+22ll);
  FLAGS_op_count = 10000;
  // key_per_client = key_num / client_num;
#endif

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << "\n ValueSize: " << FLAGS_value_size
    << "\n KeyNum: " << key_num
    << "\n BindCore: " << FLAGS_bind_core 
    << "\n Cache size: " << BytesToHumanString(FLAGS_cache_size) 
    << "\n Distribution: " << FLAGS_distribution
    << "\n Client num: " << client_num
    << "\n Core num: " << FLAGS_core_num
    << "\n";

  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  auto hist_write = std::make_shared<HistogramImpl>();
  auto hist_read = std::make_shared<HistogramImpl>();
  auto hist_insert = std::make_shared<HistogramImpl>();
  auto hist_scan = std::make_shared<HistogramImpl>();
  auto hist_tail_read = std::make_shared<HistogramImpl>();
  auto hist_tail_read_cpu = std::make_shared<HistogramImpl>();
  auto hist_tail_read_io = std::make_shared<HistogramImpl>();

  hist_.insert({kWrite, std::move(hist_write)});
  hist_.insert({kRead, std::move(hist_read)});
  hist_.insert({kScan, std::move(hist_scan)});
  hist_.insert({kInsert, std::move(hist_insert)});
  hist_.insert({kTailRead, std::move(hist_tail_read)});
  hist_.insert({kTailReadCPU, std::move(hist_tail_read_cpu)});
  hist_.insert({kTailReadIO, std::move(hist_tail_read_io)});

  // 判断数据库能不能打开，不能打开就要重新插数据
  DB *db_tmp = nullptr;
  Options opt_tmp;
  Status s_tmp = DB::Open(opt_tmp, DBPath, &db_tmp);
  delete db_tmp;
  
  // 如果数据库打不开或者强制重建数据库，才会重新插数据
  if(FLAGS_create_new_db || s_tmp != Status::OK()){
    InsertDataImpl(options_ins, DBPath, key_num, &hist_, nullptr);
  }

  Options options;
  options.use_direct_reads = true;
  options.disable_auto_compactions = FLAGS_disable_auto_compactions;
  options.level0_slowdown_writes_trigger = FLAGS_l0_stalling_limit;
  std::atomic<int64_t> op_count_;
  size_t op_count_list[100];
  std::vector<std::vector<uint64_t>> log_[100];

  if(FLAGS_disableWAL){
    options.write_buffer_size = 100ll << 30;
    options.disable_auto_compactions = true;
    options.level0_file_num_compaction_trigger = 1000000;
    options.level0_slowdown_writes_trigger = 1000000;
    options.level0_stop_writes_trigger = 1000000;
  }

  if(FLAGS_cache_size > 0) {
    std::shared_ptr<Cache> cache = NewLRUCache(FLAGS_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  auto s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";

  // TODO: 目前只判断扫表的情况
  auto IfDoSBC = [](DB *db_, std::string *begin, std::string *end, bool enable_sbc){
    auto NumTableFilesAtLevel = [&](int level, int cf_) {
      std::string property;
      if (cf_ == 0) {
        db_->GetProperty(
            "rocksdb.num-files-at-level" + NumberToString(level), &property);
      } else {
        return -1;
      }
      return atoi(property.c_str());
    };

    if(!enable_sbc || db_->IsCompacting()) {
      return false;
    }
    if (begin == nullptr && end == nullptr) {
      if (NumTableFilesAtLevel(0, 0) > 2) {
        
        return db_->DoSBC();
      }
      return false;
    }

    return false;
  };

  auto ReadWrite = [&](size_t min, size_t max, int idx, int read, int write, int scan){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    // 绑定到0-4核心
    for (int i = 0; i < FLAGS_core_num; i++) {
      CPU_SET(i, &cpuset);
    }
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    unsigned long seed_;
    Random rnd(301);
    std::default_random_engine gen;
    std::default_random_engine gen_key;
    std::uniform_int_distribution<int> op_gen(0, write+read+scan-1); // 闭区间

    std::uniform_int_distribution<size_t> key_gen_uniform(min, max);
    // UniformGenerator key_gen_uniform(min, max);
    // Random key_gen_uniform(idx);
    UniformGenerator scan_len_uniform(10, 1000);
    ZipfianGenerator key_gen_zipfian(min, max);

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
    if (true) {
      SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
      prev_write_nanos = IOSTATS(write_nanos);
      prev_fsync_nanos = IOSTATS(fsync_nanos);
      prev_range_sync_nanos = IOSTATS(range_sync_nanos);
      prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
      prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
      prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
    }

    gen_key.seed(idx);
    size_t w_count = 0;
    size_t r_count = 0;
    size_t scan_count = 0;
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
        // Write operation
        auto value_t = rnd.RandomString(FLAGS_value_size);
        auto wo = WriteOptions();
        wo.disableWAL = FLAGS_disableWAL;

        auto start_ = std::chrono::system_clock::now();
        prev_read_nanos = IOSTATS(read_nanos);
        prev_write_nanos = IOSTATS(write_nanos);
        prev_fsync_nanos = IOSTATS(fsync_nanos);
        prev_range_sync_nanos = IOSTATS(range_sync_nanos);
        prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
        prev_cpu_micros = clock->CPUMicros();
        db->Put(wo, Slice(buf, 12), value_t);
        now_cpu_micros = clock->CPUMicros();
        now_read_nanos = IOSTATS(read_nanos);
        now_write_nanos = IOSTATS(write_nanos);
        now_fsync_nanos = IOSTATS(fsync_nanos);
        now_range_sync_nanos = IOSTATS(range_sync_nanos);
        now_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        now_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        now_cpu_read_nanos = IOSTATS(cpu_read_nanos);

        auto end_ = std::chrono::system_clock::now();
        w_count++;
        uint64_t dur_ = std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count();
        hist_[kWrite]->Add(dur_);
        uint64_t io_dur = now_read_nanos + now_cpu_read_nanos + now_cpu_write_nanos + now_prepare_write_nanos + now_range_sync_nanos + now_fsync_nanos + now_write_nanos - 
            (prev_read_nanos + prev_cpu_read_nanos + prev_cpu_write_nanos + prev_prepare_write_nanos + prev_range_sync_nanos + prev_fsync_nanos + prev_write_nanos);
        io_dur /= 1000; // ns -> us

        hist_[kTailRead]->Add(dur_);
        hist_[kTailReadCPU]->Add(now_cpu_micros - prev_cpu_micros);
        hist_[kTailReadIO]->Add(io_dur);
        
        log_[idx].push_back({dur_, now_cpu_micros - prev_cpu_micros, io_dur});
      } else if(op<read+write) {
        // Get operation
        std::string value;
        auto start_ = std::chrono::system_clock::now();
        prev_read_nanos = IOSTATS(read_nanos);
        prev_write_nanos = IOSTATS(write_nanos);
        prev_fsync_nanos = IOSTATS(fsync_nanos);
        prev_range_sync_nanos = IOSTATS(range_sync_nanos);
        prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
        prev_cpu_micros = clock->CPUMicros();
        db->Get(ReadOptions(), Slice(buf, 12), &value);
        now_cpu_micros = clock->CPUMicros();
        auto end_ = std::chrono::system_clock::now();
        now_read_nanos = IOSTATS(read_nanos);
        now_write_nanos = IOSTATS(write_nanos);
        now_fsync_nanos = IOSTATS(fsync_nanos);
        now_range_sync_nanos = IOSTATS(range_sync_nanos);
        now_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        now_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        now_cpu_read_nanos = IOSTATS(cpu_read_nanos);
        
        uint64_t dur_ = std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count();

        uint64_t io_dur = now_read_nanos + now_cpu_read_nanos + now_cpu_write_nanos + now_prepare_write_nanos + now_range_sync_nanos + now_fsync_nanos + now_write_nanos - 
            (prev_read_nanos + prev_cpu_read_nanos + prev_cpu_write_nanos + prev_prepare_write_nanos + prev_range_sync_nanos + prev_fsync_nanos + prev_write_nanos);
        io_dur /= 1000; // ns -> us

        log_[idx].push_back({dur_, now_cpu_micros - prev_cpu_micros, io_dur});
        hist_[kRead]->Add(dur_);
        r_count++;
      } else {
        // Scan operation
        std::string *start = nullptr;
        std::string *end = nullptr; 
        int scan_len = scan_len_uniform.Next();
        if(start == nullptr && end == nullptr){
          scan_len = INT_MAX;
        }

        auto start_ = std::chrono::system_clock::now();

        if(IfDoSBC(db, start, end, FLAGS_enable_sbc)){
          std::cout << "SBC start: " << FilesPerLevel(db, 0) << "\n";
          auto iter = db->NewSBCIterator(ReadOptions(), nullptr, nullptr);
          iter->SeekToFirst();
          for(;iter->Valid();iter->SBCNext()) {
          }
          db->FinishSBC(iter);
          std::cout << "SBC done: " << FilesPerLevel(db, 0) << "\n";
        } else {
          auto iter = db->NewIterator(ReadOptions());
          // iter->Seek(Slice(buf, 12));
          iter->SeekToFirst();
          for (int i = 0; i < scan_len && iter->Valid(); i++) {
            iter->Next();
          }
        }
        auto end_ = std::chrono::system_clock::now();

        hist_[kScan]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
        scan_count++;
      }
      op_count_list[idx]++;
      op_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    std::cout<<"Thread: " << idx 
      <<", Write:Read:Scan (" << w_count<<", "<<r_count<<", " << scan_count << ")\n";

  };
  
  std::string core_name_list[10] = {
    "cpu0 ",
    "cpu1 ",
    "cpu2 ",
    "cpu3 ",
    "cpu4 ",
    "cpu5 ",
    "cpu6 ",
    "cpu7 ",
    "cpu8 ",
    "cpu9 "
  };

  op_count_ = FLAGS_op_count;
  size_t op_sum = op_count_;
  std::vector<std::thread> client_threads;
  std::vector<std::string> cpu_set;
  CPUStat::CPU_OCCUPY cpu_stat1[20];
  CPUStat::CPU_OCCUPY cpu_stat2[20];
  // 最多监控10个核心
  for (int i = 0; i < FLAGS_core_num && i < 10; i++) {
    cpu_set.push_back(core_name_list[i]);
  }
  
  for (size_t i = 0; i < cpu_set.size(); i++) {
    CPUStat::get_cpuoccupy((CPUStat::CPU_OCCUPY *)&cpu_stat1[i], cpu_set[i].c_str());
  }

  // Workload start
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < client_num; i++) {
    client_threads.emplace_back(std::thread(ReadWrite, 0, key_num, i, FLAGS_read_rate, FLAGS_write_rate, FLAGS_scan_rate));
  }
  // Statistic CPU
  std::thread cpu_rec = std::thread(CPUStat::GetCPUStatMs, cpu_set);

  // Statistic IO
  std::string disk_name = "nvme2n1p1 ";
  std::thread io_stat = std::thread(IOStat::GetIOStatMs, disk_name, 100000);

  // Wait workload finished
  for (size_t i = 0; i < client_num; i++) {
    client_threads[i].join();
  }
  CPUStat::run = false;
  IOStat::run = false;
  cpu_rec.join();
  io_stat.join();

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
  std::cout << "Scan: " << hist_[kScan]->ToString() << "\n";
  std::cout << "TailRead: " << hist_[kTailRead]->ToString() << "\n";
  std::cout << "TailReadCPU: " << hist_[kTailReadCPU]->ToString() << "\n";
  std::cout << "TailReadIO: " << hist_[kTailReadIO]->ToString() << "\n";

  // 将读操作的记录输出到文件
  std::vector<std::vector<uint64_t>> read_op_all_;
  for (int i = 0; i < FLAGS_client_num; i++)
  {
    read_op_all_.insert(read_op_all_.end(), log_[i].begin(), log_[i].end());
  }
  
  std::sort(read_op_all_.begin(), read_op_all_.end(), [](const std::vector<uint64_t> &a, const std::vector<uint64_t> &b) {
      return a[0] < b[0];
  });

  std::cout << "Ans write to output.csv, count: " << read_op_all_.size() << "\n";
  std::ofstream file("output.csv");
  if (file.is_open()) {
    for (size_t i = 0; i < read_op_all_.size(); i++) {
      for (int j = 0; j < 3; j++) {
        file << read_op_all_[i][j] << " ";
      }
      file << std::endl;
    }
    file.close();
  } else {
     std::cout << "Unable to open file";
  }
  
}


void TestMixWorkloadWithDiffThread() {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  if(FLAGS_bind_core) {
    // 绑定到0-4核心
    for (int i = 20; i < 30; i++) {
      CPU_SET(i, &cpuset);
    }
    int state = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  }
  
  Options options_ins;
  options_ins.create_if_missing = true;  
  DB* db = nullptr;

  uint64_t data_size = FLAGS_data_size;
  size_t value_size = FLAGS_value_size;
  size_t client_num_read = FLAGS_client_num_read;
  size_t client_num_write = FLAGS_client_num_write;
  size_t client_num_scan = FLAGS_client_num_scan;
  size_t key_num = data_size / (value_size+22ll);
  // size_t key_per_client = key_num / client_num; 

  std::string DBPath = "./sbc_bench_mix_" + 
    BytesToHumanStringConnect(data_size) +"_" + std::to_string(FLAGS_value_size);

  std::string DataRaw = DBPath + "_raw"; // 这是已经插入完成的数据，可以直接用
  if(FLAGS_disk_type == 0) {
    DBPath = "/zyn/SSD/sbc_bench_mix_" + 
      BytesToHumanStringConnect(data_size) +"_" + std::to_string(FLAGS_value_size);
  }

  // if(FLAGS_client_num_write != 0) {
  //   DBPath = "./rocksdb_bench_my_write_" + std::to_string(FLAGS_value_size);
  //   system(("rm -rf " + DBPath).c_str());
  //   system(("cp -rf rocksdb_bench_SBC_1GB_raw_1024 " + DBPath).c_str());
  // }

#ifndef NDEBUG
  DBPath += "_DBG";
  FLAGS_create_new_db = true;
  data_size = 150ul << 20;
  key_num = data_size / (value_size+22ll);
  FLAGS_op_count = 10000;
  // key_per_client = key_num / client_num;
#endif

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << "\n ValueSize: " << FLAGS_value_size
    << "\n Bind core: " << FLAGS_bind_core 
    << "\n Cache size: " << BytesToHumanString(FLAGS_cache_size) 
    << "\n Distribution: " << FLAGS_distribution
    << "\n Client num (Read:Write:Scan) (" << client_num_read << ", " << client_num_write << ", " << client_num_scan
    << ")\n Core num: " << FLAGS_core_num
    << "\n Enable sbc: " << FLAGS_enable_sbc
    << "\n Client_num_scan: " << FLAGS_client_num_scan
    << "\n Client_num_read: " << FLAGS_client_num_read
    << "\n Client_num_write: " << FLAGS_client_num_write
    << "\n Level-0 slowdown trigger: " << FLAGS_l0_stalling_limit
    << "\n";

  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  auto hist_write = std::make_shared<HistogramImpl>();
  auto hist_read = std::make_shared<HistogramImpl>();
  auto hist_insert = std::make_shared<HistogramImpl>();
  auto hist_scan = std::make_shared<HistogramImpl>();
  auto hist_tail_read = std::make_shared<HistogramImpl>();
  auto hist_tail_read_cpu = std::make_shared<HistogramImpl>();
  auto hist_tail_read_io = std::make_shared<HistogramImpl>();

  hist_.insert({kWrite, std::move(hist_write)});
  hist_.insert({kRead, std::move(hist_read)});
  hist_.insert({kScan, std::move(hist_scan)});
  hist_.insert({kInsert, std::move(hist_insert)});
  hist_.insert({kTailRead, std::move(hist_tail_read)});
  hist_.insert({kTailReadCPU, std::move(hist_tail_read_cpu)});
  hist_.insert({kTailReadIO, std::move(hist_tail_read_io)});

  if (access(DataRaw.c_str(), 0) == 0) {
    // 文件夹存在
    system(("rm -rf " + DBPath).c_str());
    system(("cp -rf " + DataRaw + " " + DBPath).c_str());
    std::cout << "Copy " << DataRaw << " to " << DBPath << "\n";
  }

  // 判断数据库能不能打开，不能打开就要重新插数据
  DB *db_tmp = nullptr;
  Options opt_tmp;
  Status s_tmp = DB::Open(opt_tmp, DBPath, &db_tmp);
  delete db_tmp;
  
  // 如果数据库打不开或者强制重建数据库，才会重新插数据
  if(FLAGS_create_new_db || s_tmp != Status::OK()){
    InsertDataImpl(options_ins, DBPath, key_num, &hist_, nullptr);
  }

  Options options;
  options.use_direct_reads = true;
  options.disable_auto_compactions = FLAGS_disable_auto_compactions;
  options.level0_slowdown_writes_trigger = FLAGS_l0_stalling_limit;
  options.use_direct_io_for_flush_and_compaction = true;
  options.max_bytes_for_level_multiplier = FLAGS_level_multiplier;
  options.enable_sbc = FLAGS_enable_sbc;
  std::atomic<bool> running = true;
  std::atomic<int64_t> op_count_ = 0;
  size_t op_count_list[100];
  std::vector<std::vector<uint64_t>> log_[100];

  if(FLAGS_cache_size > 0) {
    std::shared_ptr<Cache> cache = NewLRUCache(FLAGS_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  auto s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";

  auto ReadWrite = [&](size_t min, size_t max, int idx, OperationType op){
    if(FLAGS_bind_core) {
      // 绑定核心
      for (int i = 0; i < FLAGS_core_num; i++) {
        CPU_SET(i, &cpuset);
      }
      int state = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    }

    unsigned long seed_;
    Random rnd(301);
    std::default_random_engine gen;
    std::default_random_engine gen_key;

    std::uniform_int_distribution<size_t> key_gen_uniform(min, max);
    // UniformGenerator key_gen_uniform(min, max);
    // Random key_gen_uniform(idx);
    UniformGenerator scan_len_uniform(10, 1000);
    // ZipfianGenerator key_gen_zipfian(min, max);  // FIXME: 这里的范围过大会导致启动这个函数极慢

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
    if (true) {
      SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
      prev_write_nanos = IOSTATS(write_nanos);
      prev_fsync_nanos = IOSTATS(fsync_nanos);
      prev_range_sync_nanos = IOSTATS(range_sync_nanos);
      prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
      prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
      prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
    }

    gen_key.seed(idx);
    size_t w_count = 0;
    size_t r_count = 0;
    size_t scan_count = 0;
    char buf[100];
    
    while(running) {
      size_t key;
      key = key_gen_uniform(gen_key);
      if(FLAGS_distribution == 0) {
        
      } else if(FLAGS_distribution == 1) {
        // key = key_gen_zipfian.Next_hash();
      } else {
        abort();
      }
      snprintf(buf, sizeof(buf), "key%09ld", key);

      if(op == kWrite) {
        // Write operation
        auto value_t = rnd.RandomString(FLAGS_value_size);
        auto wo = WriteOptions();
        wo.disableWAL = FLAGS_disableWAL;

        auto start_ = std::chrono::system_clock::now();
        prev_read_nanos = IOSTATS(read_nanos);
        prev_write_nanos = IOSTATS(write_nanos);
        prev_fsync_nanos = IOSTATS(fsync_nanos);
        prev_range_sync_nanos = IOSTATS(range_sync_nanos);
        prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
        prev_cpu_micros = clock->CPUMicros();
        db->Put(wo, Slice(buf, 12), value_t);
        now_cpu_micros = clock->CPUMicros();
        now_read_nanos = IOSTATS(read_nanos);
        now_write_nanos = IOSTATS(write_nanos);
        now_fsync_nanos = IOSTATS(fsync_nanos);
        now_range_sync_nanos = IOSTATS(range_sync_nanos);
        now_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        now_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        now_cpu_read_nanos = IOSTATS(cpu_read_nanos);

        auto end_ = std::chrono::system_clock::now();
        w_count++;
        uint64_t dur_ = std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count();
        hist_[kWrite]->Add(dur_);
        uint64_t io_dur = now_read_nanos + now_cpu_read_nanos + now_cpu_write_nanos + now_prepare_write_nanos + now_range_sync_nanos + now_fsync_nanos + now_write_nanos - 
            (prev_read_nanos + prev_cpu_read_nanos + prev_cpu_write_nanos + prev_prepare_write_nanos + prev_range_sync_nanos + prev_fsync_nanos + prev_write_nanos);
        io_dur /= 1000; // ns -> us

        hist_[kTailRead]->Add(dur_);
        hist_[kTailReadCPU]->Add(now_cpu_micros - prev_cpu_micros);
        hist_[kTailReadIO]->Add(io_dur);
        
        // log_[idx].push_back({dur_, now_cpu_micros - prev_cpu_micros, io_dur});
      } else if(op == kRead) {
        // Get operation
        std::string value;
        auto start_ = std::chrono::system_clock::now();
        prev_read_nanos = IOSTATS(read_nanos);
        prev_write_nanos = IOSTATS(write_nanos);
        prev_fsync_nanos = IOSTATS(fsync_nanos);
        prev_range_sync_nanos = IOSTATS(range_sync_nanos);
        prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
        prev_cpu_micros = clock->CPUMicros();
        db->Get(ReadOptions(), Slice(buf, 12), &value);
        now_cpu_micros = clock->CPUMicros();
        auto end_ = std::chrono::system_clock::now();
        now_read_nanos = IOSTATS(read_nanos);
        now_write_nanos = IOSTATS(write_nanos);
        now_fsync_nanos = IOSTATS(fsync_nanos);
        now_range_sync_nanos = IOSTATS(range_sync_nanos);
        now_prepare_write_nanos = IOSTATS(prepare_write_nanos);
        now_cpu_write_nanos = IOSTATS(cpu_write_nanos);
        now_cpu_read_nanos = IOSTATS(cpu_read_nanos);
        
        uint64_t dur_ = std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count();

        uint64_t io_dur = now_read_nanos + now_cpu_read_nanos + now_cpu_write_nanos + now_prepare_write_nanos + now_range_sync_nanos + now_fsync_nanos + now_write_nanos - 
            (prev_read_nanos + prev_cpu_read_nanos + prev_cpu_write_nanos + prev_prepare_write_nanos + prev_range_sync_nanos + prev_fsync_nanos + prev_write_nanos);
        io_dur /= 1000; // ns -> us

        // log_[idx].push_back({dur_, now_cpu_micros - prev_cpu_micros, io_dur});
        hist_[kRead]->Add(dur_);
        r_count++;
      } else if(op == kScan) {
        // Scan operation
        std::string start = "";
        std::string end = "key9"; 
        // int scan_len = scan_len_uniform.Next();
        // if(start == nullptr && end == nullptr){
        //   scan_len = INT_MAX;
        // }
        std::cout << "Scan start: " << FilesPerLevel(db, 0) << "\n";

        auto start_ = std::chrono::system_clock::now();
        auto iter = db->NewSBCIterator(ReadOptions(), &start, &end);
        for(iter->Seek(start);iter->Valid();iter->SBCNext()) {}
        auto end_ = std::chrono::system_clock::now();

        db->FinishSBC(iter);
        std::cout << "Scan done: " << FilesPerLevel(db, 0) << "\n";

        hist_[kScan]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
        scan_count++;
      } else {
        // Do nothing
      }
      op_count_list[idx]++;
      op_count_.fetch_add(1, std::memory_order_relaxed);
    }
    std::cout<<"Thread: " << idx 
      <<", Write:Read:Scan (" << w_count<<", "<<r_count<<", " << scan_count << ")\n";
  };
  
  std::string core_name_list[10] = {
    "cpu0 ",
    "cpu1 ",
    "cpu2 ",
    "cpu3 ",
    "cpu4 ",
    "cpu5 ",
    "cpu6 ",
    "cpu7 ",
    "cpu8 ",
    "cpu9 "
  };

  std::vector<std::thread> client_threads;
  std::vector<std::string> cpu_set;
  CPUStat::CPU_OCCUPY cpu_stat1[20];
  CPUStat::CPU_OCCUPY cpu_stat2[20];
  std::vector<int> tps;
  tps.reserve(FLAGS_run_time * 1000 / FLAGS_interval);

  // 最多监控10个核心
  for (int i = 0; i < FLAGS_core_num && i < 10; i++) {
    cpu_set.push_back(core_name_list[i]);
  }
  
  for (size_t i = 0; i < cpu_set.size(); i++) {
    CPUStat::get_cpuoccupy((CPUStat::CPU_OCCUPY *)&cpu_stat1[i], cpu_set[i].c_str());
  }

  // Workload start
  auto start = GetTimeNow();
  uint64_t op_last = op_count_;
  for (size_t i = 0; i < client_num_read; i++) {
    client_threads.emplace_back(std::thread(ReadWrite, 0, FLAGS_key_range, i, kRead));
  }
  for (size_t i = 0; i < client_num_write; i++) {
    client_threads.emplace_back(std::thread(ReadWrite, 0, FLAGS_key_range, i, kWrite));
  }
  for (size_t i = 0; i < client_num_scan; i++) {
    client_threads.emplace_back(std::thread(ReadWrite, 0, FLAGS_key_range, i, kScan));
  }
  // Statistic CPU
  std::thread cpu_rec = std::thread(CPUStat::GetCPUStatMs, cpu_set);

  // Statistic IO
  // std::string disk_name = "nvme2n1p1 ";
  // std::thread io_stat = std::thread(IOStat::GetIOStatMs, disk_name, 100000);

  // sleep(FLAGS_run_time);
  auto run_time = FLAGS_run_time * 1000; // ms
  auto last_time = start;

  while (run_time > 0) {
    if(GetTimeIntervalMsFrom(last_time) > FLAGS_interval) {
      last_time = GetTimeNow();
      tps.emplace_back(op_count_ - op_last);
      run_time -= FLAGS_interval;
      op_last = op_count_;
    }
  }
  
  running = false;
  
  // Wait workload finished
  for (auto &&client_thread : client_threads) {
    client_thread.join();
  }
  CPUStat::run = false;
  IOStat::run = false;
  cpu_rec.join();
  // io_stat.join();

  // Workload end
  auto duration =  GetTimeIntervalMsFrom(start);
  for (size_t i = 0; i < cpu_set.size(); i++) {
    CPUStat::get_cpuoccupy((CPUStat::CPU_OCCUPY *)&cpu_stat2[i], cpu_set[i].c_str());
    CPUStat::cal_cpuoccupy(&cpu_stat1[i], &cpu_stat2[i]);
  }
  
  std::cout << "Throughput: [" << op_count_ << ", " << duration / 1000 << "s, " << op_count_*1.0 / (duration) << " Kop/s] \n";
  std::cout << "Read: " << hist_[kRead]->ToString() << "\n";
  std::cout << "Write: " << hist_[kWrite]->ToString() << "\n";
  std::cout << "Scan: " << hist_[kScan]->ToString() << "\n";
  std::cout << "TailRead: " << hist_[kTailRead]->ToString() << "\n";
  std::cout << "TailReadCPU: " << hist_[kTailReadCPU]->ToString() << "\n";
  std::cout << "TailReadIO: " << hist_[kTailReadIO]->ToString() << "\n";

  // 将读操作的记录输出到文件
  std::vector<std::vector<uint64_t>> read_op_all_;
  for (int i = 0; i < FLAGS_client_num; i++) {
    read_op_all_.insert(read_op_all_.end(), log_[i].begin(), log_[i].end());
  }
  
  std::sort(read_op_all_.begin(), read_op_all_.end(), [](const std::vector<uint64_t> &a, const std::vector<uint64_t> &b) {
      return a[0] < b[0];
  });

  std::cout << "Ans write to output.csv, count: " << read_op_all_.size() << "\n";
  std::ofstream file("output.csv");
  if (file.is_open()) {
    for (size_t i = 0; i < read_op_all_.size(); i++) {
      for (int j = 0; j < 3; j++) {
        file << read_op_all_[i][j] << " ";
      }
      file << std::endl;
    }
    file.close();
  } else {
     std::cout << "Unable to open output.csv";
  }

  std::ofstream optput_tps("tps.txt");
  if (optput_tps.is_open()) {
      for (const auto& element : tps) {
          optput_tps << element << "\n";
      }
      optput_tps.close();
      std::cout << "Write tps.txt" << std::endl;
  } else {
      std::cout << "Unable to open tps.txt" << std::endl;
  }
}

}  // namespace ROCKSDB_NAMESPACE


int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_workloads == 0) {

  } else if(FLAGS_workloads == 1) {
    rocksdb::TestMixWorkload();
  } else if(FLAGS_workloads == 2) {
    rocksdb::TestMixWorkloadWithDiffThread();
  } else if(FLAGS_workloads == 3) {
    rocksdb::InsertOnly();
  } else {
    std::cout << "Error workload: " << FLAGS_workloads <<" workload\n";
  }

  return 0;
}
