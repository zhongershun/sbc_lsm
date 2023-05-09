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

DEFINE_int32(value_size, 256, "");
DEFINE_bool(use_sync, false, "");
DEFINE_bool(bind_core, true, "");
DEFINE_uint64(data_size, 1ll<<30, "");
DEFINE_int32(write_rate, 100, "");
DEFINE_int32(read_rate, 100, "");
DEFINE_int32(scan_rate, 100, "");
DEFINE_int32(core_num, 4, "");
DEFINE_int32(client_num, 10, "");
DEFINE_int64(read_count, 100, "");
DEFINE_int32(workloads, 4, ""); 
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
  }

  // FIXME:
  // if(FLAGS_disk_type == 0) {
  //   DBPath = "/test/rocksdb_bench_my_mix_" + std::to_string(FLAGS_value_size);
  // }

#ifndef NDEBUG
  DBPath += "_DBG";
  FLAGS_create_new_db = true;
  data_size = 100ul << 20;
  key_num = data_size / (value_size+22ll);
  FLAGS_read_count = 10000;
  // key_per_client = key_num / client_num;
#endif

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << " MB\n ValueSize: " << FLAGS_value_size
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
    InsertData(options_ins, DBPath, key_num, hist_);
  }

  Options options;
  options.use_direct_reads = true;
  options.disable_auto_compactions = FLAGS_disable_auto_compactions;
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
    // [cnt, dur, cpu, io]
    std::vector<std::vector<uint64_t>> hist_lat_ = {
      {1, 0, 0, 0},     // [000, 250) us
      {1, 0, 0, 0},     // [250-500) us
      {1, 0, 0, 0},     // [500-750) us
      {1, 0, 0, 0},     // [750-1000) us
      {1, 0, 0, 0},     // [1000, +inf) us
    };
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
        int idx_ = dur_ / 250;
        if(idx_ < 4) {
          hist_lat_[idx_][0]++;
          hist_lat_[idx_][1] += dur_;
          hist_lat_[idx_][2] += (now_cpu_micros - prev_cpu_micros);
          hist_lat_[idx_][3] += io_dur;
        } else {
          hist_[kTailRead]->Add(dur_);
          hist_[kTailReadCPU]->Add(now_cpu_micros - prev_cpu_micros);
          hist_[kTailReadIO]->Add(io_dur);
          hist_lat_[4][0]++;
          hist_lat_[4][1] += dur_;
          hist_lat_[4][2] += (now_cpu_micros - prev_cpu_micros);
          hist_lat_[4][3] += io_dur;
        }
        log_[idx].push_back({dur_, now_cpu_micros - prev_cpu_micros, io_dur});
        hist_[kRead]->Add(dur_);
        r_count++;
      } else {
        auto iter = db->NewIterator(ReadOptions());
        iter->Seek(Slice(buf, 12));
        int scan_len = scan_len_uniform.Next();
        auto start_ = std::chrono::system_clock::now();
        for (int i = 0; i < scan_len && iter->Valid(); i++) {
          iter->Next();
        }
        auto end_ = std::chrono::system_clock::now();
        hist_[kScan]->Add(std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count());
        scan_count++;
      }
      op_count_list[idx]++;
      op_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    std::cout<<"Thread: " << idx <<", Write:Read (" << w_count<<", "<<r_count<<")\n";
    for (size_t i = 0; i < hist_lat_.size(); i++) {
      std::cout << "[" << i*250 << ", " << (i+1)*250 
        << "), Tail AVG: " << hist_lat_[i][1] / hist_lat_[i][0] 
        << " us, CPU: " << hist_lat_[i][2] / hist_lat_[i][0] 
        << " us, IO: " << hist_lat_[i][3] / hist_lat_[i][0] << " us\n";
    }
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

  op_count_ = FLAGS_read_count;
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


void TestIOStat() {
  Options options_ins;
  options_ins.create_if_missing = true;  
  options_ins.use_fsync = true;
  DB* db = nullptr;

  std::string DBPath = "./rocksdb_bench_my_mix_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = FLAGS_data_size;
  size_t value_size = FLAGS_value_size;
  size_t client_num = FLAGS_client_num;
  size_t key_num = data_size / (value_size+22ll);
  // size_t key_per_client = key_num / client_num; 

  if(FLAGS_disk_type == 0) {
    DBPath = "/test/rocksdb_bench_my_mix_" + std::to_string(FLAGS_value_size);
  }

#ifndef NDEBUG
  DBPath += "_DBG";
  FLAGS_create_new_db = true;
  data_size = 100ul << 20;
  key_num = data_size / (value_size+22ll);
  // key_per_client = key_num / client_num;
#endif

  std::cout << "DB path:" << DBPath
    << "\n Data size: " << BytesToHumanString(data_size)
    << " MB\n ValueSize: " << FLAGS_value_size
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
  hist_.insert({kWrite, std::move(hist_write)});
  hist_.insert({kRead, std::move(hist_read)});
  hist_.insert({kInsert, std::move(hist_insert)});

  InsertData(options_ins, DBPath, key_num, hist_);


  Options options;
  options.use_direct_reads = true;
  std::atomic<int64_t> op_count_;
  size_t op_count_list[100];

  auto s = DB::Open(options, DBPath, &db);
  std::cout << "Init table num: " << FilesPerLevel(db, 0) << "\n";

  auto ReadWrite = [&](size_t min, size_t max, int idx, int read, int write){
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
        auto wo = WriteOptions();
        wo.disableWAL = FLAGS_disableWAL;
        db->Put(wo, Slice(buf, 12), value_t);
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

  IOStat::run = true;
  std::string disk_name = "nvme0n1 ";
  std::thread io_stat = std::thread(IOStat::GetIOStatMs, disk_name, 100000);
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < client_num; i++) {
    client_threads.emplace_back(std::thread(ReadWrite, 0, key_num, i, FLAGS_read_rate, FLAGS_write_rate));
  }

  // Wait workload finished
  for (size_t i = 0; i < client_num; i++) {
    client_threads[i].join();
  }

  IOStat::run = false;
  io_stat.join();
  // Workload end
  auto end = std::chrono::system_clock::now();
  auto duration =  std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
  
  std::cout << "Throughput: [" << op_sum << ", " << duration / 1000 << "s, " << op_sum*1.0 / (duration) << " Kop/s] \n";
  std::cout << "Read: " << hist_[kRead]->ToString() << "\n";
  std::cout << "Write: " << hist_[kWrite]->ToString() << "\n";

}


int WriteFile(std::string file_name, int file_size = 500 << 20) {
  std::ofstream outFile(file_name.c_str(), std::ios::out | std::ios::binary);

  if (!outFile) {
      std::cout << "无法打开文件: " << file_name << std::endl;
      return 1;
  }
  // 写入文件内容
  const int BUFFER_SIZE = 1024 * 1024; // 每次写入1MB
  char buffer[BUFFER_SIZE];
  for (int i = 0; i < BUFFER_SIZE; i++) {
      buffer[i] = 'A';
  }
  int bytesWritten = 0;
  while (bytesWritten < file_size) {
      int bytesToWrite = std::min(file_size - bytesWritten, BUFFER_SIZE);
      outFile.write(buffer, bytesToWrite);
      bytesWritten += bytesToWrite;
  }
  outFile.flush();
  std::cout << "Write file: " << file_name << ", Size: " << file_size/(1<<20) << " MB" << std::endl;
  // 关闭文件
  outFile.close();
  return 0;
}


int ReadFile(std::string file_name, int file_size = 500 << 20) {
  std::ifstream inFile(file_name.c_str(), std::ios::in | std::ios::binary);

  if (!inFile) {
      std::cout << "无法打开文件" << file_name << std::endl;
      return 1;
  }
  // 获取文件大小
  inFile.seekg(0, std::ios::end);
  int fileSize = inFile.tellg();
  inFile.seekg(0, std::ios::beg);

  if (fileSize > file_size) {
      fileSize = file_size;
  }
  // 读取文件内容
  char* buffer = new char[fileSize];
  inFile.read(buffer, fileSize);
  // 输出读取的内容
  std::cout << "读取了 " << fileSize/(1<<20) << " MB的文件内容：" << std::endl;
  // 关闭文件和释放内存
  inFile.close();
  delete[] buffer;
  return 0;
}


int DirectIO(std::string file_name, int file_size = 10 << 20){
  // 打开文件
  int fd = open(file_name.c_str(), O_RDWR | O_DIRECT | O_SYNC, 0666);
  if (fd == -1) {
      std::cerr << "Failed to open file " << file_name << std::endl;
      return 1;
  }
  // 分配内存缓冲区
  char* buffer;
  const size_t kBufferSize = 1024*1024;

  if (posix_memalign((void**)&buffer, kBufferSize, kBufferSize) != 0) {
      std::cerr << "Failed to allocate buffer" << std::endl;
      close(fd);
      return 1;
  }
  // 写入文件
  for (size_t i = 0; i < kBufferSize; ++i) {
      buffer[i] = 'A' + (i % 26);
  }
  off_t offset = 0;
  auto write_fn = [&] () -> ssize_t {
    auto start_ = std::chrono::system_clock::now();
    ssize_t ret = pwrite(fd, buffer, kBufferSize, offset);
    auto end_ = std::chrono::system_clock::now();
    IOSTATS_ADD(write_nanos, std::chrono::duration_cast<std::chrono::nanoseconds>(end_-start_).count());
    return ret;
  };
  while (offset < file_size) {
      ssize_t ret = write_fn();
      if (ret == -1) {
          std::cerr << "Failed to write to file" << std::endl;
          free(buffer);
          close(fd);
          return 1;
      }
      offset += kBufferSize;
  }
  // 读取文件
  offset = 0;
  auto read_fn = [&] () -> ssize_t {  
    auto start_ = std::chrono::system_clock::now();
    ssize_t ret = pread(fd, buffer, kBufferSize, offset);
    auto end_ = std::chrono::system_clock::now();
    IOSTATS_ADD(read_nanos, std::chrono::duration_cast<std::chrono::nanoseconds>(end_-start_).count());
    return ret;
  };
  while (offset < file_size) {
      ssize_t ret = read_fn();
      if (ret == -1) {
          std::cerr << "Failed to read from file" << std::endl;
          free(buffer);
          close(fd);
          return 1;
      }
      offset += kBufferSize;
  }
  // 释放缓冲区和关闭文件
  free(buffer);
  close(fd);
  return 0;
}

void TestCPUMicros() {
  uint64_t count = 10000000;
  Env *env = Env::Default();
  uint64_t now_cpu_micros;
  SystemClock *clock = env->GetSystemClock().get();

  uint64_t prev_cpu_micros = clock->CPUMicros();
  auto start_ = std::chrono::system_clock::now();
  for (volatile size_t i = 0; i < count; i++)
  {}
  now_cpu_micros = clock->CPUMicros();
  auto end_ = std::chrono::system_clock::now();
  std::cout << "Used_cpu_micros | volatile for | CPU Micros: " << now_cpu_micros - prev_cpu_micros 
    << " us, Time: " << std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count() << " us\n";

  prev_cpu_micros = clock->CPUMicros();
  start_ = std::chrono::system_clock::now();
  sleep(1);
  now_cpu_micros = clock->CPUMicros();
  end_ = std::chrono::system_clock::now();
  std::cout << "Used_cpu_micros | Sleep | CPU Micros: " << now_cpu_micros - prev_cpu_micros 
    << " us, Time: " << std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count() << " us\n";

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

  prev_read_nanos = IOSTATS(read_nanos);
  prev_write_nanos = IOSTATS(write_nanos);
  prev_fsync_nanos = IOSTATS(fsync_nanos);
  prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
  prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
  prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  prev_cpu_micros = clock->CPUMicros();
  start_ = std::chrono::system_clock::now();
  // sleep(1);
  DirectIO("test1"); 
  now_cpu_micros = clock->CPUMicros();
  end_ = std::chrono::system_clock::now();
  now_read_nanos = IOSTATS(read_nanos);
  now_write_nanos = IOSTATS(write_nanos);
  now_fsync_nanos = IOSTATS(fsync_nanos);
  now_prepare_write_nanos = IOSTATS(prepare_write_nanos);
  now_cpu_write_nanos = IOSTATS(cpu_write_nanos);
  now_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  uint64_t io_dur = now_read_nanos + now_cpu_read_nanos + now_cpu_write_nanos + now_prepare_write_nanos + now_range_sync_nanos + now_fsync_nanos + now_write_nanos - 
    (prev_read_nanos + prev_cpu_read_nanos + prev_cpu_write_nanos + prev_prepare_write_nanos + prev_range_sync_nanos + prev_fsync_nanos + prev_write_nanos);
  io_dur /= 1000; // ns -> us
  std::cout << "Used_cpu_micros | Write | CPU Micros: " << now_cpu_micros - prev_cpu_micros 
    << " us, Time: " << std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count()
    << " us, IO dur: " << io_dur << " us\n";

 }


void TestScan() {
  std::string DBPath = "rocksdb_bench_my_scan_1GB_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = 1ll << 30;
  size_t value_size = FLAGS_value_size;
  size_t key_num = data_size / (value_size+12ll);
  FLAGS_read_count = 1;
  if(FLAGS_disk_type == 0){
    DBPath = "/zyn/SSD/test_RocksDB/" + DBPath;
  }

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

void TestCompaction() {
  std::string DBPath = "rocksdb_bench_my_compaction_1GB_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = 1ll << 30;
#ifndef NDEBUG
  data_size = 100ll << 20;
#endif
  size_t value_size = FLAGS_value_size;
  size_t key_num = data_size / (value_size+22ll);
  if(FLAGS_disk_type == 0){
    DBPath = "/zyn/SSD/test_RocksDB/" + DBPath;
  }

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


  Options options_ins;
  options_ins.create_if_missing = true;  
  
  InsertData(options_ins, DBPath, key_num, hist_);


  DB* db = nullptr;
  Options options;
  options.use_direct_reads = true;
  options.disable_auto_compactions = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.report_bg_io_stats = true;
  options.statistics = rocksdb::CreateDBStatistics();

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
  
  uint64_t dur = 0;

  options.statistics->Reset();
  auto start_ = std::chrono::system_clock::now();
  db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  auto end_ = std::chrono::system_clock::now();

  dur += std::chrono::duration_cast<std::chrono::microseconds>(end_-start_).count();

  std::cout << "Read IO: " << options.statistics->getTickerCount(COMPACTION_IO_READ) / 1000.0
            << " us, Write IO: " << options.statistics->getTickerCount(COMPACTION_IO_WRITE) / 1000.0
            << " us, CPU: " << options.statistics->getTickerCount(COMPACTION_CPU_TIME) 
            << " us, Duration: " << options.statistics->getTickerCount(COMPACTION_TIME)
            << " us, Duration2: " << dur
            << " us\nTable num: " << FilesPerLevel(db, 0) << "\n";
}

void TestScanOnCompaction() {
  std::string DBPath = "rocksdb_bench_my_compaction_1GB_" + std::to_string(FLAGS_value_size);
  uint64_t data_size = 1ll << 30;
  size_t value_size = FLAGS_value_size;
  size_t key_num = data_size / (value_size+22ll);
  FLAGS_read_count = 1;
  if(FLAGS_disk_type == 0){
    DBPath = "/zyn/SSD/test_RocksDB/" + DBPath;
  }

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
    TestCompaction();
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
    SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
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
  std::cout << "Scan on compaction, Read IO: " << read_io / 1000 / FLAGS_read_count
            << " us, Write IO: " << write_io / 1000 / FLAGS_read_count
            << " us, CPU: " << cpu_dur / FLAGS_read_count
            << " us, Duration: " << dur / FLAGS_read_count
            << "us\n";

}

}  // namespace ROCKSDB_NAMESPACE


int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_workloads == 1) {
    std::cout << FLAGS_workloads <<"TestScanOnCompaction()\n";
    rocksdb::TestScanOnCompaction();
  } else if(FLAGS_workloads == 4){
    std::cout << FLAGS_workloads <<"TestScan()\n";
    rocksdb::TestScan();
  } else if(FLAGS_workloads == 5) {
     std::cout << FLAGS_workloads <<"TestCompaction()\n";
    rocksdb::TestCompaction();
  } else if(FLAGS_workloads == 6) {
    rocksdb::TestMixWorkload();
  } else if(FLAGS_workloads == 7) {
    rocksdb::TestIOStat();
  } else if(FLAGS_workloads == 8) {
    rocksdb::TestCPUMicros();
  } else {
    std::cout << "Error workload: " << FLAGS_workloads <<" workload\n";
  }
  
  // ::testing::InitGoogleTest(&argc, argv);
  // ROCKSDB_NAMESPACE::MyTest(value_size, sub_compactions, bind_core, data_size);
  // return RUN_ALL_TESTS();
  return 0;
}
