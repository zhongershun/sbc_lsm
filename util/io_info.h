#include "sys/time.h"
#include <iostream>
#include <fstream>
#include <string.h>
#include <vector>
#include <atomic>
#include <thread>


namespace {

static inline int64_t GetUnixTimeUs() {
  struct timeval tp;
  gettimeofday(&tp, nullptr);
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}


typedef struct IOPACKED         //定义一个io occupy的结构体  
{  
    unsigned long long major;       //主设备号
    unsigned long long minor;       //次设备号,设备号是用来区分磁盘的类型和厂家信息
    char name[20];            //设备名称
    unsigned long long rd_ios;      //读完成次数
    unsigned long long rd_merges;   //合并读完成次数,为了效率可能会合并相邻的读和写.从而两次4K的读在它最终被处理到磁盘上之前可能会变成一次8K的读,才被计数（和排队）,因此只有一次I/O操作
    unsigned long long rd_sectors;  //读扇区的次数
    unsigned long long rd_ticks;    //读花费的毫秒数
    unsigned long long wr_ios;      //写完成次数
    unsigned long long wr_merges;   //合并写完成次数
    unsigned long long wr_sectors;  //写扇区次数
    unsigned long long wr_ticks;    //写花费的毫秒数
    unsigned long long cur_ios;     //正在处理的输入/输出请求数
    unsigned long long ticks;       //输入/输出操作花费的毫秒数
    unsigned long long aveq;        //输入/输出操作花费的加权毫秒数
}IO_OCCUPY;  


// Get target IO stat
int GetIOStat(IO_OCCUPY *iost, const char *name)  
{  
    FILE *fd;  
    char buff[1024];  
    IO_OCCUPY *io_occupy;  
    io_occupy = iost;  
      
    fd = fopen("/proc/diskstats", "r");  

    for (int i = 0; i < 100; i++)
    {
      fgets(buff, sizeof(buff), fd);  
      // std::cout << buff << "\n";
      if(strcmp(std::string(buff+13, strlen(name)).c_str(), name)==0){
        sscanf(buff, "%llu %llu %s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu", 
        &io_occupy->major, &io_occupy->minor, 
        io_occupy->name, &io_occupy->rd_ios, 
        &io_occupy->rd_merges, &io_occupy->rd_sectors, 
        &io_occupy->rd_ticks, &io_occupy->wr_ios,
        &io_occupy->wr_merges, &io_occupy->wr_sectors,
        &io_occupy->wr_ticks, &io_occupy->cur_ios,
        &io_occupy->ticks, &io_occupy->aveq);
        // std::cout << io_occupy->name << std::endl;
        break;
      }
    }
    fclose(fd);  
    return 0;  
}  

struct Stat
{
  uint64_t read_cnt = 0;
  uint64_t write_cnt = 0;
  uint64_t cur_ios = 0;
  double wr_wait = 0; // ms
  double rd_wait = 0; // ms
  double io_wait = 0; // ms
};


std::atomic<bool> run;


// 最多追踪20个核心
static void GetIOStatMs(std::string disk_name, int64_t interval = 100000/* us */){
  int cpu_core = 30; //这个线程使用的核心号
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_core, &cpuset);
  int state = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  run = true;
  auto last = GetUnixTimeUs();
  std::vector<Stat>ans;
  ans.reserve(10000);
  

  IO_OCCUPY io_stat1;
  IO_OCCUPY io_stat2;

  GetIOStat((IO_OCCUPY *)&io_stat1, disk_name.c_str());
  
  while (run) {
    if(GetUnixTimeUs()-last > 10000){
      Stat stat;
      GetIOStat((IO_OCCUPY *)&io_stat2, disk_name.c_str());
      auto rd_ios = io_stat2.rd_ios - io_stat1.rd_ios;
      auto wr_ios = io_stat2.wr_ios - io_stat1.wr_ios;
      auto wr_ticks = io_stat2.wr_ticks - io_stat1.wr_ticks;
      auto rd_ticks = io_stat2.rd_ticks - io_stat1.rd_ticks;

      if(rd_ios > 10000000) rd_ios = 0;
      if(wr_ios > 10000000) wr_ios = 0;
      if(wr_ticks > 10000000) wr_ticks = 0;
      if(rd_ticks > 10000000) rd_ticks = 0;

      stat.read_cnt = rd_ios;
      stat.write_cnt = wr_ios;
      stat.wr_wait = wr_ios ? wr_ticks * 1.0 / wr_ios : 0.0;    
      stat.rd_wait = rd_ios ? rd_ticks * 1.0 / rd_ios : 0.0;
      stat.io_wait = (wr_ios + rd_ios) ? (wr_ticks + rd_ticks) * 1.0 / (wr_ios + rd_ios) : 0.0;
      stat.cur_ios = io_stat2.cur_ios - io_stat1.cur_ios;
      if(stat.cur_ios > 10000000)stat.cur_ios=0;
      io_stat1 = io_stat2;

      ans.emplace_back(stat);
      last = GetUnixTimeUs();
    }
  }
  std::fstream iostat_file;
  iostat_file.open("./io_stat.csv", std::ios::out);
  iostat_file << "read_cnt, write_cnt, cur_ios, rd_wait, wr_wait, lowait\n";
  for (auto &&s : ans) {
    iostat_file << s.read_cnt << ", " << s.write_cnt << ", " 
      << s.cur_ios << ", " << s.rd_wait << ", "
      << s.wr_wait << ", " << s.io_wait << std::endl;
  }
  iostat_file.close();
  std::cout << "Printed io stat\n";
}

}

namespace IOStat
{

static std::thread StartIOStat(std::string disk_name, int64_t interval = 100000/* us */) {
  return std::thread(GetIOStatMs, disk_name, interval);
}

static void StopIOStat(std::thread &t) {
  run = false;
  t.join();
}

} // namespace IOStat

/*
// examples
IO_OCCUPY io_stat1;  
IO_OCCUPY io_stat2;

//第一次获取io使用情况  
get_iooccupy((IO_OCCUPY *)&io_stat1);  
std::thread io_rec = std::thread(GetIOStatMs);
//第二次获取io使用情况  
get_iooccupy((IO_OCCUPY *)&io_stat2);  
//计算io使用率
cal_iooccupy((IO_OCCUPY *)&io_stat1, (IO_OCCUPY *)&io_stat2);  

*/