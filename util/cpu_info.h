#include "sys/time.h"
#include <iostream>
#include <fstream>
#include <string.h>
#include <vector>
#include <atomic>

namespace CPUStat
{
static inline int64_t GetUnixTimeUs() {
  struct timeval tp;
  gettimeofday(&tp, nullptr);
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

typedef struct CPUPACKED         //定义一个cpu occupy的结构体  
{  
    char name[20];      //定义一个char类型的数组名name有20个元素  
    unsigned int user; //定义一个无符号的int类型的user  
    unsigned int nice; //定义一个无符号的int类型的nice  
    unsigned int system;//定义一个无符号的int类型的system  
    unsigned int idle; //定义一个无符号的int类型的idle  
    unsigned int lowait;  
    unsigned int irq;  
    unsigned int softirq;  
}CPU_OCCUPY;  

// Get target CPU stat
int get_cpuoccupy(CPU_OCCUPY *cpust, const char *name)  
{  
    FILE *fd;  
    char buff[1024];  
    CPU_OCCUPY *cpu_occupy;  
    cpu_occupy = cpust;  
      
    fd = fopen("/proc/stat", "r");  

    for (int i = 0; i < 100; i++)
    {
      fgets(buff, sizeof(buff), fd);  
      // std::cout << buff << "\n";
      if(strcmp(std::string(buff, 5).c_str(), name)==0){
        sscanf(buff, "%s %u %u %u %u %u %u %u", 
        cpu_occupy->name, &cpu_occupy->user, 
        &cpu_occupy->nice, &cpu_occupy->system, 
        &cpu_occupy->idle, &cpu_occupy->lowait, 
        &cpu_occupy->irq, &cpu_occupy->softirq);
        // std::cout << cpu_occupy->name << std::endl;
        break;
      } else if(buff[0] != 'c') {
        std::cout << "CPU name error! name: " << name << "\n";
        abort();
        break;
      }
    }
    fclose(fd);  
      
    return 0;  
}  
  
void cal_cpuoccupy(CPU_OCCUPY *o, CPU_OCCUPY *n)  
{  
    unsigned long od, nd;  
    double cpu_use = 0;  
      
    od = (unsigned long)(o->user + o->nice + o->system + o->idle + o->lowait + o->irq + o->softirq);//第一次(用户+优先级+系统+空闲)的时间再赋给od  
    nd = (unsigned long)(n->user + n->nice + n->system + n->idle + n->lowait + n->irq + n->softirq);//第二次(用户+优先级+系统+空闲)的时间再赋给od  
    double sum = nd - od;  
    double idle = n->idle+n->lowait - o->idle - o->lowait;  
    cpu_use = (sum-idle)*1.0 / sum;  
    // idle = n->user + n->system + n->nice - o->user - o->system - o->nice;  
    // cpu_use = idle / sum;  
    printf("usr %u, nice %u, sys %u, idle %u, lowait %u, irq %u, softirq %u\n", 
      n->user - o->user, n->nice - o->nice, n->system - o->system, 
      n->idle - o->idle, n->lowait - o->lowait, n->irq - o->irq, n->softirq - o->softirq);
    printf("CPU usage: %.3f\n",cpu_use);  
}  

struct Stat
{
  uint64_t used = 0;
  uint64_t idle = 0;
  uint64_t lowait = 0;
};

std::atomic<bool> run;
// 最多追踪20个核心
static void GetCPUStatMs(std::vector<std::string> core_set){

  
  int cpu_core = 12; //这个线程使用的核心号
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_core, &cpuset);
  int state = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  run = true;
  auto last = GetUnixTimeUs();
  std::vector<Stat>ans;
  ans.reserve(10000);
  

  CPU_OCCUPY cpu_stat1[20];
  CPU_OCCUPY cpu_stat2[20];

  for (size_t i = 0; i < core_set.size(); i++) {
    get_cpuoccupy((CPU_OCCUPY *)&cpu_stat1[i], core_set[i].c_str());
  }
  
  while (run) {
    if(GetUnixTimeUs()-last > 10000){
      Stat stat;
      for (size_t i = 0; i < core_set.size(); i++) {
        get_cpuoccupy((CPU_OCCUPY *)&cpu_stat2[i], core_set[i].c_str());
        stat.used += cpu_stat2[i].system+cpu_stat2[i].user-cpu_stat1[i].system-cpu_stat1[i].user;
        stat.idle += cpu_stat2[i].idle-cpu_stat1[i].idle;
        stat.lowait += cpu_stat2[i].lowait-cpu_stat1[i].lowait;        
        cpu_stat1[i] = cpu_stat2[i];
      }
      ans.emplace_back(stat);
      last = GetUnixTimeUs();
    }
  }
  std::fstream cpustat_file;
  cpustat_file.open("./cpu_stat.csv", std::ios::out);
  cpustat_file << "used, idle, lowait\n";
  for (auto &&s : ans) {
    cpustat_file << s.used << ", " << s.idle << ", " << s.lowait << std::endl;
  }
  cpustat_file.close();
  std::cout << "Printed cpu stat\n";
}


/*
// examples
CPU_OCCUPY cpu_stat1;  
CPU_OCCUPY cpu_stat2;

//第一次获取cpu使用情况  
get_cpuoccupy((CPU_OCCUPY *)&cpu_stat1);  
std::thread cpu_rec = std::thread(GetCPUStatMs);
//第二次获取cpu使用情况  
get_cpuoccupy((CPU_OCCUPY *)&cpu_stat2);  
//计算cpu使用率
cal_cpuoccupy((CPU_OCCUPY *)&cpu_stat1, (CPU_OCCUPY *)&cpu_stat2);  

*/

} // namespace CPUStat