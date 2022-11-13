#pragma once

#include <cmath>
#include <cassert>
#include <stdio.h>
#include <random>


const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
const uint64_t kFNVPrime64 = 1099511628211;

inline uint64_t FNVHash64(uint64_t val) {
  uint64_t hash = kFNVOffsetBasis64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash = hash ^ octet;
    hash = hash * kFNVPrime64;
  }
  return hash;
}


inline double RandomDouble(double min = 0.0, double max = 1.0) {
  static std::default_random_engine generator;
  static std::uniform_real_distribution<double> uniform(min, max);
  return uniform(generator);
}


class ZipfianGenerator {
 public:
  static double kZipfianConst;
  static const uint64_t kMaxNumItems = (UINT64_MAX >> 24);
  
  ZipfianGenerator(uint64_t min, uint64_t max,
                   double zipfian_const = 0.99) :
      num_items_(max - min + 1), base_(min), theta_(zipfian_const),
      zeta_n_(0), n_for_zeta_(0) {
    assert(num_items_ >= 2 && num_items_ < kMaxNumItems);
    zeta_2_ = Zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    RaiseZeta(num_items_);
    eta_ = Eta();
    
    Next();
  }
  
  ZipfianGenerator(uint64_t num_items) :
      ZipfianGenerator(0, num_items - 1, kZipfianConst) { }
  
  uint64_t Next(uint64_t num_items);
  
  uint64_t Next() { return Next(num_items_); }

  uint64_t Next_hash(){return FNVHash64(Next())%num_items_;}

  uint64_t Last();
  
 private:
  ///
  /// Compute the zeta constant needed for the distribution.
  /// Remember the number of items, so if it is changed, we can recompute zeta.
  ///
  void RaiseZeta(uint64_t num) {
    assert(num >= n_for_zeta_);
    zeta_n_ = Zeta(n_for_zeta_, num, theta_, zeta_n_);
    n_for_zeta_ = num;
  }
  
  double Eta() {
    return (1 - std::pow(2.0 / num_items_, 1 - theta_)) /
        (1 - zeta_2_ / zeta_n_);
  }

  ///
  /// Calculate the zeta constant needed for a distribution.
  /// Do this incrementally from the last_num of items to the cur_num.
  /// Use the zipfian constant as theta. Remember the new number of items
  /// so that, if it is changed, we can recompute zeta.
  ///
  static double Zeta(uint64_t last_num, uint64_t cur_num,
                     double theta, double last_zeta) {
    double zeta = last_zeta;
    for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
      zeta += 1 / std::pow(i, theta);
    }
    return zeta;
  }
  
  static double Zeta(uint64_t num, double theta) {
    return Zeta(0, num, theta, 0);
  }
  
  uint64_t num_items_;
  uint64_t base_; /// Min number of items to generate
  
  // Computed parameters for generating the distribution
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
  uint64_t n_for_zeta_; /// Number of items used to compute zeta_n
  uint64_t last_value_;
};


class UniformGenerator {
 public:
  // Both min and max are inclusive
  UniformGenerator(uint64_t min, uint64_t max) : dist_(min, max) { Next(); }
  
  uint64_t Next();
  uint64_t Last();
  
 private:
  std::mt19937_64 generator_;
  std::uniform_int_distribution<uint64_t> dist_;
  uint64_t last_int_;
//   std::mutex mutex_;
};


class CounterGenerator {
 public:
  CounterGenerator(uint64_t start) : counter_(start) { }
  uint64_t Next() { return ++counter_; }
  uint64_t Last() { return counter_ - 1; }
  void Set(uint64_t start) { counter_ = start; }
 private:
  uint64_t counter_;
};


// 传入Conter，使用set设定数据Counter的上限，也就是最新的值，之后每次调用Next就是产生最新值附近的value
class SkewedLatestGenerator {
 public:
  SkewedLatestGenerator(CounterGenerator &counter) :
      basis_(counter), zipfian_(basis_.Last()) {
    Next();
  }
  
  uint64_t Next();
  uint64_t Last() { return last_; }
 private:
  CounterGenerator &basis_;
  ZipfianGenerator zipfian_;
  uint64_t last_;
};




