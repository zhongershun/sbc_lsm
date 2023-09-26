//
//  scrambled_zipfian_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_
#define YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_

#include "generator.h"

#include <cstdint>

#include "zipfian_generator.h"
#include "YCSB-cpp/utils/utils.h"

namespace ycsbc {

class ScrambledZipfianGenerator : public Generator<uint64_t> {
 public:
  ScrambledZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const) :
      base_(min), num_items_(max - min + 1),
      generator_(zipfian_const == kUsedZipfianConstant ?
                    ZipfianGenerator(0, kItemCount, zipfian_const, kZetan) :
                    ZipfianGenerator(0, kItemCount, zipfian_const)) { }

  ScrambledZipfianGenerator(uint64_t min, uint64_t max) :
      ScrambledZipfianGenerator(min, max, ZipfianGenerator::kZipfianConst) { }

  ScrambledZipfianGenerator(uint64_t num_items) :
      ScrambledZipfianGenerator(0, num_items - 1) { }

  uint64_t Next();
  uint64_t Last();

 private:
  static constexpr double kUsedZipfianConstant = 0.99;
  static constexpr double kZetan = 26.46902820178302;
  static constexpr uint64_t kItemCount = 10000000000LL;
  const uint64_t base_;
  const uint64_t num_items_;
  ZipfianGenerator generator_;

  uint64_t Scramble(uint64_t value) const;
};

inline uint64_t ScrambledZipfianGenerator::Scramble(uint64_t value) const {
  return base_ + utils::FNVHash64(value) % num_items_;
}

inline uint64_t ScrambledZipfianGenerator::Next() {
  return Scramble(generator_.Next());
}

inline uint64_t ScrambledZipfianGenerator::Last() {
  return Scramble(generator_.Last());
}

}

#endif // YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_
