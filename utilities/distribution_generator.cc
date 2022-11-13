#include "utilities/distribution_generator.h"

double ZipfianGenerator::kZipfianConst = 0.99;

 uint64_t ZipfianGenerator::Next(uint64_t num) {
  assert(num >= 2 && num < kMaxNumItems);

  if (num > n_for_zeta_) { // Recompute zeta_n and eta
    RaiseZeta(num);
    eta_ = Eta();
  }
  
  double u = RandomDouble();
  double uz = u * zeta_n_;
  
  if (uz < 1.0) {
    return last_value_ = 0;
  }
  
  if (uz < 1.0 + std::pow(0.5, theta_)) {
    return last_value_ = 1;
  }

  return last_value_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
}

 uint64_t ZipfianGenerator::Last() {
  return last_value_;
}

// Uniform

 uint64_t UniformGenerator::Next() {
  // std::lock_guard<std::mutex> lock(mutex_);
  return last_int_ = dist_(generator_);
}

 uint64_t UniformGenerator::Last() {
  // std::lock_guard<std::mutex> lock(mutex_);
  return last_int_;
}


// Latest

uint64_t SkewedLatestGenerator::Next() {
  uint64_t max = basis_.Last();
  return last_ = max - zipfian_.Next(max);
}
