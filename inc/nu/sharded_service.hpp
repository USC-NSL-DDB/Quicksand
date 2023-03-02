#pragma once

#include "sharded_ds.hpp"

namespace nu {

class Service {
 public:
  using Key = uint64_t;

  Service() = default;
  Service(const Service &) = default;
  Service &operator=(const Service &) = default;
  Service(Service &&) noexcept = default;
  Service &operator=(Service &&) noexcept = default;
  void split(Key *mid_k, Service *latter_half) {}

  template <class Archive>
  void save(Archive &ar) const {}
  template <class Archive>
  void load(Archive &ar) {}

 private:
};

class ShardedService
    : public ShardedDataStructure<GeneralLockedContainer<Service>,
                                  std::false_type> {
 public:
 private:
  using Base =
      ShardedDataStructure<GeneralLockedContainer<Service>, std::false_type>;

  ShardedService() : Base(std::nullopt, std::nullopt) {}
  friend ShardedService make_sharded_service();
};

ShardedService make_sharded_service() {
  return ShardedService();
}

}  // namespace nu
