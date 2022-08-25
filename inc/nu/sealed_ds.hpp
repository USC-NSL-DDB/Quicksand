#pragma once

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "nu/proclet.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/type_traits.hpp"

namespace nu {

template <typename T>
class SealedDSConstIterator {
  static_assert(is_base_of_template_v<T, ShardedDataStructure>);

  using ContainerIter = T::Shard::GeneralContainer::ConstIterator;

 public:
  using Val = std::remove_reference_t<decltype(*std::declval<ContainerIter>())>;

  SealedDSConstIterator();
  SealedDSConstIterator(const SealedDSConstIterator &);
  SealedDSConstIterator &operator=(const SealedDSConstIterator &);
  SealedDSConstIterator(SealedDSConstIterator &&) noexcept;
  SealedDSConstIterator &operator=(SealedDSConstIterator &&) noexcept;
  bool operator==(const SealedDSConstIterator &) const;
  SealedDSConstIterator &operator++();
  SealedDSConstIterator operator++(int);
  SealedDSConstIterator &operator--();
  SealedDSConstIterator operator--(int);
  Val operator*();

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  using Shard = T::Shard;
  using ShardsVec = std::vector<WeakProclet<Shard>>;
  using ShardsVecIter = ShardsVec::iterator;

  std::shared_ptr<ShardsVec> shards_;
  ShardsVecIter shards_vec_iter_;
  ContainerIter container_iter_;

  template <typename U>

  friend class SealedDS;

  SealedDSConstIterator(std::shared_ptr<ShardsVec> &shards,
                        ShardsVecIter shards_vec_iter,
                        ContainerIter container_iter);
};

template <typename T>
class SealedDSConstReverseIterator {};

template <typename T>
class SealedDS {
  static_assert(is_base_of_template_v<T, ShardedDataStructure>);

 public:
  using ConstIterator = SealedDSConstIterator<T>;
  using ConstReverseIterator = SealedDSConstReverseIterator<T>;

  SealedDS(SealedDS &&);
  SealedDS &operator=(SealedDS &&) = delete;
  SealedDS(const SealedDS &) = delete;
  SealedDS &operator=(const SealedDS &) = delete;
  ~SealedDS();
  ConstIterator cbegin();
  ConstIterator cend();
  ConstReverseIterator crbegin();
  ConstReverseIterator crend();

 private:
  using Shard = T::Shard;
  using ShardsVec = std::vector<WeakProclet<Shard>>;

  T t_;
  std::shared_ptr<ShardsVec> shards_;

  SealedDS(T &&t);
  T &&unseal();
  template <typename U>
  friend SealedDS<U> to_sealed_ds(U &&u);
  template <typename U>
  friend U to_unsealed_ds(SealedDS<U> &&sealed);
};

template <typename T>
SealedDS<T> to_sealed_ds(T &&t);
template <typename T>
T to_unsealed_ds(SealedDS<T> &&sealed);

}  // namespace nu

#include "nu/impl/sealed_ds.ipp"
