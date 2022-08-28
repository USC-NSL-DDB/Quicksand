#pragma once

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "nu/proclet.hpp"
#include "nu/sharded_ds.hpp"
#include "nu/type_traits.hpp"

namespace nu {

template <typename T, bool Fwd>
class GeneralSealedDSConstIterator {
  static_assert(is_base_of_template_v<T, ShardedDataStructure>);

 public:
  using Val = T::Shard::GeneralContainer::Val;

  GeneralSealedDSConstIterator();
  GeneralSealedDSConstIterator(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator &operator=(const GeneralSealedDSConstIterator &);
  GeneralSealedDSConstIterator(GeneralSealedDSConstIterator &&) noexcept;
  GeneralSealedDSConstIterator &operator=(
      GeneralSealedDSConstIterator &&) noexcept;
  bool operator==(const GeneralSealedDSConstIterator &) const;
  GeneralSealedDSConstIterator &operator++();
  GeneralSealedDSConstIterator &operator--();
  GeneralSealedDSConstIterator operator++(int) = delete;
  GeneralSealedDSConstIterator operator--(int) = delete;
  Val operator*();

  template <class Archive>
  void save(Archive &ar) const;
  template <class Archive>
  void load(Archive &ar);

 private:
  using Shard = T::Shard;
  using ShardsVec = std::vector<WeakProclet<Shard>>;
  using ShardsVecIter =
      std::conditional_t<Fwd, typename ShardsVec::iterator,
                         typename ShardsVec::reverse_iterator>;
  using ContainerIter = std::conditional_t<
      Fwd, typename T::Shard::GeneralContainer::ConstIterator,
      typename T::Shard::GeneralContainer::ConstReverseIterator>;

  constexpr static uint32_t kBlockSize = 128 << 10;

  std::shared_ptr<ShardsVec> shards_;
  ShardsVecIter shards_vec_iter_;
  ContainerIter block_begin_iter_;
  ContainerIter block_end_iter_;
  std::vector<Val>::const_iterator block_iter_;
  std::vector<Val> block_data_;

  template <typename U>
  friend class SealedDS;

  GeneralSealedDSConstIterator(std::shared_ptr<ShardsVec> &shards,
                               ShardsVecIter shards_vec_iter,
                               ContainerIter begin_iter);
  ShardsVecIter shards_vec_begin() const;
  ShardsVecIter shards_vec_end() const;
};

template <typename T>
class SealedDS {
  static_assert(is_base_of_template_v<T, ShardedDataStructure>);

 public:
  using ConstIterator = GeneralSealedDSConstIterator<T, true>;
  using ConstReverseIterator = GeneralSealedDSConstIterator<T, false>;

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
  ConstIterator cbegin_;
  ConstIterator cend_;
  ConstReverseIterator crbegin_;
  ConstReverseIterator crend_;

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
