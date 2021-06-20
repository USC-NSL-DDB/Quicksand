#pragma once

#include <cereal/archives/binary.hpp>
#include <memory>
#include <sstream>
#include <utility>

#include "utils/cached_pool.hpp"

namespace nu {

template <typename Allocator> class ArchivePool {
public:
  constexpr static uint32_t kSStreamBufSize = 512 - 1;

  using CharAllocator =
      std::allocator_traits<Allocator>::template rebind_alloc<char>;
  using StringStream =
      std::basic_stringstream<char, std::char_traits<char>, CharAllocator>;
  using String = std::basic_string<char, std::char_traits<char>, CharAllocator>;

  struct IASStream {
    StringStream ss;
    cereal::BinaryInputArchive ia;
    IASStream() : ss(String(kSStreamBufSize, '\0')), ia(ss) {}
  };

  struct OASStream {
    StringStream ss;
    cereal::BinaryOutputArchive oa;
    OASStream() : ss(String(kSStreamBufSize, '\0')), oa(ss) {}
  };

  ArchivePool(uint32_t per_core_cache_size = 4);
  IASStream *get_ia_sstream();
  void put_ia_sstream(IASStream *ia_sstream);
  OASStream *get_oa_sstream();
  void put_oa_sstream(OASStream *oa_sstream);

private:
  using IAAllocator =
      std::allocator_traits<Allocator>::template rebind_alloc<IASStream>;
  using OAAllocator =
      std::allocator_traits<Allocator>::template rebind_alloc<OASStream>;

  CachedPool<IASStream, IAAllocator> ia_pool_;
  CachedPool<OASStream, OAAllocator> oa_pool_;
};

} // namespace nu

#include "impl/archive_pool.ipp"
