extern "C" {
#include <base/stddef.h>
}

namespace nu {

template <typename Allocator>
ArchivePool<Allocator>::ArchivePool(uint32_t per_core_cache_size)
    : ia_pool_(
          [] {
            IAAllocator allocator;
            auto *ia_sstream = allocator.allocate(sizeof(IASStream));
            new (ia_sstream) IASStream();
            return ia_sstream;
          },
          [](IASStream *ia_sstream) {
            IAAllocator allocator;
            ia_sstream->~IASStream();
            allocator.deallocate(ia_sstream, sizeof(IASStream));
          },
          per_core_cache_size),
      oa_pool_(
          [] {
            OAAllocator allocator;
            auto *oa_sstream = allocator.allocate(sizeof(OASStream));
            new (oa_sstream) OASStream();
            return oa_sstream;
          },
          [](OASStream *oa_sstream) {
            OAAllocator allocator;
            oa_sstream->~OASStream();
            allocator.deallocate(oa_sstream, sizeof(OASStream));
          },
          per_core_cache_size) {}

template <typename Allocator>
ArchivePool<Allocator>::IASStream *ArchivePool<Allocator>::get_ia_sstream() {
  return ia_pool_.get();
}

template <typename Allocator>
ArchivePool<Allocator>::OASStream *ArchivePool<Allocator>::get_oa_sstream() {
  return oa_pool_.get();
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"

template <typename Allocator>
void ArchivePool<Allocator>::put_ia_sstream(IASStream *ia_sstream) {
  ia_sstream->ss.seekg(0);
  return ia_pool_.put(ia_sstream);
}

template <typename Allocator>
void ArchivePool<Allocator>::put_oa_sstream(OASStream *oa_sstream) {
  oa_sstream->ss.seekp(0);
  return oa_pool_.put(oa_sstream);
}

#pragma GCC diagnostic pop

} // namespace nu
