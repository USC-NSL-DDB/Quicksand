#pragma once

static inline void *memcpy_ermsb(void *dst, const void *src, size_t n) {
  asm volatile("rep movsb" : "+D"(dst), "+S"(src), "+c"(n)::"memory");
  return dst;
}

extern void *memcpy_avx2_nt(void *dst, const void *src, size_t size);
