/* Copyright (c) 2007-2011, Stanford University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived from
 *       this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <cereal/archives/binary.hpp>
#include <ctype.h>
#include <fcntl.h>
#include <nu/runtime.hpp>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

#ifdef TBB
#include "tbb/scalable_allocator.h"
#endif

#include "map_reduce.h"
#define DEFAULT_DISP_NUM 10

#include <string>

class WordsMR : public MapReduceSort<WordsMR, std::string, std::string,
                                     uint64_t, sum_combiner> {
  char *data;
  uint64_t data_size;
  uint64_t chunk_size;
  uint64_t splitter_pos;

public:
  explicit WordsMR(char *_data, uint64_t length, uint64_t _chunk_size)
      : data(_data), data_size(length), chunk_size(_chunk_size),
        splitter_pos(0) {}

  void map(data_type &s, map_container &out) const {
    for (uint64_t i = 0; i < s.size(); i++) {
      s[i] = toupper(s[i]);
    }

    uint64_t i = 0;
    while (i < s.size()) {
      while (i < s.size() && (s[i] < 'A' || s[i] > 'Z'))
        i++;
      uint64_t start = i;
      while (i < s.size() && ((s[i] >= 'A' && s[i] <= 'Z') || s[i] == '\''))
        i++;
      if (i > start) {
        emit_intermediate(out, s.substr(start, i - start), 1);
      }
    }
  }

  /** wordcount split()
   *  Memory map the file and divide file on a word border i.e. a space.
   */
  int split(std::string &out) {
    /* End of data reached, return FALSE. */
    if ((uint64_t)splitter_pos >= data_size) {
      return 0;
    }

    /* Determine the nominal end point. */
    uint64_t end = std::min(splitter_pos + chunk_size, data_size);

    /* Move end point to next word break */
    while (end < data_size && data[end] != ' ' && data[end] != '\t' &&
           data[end] != '\r' && data[end] != '\n')
      end++;

    /* Set the start of the next data. */
    out = std::string(data + splitter_pos, end - splitter_pos);

    splitter_pos = end;

    /* Return true since the out data is valid. */
    return 1;
  }

  bool sort(keyval const &a, keyval const &b) const {
    return a.val < b.val || (a.val == b.val && a.key >= b.key);
  }
};

#define NO_MMAP

void real_main(int argc, char *argv[]) {
  int fd;
  char *fdata;
  unsigned int disp_num;
  struct stat finfo;
  char *fname, *disp_num_str;
  struct timespec begin, end;

  get_time(begin);

  // Make sure a filename is specified
  if (argv[1] == NULL) {
    printf("USAGE: %s <filename> [Top # of results to display]\n", argv[0]);
    exit(1);
  }

  fname = argv[1];
  disp_num_str = argv[2];

  printf("Wordcount: Running...\n");

  // Read in the file
  CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
  // Get the file info (for file length)
  CHECK_ERROR(fstat(fd, &finfo) < 0);
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
  // Memory map the file
  CHECK_ERROR((fdata = (char *)mmap(0, finfo.st_size + 1, PROT_READ,
                                    MAP_PRIVATE | MAP_POPULATE, fd, 0)) ==
              NULL);
#else
  // Memory map the file
  CHECK_ERROR((fdata = (char *)mmap(0, finfo.st_size + 1, PROT_READ,
                                    MAP_PRIVATE, fd, 0)) == NULL);
#endif
#else
  uint64_t r = 0;

  fdata = (char *)malloc(finfo.st_size);
  CHECK_ERROR(fdata == NULL);
  while (r < (uint64_t)finfo.st_size)
    r += pread(fd, fdata + r, finfo.st_size, r);
  CHECK_ERROR(r != (uint64_t)finfo.st_size);
#endif

  // Get the number of results to display
  CHECK_ERROR((disp_num = (disp_num_str == NULL) ? DEFAULT_DISP_NUM
                                                 : atoi(disp_num_str)) <= 0);

  get_time(end);

#ifdef TIMING
  print_time("initialize", begin, end);
#endif

  printf("Wordcount: Calling MapReduce Scheduler Wordcount\n");
  get_time(begin);
  std::vector<WordsMR::keyval> result;
  WordsMR mapReduce(fdata, finfo.st_size, 1024 * 1024);
  CHECK_ERROR(mapReduce.run(result) < 0);
  get_time(end);

#ifdef TIMING
  print_time("library", begin, end);
#endif
  printf("Wordcount: MapReduce Completed\n");

  get_time(begin);

  unsigned int dn = std::min(disp_num, (unsigned int)result.size());
  printf("\nWordcount: Results (TOP %d of %lu):\n", dn, result.size());
  uint64_t total = 0;
  for (size_t i = 0; i < dn; i++) {
    std::cout << result[result.size() - 1 - i].key << " - "
              << result[result.size() - 1 - i].val << std::endl;
  }

  for (size_t i = 0; i < result.size(); i++) {
    total += result[i].val;
  }

  printf("Total: %lu\n", total);

#ifndef NO_MMAP
  CHECK_ERROR(munmap(fdata, finfo.st_size + 1) < 0);
#else
  free(fdata);
#endif
  CHECK_ERROR(close(fd) < 0);

  get_time(end);

#ifdef TIMING
  print_time("finalize", begin, end);
#endif
}

int main(int argc, char **argv) {
  nu::runtime_main_init(argc, argv,
                        [](int argc, char **argv) { real_main(argc, argv); });
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
