/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

/* Copyright 2013-2018 the Alfalfa authors
                       and the Massachusetts Institute of Technology

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

      1. Redistributions of source code must retain the above copyright
         notice, this list of conditions and the following disclaimer.

      2. Redistributions in binary form must reproduce the above copyright
         notice, this list of conditions and the following disclaimer in the
         documentation and/or other materials provided with the distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. */

#ifndef IVF_WRITER_HH
#define IVF_WRITER_HH

#include "ivf.hh"

class IVFWriter
{
private:
  FileDescriptor fd_;
  uint64_t file_size_;
  uint32_t frame_count_;

  uint16_t width_;
  uint16_t height_;

public:
  IVFWriter( const std::string & filename,
             const std::string & fourcc,
             const uint16_t width,
             const uint16_t height,
             const uint32_t frame_rate,
             const uint32_t time_scale );

  IVFWriter( FileDescriptor && fd,
             const std::string & fourcc,
             const uint16_t width,
             const uint16_t height,
             const uint32_t frame_rate,
             const uint32_t time_scale );

  size_t append_frame( const Chunk & chunk );

  void set_expected_decoder_entry_hash( const uint32_t minihash ); /* ExCamera invention */

  uint16_t width() const { return width_; }
  uint16_t height() const { return height_; }
};

class IVFWriter_MEM
{
private:
  IVF_MEM ivf_;
  uint32_t frame_count_;
  uint16_t width_;
  uint16_t height_;

  IVF_MEM init_ivf( const std::string & fourcc,
                    const uint16_t width,
                    const uint16_t height,
                    const uint32_t frame_rate,
                    const uint32_t time_scale );

public:
  IVFWriter_MEM( const std::string & fourcc,
                 const uint16_t width,
                 const uint16_t height,
                 const uint32_t frame_rate,
                 const uint32_t time_scale );

  void append_frame( const Chunk & chunk );

  void set_expected_decoder_entry_hash( const uint32_t minihash ); /* ExCamera invention */

  uint16_t width() const { return width_; }
  uint16_t height() const { return height_; }
  IVF_MEM ivf() const { return ivf_; }
};

#endif /* IVF_WRITER_HH */
