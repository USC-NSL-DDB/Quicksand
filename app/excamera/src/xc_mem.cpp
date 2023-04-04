#include <getopt.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <chrono>
#include <filesystem>
#include <vector>

#include <runtime.h>
#include <thread.h>
#include <sync.h>
#include "nu/runtime.hpp"

#include "ivf.hh"
#include "uncompressed_chunk.hh"
#include "frame_input.hh"
#include "ivf_reader.hh"
#include "yuv4mpeg.hh"
#include "frame.hh"
#include "player.hh"
#include "vp8_raster.hh"
#include "decoder.hh"
#include "encoder.hh"
#include "macroblock.hh"
#include "ivf_writer.hh"
#include "display.hh"
#include "enc_state_serializer.hh"

using namespace std;
using namespace std::chrono;
namespace fs = std::filesystem;

size_t N = 16;

vector<IVF_MEM> vpx_ivf, xc0_ivf, xc1_ivf, rebased_ivf, final_ivf;
vector<Decoder> dec_state, enc0_state, enc1_state, rebased_state;
vector<vector<RasterHandle>> original_rasters;
uint32_t display_width, display_height;

rt::Mutex global_mutex;

void usage(const string &program_name) {
  cerr << "Usage: " << program_name << " [nu_args] [input_dir]" << endl;
}

bool decode(const string input, vector<Decoder> & output) {
  IVF_MEM ivf(input);
  global_mutex.Lock();
  vpx_ivf.push_back(ivf);
  global_mutex.Unlock();

  Decoder decoder = Decoder(ivf.width(), ivf.height());
  if ( not decoder.minihash_match( ivf.expected_decoder_minihash() ) ) {
    throw Invalid( "Decoder state / IVF mismatch" );
  }

  size_t frame_number = ivf.frame_count() - 1;
  for ( size_t i = 0; i < ivf.frame_count(); i++ ) {
    UncompressedChunk uch { ivf.frame( i ), ivf.width(), ivf.height(), false };

    if ( uch.key_frame() ) {
      KeyFrame frame = decoder.parse_frame<KeyFrame>( uch );
      decoder.decode_frame( frame );
    }
    else {
      InterFrame frame = decoder.parse_frame<InterFrame>( uch );
      decoder.decode_frame( frame );
    }

    if ( i == frame_number ) {
      global_mutex.Lock();
      output.push_back(decoder);
      global_mutex.Unlock();
      return true;
    }
  }

  throw runtime_error( "invalid frame number" );
}

void enc_given_state(const string input_file,
                     vector<IVF_MEM> & output_ivf,
                     vector<Decoder> & input_state, 
                     vector<Decoder> & output_state,
                     vector<IVF_MEM> & pred,
                     vector<Decoder> * prev_state,
                     size_t index) {
  bool two_pass = false;
  double kf_q_weight = 1.0;
  bool extra_frame_chunk = false;
  EncoderQuality quality = BEST_QUALITY;

  shared_ptr<FrameInput> input_reader = make_shared<YUV4MPEGReader>( input_file );

  Decoder pred_decoder( input_reader->display_width(), input_reader->display_height() );
  if (prev_state) {
    global_mutex.Lock();
    pred_decoder = prev_state->at(index - 1);
    global_mutex.Unlock();
  }

  IVFWriter_MEM ivf_writer { "VP80", input_reader->display_width(), input_reader->display_height(), 1, 1 };
  // pre-read original rasters
  vector<RasterHandle> original_rasters;
  for ( size_t i = 0; ; i++ ) {
    auto next_raster = input_reader->get_next_frame();

    if ( next_raster.initialized() ) {
      original_rasters.emplace_back( next_raster.get() );
    } else {
      break;
    }
  }

  /* pre-read all the prediction frames */
  vector<pair<Optional<KeyFrame>, Optional<InterFrame> > > prediction_frames;

  global_mutex.Lock();
  IVF_MEM pred_ivf = pred[index];
  global_mutex.Unlock();

  if ( not pred_decoder.minihash_match( pred_ivf.expected_decoder_minihash() ) ) {
    throw Invalid( "Mismatch between prediction IVF and prediction_ivf_initial_state" );
  }

  for ( unsigned int i = 0; i < pred_ivf.frame_count(); i++ ) {
    UncompressedChunk unch { pred_ivf.frame( i ), pred_ivf.width(), pred_ivf.height(), false };

    if ( unch.key_frame() ) {
      KeyFrame frame = pred_decoder.parse_frame<KeyFrame>( unch );
      pred_decoder.decode_frame( frame );

      prediction_frames.emplace_back( move( frame ), Optional<InterFrame>() );
    } else {
      InterFrame frame = pred_decoder.parse_frame<InterFrame>( unch );
      pred_decoder.decode_frame( frame );

      prediction_frames.emplace_back( Optional<KeyFrame>(), move( frame ) );
    }
  }

  global_mutex.Lock();
  Encoder encoder( input_state[index-1], two_pass, quality );
  global_mutex.Unlock();

  ivf_writer.set_expected_decoder_entry_hash( encoder.export_decoder().get_hash().hash() );

  encoder.reencode( original_rasters, prediction_frames, kf_q_weight,
                    extra_frame_chunk, ivf_writer );

  global_mutex.Lock();
  output_ivf.push_back(ivf_writer.ivf());
  output_state.push_back(encoder.export_decoder());
  global_mutex.Unlock();
}

void merge(vector<IVF_MEM> & input1, vector<IVF_MEM> & input2, vector<IVF_MEM> & output, size_t index) {
  IVF_MEM ivf1 = input1[index-1];
  IVF_MEM ivf2 = input2[index];

  if ( ivf1.width() != ivf2.width() or ivf1.height() != ivf2.height() ) {
    throw runtime_error( "cannot merge ivfs with different dimensions." );
  }

  IVFWriter_MEM ivf_writer("VP80", ivf1.width(), ivf1.height(), 1, 1 );

  for ( size_t i = 0; i < ivf1.frame_count(); i++ ) {
    ivf_writer.append_frame( ivf1.frame( i ) );
  }

  for ( size_t i = 0; i < ivf2.frame_count(); i++ ) {
    ivf_writer.append_frame( ivf2.frame( i ) );
  }

  output.push_back(ivf_writer.ivf());
}

void decode_all(const string prefix) {
  vector<rt::Thread> ths;
  for (size_t i = 0; i < N; i++) {
    ostringstream inputss, outputss;
    inputss << prefix << "vpx_" << std::setw(2) << std::setfill('0') << i << ".ivf";
    const string input_file = inputss.str();
    
    ths.emplace_back( [=] {decode(input_file, dec_state);} );
  }

  for (auto &th : ths) {
    th.Join();
  }
}

void encode_all(const string prefix) {
  vector <rt::Thread> ths;
  // first pass
  for (size_t i = 0; i < N; i++) {
    ostringstream inputss, instatess, outstatess;
    inputss << prefix << std::setw(2) << std::setfill('0') << i << ".y4m";
    const string input_file = inputss.str();
    
    if (i == 0) {
      xc0_ivf.push_back(vpx_ivf[0]);
      enc0_state.push_back(dec_state[0]);
    } else {
      ths.emplace_back( [=] {enc_given_state(input_file, xc0_ivf, dec_state, enc0_state, vpx_ivf, nullptr, i);} );
    }
  }
  for (auto &th : ths) {
    th.Join();
  }

  ths.clear();

  // second pass
  for (size_t i = 0; i < N; i++) {
    ostringstream inputss, instatess, prevstatess, outstatess;
    inputss << prefix << std::setw(2) << std::setfill('0') << i << ".y4m";
    const string input_file = inputss.str();
    
    if (i == 0) {
      xc1_ivf.push_back(xc0_ivf[0]);
      enc1_state.push_back(enc0_state[0]);
    } else {
      ths.emplace_back( [=] {enc_given_state(input_file, xc1_ivf, enc0_state, enc1_state, xc0_ivf, &dec_state, i);} );
    }
  }
  for (auto &th : ths) {
    th.Join();
  }
}

void rebase(const string prefix) {
  // serial rebase
  for (size_t i = 0; i < N; i++) {
    ostringstream inputss, instatess, prevstatess, outstatess;
    inputss << prefix << std::setw(2) << std::setfill('0') << i << ".y4m";
    const string input_file = inputss.str();

    if (i == 0) {
      final_ivf.push_back(xc1_ivf[0]);
      rebased_ivf.push_back(xc1_ivf[0]);
      rebased_state.push_back(enc0_state[0]);
    } else {
      enc_given_state(input_file, rebased_ivf, rebased_state, rebased_state, xc1_ivf, &enc0_state, i);
      merge(final_ivf, rebased_ivf, final_ivf, i);
    }
  }
}

void write_output(const std::string prefix) {
  const string final_file = prefix + "final.ivf";
  final_ivf[N-1].write(final_file);
}

int do_work(const string & prefix) {
  // if (argc != 2) {
  //   usage(argv[0]);
  //   return EXIT_FAILURE;
  // }

  auto t1 = microtime();
  decode_all(prefix);
  auto t2 = microtime();
  encode_all(prefix);
  auto t3 = microtime();
  rebase(prefix);
  auto t4 = microtime();
  write_output(prefix);

  auto stage1 = t2 - t1;
  auto stage2 = t3 - t2;
  auto stage3 = t4 - t3;

  cout << "decode: " << stage1 << ". encode_given_state: " << stage2 << ". rebase: " << stage3 << "." << endl;
  return 0;
}

int main(int argc, char *argv[]) {
  // TODO: take file prefix to args
  string prefix = std::string(argv[argc-1]) + "/sintel01_";
  return nu::runtime_main_init(argc, argv, [=](int, char **) { do_work(prefix); });
}
