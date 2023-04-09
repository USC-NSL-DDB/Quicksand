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
#include <functional>
#include <tuple>

#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>

#include <runtime.h>
#include <thread.h>
#include <sync.h>
#include "nu/runtime.hpp"
#include "nu/sharded_vector.hpp"
#include "nu/dis_executor.hpp"
#include "nu/sharded_ds_range.hpp"
#include "nu/zipped_ds_range.hpp"

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
using namespace chrono;
namespace fs = filesystem;

size_t N = 16;
string prefix;

// serialized decoder state
using DecoderBuffer = vector<uint8_t>;

using shard_ivf_type = nu::ShardedVector<IVF_MEM, false_type>;
using shard_decoder_type = nu::ShardedVector<DecoderBuffer, false_type>;
using sealed_shard_ivf = nu::SealedDS<shard_ivf_type>;
using sealed_shard_decoder = nu::SealedDS<shard_decoder_type>;
using shard_idx_type = nu::ShardedVector<size_t, false_type>;
using sealed_shard_idx = nu::SealedDS<shard_idx_type>;

// The excamera states
typedef struct {
  shard_ivf_type vpx0_ivf, vpx1_ivf, xc0_ivf, xc1_ivf;
  shard_decoder_type dec_state, enc0_state, enc1_state;
  vector<IVF_MEM> final_ivf;
  vector<DecoderBuffer> rebased_state;
  IVF_MEM ivf0;
  DecoderBuffer state0;
  uint16_t display_width, display_height;
} xc_t;

vector<uint32_t> stage1_time, stage2_time, stage3_time;

void usage(const string &program_name) {
  cerr << "Usage: " << program_name << " [nu_args] [input_dir]" << endl;
}

tuple<vector<RasterHandle>, uint16_t, uint16_t> read_raster(size_t idx) {
  ostringstream inputss;
  inputss << prefix << setw(2) << setfill('0') << idx << ".y4m";
  const string input_file = inputss.str();
  cout << input_file << endl;

  shared_ptr<FrameInput> input_reader = make_shared<YUV4MPEGReader>( input_file );
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

  tuple<vector<RasterHandle>, uint16_t, uint16_t> output(original_rasters, input_reader->display_width(), input_reader->display_height());
  return output;
}

DecoderBuffer decode(IVF_MEM &ivf) {
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
      EncoderStateSerializer odata;
      decoder.serialize(odata);
      return odata.get();
    }
  }

  throw runtime_error( "invalid frame number" );
}

tuple<IVF_MEM, DecoderBuffer>
enc_given_state(IVF_MEM & pred_ivf,
                DecoderBuffer & input_state,
                DecoderBuffer * prev_state,
                size_t idx) {
  bool two_pass = false;
  double kf_q_weight = 1.0;
  bool extra_frame_chunk = false;
  EncoderQuality quality = BEST_QUALITY;

  auto rasters = read_raster(idx);
  vector<RasterHandle> original_rasters = get<0>(rasters);
  uint16_t display_width = get<1>(rasters);
  uint16_t display_height = get<2>(rasters);

  Decoder pred_decoder( display_width, display_height );
  if (prev_state) {
    pred_decoder = EncoderStateDeserializer_MEM::build<Decoder>( *prev_state );
  }

  IVFWriter_MEM ivf_writer { "VP80", display_width, display_height, 1, 1 };

  /* pre-read all the prediction frames */
  vector<pair<Optional<KeyFrame>, Optional<InterFrame> > > prediction_frames;

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

  Encoder encoder( EncoderStateDeserializer_MEM::build<Decoder>(input_state) , two_pass, quality );

  ivf_writer.set_expected_decoder_entry_hash( encoder.export_decoder().get_hash().hash() );

  encoder.reencode( original_rasters, prediction_frames, kf_q_weight,
                    extra_frame_chunk, ivf_writer );

  EncoderStateSerializer odata = {};
  encoder.export_decoder().serialize(odata);
  tuple<IVF_MEM, DecoderBuffer> output(ivf_writer.ivf(), odata.get());
  return output;
}

IVF_MEM merge(IVF_MEM & ivf1, IVF_MEM & ivf2) {
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

  return ivf_writer.ivf();
}

void decode_all(xc_t &s) {
  auto sealed_ivfs = nu::to_sealed_ds(move(s.vpx0_ivf));
  auto ivfs_range = nu::make_contiguous_ds_range(sealed_ivfs);
  
  auto dis_exec = nu::make_distributed_executor(
    +[](decltype(ivfs_range) &ivfs_range) {
      vector<DecoderBuffer> outputs;
      while (true) {
        auto ivf = ivfs_range.pop();
        if (!ivf) {
          break;
        }
        outputs.push_back(decode(*ivf));
      }
      return outputs;
    },
    ivfs_range);

  auto outputs_vectors = dis_exec.get();
  auto join_view = ranges::join_view(outputs_vectors);
  auto vecs = vector<DecoderBuffer>(join_view.begin(), join_view.end());
  s.state0 = vecs[0];
  for (size_t i = 0; i < N-1; i++) {
    s.dec_state.push_back(vecs[i]);
  }
  s.vpx0_ivf = nu::to_unsealed_ds(move(sealed_ivfs));
}

void encode_all(xc_t &s) {
  shard_idx_type idxs = nu::make_sharded_vector<size_t, false_type>(N-1);
  for (size_t i = 1; i < N; i++) {
    idxs.push_back(i);
  }

  // first pass
  {
    auto sealed_ivfs = nu::to_sealed_ds(move(s.vpx1_ivf));
    auto sealed_dec_state = nu::to_sealed_ds(move(s.dec_state));
    auto sealed_idxs = nu::to_sealed_ds(move(idxs));
    auto encode_range = nu::make_zipped_ds_range(sealed_ivfs, sealed_dec_state, sealed_idxs);

    auto dist_exec = nu::make_distributed_executor(
      +[](decltype(encode_range) &encode_range) {
        vector<tuple<IVF_MEM, DecoderBuffer>> outputs;
        while (true) {
          auto t = encode_range.pop();
          if (!t) {
            break;
          }
          auto ivf = get<0>(*t);
          auto decoder = get<1>(*t);
          auto idx = get<2>(*t);
          outputs.push_back(enc_given_state(ivf, decoder, nullptr, idx));
        }
        return outputs;
      }, encode_range);

    auto outputs_vectors = dist_exec.get();
    auto join_view = ranges::join_view(outputs_vectors);
    auto vecs = vector<tuple<IVF_MEM, DecoderBuffer>>(join_view.begin(), join_view.end());

    s.vpx1_ivf = nu::to_unsealed_ds(move(sealed_ivfs));
    s.dec_state = nu::to_unsealed_ds(move(sealed_dec_state));
    idxs = nu::to_unsealed_ds(move(sealed_idxs));

    s.enc0_state.push_back(s.state0);
    for (size_t i = 0; i < N-2; i++) {
      s.xc0_ivf.push_back(get<0>(vecs[i]));
      s.enc0_state.push_back(get<1>(vecs[i]));
    }
    s.xc0_ivf.push_back(get<0>(vecs[N-1]));
  }

  // second pass
  {
    auto sealed_ivfs = nu::to_sealed_ds(move(s.xc0_ivf));
    auto sealed_enc0_state = nu::to_sealed_ds(move(s.enc0_state));
    auto sealed_dec_state = nu::to_sealed_ds(move(s.dec_state));
    auto sealed_idxs = nu::to_sealed_ds(move(idxs));
    auto encode_range = nu::make_zipped_ds_range(sealed_ivfs, sealed_enc0_state, sealed_dec_state, sealed_idxs);

    auto dist_exec = nu::make_distributed_executor(
      +[](decltype(encode_range) &encode_range) {
        vector<tuple<IVF_MEM, DecoderBuffer>> outputs;
        while (true) {
          auto t = encode_range.pop();
          if (!t) {
            break;
          }
          auto ivf = get<0>(*t);
          auto decoder = get<1>(*t);
          auto prev_decoder = get<2>(*t);
          auto idx = get<3>(*t);
          outputs.push_back(
            enc_given_state(ivf, decoder, &prev_decoder, idx)
          );
        }
        return outputs;
      }, encode_range);

    auto outputs_vectors = dist_exec.get();
    auto join_view = ranges::join_view(outputs_vectors);
    auto vecs = vector<tuple<IVF_MEM, DecoderBuffer>>(join_view.begin(), join_view.end());

    s.xc0_ivf = nu::to_unsealed_ds(move(sealed_ivfs));
    s.enc0_state = nu::to_unsealed_ds(move(sealed_enc0_state));
    s.dec_state = nu::to_unsealed_ds(move(sealed_dec_state));
    idxs = nu::to_unsealed_ds(move(sealed_idxs));

    s.enc1_state.push_back(s.state0);
    for (size_t i = 0; i < N-2; i++) {
      s.xc1_ivf.push_back(get<0>(vecs[i]));
      s.enc1_state.push_back(get<1>(vecs[i]));
    }
    s.xc1_ivf.push_back(get<0>(vecs[N-1]));
  }
}

void rebase(xc_t &s) {
  s.final_ivf[0] = s.ivf0;
  s.rebased_state[0] = s.state0;

  vector<IVF_MEM> xc1_ivf;
  auto sealed_ivfs = nu::to_sealed_ds(std::move(s.xc1_ivf));
  for (auto it = sealed_ivfs.cbegin(); it != sealed_ivfs.cend(); ++it) {
    xc1_ivf.push_back(*it);
  }
  s.xc1_ivf = nu::to_unsealed_ds(move(sealed_ivfs));

  vector<DecoderBuffer> enc0_state;
  auto sealed_enc0_state = nu::to_sealed_ds(std::move(s.enc0_state));
  for (auto it = sealed_enc0_state.cbegin(); it != sealed_enc0_state.cend(); ++it) {
    enc0_state.push_back(*it);
  }
  s.enc0_state = nu::to_unsealed_ds(move(sealed_enc0_state));

  // serial rebase
  for (size_t i = 1; i < N; i++) {
    // auto start = microtime();
    IVF_MEM ivf = xc1_ivf[i-1];
    DecoderBuffer decoder = s.rebased_state[i-1];
    DecoderBuffer prev_decoder = enc0_state[i-1];
    auto output = enc_given_state(ivf, decoder, &prev_decoder, i);

    auto rebased_ivf = get<0>(output);
    s.rebased_state.push_back(get<1>(output));

    s.final_ivf[i] = merge(s.final_ivf[i-1], rebased_ivf);
    // auto end = microtime();
    // stage3_time[i] = end - start;
  }
}

void write_output(xc_t &s) {
  const string final_file = prefix + "final.ivf";
  s.final_ivf[N-1].write(final_file);
}

void read_input(xc_t &s) {
  for (size_t i = 0; i < N; i++) {
    ostringstream vpxss;
    vpxss << prefix << "vpx_" << setw(2) << setfill('0') << i << ".ivf";
    const string vpx_file = vpxss.str();
    if (i != N - 1) {
      s.vpx0_ivf.push_back(IVF_MEM(vpx_file));
    }
    if (i != 0) {
      s.vpx1_ivf.push_back(IVF_MEM(vpx_file));
    } else {
      s.ivf0 = IVF_MEM(vpx_file);
    }
  }
}

int do_work() {
  xc_t s;

  s.vpx0_ivf = nu::make_sharded_vector<IVF_MEM, false_type>(N-1);
  s.vpx1_ivf = nu::make_sharded_vector<IVF_MEM, false_type>(N-1);
  s.xc0_ivf = nu::make_sharded_vector<IVF_MEM, false_type>(N-1);
  s.xc1_ivf = nu::make_sharded_vector<IVF_MEM, false_type>(N-1);
  s.dec_state = nu::make_sharded_vector<DecoderBuffer, false_type>(N-1);
  s.enc0_state = nu::make_sharded_vector<DecoderBuffer, false_type>(N-1);
  s.enc1_state = nu::make_sharded_vector<DecoderBuffer, false_type>(N-1);

  s.final_ivf.resize(N);
  s.rebased_state.resize(N);

  stage1_time.resize(N);
  stage2_time.resize(N);
  stage3_time.resize(N);

  read_input(s);
  auto t1 = microtime();
  decode_all(s);
  auto t2 = microtime();
  encode_all(s);
  auto t3 = microtime();
  rebase(s);
  auto t4 = microtime();
  write_output(s);

  auto stage1 = t2 - t1;
  auto stage2 = t3 - t2;
  auto stage3 = t4 - t3;

  cout << "decode: " << stage1 << ". enc_given_state: " << stage2 << ". rebase: " << stage3 << "." << endl;

  cout << "Stage 1: {";
  for (auto t : stage1_time) {
    cout << t << ", ";
  }
  cout << "}" << endl;
  cout << "Stage 2: {";
  for (auto t : stage2_time) {
    cout << t << ", ";
  }
  cout << "}" << endl;
  cout << "Stage 3: {";
  for (auto t : stage3_time) {
    cout << t << ", ";
  }
  cout << "}" << endl;

  return 0;
}

int main(int argc, char *argv[]) {
  // TODO: take file prefix to args
  prefix = string(argv[argc-1]) + "/sintel01_";
  return nu::runtime_main_init(argc, argv, [=](int, char **) { do_work(); });
}
