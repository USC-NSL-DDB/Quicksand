#!/usr/bin/env python3

import os
import shutil
import subprocess
import time

# ExCamera[6,16]
frame, N = 6, 16

workspace = os.path.join("..", "samples", "workspace")
prefix = "sintel01"

vpxenc_bin = os.path.join("..", "bin", "vpxenc")
vp8dec_bin = "vp8decode"

input_fn = os.path.join(workspace, prefix + "_{:02d}.y4m")
vpx_fn = os.path.join(workspace, prefix + "_vpx_{:02d}.ivf")

def run():
  prepare()
  vpxenc()

"""
Split the video into 6 frames chunks
"""
def prepare():
  input_file = os.path.join(workspace, prefix + ".y4m")
  output_file = os.path.join(workspace, prefix + "_%2d.y4m")
  subprocess.run(["ffmpeg", "-i", input_file, "-f", "segment", "-segment_time", "0.25", output_file])

"""
2. (Parallel)
Each thread runs Google's vpxenc VP8 encoder. The output is N compressed
frames: one key frame (typically about one megabyte) followed by N-1
interframes (about 10-30 kilobytes apiece).
"""
def vpxenc():
  for i in range(N):
    input_file = input_fn.format(i)
    output_file = vpx_fn.format(i)
    subprocess.run([vpxenc_bin, "--codec=vp8", "--ivf", "--good", "--cpu-used=0", "--end-usage=cq", "--min-q=0", "--max-q=63", "--cq-level=31", "--buf-initial-sz=10000", "--buf-optimal-sz=20000", "--buf-sz=40000", "--undershoot-pct=100", "--passes=2", "--auto-alt-ref=1", "--threads=1", "--token-parts=0", "--tune=ssim", "--target-bitrate=4294967295", "-o", output_file, input_file])

if __name__ == "__main__":
  run()
