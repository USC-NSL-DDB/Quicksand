# Sintel Trailer y4m 480p (735MB)
curl https://media.xiph.org/video/derf/y4m/sintel_trailer_2k_480p24.y4m -o sintel_trailer_480p.y4m
mkdir sintel_trailer_480p
# 96 frames per segment
ffmpeg -i sintel_trailer_480p.y4m -f segment -segment_time 4 sintel_trailer_480p/sintel%2d.y4m

# baseline setup
mkdir workspace
