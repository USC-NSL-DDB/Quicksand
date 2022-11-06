LDFLAGS = $(shell pkg-config --libs opencv4) -lpthread
CFLAGS = $(shell pkg-config --cflags opencv4)

all:
	g++ -std=c++17 $(CFLAGS) baseline.cpp -o baseline $(LDFLAGS)

clean:
	rm baseline
