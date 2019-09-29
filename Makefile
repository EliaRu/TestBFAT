IFFDIR      = -I/home/ruggeri/workspace/fastflow
IWFDIR      = -I/home/ruggeri/workspace/WindFlow/includes
INC         = $(IFFDIR) $(IWFDIR)
CC          = /usr/local/gcc-7.2.0/bin/g++
LIBS        = -Wl,-rpath=/usr/local/gcc-7.2.0/lib64 -pthread
CFLAGS      = -std=c++17 -O3 -DFF_BOUNDED_BUFFER
PROF        = -g -DNDEBUG -fno-inline-functions -fno-inline-functions-called-once -fno-optimize-sibling-calls -fno-omit-frame-pointer 

FF_ROOT		= /home/ruggeri/workspace/fastflow 
OUT_DIR		= .
INCLUDE_DIR	= /home/ruggeri/workspace/WindFlow/includes

INCLUDES	= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
NCXX		= nvcc
NCXXFLAGS	= -x cu -w -std=c++14 --expt-extended-lambda -gencode arch=compute_35,code=sm_35 
MACROS 		= # -DTRACE_FASTFLOW -DLOG_DIR="/home/mencagli/WindFlow/log"
NOPTFLAGS	=  -Xptxas -O3,-v --default-stream per-thread 
NLDFLAGS      	= 
OPT_LEVEL	=  -O3 -DFF_BOUNDED_BUFFER

TARGETS = test_inc test_fat test_bfat test_gen

all: $(TARGETS)

test_inc: test_inc.cpp
	$(CC) $(CFLAGS) $(INC) -o $@ $^ $(LIBS)

test_fat: test_fat.cpp
	$(CC) $(CFLAGS) $(INC) -o $@ $^ $(LIBS)

test_gen: test_gen.cpp
	$(CC) $(CFLAGS) $(INC) -o $@ $^ $(LIBS)

test_bfat: test_bfat.cpp
	$(NCXX) $(NCXXFLAGS) $(NOPTFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) -o $(OUT_DIR)/$@ $< $(NLDFLAGS)

.PHONY: clean

clean: 
	rm -f $(TARGETS) *.o
