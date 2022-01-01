## Makefile.am -- Process this file with automake to produce Makefile.in

bin_PROGRAMS = nstore pmem_check
LIBS_DIR:= -L../../pm_allocator/benchmark -L../../pcm  -Wl,-rpath,../../pm_allocator/benchmark
AM_CPPFLAGS = -I$(srcdir)/common -I../../pm_allocator/benchmark -I../../pcm $(LIBS_DIR) -lallocator -lpcm -Wno-pointer-arith 
AM_CXXFLAGS = -Wall -Wextra
AM_LDFLAGS  = $(PTHREAD_CFLAGS) $(LIBS_DIR)
LIBS = -lrt -lallocator -lpcm

noinst_LIBRARIES = libpm.a
libpm_a_SOURCES = libpm.cpp utils.cpp 

#AM_CPPFLAGS = $(BOOST_CPPFLAGS) 
#AM_LDFLAGS = $(BOOST_SYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS) $(PTHREAD_CFLAGS)
#LIBS = $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS) 

nstore_SOURCES  =  	main.cpp \
					wal_engine.cpp  \
					sp_engine.cpp  \
					lsm_engine.cpp  \
					opt_wal_engine.cpp  \
					opt_sp_engine.cpp  \
					opt_lsm_engine.cpp  \
					nof_lsm_engine.cpp \
	   				test_benchmark.cpp \
	   				ycsb_benchmark.cpp \
					tpcc_benchmark.cpp \
					utils.cpp

nstore_LDADD  = libpm.a

pmem_check_SOURCES = pmem_check.cpp

pmem_check_LDADD  = libpm.a


