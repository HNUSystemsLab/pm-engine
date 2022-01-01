#pragma once

#include <vector>
#include <thread>
#include <map>

#include "config.h"
#include "engine.h"
#include "timer.h"
#include "utils2.h"
#include "database.h"
#include "libpm.h"
#include "test_benchmark.h"
#include "ycsb_benchmark.h"
#include "tpcc_benchmark.h"

namespace storage {

//#ifndef ALLOCATOR
//#define ALLOCATOR
//#endif
void * load_bh2(void* args)
{
  benchmark *bh = (benchmark*)args;
  bh->load();
  return NULL;
}
void * execute_bh2(void* args)
{
  benchmark *bh = (benchmark*)args;
  int thread_id = bh->tid;
#ifdef R740

  int task_id;
  int core_id;
  cpu_set_t cpuset;
  int set_result;
  int get_result;
  CPU_ZERO(&cpuset);
  task_id = thread_id;
  core_id = PMAP[task_id];
  CPU_SET(core_id, &cpuset);
  set_result = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if(set_result != 0)
  {
    std::cout<<"error";
    exit(1);
  }
  get_result = pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if(get_result != 0)
  {
    std::cout<<"get error";
    exit(1);
  }
  pthread_barrier_wait(&barrier);

#endif

  
  bh->execute();
  return NULL;
}
class coordinator {
 public:
  coordinator()
      : single(true),
        num_executors(1),
        num_txns(0) {
  }

  coordinator(const config conf) {
    single = conf.single;
    num_executors = conf.num_executors;
    num_txns = conf.num_txns;

    for (unsigned int i = 0; i < num_executors; i++) {
      tms.push_back(timer()); //volatile
      sps.push_back(static_info()); // volatile
      //sps[i].init = 0;
      //sps[i].itr = 0;
    }
  }

  void execute_bh(benchmark* bh) {
    // Execute
    bh->execute();
  }

  void load_bh(benchmark* bh) {
    // Load
    bh->load();
  }


  void eval(const config conf) {
    if (!conf.recovery) {
      execute(conf);
    } else {
      recover(conf);
    }

  }

  void execute(const config conf) {
    benchmark** partitions = new benchmark*[num_executors]; // volatile

    for (unsigned int i = 0; i < num_executors; i++) {
      database* db = new database(conf, sp, i); // volatile
      partitions[i] = get_benchmark(conf, i, db);
    }

    assert (mtm_enable_trace == 0);
    std::cerr << "LOADING..." << std::endl;
  
#ifdef ALLOCATOR
    for (unsigned int i=0; i < num_executors; i++)
      pmemalloc_creat_thread(&load_bh2, (void*)partitions[i]);
    for (unsigned int i=0; i < num_executors; i++)
      pmemalloc_join_thread(i);
#else
    std::vector<std::thread> executors;
    std::vector<std::thread> loaders;
    for (unsigned int i = 0; i < num_executors; i++)
      loaders.push_back(
          std::thread(&coordinator::load_bh, this, partitions[i]));

    for (unsigned int i = 0; i < num_executors; i++)
      loaders[i].join();
#endif

    if(conf.is_trace_enabled) {
	    int ret = write(tracing_on, "1", 1);
        if(ret == 1) {
            printf("Tracing Enabled\n");
        }
	    mtm_enable_trace = conf.is_trace_enabled;
    }
    std::cerr << "EXECUTING..." << std::endl;
band_start();
#ifdef ALLOCATOR
    for (unsigned int i=0; i < num_executors; i++)
      pmemalloc_creat_thread(&execute_bh2, (void*)partitions[i]);
    for (unsigned int i=0; i < num_executors; i++)
      pmemalloc_join_thread(i+num_executors);
#else
    for (unsigned int i = 0; i < num_executors; i++)
      executors.push_back(
          std::thread(&coordinator::execute_bh, this, partitions[i]));

    for (unsigned int i = 0; i < num_executors; i++)
      executors[i].join();
#endif
    double max_dur = 0;
    for (unsigned int i = 0; i < num_executors; i++) {
      std::cerr << "dur :" << i << " :: " << tms[i].duration() << std::endl;
      max_dur = std::max(max_dur, tms[i].duration());
    }
    std::cerr << "max dur :" << max_dur << std::endl;
#ifdef LATENCY
    latency_output();
#endif
#ifdef FUNCT
    size_t total_ms=0;
    for(unsigned int i=0; i < num_executors; i++){
      total_ms += tms[i].duration();
    }
    std::cout<< "total dur : "<< total_ms << std::endl;  
    func_output();
#endif
    display_stats(conf.etype, max_dur, num_txns);
    band_end();
    band_output();
  }

  void recover(const config conf) {

    database* db = new database(conf, sp, 0);
    benchmark* bh = get_benchmark(conf, 0, db);

    // Load
    bh->load();

    // Crash and recover
    bh->sim_crash();

  }

  benchmark* get_benchmark(const config state, unsigned int tid, database* db) {
    benchmark* bh = NULL;

    // Fix benchmark
    switch (state.btype) {
     case benchmark_type::TEST:
	die();
      LOG_INFO("TEST");
      bh = new test_benchmark(state, tid, db, &tms[tid], &sps[tid]); // volatile
      break;

     case benchmark_type::YCSB:
        LOG_INFO("YCSB");
        bh = new ycsb_benchmark(state, tid, db, &tms[tid], &sps[tid]); // volatile
        break;

      case benchmark_type::TPCC:
        LOG_INFO("TPCC");
        bh = new tpcc_benchmark(state, tid, db, &tms[tid], &sps[tid]); // volatile
        break;

      default:
        std::cerr << "Unknown benchmark type :: " << state.btype << std::endl;
        break;
    }

    return bh;
  }

  bool single;
  unsigned int num_executors;
  unsigned int num_txns;
  std::vector<struct static_info> sps;
  std::vector<timer> tms;

};

}
