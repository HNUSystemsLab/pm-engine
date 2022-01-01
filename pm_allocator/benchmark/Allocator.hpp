#include <fcntl.h> /* For O_RDWR */
#include <linux/types.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h> /* For open(), creat() */

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <map>
#include <thread>
#include <vector>
#include <mutex>
#include "timer.h"
typedef void* VoidFunction(void*);
#define FILESIZE 200 * 1024 * 1024 * 1024UL
#define HEAPFILE "/mnt/pmem/heap.dat"
#define HEAPPATH "/mnt/pmem/"
thread_local size_t throughput_allocate = 0;
thread_local size_t throughput_deallocate = 0;
std::atomic_size_t throughput_allocate_total(0);
std::atomic_size_t throughput_deallocate_total(0);
std::atomic_size_t alloc_size(0);

 
struct ptr_s {
    void* ptr;
    size_t sz;
};

struct recover_str{
  recover_str* next;
  void* data;
  char align[48];
};




class Allocator {
 private:
  Timer sw;
  std::map<std::thread::id, std::vector<uint64_t>> latency_allocate;
  std::map<std::thread::id, std::vector<uint64_t>> latency_deallocate;
  std::map<uint64_t, size_t> fragment_map;
  std::vector<double> ptr_vec;
  std::mutex my_mutex;
  float totaltime;

 public:
  virtual const char* name() = 0;
  virtual void init() = 0;
  void* allocate(size_t sz) {
    auto tid = std::this_thread::get_id();
#ifdef LATENCY
    Timer swl;
    swl.start();
#endif
    auto r = allocate1(sz);
#ifdef LATENCY
    latency_allocate[tid].push_back(swl.elapsed<std::chrono::nanoseconds>());
#endif
    //throughput_allocate++;
    return r;
  }
  void deallocate(void* ptr) {
    auto tid = std::this_thread::get_id();
#ifdef LATENCY
    Timer swl;
    swl.start();
#endif
    deallocate1(ptr);
#ifdef LATENCY
    latency_deallocate[tid].push_back(swl.elapsed<std::chrono::nanoseconds>());
#endif
    //throughput_deallocate++;
    
  }
  virtual void* allocate1(size_t sz) = 0;
  virtual void deallocate1(void* ptr) = 0;
  virtual void active(void* ptr) = 0;
  virtual void creat_thread(VoidFunction func, void* /* arg */) = 0;
  virtual void thread_join(int i) = 0;
  virtual void pm_close() = 0;
  virtual void recover() = 0;
  virtual void output(int i, size_t count, std::vector<double> objsize) = 0;
  virtual void set_root(void* root_ptr) = 0;
  virtual void* pm_get_root(uint32_t i) = 0;
  virtual void* fetch_heap_start() = 0;
  virtual void creat_single(VoidFunction func, void*) = 0;
  virtual void join_single() = 0;
  void time_start() { sw.start(); }
  void time_end() { totaltime = sw.elapsed<std::chrono::milliseconds>(); }
  float get_total_time() { return totaltime; }
  void before_thread_exit() {
  }
  size_t get_allocate_total(){return throughput_allocate_total;}
  size_t get_deallocate_total(){return throughput_deallocate_total;}
  size_t get_alloc_size(){}
  void print() {
#ifdef FRAGMENT
    output(0, alloc_size, ptr_vec);
    output(1, alloc_size, ptr_vec);
#endif
  }
};
extern "C" Allocator* get_Allocator();