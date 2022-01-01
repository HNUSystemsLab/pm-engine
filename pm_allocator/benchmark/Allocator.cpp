#include "Allocator.hpp"

#include <jemalloc/jemalloc.h>
#include <libpmemobj.h>
#include <makalu.h>
#include <nvm_malloc.h>
#include <thread>

thread_local pthread_t nstore_thread;
thread_local std::thread nstore_thread2;
_Float64* fragment_array[2];


class makalu : public Allocator {
 private:
  pthread_attr_t attr;
  std::vector<pthread_t> thread_ids;
  uint32_t root_idx=0;
 public:
  static char* base_addr;
  static char* curr_addr;
  makalu() {
#ifndef RECOVER
    __map_persistent_region(); 
    int arg = 1000;
    pthread_attr_init(&attr);
    MAK_start(&__nvm_region_allocator);
    std::cout<<"init normal"<<std::endl;
#endif   
  };
  const char* name() { return "makalu"; };
  void init(){ 
#ifdef RECOVER
    std::string file_name = HEAPFILE;
    std::ifstream f(file_name.c_str());
    bool if_restart = f.good();
    if(if_restart) // this is a recovery
    {
      __remap_persistent_region();
      recover();
      curr_addr = base_addr + 0x18;
    }
    else // this is a clean start
    {
      __map_persistent_region();
      int arg = 1000;
      pthread_attr_init(&attr);
      MAK_start(&__nvm_region_allocator);
    }
#endif
  };
  void* allocate1(size_t sz) { return MAK_malloc(sz); }
  void deallocate1(void* ptr) { MAK_free(ptr); };
  void creat_thread(VoidFunction func, void* arg) {
    pthread_t thread_id;
    MAK_pthread_create(&thread_id, &attr, func, arg);
    thread_ids.push_back(thread_id);
  };
  void thread_join(int i) { MAK_pthread_join(thread_ids[i], NULL); };
  static int __nvm_region_allocator(void** memptr, size_t alignment,
                                    size_t size) {
    char* next;
    char* res;
    if (size < 0) return 1;

    if (((alignment & (~alignment + 1)) !=
         alignment) ||  // should be multiple of 2
        (alignment < sizeof(void*)))
      return 1;  // should be atleast the size of void*
    size_t aln_adj = (size_t)curr_addr & (alignment - 1);

    if (aln_adj != 0) curr_addr += (alignment - aln_adj);

    res = curr_addr;
    next = curr_addr + size;
    if (next > base_addr + FILESIZE) {
      printf("\n----Ran out of space in mmaped file-----\n");
      return 1;
    }
    curr_addr = next;
    *memptr = res;
    return 0;
  }
  void __map_persistent_region() {
    int fd;
    fd = open(HEAPFILE, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

    off_t offt = lseek(fd, FILESIZE - 1, SEEK_SET);
    assert(offt != -1);

    int result = write(fd, "", 1);
    assert(result != -1);

    void* addr = mmap(0, FILESIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(addr != MAP_FAILED);
    *((intptr_t*)addr) = (intptr_t)addr;
    base_addr = (char*)addr;
    // adress to remap to, the root pointer to gc metadata,
    // and the curr pointer at the end of the day
    curr_addr = (char*)((size_t)addr + 3 * sizeof(intptr_t));
    printf("Addr: %p\n", addr);
    printf("Base_addr: %p\n", base_addr);
    printf("Current_addr: %p\n", curr_addr);
  }
  void recover(){
    char* basemd_start = base_addr + 0x18;
    //MAK_close();
    MAK_start_off(basemd_start, &__nvm_region_allocator);
    //int gc_result = MAK_collect_off();
    //std::cout<<"finish gc and result is"<<gc_result<<std::endl;
    MAK_restart(basemd_start, &__nvm_region_allocator);
    int gc_result = MAK_collect_off();
    std::cout<<"gc result is"<<gc_result<<std::endl;
  };

  void __remap_persistent_region(){
    int fd;
    fd = open(HEAPFILE, O_RDWR, S_IRUSR | S_IWUSR);

    off_t offt = lseek(fd, FILESIZE - 1, SEEK_SET);
    assert(offt != -1);
    int result = write(fd, "", 1);
    assert(result != -1);
    void* addr = mmap(0, FILESIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_NORESERVE, fd, 0);
    assert(addr != MAP_FAILED);
    base_addr = (char*)addr;
    curr_addr = base_addr + FILESIZE;
  };

  void pm_close(){
    MAK_fragment(base_addr, curr_addr, 130, fragment_array);
    //MAK_close();
  };
  void output(int i, size_t count, std::vector<double> objsize){
    fragment_array[0] = new _Float64 [130];//makalu has 2048 size classes,1-2048, 2049 for large alloc
    fragment_array[1] = new _Float64 [130];//[0] for used block, [1] for alloc block
    memset(fragment_array[0], 0, sizeof(_Float64) * 130);
    memset(fragment_array[1], 0, sizeof(_Float64) * 130);
    MAK_fragment(base_addr, curr_addr, 130, fragment_array);
    _Float64 total_alloc=0;
    for(int j=0; j<130; j++)
    {
      total_alloc += fragment_array[i][j] * 4096;
    }
    printf("alloc bytes is %13f, %13f\n", (_Float64)count, total_alloc);
    std::cout<<"fragment rate "<<i<<" is "<< (_Float64)count / total_alloc<<std::endl;
  };
  void set_root(void* root_ptr){
    //MAK_set_persistent_root(root_idx, root_ptr);
    //root_idx++;
  }
  void* pm_get_root(uint32_t i){
    return MAK_persistent_root(i);
  };
  void* fetch_heap_start(){return base_addr+0X18;}
  void active(void* ptr){};
  void creat_single(VoidFunction func, void* args){
    MAK_pthread_create(&nstore_thread, &attr, func, args);
  }
  void join_single(){
    MAK_pthread_join(nstore_thread, NULL);
  }
};
char* makalu::base_addr = NULL;
char* makalu::curr_addr = NULL;

class nvm_malloc : public Allocator {
 private:
  std::vector<std::thread> thread_ids;
  int restart_flag=0; // for recover call output func
  void* heap_start;
 public:
  nvm_malloc() {
#ifndef RECOVER 
    heap_start = nvm_initialize(HEAPPATH, 0);
#endif
  }
  const char* name() { return "nvm_malloc"; };
  void init(){
#ifdef RECOVER
    std::cout<<"recover!"<<std::endl;
    std::string file_name = HEAPPATH;
    file_name = file_name + "meta";
    std::ifstream f(file_name.c_str());
    bool if_restart = f.good();
    if(if_restart) // this is a recovery
    {
      restart_flag = -1;
      nvm_initialize(HEAPPATH, 1);
    }
    else
    {
      nvm_initialize(HEAPPATH, 0);
    }
#endif
  };
  void* allocate1(size_t sz) {
    void* alloc_ptr = nvm_reserve(sz);
    return alloc_ptr;
  }
  void deallocate1(void* ptr) {
    nvm_free(ptr, nullptr, nullptr, nullptr, nullptr);
  };
  void creat_thread(VoidFunction func, void* arg) {
    thread_ids.push_back(std::thread(func, arg));
  };
  void thread_join(int i) { thread_ids[i].join();};
  void recover(){
    nvm_get_fragment(0,0, nullptr);
    //nvm_initialize(HEAPPATH, true);
  };
  void pm_close(){};
  void output(int i, size_t count, std::vector<double> objsize){

    fragment_array[0] = new _Float64 [34]; //32bins, 1 large alloc, 1 huge alloc
    fragment_array[1] = new _Float64 [34];
    memset(fragment_array[0], 0, sizeof(_Float64) * 34);
    memset(fragment_array[1], 0, sizeof(_Float64) * 34); 
#ifdef RECOVER
    nvm_get_fragment(restart_flag, 34, fragment_array);
#else
    nvm_get_fragment(1, 34, fragment_array);
#endif
    _Float64 total_alloc=0;
    for(int j=0; j<34; j++)
    {
      total_alloc += fragment_array[i][j] * 4096;
    }
    printf("alloc bytes is %13f, %13f\n", (_Float64)count, total_alloc);
    std::cout<<"fragment rate "<<i<<" is "<< (_Float64)count / total_alloc<<std::endl;
    //nvm_get_fragment(1, count, uobjsize);
  };
  void* pm_get_root(uint32_t i){return nvm_get_id("0");};
  void set_root(void* root_ptr){
    nvm_reserve_id("0", sizeof(recover_str));
    nvm_activate_id("0");
  };
  void* fetch_heap_start(){
    return heap_start;
  };
  void active(void* ptr){
    nvm_activate(ptr, nullptr, nullptr, nullptr, nullptr);
  }
  void creat_single(VoidFunction func, void* args){
    nstore_thread2 = std::thread(func, args);
  };
  void join_single(){
    nstore_thread2.join();
  };
};


class pmdk : public Allocator {
 private:
  std::vector<std::thread> thread_ids;
  PMEMobjpool* pop;
  PMEMoid root;
  struct PMDK_roots {
    void* roots[1024];
  };
  static int dummy_construct(PMEMobjpool* pop, void* ptr, void* arg) {
    return 0;
  }

 public:
  pmdk() {
#ifndef RECOVER
    pop = pmemobj_create(HEAPFILE, "ALLOC_LAOUT", FILESIZE, 0666);
    if (pop == nullptr) {
      perror("pmemobj_create");
    } else {
      root = pmemobj_root(pop, sizeof(PMDK_roots));
    }
#endif
  }
  const char* name() { return "pmdk"; };
  void init(){
#ifdef RECOVER
    std::string file_name = HEAPFILE;
    std::ifstream f(file_name.c_str());
    bool if_restart = f.good();
    if(if_restart)
    {
      recover();
      pop = pmemobj_open(HEAPFILE, "ALLOC_LAOUT");
      root = pmemobj_root(pop, sizeof(PMDK_roots));
    }
    else
    {
      pop = pmemobj_create(HEAPFILE, "ALLOC_LAOUT", FILESIZE, 0666);
      if (pop == nullptr) {
        perror("pmemobj_create");
      } else {
        root = pmemobj_root(pop, sizeof(PMDK_roots));
      }
    }
#endif
  };
  void* allocate1(size_t sz) {
    PMEMoid temp_ptr;
    int ret = pmemobj_alloc(pop, &temp_ptr, sz, 1, dummy_construct, nullptr);
    if (ret<0)
    {
      std::cout<<"error!"<<std::endl;
      return NULL;
    }
    return pmemobj_direct(temp_ptr);
  }
  void deallocate1(void* ptr) {
    if (ptr == nullptr) return;
    PMEMoid temp_ptr;
    temp_ptr = pmemobj_oid(ptr);
    pmemobj_free(&temp_ptr);
  };
  void creat_thread(VoidFunction func, void* arg) {
    thread_ids.push_back(std::thread(func, arg));
  };
  void thread_join(int i) { thread_ids[i].join(); };
  void recover(){
    int check_result = pmemobj_check(HEAPFILE, "ALLOC_LAOUT");
    std::cout<<"consistent result is "<<check_result<<std::endl;
  };
  void pm_close(){pmemobj_close(pop);};
  void output(int i, size_t count, std::vector<double> objsize){
    std::cout<<"total alloc bytes is"<<count<<std::endl;
    uint64_t output[8];
    uint64_t output2[8];
    memset(output, 0, sizeof(uint64_t)*8);
    memset(output2, 0, sizeof(uint64_t)*8);
    int result;
    int result2;
    //int *output2;
    result = pmemobj_ctl_get(pop, "stats.heap.run_allocated", (void*)output);
    //int result2 = pmemobj_ctl_get(pop, "heap.narenas.automatic", (void*)output2);
    result2 = pmemobj_ctl_get(pop, "stats.heap.run_active", (void*)output2);
    //std::cout<<result<<result2<<std::endl;
    std::cout<<"run alloc "<<output[0]<<"bytes and run acitve"<<output2[0]<<"bytes"<<std::endl;
    std::cout<<"fragment rate 0 is "<<(double)count / (double) output[0]<<std::endl;
  };
  void* pm_get_root(uint32_t i){
  };
  void set_root(void* root_ptr){};
  void* fetch_heap_start(){
    return (void*) pop;
  };
  void active(void* ptr){};
  void creat_single(VoidFunction func, void* args){
    nstore_thread2 = std::thread(func, args);
  };
  void join_single(){
    nstore_thread2.join();
  };
};

class Jemalloc : public Allocator {
 private:
  std::vector<std::thread> thread_ids;

 public:
  Jemalloc() {}
  void* allocate1(size_t sz) { return malloc(sz); }
  const char* name() { return "jemalloc"; };
  void deallocate1(void* ptr) { free(ptr); };
  void init(){};
  void creat_thread(VoidFunction func, void* arg) {
    thread_ids.push_back(std::thread(func, arg));
  };
  void thread_join(int i) { thread_ids[i].join(); };
  void pm_close(){};
  void recover(){std::cout<<"not supported!"<<std::endl;};
  void output(int i, size_t count, std::vector<double> objsize){};
  void* pm_get_root(uint32_t i){};
  void set_root(void* root_ptr){};
  void* fetch_heap_start(){};
  void active(void* ptr){};
  void creat_single(VoidFunction func, void* args){};
  void join_single(){};
};


extern "C" Allocator* get_Allocator() {
  Allocator* allocator = NULL;
#ifdef MAKALU
  return new makalu();
#elif NVMMALLOC
  return new nvm_malloc();
#elif PMDK
  return new pmdk();
#elif JEMALLOC
  return new Jemalloc();
#endif
}