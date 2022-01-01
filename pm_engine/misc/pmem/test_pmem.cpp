#include <iostream>
#include <cstring>
#include <string>
#include <cassert>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <thread>
#include <vector>

#include "libpm.h"

using namespace std;

void do_task(unsigned int tid) {

  std::cerr << "tid :: " << tid << std::endl;
  std::vector<void*> ptrs;

  int ops = 1024 * 1024 * 4;
  int ptrs_offset = 0;
  size_t sz;

  std::chrono::time_point<std::chrono::system_clock> start, end;
  std::chrono::duration<double> elapsed_seconds;
  char* vc;

  start = std::chrono::system_clock::now();
  srand(0);

  for (int i = 0; i < ops; i++) {
    sz = 1 + rand() % 32;
    vc = new char[sz];

    storage::pmemalloc_activate(vc);

    ptrs.push_back(vc);
    ptrs_offset = rand() % ptrs.size();

    if (rand() % 1024 != 0 && ptrs.size() >= 3) {
      delete (char*) ptrs[ptrs_offset];
      ptrs.erase(ptrs.begin() + ptrs_offset);
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cerr << "elapsed time: " << elapsed_seconds.count() << "s\n";
  cerr.flush();

  ptrs.clear();

  start = std::chrono::system_clock::now();
  srand(0);

  for (int i = 0; i < ops; i++) {
    sz = 1 + rand() % 32;
    vc = (char*) malloc(sz);

    ptrs.push_back(vc);
    ptrs_offset = rand() % ptrs.size();

    if (rand() % 1024 != 0 && ptrs.size() >= 3) {
      free(ptrs[ptrs_offset]);
      ptrs.erase(ptrs.begin() + ptrs_offset);
    }
  }

  end = std::chrono::system_clock::now();
  elapsed_seconds = end - start;
  cerr << "elapsed time: " << elapsed_seconds.count() << "s\n";
}

int main(int argc, char *argv[]) {
  const char* path = "./zfile";

  // cleanup
  unlink(path);

  size_t pmp_size = 64 * 1024 * 1024;
  if ((storage::pmp = storage::pmemalloc_init(path, pmp_size)) == NULL)
    std::cerr << "pmemalloc_init on :" << path << std::endl;

  storage::sp = (storage::static_info *) storage::pmemalloc_static_area();

  std::vector<std::thread> executors;
  unsigned int num_executors = 2;

  for (unsigned int i = 0; i < num_executors; i++)
    executors.push_back(std::thread(&do_task, i));

  for (unsigned int i = 0; i < num_executors; i++)
    executors[i].join();

  return 0;
}

