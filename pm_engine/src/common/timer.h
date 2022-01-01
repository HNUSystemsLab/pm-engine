#pragma once
#include <iostream>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <sys/time.h>
#include <chrono>
#include <ratio>
#include <type_traits>

template <typename T>
struct is_duration : std::false_type {};

template <class Rep, std::intmax_t Num, std::intmax_t Denom>
struct is_duration<std::chrono::duration<Rep, std::ratio<Num, Denom>>>
    : std::true_type {};


class timer2 {
  private:
    std::chrono::high_resolution_clock::time_point _start;
  public:
    timer2() noexcept {}
    ~timer2() noexcept {}
    void start() noexcept {_start = std::chrono::high_resolution_clock::now(); }
    template <typename T>
    float elapsed() noexcept {
      static_assert(is_duration<T>::value);
      auto stop = std::chrono::high_resolution_clock::now();
      std::chrono::duration<float, typename T::period> e = stop - _start;
      return e.count();
  }
};




namespace storage {




class timer {
 public:

  timer() {
    total = timeval();
  }

  double duration() {
    double duration;

    duration = (total.tv_sec) * 1000.0;      // sec to ms
    duration += (total.tv_usec) / 1000.0;      // us to ms

    return duration;
  }

  void start() {
    gettimeofday(&t1, NULL);
  }

  void end() {
    gettimeofday(&t2, NULL);
    timersub(&t2, &t1, &diff);
    timeradd(&diff, &total, &total);
  }

  void reset(){
    total = timeval();
  }

  timeval t1, t2, diff;
  timeval total;
};


}
