#include <algorithm>
#include <random>
#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cassert>


#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include "timer.h"
#include <map>
#include <thread>

#include "latency.h"
#include "utils2.h"
#include "record.h"
#include "pcm-memory.h"
//#include "pcm-memory.h"
/* Tracing infrastructure */
__thread struct timeval mtm_time;
__thread int mtm_tid = -1;

__thread char tstr[TSTR_SZ];
__thread unsigned long long tsz = 0;
__thread unsigned long long tbuf_ptr = 0;

/* Can we make these thread local ? */
char *tbuf;
pthread_spinlock_t tbuf_lock, tot_epoch_lock;
unsigned long long tbuf_sz;
int mtm_enable_trace = 0;
int mtm_debug_buffer = 1;
int trace_marker = -1, tracing_on = -1;
struct timeval glb_time;
unsigned long long start_buf_drain = 0, end_buf_drain = 0, buf_drain_period = 0;
unsigned long long glb_tv_sec = 0, glb_tv_usec = 0, glb_start_time = 0;
unsigned long long tot_epoch = 0;
Latency lat;
double rev_ms;
#ifdef PCMTOOL
    PCM_memory my_pcm;
#endif
thread_local timer2 tim;
thread_local size_t func_array[5] = {0, 0, 0, 0, 0};
__thread int reg_write = 0;
__thread unsigned long long n_epoch = 0;

void __pm_trace_print(int unused, ...)
{
#ifdef ENABLE_TRACE
        va_list __va_list;
        va_start(__va_list, unused);
        va_arg(__va_list, int); /* ignore first arg */
        char* marker = va_arg(__va_list, char*);
        unsigned long long addr = 0;

        if(!strcmp(marker, PM_FENCE_MARKER) ||
                !strcmp(marker, PM_TX_END)) {
                /*
                 * Applications are notorious for issuing
                 * fences, even when they didn't write to 
                 * PM. For eg., a fence for making a store
                 * to local, volatile variable visible.
                 */

                if(reg_write) { 
                        n_epoch += 1;
                        pthread_spin_lock(&tot_epoch_lock);
                        tot_epoch += 1;
                        pthread_spin_unlock(&tot_epoch_lock);
                }
                reg_write = 0;

        } else if(!strcmp(marker, PM_WRT_MARKER) ||
                !strcmp(marker, PM_DWRT_MARKER) ||
                !strcmp(marker, PM_DI_MARKER) ||
                !strcmp(marker, PM_NTI)) {
                addr = va_arg(__va_list, unsigned long long);
                if((PSEGMENT_RESERVED_REGION_START < addr &&
                        addr < PSEGMENT_RESERVED_REGION_END))
		{
                        reg_write = 1;
		}
        } else {;}
        va_end(__va_list);
#endif
}

unsigned long long get_epoch_count(void)
{
        return n_epoch;
}

unsigned long long get_tot_epoch_count(void)
{
        return tot_epoch;
}

namespace storage {
    // RAND GEN
    std::string get_rand_astring(size_t len) {
        static const char alphanum[] = "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        char rep = alphanum[rand() % (sizeof(alphanum) - 1)];
        std::string str(len, rep);
        return str;
    }

    double get_rand_double(double d_min, double d_max) {
        double f = (double) rand() / RAND_MAX;
        return d_min + f * (d_max - d_min);
    }

    bool get_rand_bool(double ratio) {
        double f = (double) rand() / RAND_MAX;
        return (f < ratio) ? true : false;
    }

    int get_rand_int(int i_min, int i_max) {
        double f = (double) rand() / RAND_MAX;
        return i_min + (int) (f * (double) (i_max - i_min));
    }

    int get_rand_int_excluding(int i_min, int i_max, int excl) {
        int ret;
        double f;

        if (i_max == i_min + 1) {
            if (excl == i_max)
                return i_min;
            else
                return i_max;
        }

        while (1) {
            f = (double) rand() / RAND_MAX;
            ret = i_min + (int) (f * (double) (i_max - i_min));
            if (ret != excl)
                break;
        }

        return ret;
    }

    std::string get_tuple(std::stringstream& entry, schema* sptr) {
        if (sptr == NULL)
            return "";

        std::string tuple, field;

        for (unsigned int col = 0; col < sptr->num_columns; col++) {
            entry >> field;
            tuple += field + " ";
        }

        return tuple;
    }

    // TIMER

    void display_stats(engine_type etype, double duration, int num_txns) {
        double throughput;

        switch (etype) {
            case engine_type::WAL:
                std::cerr << "WAL :: ";
                break;

            case engine_type::SP:
                std::cerr << "SP :: ";
                break;

            case engine_type::LSM:
                std::cerr << "LSM :: ";
                break;

            case engine_type::OPT_WAL:
                std::cerr << "OPT_WAL :: ";
                break;

            case engine_type::OPT_SP:
                std::cerr << "OPT_SP :: ";
                break;

            case engine_type::OPT_LSM:
                std::cerr << "OPT_LSM :: ";
                break;

            default:
                std::cerr << "Unknown engine type " << std::endl;
                break;
        }

        std::cerr << std::fixed << std::setprecision(2);
        std::cerr << "Duration(s) : " << (duration / 1000.0) << " ";

        throughput = (num_txns * 1000.0) / duration;
        std::cerr << "Throughput  : " << throughput << std::endl;
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Throughput  : " << throughput << std::endl;
    }

    // RANDOM DIST
    void zipf(std::vector<int>& zipf_dist, double alpha, int n, int num_values) {
        static double c = 0;          // Normalization constant
        double z;          // Uniform random number (0 < z < 1)
        double sum_prob;          // Sum of probabilities
        double zipf_value = 0;          // Computed exponential value to be returned
        int i, j;

        double* powers = new double[n + 1];
        int seed = 0;
        std::mt19937 gen(seed);
        std::uniform_real_distribution<> dis(0.001, 0.099);

        for (i = 1; i <= n; i++)
            powers[i] = 1.0 / pow((double) i, alpha);

        // Compute normalization constant on first call only
        for (i = 1; i <= n; i++)
            c = c + powers[i];
        c = 1.0 / c;

        for (i = 1; i <= n; i++)
            powers[i] = c * powers[i];

        for (j = 1; j <= num_values; j++) {

            // Pull a uniform random number in (0,1)
            z = dis(gen);

            // Map z to the value
            sum_prob = 0;
            for (i = 1; i <= n; i++) {
                sum_prob = sum_prob + powers[i];
                if (sum_prob >= z) {
                    zipf_value = i;
                    break;
                }
            }

            //std::cerr << "zipf_val :: " << zipf_value << std::endl;
            zipf_dist.push_back(zipf_value);
        }

        delete[] powers;
    }

    // Simple skew generator
    void simple_skew(std::vector<int>& simple_dist, double alpha, int n,
            int num_values) { // num_values is transaction, n is tuple number
        int seed = 0;
        std::mt19937 gen(seed);
        std::uniform_real_distribution<> dis(0, 1);
        double i, z;
        long bound;
        if(alpha == 0.5)
            bound = n * 0.2;  // alpha fraction goes to skewed values
        else
            bound = n * 0.1;
        long val, diff;

        diff = n - bound; // diff = 0.9*n

        for (i = 0; i < num_values; i++) {
            z = dis(gen);

            if (z < alpha)
                val = z * bound;
            else
                val = bound + z * diff;

            //std::cerr << "simple_val :: " << val << " bound : "<<bound<<" alpha : "<<alpha<< " z : "<<z<<std::endl;
            simple_dist.push_back(val);
        }
    }

    void uniform(std::vector<double>& uniform_dist, int num_values) {
        int seed = 0;
        std::mt19937 gen(seed);
        std::uniform_real_distribution<> dis(0, 1);
        double i;

        for (i = 0; i < num_values; i++)
            uniform_dist.push_back(dis(gen));
    }

    // PTHREAD WRAPPERS

    void wrlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_wrlock(access);
        if (rc != 0) {
            perror("wrlock failed \n");
            exit(EXIT_FAILURE);
        }
    }

    void rdlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_rdlock(access);
        if (rc != 0) {
            perror("rdlock failed \n");
            exit(EXIT_FAILURE);
        }
    }

    int try_wrlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_trywrlock(access);
        if (rc != 0) {
            perror("trywrlock failed \n");
            exit(EXIT_FAILURE);
        }

        return rc;
    }

    int try_rdlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_tryrdlock(access);
        if (rc != 0) {
            perror("tryrdlock failed \n");
        }

        return rc;
    }

    void unlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_unlock(access);
        if (rc != 0) {
            perror("unlock failed \n");
        }
    }

    void start_lat(){
        tim.start();
    }

    void end_lat(int op){
        auto tid = std::this_thread::get_id();
        std::vector<uint64_t>::iterator iter;
        switch(op){
            case OP_SELECT:
                lat.latency_select[tid].push_back(tim.elapsed<std::chrono::nanoseconds>());
                break;
            case OP_INSERT:
                lat.latency_insert[tid].push_back(tim.elapsed<std::chrono::nanoseconds>());
                break;
            case OP_DELETE:
                lat.latency_delete[tid].push_back(tim.elapsed<std::chrono::nanoseconds>());
                break;
            case OP_UPDATE:
                lat.latency_update[tid].push_back(tim.elapsed<std::chrono::nanoseconds>());
                break;
            case OP_STOCK:
                lat.latency_stock[tid].push_back(tim.elapsed<std::chrono::nanoseconds>());
                break;
            case OP_IND:
                if(lat.latency_insert[tid].size()==0)
                    break;
                iter = lat.latency_insert[tid].begin();
                *iter += tim.elapsed<std::chrono::nanoseconds>();
                break;
            case OP_LOG:
                if(lat.latency_delete[tid].size()==0)
                    break;
                iter = lat.latency_delete[tid].begin();
                *iter += tim.elapsed<std::chrono::nanoseconds>();
                break;
            case OP_MOD:
                if(lat.latency_update[tid].size()==0)
                    break;
                iter = lat.latency_update[tid].begin();
                *iter += tim.elapsed<std::chrono::nanoseconds>();
                break;
            /*case OP_TXN:
                if(lat.latency_select[tid].size()==0)
                    break;
                iter = lat.latency_select[tid].begin();
                *iter += tim.elapsed<std::chrono::nanoseconds>();
                break;*/
/*            case OP_PERSIST:
                flush_ms += tim.elapsed<std::chrono::nanoseconds>();
                break;*/
            case OP_ALC:
                if(lat.latency_select[tid].size()==0)
                    break;
                iter = lat.latency_select[tid].begin();
                *iter += tim.elapsed<std::chrono::nanoseconds>();
                break;
            case OP_RECOVER:
                rev_ms = tim.elapsed<std::chrono::microseconds>();
                rev_ms = rev_ms / 1000;
                std::cerr<<"recover cost ::" << rev_ms << " ms "<<std::endl;
                break;
            case OP_FLUSH:
                if(lat.latency_insert[tid].size()==0)
                    break;
                iter = lat.latency_insert[tid].begin();
                *iter += 1;
                break;
            case OP_FENCE:
                if(lat.latency_delete[tid].size()==0)
                    break;
                iter = lat.latency_delete[tid].begin();
                *iter += 1;
                break;
            default:
                break;
        }
        
    }
    void latency_output(){      
#ifdef LATENCY
        lat.output_all();
#endif   
    }
    void func_output(){
#ifdef FUNCT
        lat.output_all_func();
#endif    
    }
    void band_start(){
#ifdef PCMTOOL
        my_pcm.calculate_start();
        //my_pcm.calculate_cache_start();
#endif    
    }
    void band_end(){
#ifdef PCMTOOL    
        my_pcm.calculate_end();
        //my_pcm.calculate_cache_end();
#endif
    }
    void band_output(){
 #ifdef PCMTOOL       
        my_pcm.output();
 #endif   
    }
    void band_clear(){
 #ifdef PCMTOOL
        my_pcm.clear_total();
 #endif       
    }
    void persist_clear(){
        //flush_ms = 0;
    }
    void persist_output(){
        //std::cout<<"total persist: " << flush_ms / 1000 << " ms "<<std::endl;
    }
    void func_init(){
#ifdef FUNCT   
        auto tid = std::this_thread::get_id();
        lat.init_func(tid);
#endif
    }
}

