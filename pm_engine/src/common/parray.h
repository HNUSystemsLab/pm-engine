#include "libpm.h"
#include <string>
#define MAX_BUF 80*1024*1024 //80M for per thread
#define PTR_LEN 20
namespace storage{
    //simple array work only for string case, used as ss table in lsm
    class parray{
        public:
            parray(){
                _global_root = pmalloc(MAX_BUF);
                curr_off=0;
            };
            parray(int size){
                _global_root = pmalloc(MAX_BUF * size);
                curr_off = 0;
            }
            off_t push_back(std::string val){
                if(val.size() > PTR_LEN){
                    printf("ptr overflow detected!\n");
                    exit(EXIT_FAILURE);
                }
                val.resize(PTR_LEN);
                char* start_addr = reinterpret_cast<char*> (_global_root + curr_off * PTR_LEN);
                // get value
                strcpy(start_addr, val.c_str());
                //flush the real ptr to keep consistent
#ifndef NOFLUSH                
                pmem_persist((void*) start_addr, PTR_LEN, true);
#endif
#ifdef FLUSH_C
                end_lat(OP_FLUSH);
                end_lat(OP_FENCE);
#endif                
                return curr_off++;
            }
            std::string at(off_t pos){
                char* ret_ptr = reinterpret_cast<char*> (_global_root + pos * PTR_LEN);
                std::string val;
                val.assign(ret_ptr, 0, PTR_LEN);
                return val;
            }

            void resize(){
                //allocate double space
                size_base = size_base * 2;
                void* new_root = pmalloc(sizeof(MAX_BUF * size_base));
                //copy elements
                for(int i=0; i<curr_off; i++)
                {
                    
                }
                //release previous space
                pfree(_global_root);
                _global_root = new_root;
            }

            ~parray(){
                pfree(_global_root);
            };
        private:
            void* _global_root;
            off_t curr_off;
            size_t size_base=1;
    };
}