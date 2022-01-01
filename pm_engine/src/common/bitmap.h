#include <string>
#include <sys/types.h>
#include <iostream>
#include "libpm.h"

class Bitmap{
    private:
        char* bitroot;
        size_t bit_n;
    public:
        Bitmap(size_t n){
            bit_n = n;
            int real_size = n / 8;
            bitroot = (char*) malloc(real_size);
            memset(bitroot, 0, real_size);   
        };

        bool find(size_t x){
            int char_pos = x / 8;
            int bit_pos = x % 8;
            if(bitroot[char_pos] & 1<<bit_pos)
                return true;
            return false;
        }

        bool set(size_t x){
            if(x >= bit_n){
                return false;
            }
            int char_pos  = x / 8;
            int bit_pos = x % 8;
            bitroot[char_pos] |= (1 << bit_pos);
            //pmem_persist(&bitroot[char_pos], 8, 1);
            return true;
        }

       /* bool erase(int x){
            if(x >= bit_n){
                return false;
            }
            int char_pos = x / 8;
            int bit_pos = x % 8;
            bitroot[char_pos] &= ~(1 << bit_pos);
            return true;

        }*/
        
        size_t bit_size(){return bit_n;}

        ~Bitmap(){
            free((void*)bitroot);
        }

};