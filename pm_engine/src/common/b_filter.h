#include "bitmap.h"
#include "utils2.h"
#include <string>
#include <tr1/functional>

#define MAX_BM 1000 * 10000


class BloomFilter{
    private:
        Bitmap *bm;
    public:

        BloomFilter(size_t size){
            bm = new ((Bitmap*) pmalloc(sizeof(Bitmap))) Bitmap(size);
        };

        void set(std::string value, int hash_num){
            //get a hash func
            for(int i=0; i<hash_num; i++){
                size_t key1 = std::tr1::_Fnv_hash_base<4>::hash(value.c_str()+('a' + i), value.size()+1);
                size_t pos = key1 % MAX_BM;
                if(bm->set(pos) == false){
                    printf("set fail!");
                    abort();
                }
            }  
        }

        bool find(std::string value, int hash_num){
            for(int i=0; i<hash_num; i++){
                size_t key1 = std::tr1::_Fnv_hash_base<4>::hash(value.c_str()+('a' + i), value.size()+1);
                size_t pos = key1%MAX_BM;
                if(bm->find(pos) == false){
                    return false;
                }
            }
            return true;
        }

};