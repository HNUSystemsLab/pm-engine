#include <string>
#include <vector>

#include "engine_api.h"
#include "libpm.h"
#include "transaction.h"
#include "record.h"
#include "serializer.h"
#include "database.h"
#include "plist.h"
#include "timer.h"
#include "config.h"

namespace storage{
    class nof_lsm_engine : public engine_api{
        public:
        //func
            nof_lsm_engine(const config& _conf, database* _db, bool _read_only, unsigned int _tid);
            ~nof_lsm_engine();
            std::string select(const statement& st);
            int insert(const statement& st);
            int remove(const statement& st);
            int update(const statement& st);
            void load(const statement& st);
            void txn_begin();
            void txn_end(__attribute__((unused)) bool commit);
            void recovery();
            void merge_check();
            void merge(bool force);
        //arg
            serializer sr;    
            database* db;
            plist<char*>* pm_log;
            std::hash<std::string> hash_fun;
            const config& conf;
            bool read_only = false;
            unsigned int tid;
            size_t merge_looper;
            std::stringstream entry_stream;
            std::string entry_str;
            std::vector<void*> commit_free_list;
            //size_t merge_counter=0;
    };
}