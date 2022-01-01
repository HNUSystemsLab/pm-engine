#include "nof_lsm_engine.h"

namespace storage{
    nof_lsm_engine::nof_lsm_engine(const config& _conf, database* _db, bool _read_only, unsigned int tid):
        conf(_conf),
        db(_db),
        read_only(_read_only) {
        pm_log = db->log;
        etype = engine_type::NOF_LSM;
        merge_looper = 0;
        pm_log = db->log;
    }

    nof_lsm_engine::~nof_lsm_engine(){
        merge(true);
        //pmemalloc_recover();
    }

    std::string nof_lsm_engine::select(const statement &st){
//#ifdef LATENCY
        //start_lat();
//#endif
        std::string ret_val;
        record* rec_ptr = st.rec_ptr;
        record* mem_rec = NULL;
        record* ss_rec = NULL;
        table* tab = db->tables->at(st.table_id);
        table_index* tab_index = tab->indices->at(st.table_index_id);
        std::string key_str = sr.serialize(rec_ptr, tab_index->sptr);
        unsigned long key = hash_fun(key_str);
        off_t ss_off = -1;
#ifdef FUNC
        start_lat();
#endif
        tab_index->pm_map->at(key, &mem_rec);
        tab_index->off_map->at(key, &ss_off);
        if(ss_off != -1){
            //get data from sstable
            std::string val = db->ss_tables->at(ss_off);
            std::sscanf((char*) val.c_str(), "%p", &ss_rec);
        }
        if(mem_rec != NULL && ss_rec == NULL){
            ret_val = sr.serialize(mem_rec, st.projection);
        } else if(mem_rec == NULL && ss_rec != NULL){
            ret_val = sr.serialize(ss_rec, st.projection);
        } else if(mem_rec != NULL && ss_rec != NULL){
            int num_cols = mem_rec->sptr->num_columns;
            for(int i=0; i < num_cols; i++){
                if (mem_rec->sptr->columns[i].enabled)
                    ss_rec->set_data(i, mem_rec);
            }
            ret_val = sr.serialize(ss_rec, st.projection);
        }
        
//#ifdef LATENCY
        //end_lat(OP_SELECT);
//#endif
        delete rec_ptr;
#ifdef FUNC
        end_lat(OP_IND);
#endif
        return ret_val;
    }

    int nof_lsm_engine::insert(const statement &st){
        record* ins_rec = st.rec_ptr;
        table* tab = db->tables->at(st.table_id);
        plist<table_index*>* indices = tab->indices;
        std::string key_str = sr.serialize(ins_rec, indices->at(0)->sptr);
        unsigned long key = hash_fun(key_str);
        //check
#ifdef FUNC
        start_lat();
#endif
        if(indices->at(0)->pm_map->exists(key) || indices->at(0)->off_map->exists(key)){
            ins_rec->clear_data();
            delete ins_rec;
            return EXIT_FAILURE;
        }
#ifdef FUNC
        end_lat(OP_IND);
        start_lat();
#endif
        //log entry
        entry_stream.str("");
        entry_stream << st.transaction_id << " " << st.op_type << " " <<st.table_id
                     << " " << ins_rec << "\n";
        entry_str = entry_stream.str();
        size_t entry_str_sz = entry_str.size() + 1;
#ifdef FUNC
        end_lat(OP_LOG);
        start_lat();
#endif
        char* entry = (char*) pmalloc(sizeof(char) * entry_str_sz);
#ifdef FUNC
        end_lat(OP_ALC);
        start_lat();
#endif                
        PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));
        pmemalloc_activate(entry);
        pm_log->push_back(entry);
        //necessary?
#ifdef FUNC
        end_lat(OP_LOG);
        start_lat();
#endif
        ins_rec->persist_data();
        pmemalloc_activate(ins_rec);
        for(int i=0; i< tab->num_indices; i++){
            key_str = sr.serialize(ins_rec, indices->at(i)->sptr);
            key = hash_fun(key_str);
            indices->at(i)->pm_map->insert(key, ins_rec);
        }
#ifdef FUNC
        end_lat(OP_MOD);
#endif
        return EXIT_SUCCESS;
    }

    int nof_lsm_engine::remove(const statement &st){
        record* rem_rec = st.rec_ptr;
        table* tab = db->tables->at(st.table_id);
        plist<table_index*>* indices = tab->indices;
        std::string key_str = sr.serialize(rem_rec, indices->at(0)->sptr);
        unsigned long key = hash_fun(key_str);
        record* log_rec=NULL;
        off_t rec_off = -1;
#ifdef FUNC
        start_lat();
#endif
        if (indices->at(0)->pm_map->at(key, &log_rec) == 0
            && indices->at(0)->off_map->at(key, &rec_off) == 0){
            delete rem_rec;
            return EXIT_FAILURE;
        }
        if(log_rec == NULL){
            std::string val = db->ss_tables->at(rec_off);
            std::sscanf((char*)val.c_str(), "%p", &log_rec);
        }
#ifdef FUNC
        end_lat(OP_IND);
        start_lat();
#endif

        int num_cols = log_rec->sptr->num_columns;
        for (int field_itr = 0; field_itr < num_cols; field_itr++){
            if(log_rec->sptr->columns[field_itr].inlined==0){
                void* before_field = log_rec->get_pointer(field_itr);
                commit_free_list.push_back(before_field);
            }
        }
        commit_free_list.push_back(log_rec);
        //log entry free_list for log
        entry_stream.str("");
        entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                     << " " << log_rec << "\n";
        entry_str = entry_stream.str();
#ifdef FUNC
        end_lat(OP_LOG);
        start_lat();
#endif
        char* entry = (char*) pmalloc(entry_str.size() + 1);
#ifdef FUNC
        end_lat(OP_ALC);
        start_lat();
#endif        
        PM_MEMCPY((entry), (entry_str.c_str()), (entry_str.size() + 1));
        pmemalloc_activate(entry);
  // Add log entry
        pm_log->push_back(entry);
        //log done
        /*if (indices->at(0)->pm_map->at(key, &before_rec)) {
            delete before_rec;
        }*/
#ifdef FUNC
        end_lat(OP_LOG);
        start_lat();
#endif
        for(int i=0; i<tab->num_indices; i++){
            key_str = sr.serialize(rem_rec, indices->at(i)->sptr);
            key = hash_fun(key_str);
            indices->at(i)->pm_map->erase(key);
            indices->at(i)->off_map->erase(key);
        }
#ifdef FUNC
        end_lat(OP_MOD);
#endif
#ifdef FUNC
        start_lat();
#endif
        delete rem_rec;
#ifdef FUNC
        end_lat(OP_ALC);
#endif        
        return EXIT_SUCCESS;
    }

    int nof_lsm_engine::update(const statement &st){
        record* upd_rec = st.rec_ptr;
        table* tab = db->tables->at(st.table_id);
        plist<table_index*>* indices = tab->indices;
        std::string key_str = sr.serialize(upd_rec, indices->at(0)->sptr);
        unsigned long key = hash_fun(key_str);
        record* before_rec = NULL;
        off_t ss_off = -1;
#ifdef FUNC
        start_lat();
#endif
        if(!indices->at(0)->pm_map->at(key, &before_rec) && !indices->at(0)->off_map->at(key, &ss_off)){
            upd_rec->clear_data();
            delete upd_rec;
            return EXIT_FAILURE;
        }
        if(before_rec == NULL){ // find tuple in sstable
            std::string val = db->ss_tables->at(ss_off);
            std::sscanf((char*)val.c_str(), "%p", &before_rec);
        }
        void *before_field, *after_field;
#ifdef FUNC
        end_lat(OP_IND);
        start_lat();
#endif
        // log 
        int num_fields = st.field_ids.size();
        entry_stream.str("");
        entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                     << " " << num_fields << " " << before_rec << " ";
        for (int field_itr : st.field_ids){
            if(upd_rec->sptr->columns[field_itr].inlined == 0) {
                before_field = before_rec->get_pointer(field_itr);
                after_field = upd_rec->get_pointer(field_itr);
                entry_stream << field_itr << " " << before_field << " ";
            }
            else{
                std::string before_data = before_rec->get_data(field_itr);
                entry_stream << field_itr << " " << " "<<before_data << " ";
            }
        }
        entry_str = entry_stream.str();
        size_t entry_str_sz = entry_str.size() + 1;
        //char* entry = new char[entry_str_sz];
#ifdef FUNC
        end_lat(OP_LOG);
        start_lat();
#endif
        char* entry = (char*) pmalloc(sizeof(char) * entry_str_sz);
#ifdef FUNC
        end_lat(OP_ALC);
        start_lat();
#endif
        PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));

         // Add log entry
        pmemalloc_activate(entry);
        pm_log->push_back(entry);
        // log done
#ifdef FUNC
        end_lat(OP_LOG);
        start_lat();
#endif
        for(int field_itr : st.field_ids){
            if(upd_rec->sptr->columns[field_itr].inlined == 0){
                before_field = before_rec->get_pointer(field_itr);
                after_field = upd_rec->get_pointer(field_itr);
                pmemalloc_activate(after_field);
                //freelist ?
                commit_free_list.push_back(before_field);

            }

            //update 
            before_rec->set_data(field_itr, upd_rec);
        }
#ifdef FUNC
        end_lat(OP_MOD);
#endif
        return EXIT_SUCCESS;
    }

    void nof_lsm_engine::load(const statement &st){
        record* load_rec = st.rec_ptr;
        table* tab = db->tables->at(st.table_id);
        plist<table_index*>* tab_idx = tab->indices;
        std::string key_str = sr.serialize(load_rec, tab_idx->at(0)->sptr);
        unsigned key = hash_fun(key_str);
        //log
        entry_stream.str("");
        entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id << " "
                     << load_rec << "\n";
        entry_str = entry_stream.str();
        char* entry = (char*) pmalloc(entry_str.size() + 1);
        memcpy(entry, entry_str.c_str(), entry_str.size() + 1);

        //log done
        pmemalloc_activate(load_rec);
        pmemalloc_activate(entry);
        pm_log->push_back(entry);
        for(int i=0; i<tab->num_indices; i++){
            key_str = sr.serialize(load_rec, tab_idx->at(i)->sptr);
            key = hash_fun(key_str);
            tab_idx->at(i)->pm_map->insert(key, load_rec);

        }
    }

    void nof_lsm_engine::merge_check(){
        if(++merge_looper % conf.merge_interval == 0){
            merge(false);
            merge_looper = 0;
        }
    }

    void nof_lsm_engine::txn_begin(){}

    void nof_lsm_engine::txn_end(__attribute__((unused)) bool commit) {
#ifdef FUNC
        start_lat();
#endif
        for (void* ptr : commit_free_list) {
            delete  (char*) ptr;
        }
        commit_free_list.clear();
#ifdef FUNC
        end_lat(OP_ALC);
        start_lat();
#endif 
        /*if(read_only)
            return;
       */
        merge_check();
#ifdef FUNC
        end_lat(OP_LOG);
        start_lat();
#endif
    }

    void nof_lsm_engine::merge(bool force){
        std::vector<table*> tables = db->tables->get_data();
        for (table* tab : tables){
            table_index *p_index = tab->indices->at(0);
            std::vector<table_index*> indices = tab->indices->get_data();
            pbtree<unsigned long, record*>* pm_map = p_index->pm_map;
            
            size_t compact_threshold = conf.merge_ratio * p_index->off_map->size();
            bool compact = (pm_map->size() > compact_threshold);

            if (force || compact){
                pbtree<unsigned long, record*>::const_iterator itr;
                record *pm_rec, *ss_rec;
                unsigned long key;
                off_t ss_off;
                std::string val;
                char ptr_buf[32];
                for(itr = pm_map->begin(); itr != pm_map->end(); itr++){
                    key = (*itr).first;
                    pm_rec = (*itr).second;
                    ss_rec = NULL;
                    if(p_index->off_map->at(key, &ss_off)){
                        val = db->ss_tables->at(ss_off);
                        std::sscanf((char*)val.c_str(), "%p", &ss_rec);
                        int num_cols = pm_rec->sptr->num_columns;
                        for(int i=0; i<num_cols; i++){
                            ss_rec->set_data(i, pm_rec);
                        }
                    } else{
                        std::sprintf(ptr_buf, "%p", pm_rec);
                        val = std::string(ptr_buf);
                        ss_off = db->ss_tables->push_back(val);
                        //std::string this_val = db->ss_tables->at(ss_off);
                        //record* test_rec = NULL;
                        //std::sscanf(this_val.c_str(), "%p", &test_rec);
                        
                        for(table_index* index : indices){
                            std::string key_str = sr.serialize(pm_rec, index->sptr);
                            key = hash_fun(key_str);
                            index->off_map->insert(key, ss_off);
                        }
                    }
                }
#ifdef FUNC
                end_lat(OP_LOG);
                start_lat();
#endif
                for (table_index* index : indices)
                    index->pm_map->clear();
            }//end merge one tab
            
        }//end merge all tab
        //clear log
        //if(force)
            pm_log->clear();
#ifdef FUNC
        end_lat(OP_ALC);
        start_lat();
#endif                 
    }

    void nof_lsm_engine::recovery(){
        std::vector<char*> log_vec = pm_log->get_data();
        int op_type, txn_id, table_id;
        int txn_count=0;
        table *tab;
        plist<table_index*>* indices;
        std::string ptr_str;
        uint32_t index_itr;
        record *before_rec, *after_rec;
        field_info finfo;
        timer rec_t;
        int num_index;
        rec_t.start();
        int total_txn = log_vec.size();
        for(char* ptr : log_vec){
            txn_count++;
            //if(txn_count)
            if(total_txn - txn_count < conf.active_txn_threshold)
                continue;
            std::stringstream entry(ptr);
            entry >> txn_id >> op_type >>table_id;
            switch(op_type){
                case operation_type::Insert:
                    entry >> ptr_str;
                    std::sscanf(ptr_str.c_str(), "%p", &after_rec);
                    
                    tab = db->tables->at(table_id);
                    indices = tab->indices;
                    num_index = tab->num_indices;
                    //undo
                    //tab->pm_data->erase(after_rec);
                    for(index_itr = 0; index_itr < num_index; index_itr++) {
                        std::string key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
                        unsigned long key = hash_fun(key_str);
                        indices->at(index_itr)->pm_map->erase(key);

                    }
                    break;
                case operation_type::Delete:
                    entry >> ptr_str;
                    std::sscanf(ptr_str.c_str(), "%p", &before_rec);
                    
                    tab = db->tables->at(table_id);
                    indices = tab->indices;
                    num_index = tab->num_indices;
                    //undo
                    for(index_itr = 0; index_itr < num_index; index_itr++){
                        std::string key_str = sr.serialize(before_rec, indices->at(index_itr)->sptr);
                        unsigned long key = hash_fun(key_str);
                        indices->at(index_itr)->pm_map->insert(key, before_rec);

                    }
                    break;
                case operation_type::Update:
                    int num_fields;
                    int field_itr;

                    entry >> num_fields >> ptr_str;
                    std::sscanf(ptr_str.c_str(), "%p", &before_rec);
                    for(field_itr = 0; field_itr < num_fields; field_itr++){
                        entry >>field_itr;
                        tab = db->tables->at(table_id);
                        indices = tab->indices;
                        finfo = before_rec->sptr->columns[field_itr];
                        if(finfo.inlined ==0) {
                            void* before_field;
                            entry >> ptr_str;
                            std::sscanf(ptr_str.c_str(), "%p", &before_field);
                            before_rec->set_pointer(field_itr, before_field);
                        }
                        else{
                            field_type ftype = finfo.type;
                            switch(ftype){
                                case field_type::INTEGER:
                                    int ival;
                                    entry >> ival;
                                    before_rec->set_int(field_itr, ival);
                                    break;
                                case field_type::DOUBLE:
                                    double dval;
                                    entry >> dval;
                                    before_rec->set_double(field_itr, dval);
                                    break;
                                default:
                                    std::cerr << "Invalid field type : " << op_type << std::endl;
                                    break;
                            }
                        }
                    }
                    break;
                default:
                    std::cerr <<"Invalid op type" << op_type <<std::endl;
                    break;
            }
            delete ptr;
        }

        pm_log->clear();
        rec_t.end();
        std::cerr << "NOF_LSM :: Recovery duration (ms) : " << rec_t.duration() << std::endl;
        std::cout << "NOF_LSM :: Recovery duration (ms) : " << rec_t.duration() << std::endl;
        
    }


}