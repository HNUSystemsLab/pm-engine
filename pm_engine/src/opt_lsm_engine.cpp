// OPT LSM - PM BASED

#include "opt_lsm_engine.h"

namespace storage {

opt_lsm_engine::opt_lsm_engine(const config& _conf, database* _db,
bool _read_only,
                               unsigned int _tid)
    : conf(_conf),
      db(_db),
      tid(_tid) {

  etype = engine_type::OPT_LSM;
  read_only = _read_only;
  merge_looper = 0;
  pm_log = db->log;

  std::vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    std::string table_file_name = conf.fs_path + std::to_string(_tid) + "_"
        + std::string(tab->table_name);
    // Storing pointer only
    //tab->pm_data.
    tab->fs_data.configure(table_file_name, 15, false);
  }

}

opt_lsm_engine::~opt_lsm_engine() {

  if (!read_only) {
    merge(true);

    std::vector<table*> tables = db->tables->get_data();
    for (table* tab : tables) {
      tab->fs_data.sync();
      tab->fs_data.close();
    }
  }

}

std::string opt_lsm_engine::select(const statement& st) {
  LOG_INFO("Select");
  std::string val;

  record *rec_ptr = st.rec_ptr;
  record *pm_rec = NULL, *fs_rec = NULL;
  table *tab = db->tables->at(st.table_id);
  table_index *table_index = tab->indices->at(st.table_index_id);
  std::string key_str = sr.serialize(rec_ptr, table_index->sptr);

  unsigned long key = hash_fn(key_str);
  std::string string_key = std::to_string(key);
  off_t storage_offset = -1;

  // Check if key exists in mem
  table_index->pm_map->at(key, &pm_rec);

  // Check if key exists in fs
  // first in filter
/*#ifdef BLOOM
  if(tab->tbfilter->find(string_key, 5))
    table_index->off_map->at(key, &storage_offset);
#endif*/
  table_index->off_map->at(key, &storage_offset);
  if (storage_offset != -1) {
    val = tab->fs_data.at(storage_offset);
    std::sscanf((char*) val.c_str(), "%p", &fs_rec);
  }

  if (pm_rec != NULL && fs_rec == NULL) {
    // From Memtable
    val = sr.serialize(pm_rec, st.projection);
  } else if (pm_rec == NULL && fs_rec != NULL) {
    // From SSTable
    val = sr.serialize(fs_rec, st.projection);

  } else if (pm_rec != NULL && fs_rec != NULL) {
    // Merge
    int num_cols = pm_rec->sptr->num_columns;
    for (int field_itr = 0; field_itr < num_cols; field_itr++) {
      if (pm_rec->sptr->columns[field_itr].enabled)
        fs_rec->set_data(field_itr, pm_rec);
    }

    val = sr.serialize(fs_rec, st.projection);
  }

  LOG_INFO("val : %s", val.c_str());
  delete rec_ptr;
  return val;
}

int opt_lsm_engine::insert(const statement& st) {
  //LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  //LOG_INFO("Key_str :: --%s-- ", key_str.c_str());
  unsigned long key = hash_fn(key_str);
#ifdef FUNC
  start_lat();
#endif
  // Check if key exists
  if (indices->at(0)->pm_map->exists(key)
      || indices->at(0)->off_map->exists(key)) {
    after_rec->clear_data();
    delete after_rec;
    return EXIT_FAILURE;
  }
#ifdef FUNC
  end_lat(OP_IND);
  start_lat();
#endif
  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << after_rec << "\n";

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  //char* entry = new char[entry_str_sz];
  char* entry = (char*) pmalloc(sizeof(char) * entry_str_sz);
  PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));
  pmemalloc_activate(entry);
  after_rec->persist_data();
  // Activate new record
  pmemalloc_activate(after_rec);
  pm_log->push_back(entry);
  // Add log entry
#ifdef FUNC
  end_lat(OP_LOG);
  start_lat();
#endif
  // Add entry in indices
/*#ifdef BLOOM
  std::string key_string = std::to_string(key);
  tab->tbfilter->set(key_string, 5);
#endif*/
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }
//  delete entry;
#ifdef FUNC
  end_lat(OP_MOD);
#endif
  return EXIT_SUCCESS;
}

int opt_lsm_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  // Check if key does not exist
#ifdef FUNC
  start_lat();
#endif
  record* before_rec=NULL;
  off_t rec_off=-1;
  if (indices->at(0)->pm_map->at(key, &before_rec) == 0
      && indices->at(0)->off_map->at(key, &rec_off) == 0) {
    delete rec_ptr;
    return EXIT_FAILURE;
  }
  if(before_rec == NULL){
    std::string val = tab->fs_data.at(rec_off);
    std::sscanf((char*) val.c_str(), "%p", &before_rec);
  }
#ifdef FUNC
  end_lat(OP_IND);
  start_lat();
#endif
  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << before_rec << "\n";

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  //char* entry = new char[entry_str_sz];
  char* entry = (char*) pmalloc(sizeof(char) * entry_str_sz);
  /*if(st.transaction_id >= 2007){
    record* test;
    std::sscanf("0x7f51543e8cc8", "%p", &test);
    int len = test->data_len;
    char* this_data = test->data;
    char shit = this_data[1];
  }*/
  PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));
  pmemalloc_activate(entry);
  // Add log entry
  pm_log->push_back(entry);
  //record* before_rec = NULL;
/*  if (indices->at(0)->pm_map->at(key, &before_rec)) {
    delete before_rec;
  }*/
  // Remove entry in indices
#ifdef FUNC
  end_lat(OP_LOG);
  start_lat();
#endif
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->erase(key);
    indices->at(index_itr)->off_map->erase(key);
  }
  delete rec_ptr;
#ifdef FUNC
  end_lat(OP_MOD);
#endif  
  return EXIT_SUCCESS;
}

int opt_lsm_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  std::string val;
  record* before_rec=NULL;
  void *before_field, *after_field;
  bool update_rec = false;
  off_t storage_offset;
  // Check if key does not exist
#ifdef FUNC
  start_lat();
#endif
  if (indices->at(0)->pm_map->at(key, &before_rec) == false && indices->at(0) ->off_map->at(key, &storage_offset) == false) {
    rec_ptr->clear_data();
    delete rec_ptr;
    return EXIT_FAILURE;
    /*before_rec = rec_ptr;

    entry_stream.str("");
    entry_stream << st.transaction_id << " " << operation_type::Insert << " "
                 << st.table_id << " " << before_rec << "\n";*/

  } else {
    if(before_rec == NULL)//for file case
    {
      val = tab->fs_data.at(storage_offset);
      std::sscanf((char*) val.c_str(), "%p", &before_rec);
    }
    int num_fields = st.field_ids.size();
    update_rec = true;
#ifdef FUNC
    end_lat(OP_IND);
    start_lat();
#endif
    entry_stream.str("");
    entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                 << " " << num_fields << " " << before_rec << " ";

    for (int field_itr : st.field_ids) {
      // Pointer field
      if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
        before_field = before_rec->get_pointer(field_itr);
        after_field = rec_ptr->get_pointer(field_itr);
        entry_stream << field_itr << " " << before_field << " ";
      }
      // Data field
      else {
        std::string before_data = before_rec->get_data(field_itr);
        entry_stream << field_itr << " " << " " << before_data << " ";
      }
    }

  }

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  //char* entry = new char[entry_str_sz];
  char* entry = (char*) pmalloc(sizeof(char) * entry_str_sz);
  PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));

  // Add log entry
  pmemalloc_activate(entry);
  pm_log->push_back(entry);
#ifdef FUNC
  end_lat(OP_LOG);
  start_lat();
#endif
  if (update_rec) {
    for (int field_itr : st.field_ids) {
      // Activate new field and garbage collect previous field
      if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
        before_field = before_rec->get_pointer(field_itr);
        after_field = rec_ptr->get_pointer(field_itr);

        pmemalloc_activate(after_field);
        commit_free_list.push_back(before_field);
      }

      // Update existing record
      before_rec->set_data(field_itr, rec_ptr);
    }
  } else {
    // Activate new record
    pmemalloc_activate(before_rec);
    before_rec->persist_data();

    // Add entry in indices
    for (index_itr = 0; index_itr < num_indices; index_itr++) {
      key_str = sr.serialize(before_rec, indices->at(index_itr)->sptr);
      key = hash_fn(key_str);

      indices->at(index_itr)->pm_map->insert(key, before_rec);
    }
  }
#ifdef FUNC
  end_lat(OP_MOD);
#endif
  return EXIT_SUCCESS;
}

void opt_lsm_engine::load(const statement& st) {
  //LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << after_rec << "\n";

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  //char* entry = new char[entry_str_sz];
  char* entry = (char*) pmalloc(sizeof(char) * entry_str_sz);
  memcpy(entry, entry_str.c_str(), entry_str_sz);

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add log entry
  pmemalloc_activate(entry);
  pm_log->push_back(entry);
#ifdef BLOOM
  std::string key_string = std::to_string(key);
  tab->tbfilter->set(key_string, 5);
#endif
  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }
}

void opt_lsm_engine::merge_check() {
  if (++merge_looper % conf.merge_interval == 0) {
    merge(false);
    merge_looper = 0;
  }
}

void opt_lsm_engine::merge(bool force) {
  //std::std::cerr << "Merging ! " << merge_looper << std::endl;

  std::vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    table_index *p_index = tab->indices->at(0);
    std::vector<table_index*> indices = tab->indices->get_data();

    pbtree<unsigned long, record*>* pm_map = p_index->pm_map;

    size_t compact_threshold = conf.merge_ratio * p_index->off_map->size();
    bool compact = (pm_map->size() > compact_threshold);

    // Check if need to merge
    if (force || compact) {
      pbtree<unsigned long, record*>::const_iterator itr;
      record *pm_rec, *fs_rec;
      unsigned long key;
      off_t storage_offset;
      std::string val;
      char ptr_buf[32];

      // All tuples in table
      for (itr = pm_map->begin(); itr != pm_map->end(); itr++) {
        key = (*itr).first;
        pm_rec = (*itr).second;

        fs_rec = NULL;

        // Check if we need to merge
        if (p_index->off_map->at(key, &storage_offset)) {
          //LOG_INFO("Merge :: update :: val :: %s ", val.c_str());

          val = tab->fs_data.at(storage_offset);

          std::sscanf((char*) val.c_str(), "%p", &fs_rec);
          //printf("fs_rec :: %p \n", fs_rec);

          int num_cols = pm_rec->sptr->num_columns;
          for (int field_itr = 0; field_itr < num_cols; field_itr++) {
            fs_rec->set_data(field_itr, pm_rec);
          }

        } else {
          // Insert tuple
          std::sprintf(ptr_buf, "%p", pm_rec);
          val = std::string(ptr_buf);
          //LOG_INFO("Merge :: insert new :: val :: %s ", val.c_str());

          storage_offset = tab->fs_data.push_back(val);

          for (table_index* index : indices) {
            std::string key_str = sr.serialize(pm_rec, index->sptr);
            key = hash_fn(key_str);
            index->off_map->insert(key, storage_offset);
          }
        }
      }

      // Clear mem table
      for (table_index* index : indices)
        index->pm_map->clear();
    }
  }

  // Truncate log
  if (force)
    pm_log->clear();

}

void opt_lsm_engine::txn_begin() {
}

void opt_lsm_engine::txn_end(__attribute__((unused)) bool commit) {
#ifdef FUNC
  start_lat();
#endif
  if (read_only)
    return;

  // Clear commit_free list
  for (void* ptr : commit_free_list) {
    delete  (char*) ptr;
  }
  commit_free_list.clear();
#ifdef FUNC
  end_lat(OP_ALC);
  start_lat();
#endif
  merge_check();
#ifdef FUNC
  end_lat(OP_LOG);
#endif
}

void opt_lsm_engine::recovery() {

  LOG_INFO("OPT LSM recovery");

  std::vector<char*> undo_log = pm_log->get_data();

  int op_type, txn_id, table_id;
  unsigned int num_indices, index_itr;
  table *tab;
  plist<table_index*>* indices;

  std::string ptr_str;

  record *before_rec, *after_rec;
  field_info finfo;

  timer rec_t;
  rec_t.start();

  int total_txns = undo_log.size();
  int txn_cnt = 0;

  for (char* ptr : undo_log) {
    txn_cnt++;
    //std::cout << "entry : --" << ptr << "-- " << std::endl;

    if (total_txns - txn_cnt < conf.active_txn_threshold)
      continue;

    std::stringstream entry(ptr);

    entry >> txn_id >> op_type >> table_id;

    switch (op_type) {
      case operation_type::Insert:
        LOG_INFO("Undo Insert");
        entry >> ptr_str;
        std::sscanf(ptr_str.c_str(), "%p", &after_rec);

        tab = db->tables->at(table_id);
        indices = tab->indices;
        num_indices = tab->num_indices;

        //tab->pm_data->erase(after_rec);

        // Remove entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = sr.serialize(after_rec,
                                             indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->pm_map->erase(key);
        }

        // Free after_rec
        for (unsigned int field_itr = 0;
            field_itr < after_rec->sptr->num_columns; field_itr++) {
          if (after_rec->sptr->columns[field_itr].inlined == 0) {
            void* before_field = after_rec->get_pointer(field_itr);
            commit_free_list.push_back(before_field);
          }
        }
        commit_free_list.push_back(after_rec);
        break;

      case operation_type::Delete:
        LOG_INFO("Undo Delete");
        entry >> ptr_str;
        std::sscanf(ptr_str.c_str(), "%p", &before_rec);

        tab = db->tables->at(table_id);
        indices = tab->indices;
        num_indices = tab->num_indices;

        //tab->pm_data->push_back(after_rec);

        // Fix entry in indices to point to before_rec
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = sr.serialize(before_rec,
                                             indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->pm_map->insert(key, before_rec);
        }
        break;

      case operation_type::Update:
        LOG_INFO("Undo Update");
        int num_fields;
        int field_itr;

        entry >> num_fields >> ptr_str;
        std::sscanf(ptr_str.c_str(), "%p", &before_rec);
        //printf("before rec :: --%p-- \n", before_rec);

        for (field_itr = 0; field_itr < num_fields; field_itr++) {
          entry >> field_itr;

          tab = db->tables->at(table_id);
          indices = tab->indices;
          finfo = before_rec->sptr->columns[field_itr];

          // Pointer
          if (finfo.inlined == 0) {
            LOG_INFO("Pointer ");
            void *before_field;

            entry >> ptr_str;
            std::sscanf(ptr_str.c_str(), "%p", &before_field);

            //after_field = before_rec->get_pointer(field_itr);
            before_rec->set_pointer(field_itr, before_field);

            //commit_free_list.push_back(after_field);
          }
          // Data
          else {
            LOG_INFO("Inlined ");
            field_type type = finfo.type;

            switch (type) {
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
        std::cerr << "Invalid operation type" << op_type << std::endl;
        break;
    }

    delete ptr;
  }

  // Clear log
  pm_log->clear();

  rec_t.end();
  std::cerr << "OPT_LSM :: Recovery duration (ms) : " << rec_t.duration()
            << std::endl;
  std::cout << "OPT_LSM :: Recovery duration (ms) : " << rec_t.duration() << std::endl;

}

}
