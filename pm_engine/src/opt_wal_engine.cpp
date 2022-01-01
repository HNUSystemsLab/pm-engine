// OPT WRITE-AHEAD LOGGING

#include "opt_wal_engine.h"

namespace storage {

opt_wal_engine::opt_wal_engine(const config& _conf, database* _db,
bool _read_only,
                               unsigned int _tid)
    : conf(_conf),
      db(_db),
      tid(_tid) {

  etype = engine_type::OPT_WAL;
  read_only = _read_only;
  pm_log = db->log;

}

opt_wal_engine::~opt_wal_engine() {
  //pmemalloc_recover();
}

std::string opt_wal_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  record* select_ptr = NULL;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = sr.serialize(rec_ptr, table_index->sptr);

  unsigned long key = hash_fn(key_str);
  std::string val;
#ifdef FUNC
  start_lat();
#endif
  table_index->pm_map->at(key, &select_ptr);
  if (select_ptr)
    val = sr.serialize(select_ptr, st.projection);
  LOG_INFO("val : %s", val.c_str());

  delete rec_ptr;
#ifdef FUNC
  end_lat(OP_IND);
#endif
  return val;
}

int opt_wal_engine::insert(const statement& st) {
  //LOG_INFO("Insert");
//#ifdef FUNC
//#endif 
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key exists
#ifdef FUNC
  start_lat();
#endif
  if (indices->at(0)->pm_map->exists(key) != 0) {
    after_rec->clear_data();
    delete after_rec;
    return EXIT_FAILURE;
  }
#ifdef FUNC
  end_lat(OP_IND);
#endif
  // Add log entry
#ifdef FUNC
  start_lat();
#endif
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << after_rec;

  entry_str = entry_stream.str();

  size_t entry_str_sz = entry_str.size() + 1;
#ifdef FUNC
  end_lat(OP_LOG);
  start_lat();
#endif
  char* entry = (char*) pmalloc(entry_str_sz*sizeof(char));//new char[entry_str_sz];
#ifdef FUNC
  end_lat(OP_ALC);
  start_lat();
#endif

  PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));
  pmemalloc_activate(entry);
  pm_log->push_back(entry);
#ifdef FUNC
  end_lat(OP_LOG);
  start_lat();
#endif
  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();
#ifndef PRECISE
  tab->pm_data->push_back(after_rec);
#endif
  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);
    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }
#ifdef FUNC
  end_lat(OP_MOD);
#endif
  return EXIT_SUCCESS;
}

int opt_wal_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  record* before_rec = NULL;
#ifdef FUNC
  start_lat();
#endif
  // Check if key does not exist
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
    delete rec_ptr;
    return EXIT_FAILURE;
  }
#ifdef FUNC
  end_lat(OP_IND);
#endif

  int num_cols = before_rec->sptr->num_columns;

  for (int field_itr = 0; field_itr < num_cols; field_itr++) {
    if (before_rec->sptr->columns[field_itr].inlined == 0) {
      void* before_field = before_rec->get_pointer(field_itr);
      commit_free_list.push_back(before_field);
    }
  }
  commit_free_list.push_back(before_rec);
#ifdef FUNC
  start_lat();
#endif
  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << before_rec;

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
#ifdef FUNC
  start_lat();
#endif

  char* entry = (char*) pmalloc(entry_str_sz*sizeof(char));//new char[entry_str_sz];
#ifdef FUNC
  end_lat(OP_ALC);
#endif 
  PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));

  pmemalloc_activate(entry);
  pm_log->push_back(entry);
#ifdef FUNC
  end_lat(OP_LOG);
#endif
/*#ifndef PRECISE

  tab->pm_data->erase(before_rec);
#endif*/
  // Remove entry in indices
#ifdef FUNC
  start_lat();
#endif
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->erase(key);
  }
#ifdef FUNC
  end_lat(OP_MOD);
#endif
#ifdef FUNC
  start_lat();
#endif
  delete rec_ptr;
#ifdef FUNC
  end_lat(OP_ALC);
#endif
  return EXIT_SUCCESS;
}

int opt_wal_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  record* before_rec;
#ifdef FUNC
  start_lat();
#endif
  // Check if key exists. If not, return. There is nothing to update.
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
    rec_ptr->clear_data(); // This here gives really small Txns !
    delete rec_ptr;
    return EXIT_FAILURE;
  }
#ifdef FUNC
  end_lat(OP_IND);
  start_lat();
#endif
  void *before_field;
  int num_fields = st.field_ids.size();

  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << num_fields << " " << before_rec << " ";

  for (int field_itr : st.field_ids) {
    // Pointer field
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      before_field = before_rec->get_pointer(field_itr);

      entry_stream << field_itr << " " << before_field << " ";
    }
    // Data field
    else {
      std::string before_data = before_rec->get_data(field_itr);

      entry_stream << field_itr << " " << " " << before_data << " ";
    }
  }

  // Add log entry
  entry_str = entry_stream.str();

  size_t entry_str_sz = entry_str.size() + 1;
#ifdef FUNC
  end_lat(OP_LOG);
  start_lat();
#endif
  char* entry = (char*) pmalloc(entry_str_sz*sizeof(char));//new char[entry_str_sz];
#ifdef FUNC
  end_lat(OP_ALC);
  start_lat();
#endif  
  PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));

  pmemalloc_activate(entry);
  pm_log->push_back(entry);
#ifdef FUNC
  end_lat(OP_LOG);
  start_lat();
#endif
  for (int field_itr : st.field_ids) {
    // Garbage collect previous field
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      before_field = before_rec->get_pointer(field_itr);
      commit_free_list.push_back(before_field);
    }

    // Update existing record
    before_rec->set_data(field_itr, rec_ptr);
  }
  before_rec->persist_data();
#ifdef FUNC
  end_lat(OP_MOD);
  start_lat();
#endif
  delete rec_ptr;
#ifdef FUNC
  end_lat(OP_ALC);
#endif
  return EXIT_SUCCESS;
}

void opt_wal_engine::load(const statement& st) {
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
               << " " << after_rec;

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  char* entry = (char*) pmalloc(entry_str_sz*sizeof(char));//new char[entry_str_sz];
  PM_MEMCPY((entry), (entry_str.c_str()), (entry_str_sz));
  pmemalloc_activate(entry);
  pm_log->push_back(entry);
#ifndef PRECISE
  tab->pm_data->push_back(after_rec);
#endif
  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);
    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }

}

void opt_wal_engine::txn_begin() {
	PM_START_TX();
}

void opt_wal_engine::txn_end(__attribute__((unused)) bool commit) {
#ifdef FUNC
  start_lat();
#endif
  // Clear commit_free list
  for (void* ptr : commit_free_list) {
    delete (char*) ptr;
  }
  commit_free_list.clear(); // STL Vector, not plist

  // Clear log
  std::vector<char*> undo_log = pm_log->get_data();
  for (char* ptr : undo_log)
    delete ptr;
  pm_log->clear(); // This gives non-volatile accesses
  PM_FENCE();
  __builtin_ia32_sfence();
  PM_END_TX();
#ifdef FUNC
  end_lat(OP_ALC);
#endif
}

void opt_wal_engine::recovery() {

  LOG_INFO("OPT WAL recovery");

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

  for (char* ptr : undo_log) {
    //std::cerr << "entry : --" << ptr << "-- " << std::endl;
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
/*#ifndef PRECISE
        tab->pm_data->erase(after_rec);
#endif*/
        // Remove entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = sr.serialize(after_rec,
                                             indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->pm_map->erase(key);
        }

        // Free after_rec
        //after_rec->clear_data();
        //delete after_rec;
        break;

      case operation_type::Delete:
        LOG_INFO("Undo Delete");
        entry >> ptr_str;
        std::sscanf(ptr_str.c_str(), "%p", &before_rec);

        tab = db->tables->at(table_id);
        indices = tab->indices;
        num_indices = tab->num_indices;
/*#ifndef PRECISE
        tab->pm_data->push_back(after_rec);
#endif*/
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
            void *before_field, *after_field;

            entry >> ptr_str;
            std::sscanf(ptr_str.c_str(), "%p", &before_field);

            after_field = before_rec->get_pointer(field_itr);
            before_rec->set_pointer(field_itr, before_field);

            // Free after_field
            delete ((char*) after_field);
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
                before_rec->set_int(field_itr, dval);
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
  std::cerr << "OPT_WAL :: Recovery duration (ms) : " << rec_t.duration() << std::endl;
  std::cout << "OPT_WAL :: Recovery duration (ms) : " << rec_t.duration() <<std::endl;

}

}
