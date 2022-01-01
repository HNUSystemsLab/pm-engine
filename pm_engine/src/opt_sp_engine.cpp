// OPT SP - based on PM

#include "opt_sp_engine.h"

namespace storage {
void* group_commit2(void* args){
#ifdef FUNCT
  func_init();
#endif
  opt_sp_engine* this_engine = (opt_sp_engine*) args;
  while(this_engine->ready){
    
    if (!this_engine->read_only && this_engine->txn_ptr != NULL){
      wrlock(&this_engine->gc_rwlock);
      
      assert(this_engine->bt->txn_commit(this_engine->txn_ptr) == BT_SUCCESS);
      this_engine->txn_ptr = this_engine->bt->txn_begin(0);
      assert(this_engine->txn_ptr);
      
      unlock(&this_engine->gc_rwlock);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(this_engine->conf.gc_interval));
    //this_engine->thread_id = std::this_thread::get_id();
  }
  return NULL;
}
void opt_sp_engine::group_commit() {

  while (ready) {

    if (!read_only && txn_ptr != NULL) {
      wrlock(&gc_rwlock);

      assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
      txn_ptr = bt->txn_begin(0);
      assert(txn_ptr);

      unlock(&gc_rwlock);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

opt_sp_engine::opt_sp_engine(const config& _conf, database* _db, bool _read_only,
                             unsigned int _tid)
    : conf(_conf),
      db(_db),
      bt(NULL),
      txn_ptr(NULL),
      tid(_tid) {

  etype = engine_type::OPT_SP;
  read_only = _read_only;

  bt = db->dirs->t_ptr;
  txn_ptr = bt->txn_begin(read_only);
  assert(txn_ptr);

  // Commit only if needed
  if (!read_only) {
#ifdef ALLOCATOR
    ready = true;
    pmemalloc_creat_single(group_commit2, (void*)this);
#else
    gc = std::thread(&opt_sp_engine::group_commit, this);
#endif
  }
}

opt_sp_engine::~opt_sp_engine() {

  if (!read_only) {
    ready = false;
#ifdef ALLOCATOR
    pmemalloc_join_single();
#else    
    gc.join();
#endif
  }

  if (!read_only && txn_ptr != NULL) {
    assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
  }
  txn_ptr = NULL;
 //pmemalloc_recover();
}

std::string opt_sp_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  record* select_ptr;
  struct cow_btval key, val;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = sr.serialize(rec_ptr, table_index->sptr);

  unsigned long key_id = hasher(hash_fn(key_str), st.table_id,
                                st.table_index_id);
  std::string comp_key_str = std::to_string(key_id);
  key.data = (void*) comp_key_str.c_str();
  key.size = comp_key_str.size();
  std::string value;
  // Read from latest clean version
#ifdef FUNC
  start_lat();
#endif
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    memcpy(&select_ptr, val.data, sizeof(record*));
    value = sr.serialize(select_ptr, st.projection);
  }

  LOG_INFO("val : %s", value.c_str());
  delete rec_ptr;
  //pfree(rec_ptr);
#ifdef FUNC
  end_lat(OP_IND);
#endif
  return value;
}

int opt_sp_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  key.data = (void*) key_str.c_str();
  key.size = key_str.size();
#ifdef FUNC
  start_lat();
#endif
  // Check if key exists in current version
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    after_rec->clear_data();
    delete after_rec;
    //pfree(after_rec);
    return EXIT_FAILURE;
  }
#ifdef FUNC
  end_lat(OP_IND);
  start_lat();
#endif
  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  //val.data = new char[sizeof(record*) + 1];
#ifdef FUNC
  end_lat(OP_MOD);
  start_lat();
#endif
  val.data = (char*) pmalloc(sizeof(record*) + 1);
#ifdef FUNC
  end_lat(OP_ALC);
  start_lat();
#endif
  memcpy(val.data, &after_rec, sizeof(record*));
  val.size = sizeof(record*);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->insert(txn_ptr, &key, &val);
  }
#ifdef FUNC
  end_lat(OP_MOD);
  start_lat();
#endif
  delete ((char*) val.data);
#ifdef FUNC
  end_lat(OP_ALC);
  start_lat();
#endif
  //pfree(val.data);
#ifdef FUNC
  end_lat(OP_MOD);
#endif
  return EXIT_SUCCESS;
}

int opt_sp_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();
#ifdef FUNC
  start_lat();
#endif

  // Check if key does not exist
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    //pfree(rec_ptr);
    delete(rec_ptr);
    return EXIT_FAILURE;
  }
#ifdef FUNC
  end_lat(OP_IND);
  start_lat();
#endif
  // Free record
  record* before_rec;
  memcpy(&before_rec, val.data, sizeof(record*));

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(rec_ptr, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->remove(txn_ptr, &key, NULL);
  }
  //pfree(rec_ptr);
#ifdef FUNC
  end_lat(OP_MOD);
  start_lat();
#endif
  delete rec_ptr;
  before_rec->clear_data();
  //delete before_rec;
#ifdef FUNC
  end_lat(OP_ALC);
#endif
  return EXIT_SUCCESS;
}

int opt_sp_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val, update_val;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key does not exist in current version
#ifdef FUNC
  start_lat();
#endif
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    rec_ptr->clear_data();
    delete rec_ptr;
    //pfree(rec_ptr);
    return EXIT_FAILURE;
  }
#ifdef FUNC
  end_lat(OP_IND);
  start_lat();
#endif
  // Read from current version
  record* before_rec;
  memcpy(&before_rec, val.data, sizeof(record*));

  char *before_field, *after_field;
  assert(before_rec->data != NULL);
#ifdef FUNC
  end_lat(OP_MOD);
  start_lat();
#endif
  record* after_rec = new ((record*)pmalloc(sizeof(record))) record(before_rec->sptr);
#ifdef FUNC
  end_lat(OP_ALC);
  start_lat();
#endif  
  memcpy(after_rec->data, before_rec->data, before_rec->data_len);

  // Update record
  for (int field_itr : st.field_ids) {
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      before_field = (char*) before_rec->get_pointer(field_itr);
      after_field = (char*) rec_ptr->get_pointer(field_itr);
      pmemalloc_activate(after_field);
#ifdef FUNC
  end_lat(OP_MOD);
  start_lat();
#endif
      pfree(before_field);
#ifdef FUNC
  end_lat(OP_ALC);
  start_lat();
#endif
    }

    after_rec->set_data(field_itr, rec_ptr);
  }

  // Activate new record
  pmemalloc_activate(after_rec);
  //update_val.data = new char[sizeof(record*) + 1];
  update_val.data =(char*) pmalloc(sizeof(record*) + 1);
  memcpy(update_val.data, &after_rec, sizeof(record*));
  update_val.size = sizeof(record*);

  // Update entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->remove(txn_ptr, &key, NULL);
    bt->insert(txn_ptr, &key, &update_val);
  }
#ifdef FUNC
  end_lat(OP_MOD);
  start_lat();
#endif
  delete rec_ptr;
  delete before_rec;
  pfree(update_val.data);
#ifdef FUNC
  end_lat(OP_ALC);
#endif
  //delete rec_ptr;
  //delete before_rec;
  //delete ((char*) update_val.data);

  return EXIT_SUCCESS;
}

void opt_sp_engine::load(const statement& st) {
  //LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();


  //val.data = new char[sizeof(record*) + 1];
  val.data = (char*) pmalloc(sizeof(record*) + 1);
  memcpy(val.data, &after_rec, sizeof(record*));
  val.size = sizeof(record*);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->insert(txn_ptr, &key, &val);
  }

  //delete ((char*) val.data);
  pfree(val.data);
}

void opt_sp_engine::txn_begin() {
#ifdef FUNC
  start_lat();
#endif
  if (!read_only) {
    wrlock(&gc_rwlock);
  }
#ifdef FUNC
  end_lat(OP_LOG);
#endif
}

void opt_sp_engine::txn_end(__attribute__((unused)) bool commit) {
#ifdef FUNC
  start_lat();
#endif
  if (!read_only) {
    unlock(&gc_rwlock);
  }
#ifdef FUNC
  end_lat(OP_LOG);
#endif
}

void opt_sp_engine::recovery() {

  std::cerr << "OPT_SP :: Recovery duration (ms) : " << 0.0 << std::endl;

}

}

