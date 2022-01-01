// YCSB BENCHMARK
/*
*/
#include "ycsb_benchmark.h"

namespace storage {

class usertable_record : public record {
	public:

	usertable_record(schema* _sptr, int key, const std::string& val,
                   int num_val_fields, bool update_one, int is_persistent = 0)
	: record(_sptr, is_persistent) {

		if(update_one) assert(0);
		set_int(0, key);
		if (val.empty())
			return;
		for (int itr = 1; itr <= num_val_fields; itr++)
        		set_varchar(itr, val);
	}
};

// USERTABLE
table* create_usertable(config& conf) {

  std::vector<field_info> cols;
  off_t offset;

  offset = 0;
  field_info key(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += key.ser_len;
  cols.push_back(key);

  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    field_info val = field_info(offset, 12, conf.ycsb_field_size,
                                field_type::VARCHAR, 0, 1);
    offset += val.ser_len;
    cols.push_back(val);
  }

  // SCHEMA
  schema* user_table_schema = new ((schema*) pmalloc(sizeof(schema))) schema(cols);
  pmemalloc_activate(user_table_schema);

  table* user_table = new ((table*) pmalloc(sizeof(table))) table("user", user_table_schema, 1, conf, sp);
  pmemalloc_activate(user_table);

  // PRIMARY INDEX
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    cols[itr].enabled = 0;
  }

  schema* user_table_index_schema = new ((schema*) pmalloc(sizeof(schema))) schema(cols);
  pmemalloc_activate(user_table_index_schema);

  table_index* key_index = new ((table_index*) pmalloc(sizeof(table_index))) table_index(user_table_index_schema,	\
                                           					conf.ycsb_num_val_fields + 1, conf,	\
                                           						sp);
  pmemalloc_activate(key_index);
  user_table->indices->push_back(key_index);

  return user_table;
}

ycsb_benchmark::ycsb_benchmark(config _conf, unsigned int tid, database* _db,
                               timer* _tm, struct static_info* _sp)
    : benchmark(tid, _db, _tm, _sp),
      conf(_conf),
      txn_id(0) {

  btype = benchmark_type::YCSB;

  // Partition workload
  num_keys = conf.num_keys / conf.num_executors;
  num_txns = conf.num_txns / conf.num_executors;
  std::cerr << "num_keys :: " << num_keys << std::endl;
  std::cerr << "num_txns :: " << num_txns << std::endl;
  std::cerr << "num_exec :: " << conf.num_executors << std::endl;

  // Initialization mode
  if (sp->init == 0) {
    std::cerr << "Initialization Mode" << std::endl << std::flush;
    sp->ptrs[0] = _db;

    table* usertable = create_usertable(conf);
    db->tables->push_back(usertable);

    sp->init = 1;
  } else {
    std::cout << "Recovery Mode " << std::endl << std::flush;
    database* db = (database*) sp->ptrs[0]; // We are reusing old tables
    db->reset(conf, tid);
  }

  user_table_schema = db->tables->at(USER_TABLE_ID)->sptr;

  if (conf.recovery) {
    num_txns = conf.num_txns;
    num_keys = conf.num_keys;
    conf.ycsb_per_update = 0.5;
    conf.ycsb_tuples_per_txn = 3;
  }

  if (conf.ycsb_update_one == false) {
    for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
      update_field_ids.push_back(itr);
  } else {
    // Update only first field
    update_field_ids.push_back(1);
  }

  // Generate skewed dist
  simple_skew(zipf_dist, conf.ycsb_skew, num_keys,
              num_txns * conf.ycsb_tuples_per_txn);
  uniform(uniform_dist, num_txns);

}

void ycsb_benchmark::load() {
  engine* ee = new engine(conf, tid, db, false);

  schema* usertable_schema = db->tables->at(USER_TABLE_ID)->sptr;
  unsigned int txn_itr;
  status ss(num_keys);
  ee->txn_begin();
  for (txn_itr = 0; txn_itr < num_keys; txn_itr++) {

    if (txn_itr % conf.load_batch_size == 0) {
      ee->txn_end(true);
      txn_id++;
      ee->txn_begin();
    }

    // LOAD
    int key = txn_itr;
    int is_persistent = 1;
    std::string value = get_rand_astring(conf.ycsb_field_size);

    record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(usertable_schema, key, value,
                                           					conf.ycsb_num_val_fields, false, is_persistent);

    statement st(txn_id, operation_type::Insert, USER_TABLE_ID, rec_ptr);

    ee->load(st);

    if (tid == 0)
      ss.display();
  }
  ee->txn_end(true);
  delete ee;
}

void ycsb_benchmark::do_update(engine* ee, unsigned int update_pos) {

// UPDATE
  std::string updated_val(conf.ycsb_field_size, 'x');
  int zipf_dist_offset = update_pos * conf.ycsb_tuples_per_txn;
  txn_id++;
  int rc;
  TIMER(ee->txn_begin())

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];
    // int key = txn_id % num_keys; 
   // if(txn_id % 10000 == 0)
	  //  std::cerr << "worker " << tid << " completed " << txn_id << " transactions" << std::endl << std::flush;
    int is_persistent = 1;
	// The assumption of homogeneous memory has allowed you to reuse code.
    record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, updated_val,
                          				conf.ycsb_num_val_fields,
                                 			conf.ycsb_update_one, is_persistent);
    /*record* rec_ptr = new usertable_record(user_table_schema, key, updated_val,
                          				conf.ycsb_num_val_fields,
                                 			conf.ycsb_update_one, is_persistent);*/

    statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr,
                 update_field_ids);
#ifdef LATENCY
    start_lat();
#endif
    TIMER(rc = ee->update(st))
#ifdef LATENCY
  if(rc == EXIT_FAILURE)
    end_lat(OP_FAIL);
  else
    end_lat(OP_UPDATE);
#endif    
    if (rc != 0) {
      TIMER(ee->txn_end(false))
      return;
    }
  }

  TIMER(ee->txn_end(true));
}

unsigned int ycsb_benchmark::do_insert(engine* ee){
  int insert_dist_offset = txn_id; 
  int rc;
  txn_id++;
  std::string insert_val(conf.ycsb_field_size, 'x');
  TIMER(ee->txn_begin())
  //ee->txn_begin();
  unsigned int key = zipf_dist[insert_dist_offset];
  key = txn_id + num_keys; // make sure insert key is never used
  record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, insert_val,
                                                                  conf.ycsb_num_val_fields, conf.ycsb_update_one, 1);
  statement st(txn_id, operation_type::Insert, USER_TABLE_ID, rec_ptr, update_field_ids);
#ifdef LATENCY
  start_lat();
#endif
  TIMER(rc = ee->insert(st))
  //rc = ee->insert(st);
#ifdef LATENCY
  if(rc == EXIT_FAILURE)
    end_lat(OP_FAIL);
  else
    end_lat(OP_INSERT);
#endif  
  TIMER(ee->txn_end(true));
  //ee->txn_end(true);
  return key;

}

void ycsb_benchmark::do_delete(engine* ee, unsigned int delete_pos){
  //first insert one than delete, make sure tuple exista
  //int delete_dist_offset = txn_id;
  int rc;
  txn_id++;
  int key = delete_pos;
  TIMER(ee->txn_begin())
  //int key = txn_id;
  //int key = delete_pos;
  //int key = zipf_dist[delete_dist_offset];
  //now delete
  std::string empty;
  record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, empty,
                                                                    conf.ycsb_num_val_fields, false, 1);
  statement st(txn_id, operation_type::Delete, USER_TABLE_ID, rec_ptr, 0, user_table_schema);
#ifdef LATENCY
  start_lat();
#endif
  TIMER(rc = ee->remove(st))
  TIMER(ee->txn_end(true))
#ifdef LATENCY
  if(rc == EXIT_FAILURE)
    end_lat(OP_FAIL);
  else
    end_lat(OP_DELETE);
#endif  
}

void ycsb_benchmark::do_read(engine* ee, unsigned int read_pos) {

// SELECT
  int zipf_dist_offset = read_pos * conf.ycsb_tuples_per_txn;
  txn_id++;
  std::string empty;
  std::string rc;

  TIMER(ee->txn_begin());

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(user_table_schema, key, empty,
                                           conf.ycsb_num_val_fields, false);

    statement st(txn_id, operation_type::Select, USER_TABLE_ID, rec_ptr, 0,
                 user_table_schema);
#ifdef LATENCY
    start_lat();
#endif
    TIMER(ee->select(st));
#ifdef LATENCY
    end_lat(OP_SELECT);
    
#endif
  }
  TIMER(ee->txn_end(true));
}

void ycsb_benchmark::sim_crash() {
  engine* ee = new engine(conf, tid, db, conf.read_only);
  unsigned int txn_itr;

  // UPDATE
  std::vector<int> field_ids;
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
    field_ids.push_back(itr);

  std::string updated_val(conf.ycsb_field_size, 'x');
  int zipf_dist_offset = 0;

  // No recovery needed
  if (conf.etype == engine_type::SP || conf.etype == engine_type::OPT_SP) {
    ee->recovery();
    return;
  }

  // Always in sync
  //if (conf.etype == engine_type::OPT_WAL || conf.etype == engine_type::OPT_LSM)
    //num_txns = 1;

  ee->txn_begin();
  size_t rec_txn = num_txns / 10; //

  //here add more op may be better >_<
  for (txn_itr = 0; txn_itr < rec_txn; txn_itr++) {
    //if(txn_itr)
    txn_id++;
    for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {
      if(stmt_itr == 0){
          int key = zipf_dist[zipf_dist_offset + stmt_itr];

          record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key,
                                                    updated_val,
                                                    conf.ycsb_num_val_fields,
                                                    conf.ycsb_update_one, 1);

          statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr,
                        field_ids);

          ee->update(st);
      }
      else if(stmt_itr == 1){
        int key = num_keys + txn_itr;
        std::string insert_val(conf.ycsb_field_size, 'x');
        record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, insert_val,
                                                                                                conf.ycsb_num_val_fields, conf.ycsb_update_one, 1);
        statement st(txn_id, operation_type::Insert, USER_TABLE_ID, rec_ptr);
        ee->insert(st);

        /* automate instrument memory overflow code, fuck segmentation fault >_>
        if(st.transaction_id >= 20001){
            record* test;
            std::sscanf("0x7fcfab0c1578", "%p", &test);
            int len = test->data_len;
            if(len == 0)
            {
              abort();
            }
            char* this_data = test->data;
            char shit = this_data[1];
        }*/
      }
      else if (stmt_itr == 2){
        int key = num_keys + txn_itr;
        std::string empty;
        record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, empty,
                                                                                  conf.ycsb_num_val_fields, false);
        statement st(txn_id, operation_type::Delete, USER_TABLE_ID, rec_ptr);
        ee->remove(st);
      }

    }

  }
  // Recover(undo txn)
  ee->recovery();
  delete ee;
}

void ycsb_benchmark::do_id(engine* ee, unsigned int id_pos){
  unsigned int key = do_insert(ee);
  do_delete(ee, key);
}


void ycsb_benchmark::execute() {
#ifdef FUNCT
  func_init();
#endif
  engine* ee = new engine(conf, tid, db, conf.read_only);
  unsigned int txn_itr;
  status ss(num_txns);
  //std::cerr << "num_txns :: " << num_txns << std::endl;
  //int start_hold = 0;
  //int end_hold = 5000;
  //int se_count=0;
  //int ins_count=0;
  //int up_count=0;
  //int de_count=0;
 /* if(conf.ycsb_per_delete!=0 && num_txns > num_keys){
    std::cerr<< " txn out of range! " << std::endl;
    return;
  }
  num_txns = num_txns * conf.ycsb_per_delete;*/
  for (txn_itr = 0; txn_itr < num_txns; txn_itr++) {
    double u = uniform_dist[txn_itr];
    if (u < conf.ycsb_per_update) {
      do_update(ee, txn_itr);
      //up_count++;
    }
    else if(u>conf.ycsb_per_update && u<conf.ycsb_per_update + conf.ycsb_per_insert){
      /*if(txn_itr >= start_hold && txn_itr <= start_hold + 5000)
        band_start();*/
      do_insert(ee);
      /*if(txn_itr >= start_hold && txn_itr <= start_hold + 5000)
        band_end();*/
      //ins_count++;
    }
    else if(u>conf.ycsb_per_update + conf.ycsb_per_insert && u<conf.ycsb_per_update + conf.ycsb_per_insert + conf.ycsb_per_delete){
      /*if(txn_itr>=start_hold && txn_itr<=end_hold){
        band_start();
      }*/
      do_delete(ee, txn_itr);
      /*if(txn_itr >= start_hold && txn_itr <= end_hold){
        band_end();
      }*/   
    }
    else {
      //do_delete(ee);
      do_read(ee, txn_itr);
      //se_count++;
    }
    /*if(txn_itr == start_hold + 5000)
    {
      band_output();
      start_hold += 100000;
      //end_hold += 1000000;
      band_clear();
    }*/
    if (tid == 0)
    {

      ss.display();
      /*if(txn_itr % 5000 == 0 && txn_itr != 0 ){
        std::cout<<"finished 50000 and "<< num_txns - txn_itr <<"to go"<<std::endl;
        /*
        std::cout<<"ins "<<ins_count<<" del "<<de_count<<" up "<<up_count<<" se "<<se_count<<std::endl;
        se_count=0;
        ins_count=0;
        up_count=0;
        de_count=0; 
      }*/
    }
  }

  //std::cerr << "duration :: " << tm->duration() << std::endl;
#ifdef FUNCT
  persist_output();
#endif  
  delete ee;
}

}
