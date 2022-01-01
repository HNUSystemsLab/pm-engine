#pragma once

#include <cstdio>
#include <cstring>
#include <vector>

#include "benchmark.h"
#include "field.h"
#include "record.h"
#include "statement.h"
#include "utils2.h"
#include "config.h"
#include "status.h"
#include "libpm.h"
#include "plist.h"
#include "serializer.h"

namespace storage {

class ycsb_benchmark : public benchmark {
 public:
  ycsb_benchmark(config _conf, unsigned int tid, database* _db, timer* _tm, struct static_info* _sp);

  void load();
  void execute();

  void sim_crash();

  void do_update(engine* ee, unsigned int update_pos);
  void do_read(engine* ee, unsigned int read_pos);
  unsigned int do_insert(engine* ee);
  void do_delete(engine* ee, unsigned int delete_pos);
  void do_id(engine* ee, unsigned int id_pos);
  // Table Ids
  static constexpr int USER_TABLE_ID = 0;

  // Schema
  schema* user_table_schema;

  std::vector<int> zipf_dist;
  std::vector<double> uniform_dist;
  //std::vector<int> key_list;
  config conf;
  std::vector<int> update_field_ids;
  serializer sr;

  benchmark_type btype;

  unsigned int txn_id;
  unsigned int num_keys;
  unsigned int num_txns;
};

}
