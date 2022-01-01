#pragma once

#include <string>
#include <map>
#include <thread>
#include <vector>
#include "wal_engine.h"
#include "sp_engine.h"
#include "lsm_engine.h"
#include "opt_wal_engine.h"
#include "opt_sp_engine.h"
#include "opt_lsm_engine.h"
#include "nof_lsm_engine.h"
#include "timer.h"

namespace storage {

class engine {
 public:
  engine()
      : etype(engine_type::WAL),       
        de(NULL) {
  }

  engine(const config& conf, unsigned int tid, database* db, bool read_only)
      : etype(conf.etype) {

    switch (conf.etype) {
      case engine_type::WAL:
        de = new wal_engine(conf, db, read_only, tid);
        break;
      case engine_type::SP:
  //die();
        de = new sp_engine(conf, db, read_only, tid);
        break;
      case engine_type::LSM:
  //die();
        de = new lsm_engine(conf, db, read_only, tid);
        break;
      case engine_type::OPT_WAL:
        de = new opt_wal_engine(conf, db, read_only, tid);
        break;
      case engine_type::OPT_SP:
  //die();
        de = new opt_sp_engine(conf, db, read_only, tid);
        break;
      case engine_type::OPT_LSM:
  //die();
        de = new opt_lsm_engine(conf, db, read_only, tid);
        break;
      case engine_type::NOF_LSM:
        de = new nof_lsm_engine(conf, db, read_only, tid);
        break;
      default:
        std::cerr << "Unknown engine type :: " << etype << std::endl;
        exit(EXIT_FAILURE);
        break;
    }

  }

  virtual ~engine() {
    delete de;
  }

  virtual std::string select(const statement& st) {
    std::string result = de->select(st);
    return result;
  }

  virtual int insert(const statement& st) {
    int result = de->insert(st);   
    return result;
  }

  virtual int remove(const statement& st) {
    int result = de->remove(st);
    return result;
  }

  virtual int update(const statement& st) {
    int result = de->update(st);
    return result;
  }

  virtual void display() {
    std::cerr << "ST" << std::endl;
  }

  void load(const statement& st) {
    de->load(st);
  }

  virtual void txn_begin() {
    de->txn_begin();
  }

  virtual void txn_end(bool commit) {
    de->txn_end(commit);
  }



  void recovery() {
    de->recovery();
  }

  engine_type etype;
  engine_api* de;

};

}

