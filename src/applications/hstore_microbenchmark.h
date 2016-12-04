//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_HSTORE_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_HSTORE_MICROBENCHMARK_H_

#include <set>
#include <vector>
#include "applications/application.h"

using std::set;
using std::vector;
using std::string;

class Hstore_Microbenchmark : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    MICROTXN_SP = 1,
    MICROTXN_MP = 2,
    MICROTXN_SUPER_LONG = 3,
  };

  Hstore_Microbenchmark(int hot_records, int percent_mp, Storage* storage) {
    percent_mp_ = percent_mp;
    storage_ = storage;

    if (hot_records % WORKER_THREADS == 0) {
      for (uint32_t i = 0; i < WORKER_THREADS;i++) {
        hot_per_worker[i] = hot_records / WORKER_THREADS;
      }
    } else {
      uint32_t left = hot_records - (hot_records / WORKER_THREADS)*WORKER_THREADS;
      for (uint32_t i = 0; i < WORKER_THREADS; i++) {
        if (i < left) {
          hot_per_worker[i] = hot_records / WORKER_THREADS + 1;
        } else {
          hot_per_worker[i] = hot_records / WORKER_THREADS;
        }
      }
    }

  }

  virtual ~Hstore_Microbenchmark() {}

  virtual void NewTxn(Txn* txn, uint64_t txn_id) const;
  virtual int Execute(Txn* txn) const; 
  virtual uint32_t LookupPartition(const uint64_t& key) const;

  void MicroTxnMP(Txn* txn, uint64_t txn_id, int mp) const;
  void MicroTxnSP(Txn* txn, uint64_t txn_id) const;
  void MicroTxnRandom(Txn* txn, uint64_t txn_id) const;
  virtual uint32_t GetTableNum() const;

  int percent_mp_;
  int hot_per_worker[WORKER_THREADS];
  Storage* storage_;

  static const int kRWSetSize = 10;  // MUST BE EVEN
  static const uint64_t kDBSize = 10000000;
  static const int kValueSize = 1000;


  virtual void InitializeStorage() const;
  virtual void InitializeTable(uint32_t table_id) const;

  virtual int Execute2(LockUnit* lock_unit) const;
  virtual int Rollback(LockUnit* lock_unit) const;

 private:
  void GetRandomKeys(vector<uint64_t>* keys, int num_keys, uint64_t key_start, uint64_t key_limit) const;
  Hstore_Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_HSTORE_MICROBENCHMARK_H_
