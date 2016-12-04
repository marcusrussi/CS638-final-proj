//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_PHYSICAL_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_PHYSICAL_MICROBENCHMARK_H_

#include <set>
#include <vector>
#include "applications/application.h"

using std::set;
using std::vector;
using std::string;

class Physical_Microbenchmark : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    MICROTXN_SP = 1,
    MICROTXN_MP = 2,
    MICROTXN_SUPER_LONG = 3,
  };

  Physical_Microbenchmark(int lm_count, int hotcount, int percent_mp, Storage* storage) {
    lm_count_ = lm_count;
    hot_records = hotcount;
    if (hotcount % WORKER_THREADS == 0) {
      for (uint32_t i = 0; i < WORKER_THREADS;i++) {
        hot_records_per_worker[i] = hotcount / WORKER_THREADS;
      }
    } else {
      uint32_t left = hotcount - (hotcount / WORKER_THREADS)*WORKER_THREADS;
      for (uint32_t i = 0; i < WORKER_THREADS; i++) {
        if (i < left) {
          hot_records_per_worker[i] = hotcount / WORKER_THREADS + 1;
        } else {
          hot_records_per_worker[i] = hotcount / WORKER_THREADS;
        }
      }
    }
    percent_mp_ = percent_mp;
    storage_ = storage;
  }

  virtual ~Physical_Microbenchmark() {}

  virtual void NewTxn(Txn* txn, uint64_t txn_id) const;
  virtual int Execute(Txn* txn) const; 
  virtual uint32_t LookupPartition(const uint64_t& key) const;

  void MicroTxnSP(Txn* txn, uint64_t txn_id) const;
  void MicroTxnMP(Txn* txn, uint64_t txn_id, int mp) const;
  void MicroTxnRandom(Txn* txn, uint64_t txn_id) const;
  virtual uint32_t GetTableNum() const;

  int percent_mp_;
  Storage* storage_;
  int lm_count_;
  int hot_records;
  int hot_records_per_worker[WORKER_THREADS];

  static const int kRWSetSize = 10;  // MUST BE EVEN
  static const uint64_t kDBSize = 10000000;
  static const int kValueSize = 1000;


  virtual void InitializeStorage() const;
  virtual void InitializeTable(uint32_t table_id) const;

  virtual int Execute2(LockUnit* lock_unit) const;
  virtual int Rollback(LockUnit* lock_unit) const;

 private:
  void GetRandomKeys(vector<uint64_t>* keys, int num_keys, uint64_t key_start, uint64_t key_limit) const;
  Physical_Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_PHYSICAL_MICROBENCHMARK_H_
