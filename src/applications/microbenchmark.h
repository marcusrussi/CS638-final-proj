// Microbenchmark/YCSB for Orthrus
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_MICROBENCHMARK_H_

#include <set>

#include "applications/application.h"

using std::set;
using std::string;

class Microbenchmark : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    MICROTXN_SP = 1,
    MICROTXN_MP = 2,
    MICROTXN_SUPER_LONG = 3,
  };

  Microbenchmark(int lm_count, int lm_threads_per_partition, int hotcount, int percent_mp, Storage* storage) {
    lm_count_ = lm_count;
    lm_threads_per_partition_ = lm_threads_per_partition;
    hot_records = hotcount;
    if (hotcount % lm_count == 0) {
      hot_records_per_lm = hotcount / lm_count;
    } else {
      hot_records_per_lm = hotcount / lm_count + 1;
      hot_records = hot_records_per_lm*lm_count;
    }

    percent_mp_ = percent_mp;
    storage_ = storage;
  }

  virtual ~Microbenchmark() {}

  virtual void NewTxn(Txn* txn, uint64_t txn_id) const;
  virtual int Execute(Txn* txn) const;
  virtual int Execute2(LockUnit* lock_unit) const;
  virtual int Rollback(LockUnit* lock_unit) const;

  void MicroTxnSP(Txn* txn, uint64_t txn_id, int part) const;
  void MicroTxnMltiple(Txn* txn, uint64_t txn_id, int mp) const;
  virtual uint32_t LookupPartition(const uint64_t& key) const;
  void MicroTxnRandom(Txn* txn, uint64_t txn_id, int part) const;
  virtual uint32_t GetTableNum() const;

  int lm_count_;
  int hot_records;
  int hot_records_per_lm;
  int percent_mp_;
  Storage* storage_;

  static const int kRWSetSize = 10;  // MUST BE EVEN
  static const uint64_t kDBSize = 10000000;  // Database size
  static const int kValueSize = 1000;   // Value size


  virtual void InitializeStorage() const;
  virtual void InitializeTable(uint32_t table_id) const;

 private:
  void GetRandomKeys(set<uint64_t>* keys, int num_keys, uint64_t key_start,
                     uint64_t key_limit, int part) const;
  Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_MICROBENCHMARK_H_
