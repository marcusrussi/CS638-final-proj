//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_TRADITIONAL_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_TRADITIONAL_MICROBENCHMARK_H_

#include <set>
#include <vector>
#include "applications/application.h"

using std::set;
using std::vector;
using std::string;

class Traditional_Microbenchmark : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    MICROTXN_SP = 1,
    MICROTXN_MP = 2,
    MICROTXN_SUPER_LONG = 3,
  };

  Traditional_Microbenchmark(int hotcount, Storage* storage) {
    hot_records = hotcount;
    storage_ = storage;
  }

  virtual ~Traditional_Microbenchmark() {}

  virtual void NewTxn(Txn* txn, uint64_t txn_id) const;
  virtual int Execute(Txn* txn) const; 
  virtual uint32_t LookupPartition(const uint64_t& key) const;

  void MicroTxnSP(Txn* txn, uint64_t txn_id) const;
  virtual uint32_t GetTableNum() const;

  virtual int Execute2(LockUnit* lock_unit) const;
  virtual int Rollback(LockUnit* lock_unit) const;

  int hot_records;
  Storage* storage_;

  static const int kRWSetSize = 10;  // MUST BE EVEN
  static const uint64_t kDBSize = 10000000;
  static const int kValueSize = 1000;


  virtual void InitializeStorage() const;
  virtual void InitializeTable(uint32_t table_id) const;
 private:
  void GetRandomKeys(vector<uint64_t>* keys, int num_keys, uint64_t key_start, uint64_t key_limit) const;
  Traditional_Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_TRADITIONAL_MICROBENCHMARK_H_
