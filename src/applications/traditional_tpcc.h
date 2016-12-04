
#ifndef _DB_APPLICATIONS_TRADITIONAL_TPCC_H_
#define _DB_APPLICATIONS_TRADITIONAL_TPCC_H_

#include <set>
#include "applications/application.h"
#include "applications/tpcc_utils.h"

using std::set;
using std::string;


//--------------------------------------------

class Traditional_TPCC : public Application {
  public:
   enum TxnType {
     NEW_ORDER = 0,
     PAYMENT = 1,
  };

  Traditional_TPCC(Storage* storage) {
    storage_ = storage;
  }

  virtual ~Traditional_TPCC() {}

  virtual void NewTxn(Txn* txn, uint64_t txn_id) const;
  virtual int Execute(Txn* txn) const;
  virtual int Execute2(LockUnit* lock_unit) const;
  virtual int Rollback(LockUnit* lock_unit) const;

  void NewOrderTxn(Txn* txn, uint64_t txn_id) const;
  void PaymentTxn(Txn* txn, uint64_t txn_id) const;
  int ExecuteNewOrderTxn(Txn* txn) const;
  int ExecutePaymentTxn(Txn* txn) const;
  virtual uint32_t LookupPartition(const uint64_t& key) const;
  virtual uint32_t GetTableNum() const;

  virtual void InitializeStorage() const;

  Storage* storage_;

  // Experiment parameters
  static const uint32_t s_num_tables = 9;
  static const uint32_t s_num_warehouses = WAREHOUSE_CNT;
  static const uint32_t s_num_items = NUMBER_ITEMS;  
  static const uint32_t s_districts_per_wh = DISTRICT_PER_WH;
  static const uint32_t s_customers_per_dist = CUSTOMER_PER_DIST;

  TPCCUtil random;

  virtual void InitializeTable(uint32_t table_id) const;

 private:
  Traditional_TPCC() {}

};

#endif  // _DB_APPLICATIONS_TRADITIONAL_TPCC_H_
