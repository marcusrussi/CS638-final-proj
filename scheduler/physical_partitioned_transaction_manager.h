// Transaction manager for physical partitioned Orthrus
//


#ifndef _DB_SCHEDULER_PHYSICAL_PARTITIONED_MANAGER_H_
#define _DB_SCHEDULER_PHYSICAL_PARTITIONED_MANAGER_H_

#include "common/types.h"
#include "applications/application.h"
#include "common/utils.h"

class Txn;
class SubTxn;

class PhysicalPartitioned_TransactionManager {
 public:

  PhysicalPartitioned_TransactionManager();

  ~PhysicalPartitioned_TransactionManager();
  
  void Setup(Txn* txn, const Application* application);
  SubTxn* NextLmRequest();
  SubTxn* GetFirstLMRequest();
  void SortKeys();
  //static uint32_t GetLMId(uint64_t);

  // Transaction that corresponds to this instance of a StorageManager.
  Txn* txn_;
  const Application* application_;
//  SubTxn* sub_txns;
  
  SubTxn sub_txns[MAX_SUB_TXNS];
  uint32_t sub_txns_cnt;
  uint32_t current_index;
  bool involved_lms[LOCK_MANAGER_THREADS];

};

#endif  // _DB_SCHEDULER_TRANSACTION_MANAGER_H_


