// Transaction manager for traditional approach
//


#ifndef _DB_SCHEDULER_TRADITIONAL_MANAGER_H_
#define _DB_SCHEDULER_TRADITIONAL_MANAGER_H_

#include "common/types.h"
#include "scheduler/traditional_lock_manager.h"

class Txn;

class Traditional_TransactionManager {
 public:

  Traditional_TransactionManager();

  ~Traditional_TransactionManager();

  void Setup(Txn* txn, bool sorted);
  
  LockUnit* NextLockUnit();

  void Reset();
  LockUnit* CurrentLMRequest();

  Txn* txn_;
 // LockUnit* lock_units;
LockUnit lock_units[MAX_WRITE_CNT + MAX_READ_CNT];
  uint32_t current_index_;
};

#endif  // _DB_SCHEDULER_TRADITIONAL_MANAGER_H_


