// Lock manager for traditional physical partitioned approach
//

#ifndef _DB_SCHEDULER_PHYSICAL_TRADITIONAL_LOCK_MANAGER_H_
#define _DB_SCHEDULER_PHYSICAL_TRADITIONAL_LOCK_MANAGER_H_

#include "scheduler/lock_manager.h"

class PhysicalTraditional_LockManager {
 public:
  PhysicalTraditional_LockManager(uint32_t table_num);
  virtual ~PhysicalTraditional_LockManager() {}
  virtual bool Lock(LockUnit* lock);
  virtual void Release(Txn* txn);
  virtual void Release(const TableKey table_key, Txn* txn);
  virtual void Setup(int worker_id);

 private:

  Traditional_Bucket* lock_table_;
  Keys_Freelist* keys_freelist[WORKER_THREADS];
  Lockrequest_Freelist* lockrequest_freelist[WORKER_THREADS];
  uint32_t table_num_;

  uint32_t table_buckets[80];
  uint32_t table_sum_buckets[80];

};

#endif  // _DB_SCHEDULER_PHYSICAL_TRADITIONAL_LOCK_MANAGER_H_
