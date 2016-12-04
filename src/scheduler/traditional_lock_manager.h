// Traditional lock manager
//

#ifndef _DB_SCHEDULER_TRADITIONAL_LOCK_MANAGER_H_
#define _DB_SCHEDULER_TRADITIONAL_LOCK_MANAGER_H_

#include "lock_manager.h"

class Traditional_LockManager {
 public:
  Traditional_LockManager(uint32_t table_num);
  virtual ~Traditional_LockManager() {}
  virtual bool Lock(LockUnit* lock);
  virtual void Release(Txn* txn);
  virtual void Release(const TableKey table_key, Txn* txn);
  virtual void Setup(int worker_id);

 private:

  Traditional_Bucket* lock_table_;
  Keys_Freelist* keys_freelist[WORKER_THREADS];
  Lockrequest_Freelist* lockrequest_freelist[WORKER_THREADS];
  uint32_t table_num_;

  uint32_t table_buckets[9];
  uint32_t table_sum_buckets[9];

};

#endif  // _DB_SCHEDULER_TRADITIONAL_LOCK_MANAGER_H_
