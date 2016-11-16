// Lock manager for wait-die
//

#ifndef _DB_SCHEDULER_WAITDIE_LOCK_MANAGER_H_
#define _DB_SCHEDULER_WAITDIE_LOCK_MANAGER_H_

#include "scheduler/lock_manager.h"

class Waitdie_LockManager : public Deadlock_LockManager {
 public:
  Waitdie_LockManager(uint32_t table_num);
  virtual ~Waitdie_LockManager() {}
  virtual int Lock(LockUnit* lock_unit) ;
  virtual void Release(Txn* txn) ;
  virtual void Release(const TableKey table_key, Txn* txn) ;
  virtual void DeadlockRelease(LockUnit* lock_unit) ;
  virtual bool CheckWaitDie(LockUnit* lock_unit, KeysList* key_list);
  virtual void RemoveToWaitforgraph(uint32_t worker, uint64_t txn1);

  virtual void Setup(int worker_id);

 private:
  Traditional_Bucket* lock_table_;
  Keys_Freelist* keys_freelist[WORKER_THREADS];
  Lockrequest_Freelist* lockrequest_freelist[WORKER_THREADS];
  uint32_t table_num_;

  uint32_t table_buckets[9];
  uint32_t table_sum_buckets[9];

};

#endif  // _DB_SCHEDULER_WAITDIE_LOCK_MANAGER_H_
