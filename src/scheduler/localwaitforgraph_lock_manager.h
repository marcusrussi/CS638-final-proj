// Optimized wait-for graph lock manager
//

#ifndef _DB_SCHEDULER_LOCALWAITFORGRAPH_LOCK_MANAGER_H_
#define _DB_SCHEDULER_LOCALWAITFORGRAPH_LOCK_MANAGER_H_

#include "scheduler/lock_manager.h"

class LocalWaitforgraph_LockManager : public Deadlock_LockManager {
 public:
  LocalWaitforgraph_LockManager(uint32_t table_num);
  virtual ~LocalWaitforgraph_LockManager() {}
  virtual int Lock(LockUnit* lock_unit) ;
  virtual void Release(Txn* txn) ;
  virtual void Release(const TableKey table_key, Txn* txn) ;
  virtual void DeadlockRelease(LockUnit* lock_unit) ;
  virtual void AddToWaitforgraph(uint32_t worker, uint64_t txn1, uint64_t txn2);
  virtual bool CheckDeadlock(uint32_t worker, uint64_t txn1);
  virtual void RemoveToWaitforgraph(uint32_t worker, uint64_t txn1);

  virtual void Setup(int worker_id);

 private:
  Traditional_Bucket* lock_table_;
  Keys_Freelist* keys_freelist[WORKER_THREADS];
  Lockrequest_Freelist* lockrequest_freelist[WORKER_THREADS];
  uint32_t table_num_;

  pthread_mutex_t waitforgraph_latch[WORKER_THREADS];
  HashMap_Waitforgraph* wait_for_graph[WORKER_THREADS];

  uint32_t table_buckets[9];
  uint32_t table_sum_buckets[9];
};

#endif  // _DB_SCHEDULER_LOCALWAITFORGRAPH_LOCK_MANAGER_H_
