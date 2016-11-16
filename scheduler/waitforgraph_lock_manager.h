// Lock manager for basic wait-for graph
//

#ifndef _DB_SCHEDULER_WAITFORGRAPH_LOCK_MANAGER_H_
#define _DB_SCHEDULER_WAITFORGRAPH_LOCK_MANAGER_H_

#include "scheduler/lock_manager.h"

class Waitforgraph_LockManager : public Deadlock_LockManager {
 public:
  Waitforgraph_LockManager(uint32_t table_num);
  virtual ~Waitforgraph_LockManager() {}
  virtual int Lock(LockUnit* lock_unit) ;
  virtual void Release(Txn* txn) ;
  virtual void Release(const TableKey table_key, Txn* txn) ;
  virtual void DeadlockRelease(LockUnit* lock_unit) ;
  virtual void AddToWaitforgraph(uint64_t txn1, uint64_t txn2);
  virtual bool CheckDeadlock(uint64_t txn1);
  virtual void RemoveToWaitforgraph(uint64_t txn1);
  virtual void RemoveToWaitforgraph(uint32_t worker, uint64_t txn1);

  virtual void Setup(int worker_id);

 private:
  Traditional_Bucket* lock_table_;
  Keys_Freelist* keys_freelist[WORKER_THREADS];
  Lockrequest_Freelist* lockrequest_freelist[WORKER_THREADS];
  uint32_t table_num_;

  pthread_mutex_t waitforgraph_latch;
  HashMap_Waitforgraph* wait_for_graph;
  //unordered_map<uint64_t, uint64_t> wait_for_graph;

  uint32_t table_buckets[9];
  uint32_t table_sum_buckets[9];
};

#endif  // _DB_SCHEDULER_WAITFORGRAPH_LOCK_MANAGER_H_
