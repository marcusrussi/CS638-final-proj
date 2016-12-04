// Lock manager for physical partitioned Orthrus
//

#ifndef _DB_SCHEDULER_PHYSICAL_PARTITIONED_LOCK_MANAGER_H_
#define _DB_SCHEDULER_PHYSICAL_PARTITIONED_LOCK_MANAGER_H_

#include "scheduler/lock_manager.h"
#include "scheduler/physical_partitioned_executor.h"

class PhysicalPartitionedExecutor;

class PhysicalLockManager {
 public:
  PhysicalLockManager(int lm_id, uint32_t table_num);
  virtual ~PhysicalLockManager() {}
  virtual void Lock(SubTxn* sub_txn);
  virtual void Release(SubTxn* sub_txn);
  virtual void Release(const TableKey table_key, Txn* txn);
  void Setup(PhysicalPartitionedExecutor* scheduler);

  LatchFreeQueue<SubTxn*>* request_locks_queue_[WORKER_THREADS];
  LatchFreeQueue<uint64_t>* acquired_locks_queue_[WORKER_THREADS];
  LatchFreeQueue<SubTxn*>* release_locks_queue_[WORKER_THREADS];

  LatchFreeQueue<SubTxn*>* communication_receive_queue_[LOCK_MANAGER_THREADS];

  LatchFreeQueue<SubTxn*>* communication_send_queue_[LOCK_MANAGER_THREADS];

 private:

  Bucket* lock_table_;

  int lm_id_;
  
  Keys_Freelist* keys_freelist;
  Lockrequest_Freelist* lockrequest_freelist;

  HashMap_Lm* txn_wait;

  uint32_t table_num_;

  uint32_t table_buckets[80];
  uint32_t table_sum_buckets[80];

};


#endif  // _DB_SCHEDULER_PARTITIONED_LOCK_MANAGER2_H_
