// Executor for phsical partitioned Orthrus
//

#ifndef _DB_SCHEDULER_PHYSICAL_PARTITIONED_EXECUTOR2_H_
#define _DB_SCHEDULER_PHYSICAL_PARTITIONED_EXECUTOR2_H_

#include "common/utils.h"
#include "scheduler/physical_partitioned_lock_manager.h"
#include "applications/application.h"
#include "scheduler/physical_partitioned_transaction_manager.h"


class PhysicalLockManager;
class Txn;
class SubTxn;

class PhysicalPartitionedExecutor {
 public:
  PhysicalPartitionedExecutor(const Application* application);
  virtual ~PhysicalPartitionedExecutor();

  PhysicalLockManager* lock_manager_[LOCK_MANAGER_THREADS];

 private:
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);
  
  static void* LockManagerThread(void* arg);

  // Thread contexts and their associated Connection objects.
  pthread_t worker_threads_[WORKER_THREADS];
  pthread_t lm_threads_[LOCK_MANAGER_THREADS];
  
  // Application currently being run.
  const Application* application_;

  volatile uint64_t print_word;

  atomic<int> g_ctr1;
  atomic<int> g_ctr2;
  atomic<int> g_ctr3;

  // The per-node lock manager tracks what transactions have temporary ownership
  // of what database objects, allowing the scheduler to track LOCAL conflicts
  // and enforce equivalence to transaction orders.
  
};

class PhysicalHashEntry_Worker {
 public:
   uint64_t key;
   PhysicalPartitioned_TransactionManager* value;
   PhysicalHashEntry_Worker(uint64_t key, PhysicalPartitioned_TransactionManager* value) {
     this->key = key;
     this->value = value;
   }
 
   uint64_t GetKey() {
     return key;
   }
 
   PhysicalPartitioned_TransactionManager* getValue() {
     return value;
   }
};

class PhysicalHashMap_Worker {
 private:
   PhysicalHashEntry_Worker* table;
   uint32_t size;

 public:
   PhysicalHashMap_Worker() {
     table = (PhysicalHashEntry_Worker*)malloc(sizeof(PhysicalHashEntry_Worker) * HASH_MAP_SIZE);
     for (int i = 0; i < HASH_MAP_SIZE; i++) {
       table[i].key = MAX_UINT64;
       table[i].value = NULL;
     }
     size = 0;
   }
 
   PhysicalPartitioned_TransactionManager* Get(uint64_t key) {
     int hash = Hash(key) % HASH_MAP_SIZE;
     while (table[hash].key != key) {
       hash = (hash + 1) % HASH_MAP_SIZE;
     }

     return table[hash].value;
   }
 
   void Put(uint64_t key, PhysicalPartitioned_TransactionManager* value) {
     int hash = Hash(key) % HASH_MAP_SIZE;
     while (table[hash].key != MAX_UINT64) {
       hash = (hash + 1) % HASH_MAP_SIZE;
     }
     table[hash].key = key;
     table[hash].value = value;
     size++;
   }
   
   void Erase(uint64_t key) {
     int hash = Hash(key) % HASH_MAP_SIZE;
     while (table[hash].key != key) {
       hash = (hash + 1) % HASH_MAP_SIZE;
     }
     table[hash].key = MAX_UINT64;
     table[hash].value = NULL;
     size--;
   }
   
   uint32_t Size() {
     return size;
   }
   
   ~PhysicalHashMap_Worker() {
     delete  table;
   }
};

#endif  // _DB_SCHEDULER_PHYSICAL_PARTITIONED_EXECUTOR2_H_
