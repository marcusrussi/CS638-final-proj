// Executor for Orthrus
//

#ifndef _DB_SCHEDULER_PARTITIONED_EXECUTOR_H_
#define _DB_SCHEDULER_PARTITIONED_EXECUTOR_H_

#include "common/utils.h"
#include "scheduler/partitioned_lock_manager.h"
#include "applications/application.h"
#include "scheduler/partitioned_transaction_manager.h"


class LockManager;
class Txn;
class SubTxn;

class PartitionedExecutor {
 public:
  PartitionedExecutor(const Application* application);
  virtual ~PartitionedExecutor();

  LockManager* lock_manager_[LOCK_MANAGER_THREADS];

 private:
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);
  
  static void* LockManagerThread(void* arg);

  // Thread contexts and their associated Connection objects.
  pthread_t worker_threads_[WORKER_THREADS];
  pthread_t lm_threads_[LOCK_MANAGER_THREADS];
  
  // Application currently being run.
  const Application* application_;

  atomic<int> g_ctr1;
  atomic<int> g_ctr2;
  atomic<int> g_ctr3;

  volatile uint64_t print_word;

  // The per-node lock manager tracks what transactions have temporary ownership
  // of what database objects, allowing the scheduler to track LOCAL conflicts
  // and enforce equivalence to transaction orders.
  
};

class HashEntry_Worker {
 public:
   uint64_t key;
   Partitioned_TransactionManager* value;
   HashEntry_Worker(uint64_t key, Partitioned_TransactionManager* value) {
     this->key = key;
     this->value = value;
   }
 
   uint64_t GetKey() {
     return key;
   }
 
   Partitioned_TransactionManager* getValue() {
     return value;
   }
};

class HashMap_Worker {
 private:
   HashEntry_Worker* table;
   uint32_t size;

 public:
   HashMap_Worker() {
     table = (HashEntry_Worker*)malloc(sizeof(HashEntry_Worker) * HASH_MAP_SIZE);
     for (int i = 0; i < HASH_MAP_SIZE; i++) {
       table[i].key = MAX_UINT64;
       table[i].value = NULL;
     }
     size = 0;
   }
 
   Partitioned_TransactionManager* Get(uint64_t key) {
     int hash = Hash(key) % HASH_MAP_SIZE;
     while (table[hash].key != key) {
       hash = (hash + 1) % HASH_MAP_SIZE;
     }

     return table[hash].value;
   }
 
   void Put(uint64_t key, Partitioned_TransactionManager* value) {
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
   
   ~HashMap_Worker() {
     delete  table;
   }
};

#endif  // _DB_SCHEDULER_PARTITIONED_EXECUTOR_H_
