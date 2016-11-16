// Executor for traditional deadlock-free approach
//

#ifndef _DB_SCHEDULER_TRADITIONAL_EXECUTOR_H_
#define _DB_SCHEDULER_TRADITIONAL_EXECUTOR_H_


#include "common/utils.h"
#include "scheduler/traditional_lock_manager.h"
#include "applications/application.h"
#include "scheduler/traditional_transaction_manager.h"

class Traditional_LockManager;
class Txn;

extern Traditional_TransactionManager* traditional_transactions_managers[WORKER_THREADS];

class TraditionalExecutor {
 public:
  TraditionalExecutor(const Application* application);
  virtual ~TraditionalExecutor();

 private:
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);
  
  static void* LockManagerThread(void* arg);

  // Thread contexts and their associated Connection objects.
  pthread_t worker_threads_[WORKER_THREADS];
  
  // Application currently being run.
  const Application* application_;

  atomic<int> g_ctr1;

  // The per-node lock manager tracks what transactions have temporary ownership
  // of what database objects, allowing the scheduler to track LOCAL conflicts
  // and enforce equivalence to transaction orders.

  Traditional_LockManager* lock_manager_;

  volatile uint64_t print_word;
  
};

class HashEntry_Worker2 {
 public:
   uint64_t key;
   Traditional_TransactionManager* value;
   HashEntry_Worker2(uint64_t key, Traditional_TransactionManager* value) {
     this->key = key;
     this->value = value;
   }
 
   uint64_t GetKey() {
     return key;
   }
 
   Traditional_TransactionManager* getValue() {
     return value;
   }
};

class HashMap_Worker2 {
 private:
   HashEntry_Worker2* table;
   uint32_t size;

 public:
   HashMap_Worker2() {
     table = (HashEntry_Worker2*)malloc(sizeof(HashEntry_Worker2) * HASH_MAP_SIZE);
     for (int i = 0; i < HASH_MAP_SIZE; i++) {
       table[i].key = MAX_UINT64;
       table[i].value = NULL;
     }
     size = 0;
   }
 
   Traditional_TransactionManager* Get(uint64_t key) {
     int hash = Hash(key) % HASH_MAP_SIZE;
     while (table[hash].key != key) {
       hash = (hash + 1) % HASH_MAP_SIZE;
     }

     return table[hash].value;
   }
 
   void Put(uint64_t key, Traditional_TransactionManager* value) {
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
   
   ~HashMap_Worker2() {
     delete  table;
   }
};
#endif  // _DB_SCHEDULER_TRADITIONAL_EXECUTOR_H_
