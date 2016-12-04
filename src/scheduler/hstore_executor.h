// Hstore Implementation

#ifndef _DB_SCHEDULER_HSTORE_EXECUTOR_H_
#define _DB_SCHEDULER_HSTORE_EXECUTOR_H_


#include "common/utils.h"
#include "applications/application.h"
#include "scheduler/hstore_transaction_manager.h"

class Txn;

extern Hstore_TransactionManager* hstore_transactions_managers[WORKER_THREADS];
extern char spin_locks[CACHE_LINE*WORKER_THREADS*8];
//extern char* spin_locks;

class HstoreExecutor {
 public:
  HstoreExecutor(const Application* application);
  virtual ~HstoreExecutor();

 private:
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);

  // Thread contexts and their associated Connection objects.
  pthread_t worker_threads_[WORKER_THREADS];
  
  // Application currently being run.
  const Application* application_;

  atomic<int> g_ctr1;

  volatile uint64_t print_word;
  
};

#endif  // _DB_SCHEDULER_HSTORE_EXECUTOR_H_
