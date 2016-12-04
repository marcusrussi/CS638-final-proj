// Executor for traditional approach(deadlock may happen)
//

#ifndef _DB_SCHEDULER_TRADITIONAL_DEADLOCK_EXECUTOR_H_
#define _DB_SCHEDULER_TRADITIONAL_DEADLOCK_EXECUTOR_H_


#include "common/utils.h"
#include "scheduler/waitforgraph_lock_manager.h"
#include "scheduler/waitdie_lock_manager.h"
#include "scheduler/dreadlock_lock_manager.h"
#include "scheduler/localwaitforgraph_lock_manager.h"
#include "applications/application.h"
#include "scheduler/traditional_transaction_manager.h"

class Traditional_LockManager;
class Txn;

extern Traditional_TransactionManager* traditional_transactions_managers[WORKER_THREADS];

struct TxnManager {
  Txn* txn;
  Traditional_TransactionManager* manager;
};

class TraditionalDeadlockExecutor {
 public:
  TraditionalDeadlockExecutor(const Application* application, int deadlock_method);
  virtual ~TraditionalDeadlockExecutor();

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

  Deadlock_LockManager* deadlock_lockmanager_;

  int deadlock_method_;
//pthread_mutex_t global_;

  volatile uint64_t print_word;
};

#endif  // _DB_SCHEDULER_TRADITIONAL_DEADLOCK_EXECUTOR_H_
