// Executor for traditional physical partitioned approach
//

#ifndef _DB_SCHEDULER_PHYSICAL_TRADITIONAL_EXECUTOR_H_
#define _DB_SCHEDULER_PHYSICAL_TRADITIONAL_EXECUTOR_H_


#include "common/utils.h"
#include "scheduler/physical_traditional_lock_manager.h"
#include "applications/application.h"
#include "scheduler/traditional_transaction_manager.h"

class PhysicalTraditional_LockManager;
class Txn;

extern Traditional_TransactionManager* traditional_transactions_managers[WORKER_THREADS];

class PhysicalTraditionalExecutor {
 public:
  PhysicalTraditionalExecutor(const Application* application);
  virtual ~PhysicalTraditionalExecutor();

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

  PhysicalTraditional_LockManager* lock_manager_;

  volatile uint64_t print_word;

};
#endif  // _DB_SCHEDULER_PHYSICAL_TRADITIONAL_EXECUTOR_H_
