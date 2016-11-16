// Hstore transaction manager


#ifndef _DB_SCHEDULER_HSTORE_MANAGER_H_
#define _DB_SCHEDULER_HSTORE_MANAGER_H_

#include "common/types.h"
#include "common/utils.h"
class Txn;

class Hstore_TransactionManager {
 public:

  Hstore_TransactionManager();

  ~Hstore_TransactionManager();

  void Setup(Txn* txn);
  
  int NextLock();

  Txn* txn_;

  bool lock_involved[WORKER_THREADS];
  uint32_t current_index_;
};

#endif  // _DB_SCHEDULER_HSTORE_MANAGER_H_


