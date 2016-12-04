
#include "scheduler/hstore_transaction_manager.h"
#include "common/utils.h"

void Hstore_TransactionManager::Setup(Txn* txn) {

  txn_ = txn;

  for (uint32_t i = 0; i < WORKER_THREADS; i++) {
    lock_involved[i] = false;
  }

  for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
    lock_involved[txn->GetReadSet(i).table_id] = true;
  }

  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    lock_involved[txn->GetReadWriteSet(i).table_id] = true;
  }

  current_index_ = 0;
}

int Hstore_TransactionManager::NextLock() {
  int i;
  for (i = current_index_; i < WORKER_THREADS; i++) {
    if (lock_involved[i] == true) {
      current_index_ = i + 1;
      return i;
    }
  }

  current_index_ = 0;
  return -1;
}

Hstore_TransactionManager::Hstore_TransactionManager() {
}

Hstore_TransactionManager::~Hstore_TransactionManager() {
}


