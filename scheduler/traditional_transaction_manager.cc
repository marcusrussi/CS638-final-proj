
#include "scheduler/traditional_transaction_manager.h"
#include "common/utils.h"

int compare2(const void* a, const void* b) {
  if (((LockUnit*) a)->table_id != ((LockUnit*) b)->table_id) {
    if (((LockUnit*) a)->table_id > ((LockUnit*) b)->table_id)
      return 1;
    else if (((LockUnit*) a)->table_id < ((LockUnit*) b)->table_id)
      return -1;
    else
      return 0;
  } else {
    if(((LockUnit*) a)->key > ((LockUnit*) b)->key) 
      return 1;
    else if(((LockUnit*) a)->key < ((LockUnit*) b)->key) 
      return -1;
    else
      return 0;
  }
}

void Traditional_TransactionManager::Setup(Txn* txn, bool sort) {

  txn_ = txn;

//  lock_units = (LockUnit*)malloc(sizeof(LockUnit)*(txn->ReadSetSize() + txn->ReadWriteSetSize()));

  for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
    lock_units[i].txn = txn;
    lock_units[i].table_id = txn->GetReadSet(i).table_id;
    lock_units[i].key = txn->GetReadSet(i).key;
    lock_units[i].mode = READ;
  }

  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    lock_units[i + txn->ReadSetSize()].txn = txn;
    lock_units[i + txn->ReadSetSize()].table_id = txn->GetReadWriteSet(i).table_id;
    lock_units[i + txn->ReadSetSize()].key = txn->GetReadWriteSet(i).key;
    lock_units[i + txn->ReadSetSize()].mode = WRITE;
  }

  if (sort == true) {
    qsort(lock_units, txn->ReadSetSize() + txn->ReadWriteSetSize(), sizeof(LockUnit), compare2);
  }

  current_index_ = 0;
}

LockUnit* Traditional_TransactionManager::NextLockUnit() {
  if (current_index_ < txn_->ReadSetSize() + txn_->ReadWriteSetSize()) {
    return lock_units + current_index_++;
  } else {
    current_index_ = 0;
    return NULL;
  }
}

void Traditional_TransactionManager::Reset() {
  current_index_ = 0;
}

LockUnit* Traditional_TransactionManager::CurrentLMRequest() {
  int index = current_index_ - 1;
  return lock_units + index;
}

Traditional_TransactionManager::Traditional_TransactionManager() {
}

Traditional_TransactionManager::~Traditional_TransactionManager() {
}


