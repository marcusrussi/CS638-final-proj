// Orthrus transaction manager

#include "scheduler/partitioned_transaction_manager.h"

const Application* application2_;

int compare(const void* a, const void* b) {
  if (application2_->LookupPartition(((TableKey*) a)->key) == application2_->LookupPartition(((TableKey*) b)->key)) {
    if (((TableKey*) a)->key > ((TableKey*) b)->key)
      return 1;
    else if (((TableKey*) a)->key < ((TableKey*) b)->key)
      return -1;
    else
      return 0;
  } else {
    if (application2_->LookupPartition(((TableKey*) a)->key) > application2_->LookupPartition(((TableKey*) b)->key))
      return 1;
    else if (application2_->LookupPartition(((TableKey*) a)->key) < application2_->LookupPartition(((TableKey*) b)->key))
      return -1;
    else
      return 0;
  }
}

/**int compare(const void* a, const void* b) {
  if (((TableKey*) a)->key % LOCK_MANAGER_THREADS > ((TableKey*) b)->key % LOCK_MANAGER_THREADS)
    return 1;
  else if (((TableKey*) a)->key % LOCK_MANAGER_THREADS < ((TableKey*) b)->key % LOCK_MANAGER_THREADS)
    return -1;
  else
    return 0;
}**/

void Partitioned_TransactionManager::SortKeys() {

  if (txn_->ReadSetSize() > 0) {
    qsort(txn_->GetReadSet(), txn_->ReadSetSize(), sizeof(TableKey), compare);
  }
  if (txn_->ReadWriteSetSize() > 0) {
    qsort(txn_->GetReadWriteSet(), txn_->ReadWriteSetSize(), sizeof(TableKey), compare);
  }
/**std::cout<<"------------ After------------\n"<<std::flush;
  for (uint32_t i = 0; i < txn_->ReadWriteSetSize(); i++) {
    std::cout<<i<<": "<<txn_->GetReadWriteSet(i).table_id<<":  "<<txn_->GetReadWriteSet(i).key<<".  "<<application_->LookupPartition(txn_->GetReadWriteSet(i).key)<<".\n"<<std::flush;
  }**/
}

void Partitioned_TransactionManager::Setup(Txn* txn, const Application* application) {
  txn_ = txn;
  application_ = application;
  application2_ = application_;

  SortKeys();

  for(int i = 0; i < LOCK_MANAGER_THREADS; i++) {
    involved_lms[i] = false;
  }

  for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
    involved_lms[application_->LookupPartition(txn->GetReadSet(i).key)] = true;
  }

  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    involved_lms[application_->LookupPartition(txn->GetReadWriteSet(i).key)] = true;
  }
  
  int actual_lm = 0;
  for(int i = 0; i < LOCK_MANAGER_THREADS; i++) {
    if (involved_lms[i] == true) {
      actual_lm++;
    }
  }
  
//  sub_txns = (SubTxn*)malloc(sizeof(SubTxn) * actual_lm);
  
  current_index = 0;
  sub_txns_cnt = 0;

  uint32_t read_key_start = 0;
  uint32_t read_key_end = txn->ReadSetSize();
  uint32_t write_key_start = 0;
  uint32_t write_key_end = txn->ReadWriteSetSize();

  for (uint32_t lm = 0; lm < LOCK_MANAGER_THREADS; lm++) {
    if (involved_lms[lm] == false) {
      continue;
    }

    for (uint32_t i = read_key_start; i < txn->ReadSetSize(); i++) {
      if (application_->LookupPartition(txn->GetReadSet(i).key) != lm) {
        read_key_end = i;
        break;
      }
    }

    for (uint32_t i = write_key_start; i < txn->ReadWriteSetSize(); i++) {
      if (application_->LookupPartition(txn->GetReadWriteSet(i).key) != lm) {
        write_key_end = i;
        break;
      }
    }
//std::cout<<"Txn: "<<txn->GetTxnId()<<", start and end is: "<<write_key_start<<"   "<<write_key_end<<". LM id:"<<lm<<".\n"<<std::flush;
    sub_txns[sub_txns_cnt].read_key_start = read_key_start;
    sub_txns[sub_txns_cnt].read_key_end = read_key_end;
    sub_txns[sub_txns_cnt].write_key_start = write_key_start;
    sub_txns[sub_txns_cnt].write_key_end = write_key_end;
    sub_txns[sub_txns_cnt].txn = txn;
    sub_txns[sub_txns_cnt].lm_id = lm;

    sub_txns_cnt++;

    read_key_start = read_key_end;
    write_key_start = write_key_end;
    read_key_end = txn->ReadSetSize();
    write_key_end = txn->ReadWriteSetSize();
  }

  for (uint32_t i = 0; i < sub_txns_cnt; i++) {
    if (i != sub_txns_cnt - 1) {
      sub_txns[i].next_sub_txn = &(sub_txns[i+1]);
      sub_txns[i].next_lm_id = sub_txns[i+1].lm_id;
    } else {
      sub_txns[i].next_sub_txn = NULL;
      sub_txns[i].next_lm_id = -1;
    }
  }
}

SubTxn* Partitioned_TransactionManager::NextLmRequest() {
  if (current_index < sub_txns_cnt) {
    return sub_txns + current_index++;
  } else {
    current_index = 0;
    return NULL;
  }
}

SubTxn* Partitioned_TransactionManager::GetFirstLMRequest() {
  return sub_txns;
}

Partitioned_TransactionManager::Partitioned_TransactionManager() {
}

Partitioned_TransactionManager::~Partitioned_TransactionManager() {
}

