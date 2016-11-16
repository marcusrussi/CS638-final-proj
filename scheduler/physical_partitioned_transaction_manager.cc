
#include "scheduler/physical_partitioned_transaction_manager.h"


int compare3(const void* a, const void* b) {
    uint32_t a_tableid = ((TableKey*) a)->table_id;
    uint32_t b_tableid = ((TableKey*) b)->table_id;

    uint32_t a_cc = (a_tableid%(10-CC_PER_NUMA))%CC_PER_NUMA + a_tableid/(10-CC_PER_NUMA)*CC_PER_NUMA;
    uint32_t b_cc = (b_tableid%(10-CC_PER_NUMA))%CC_PER_NUMA + a_tableid/(10-CC_PER_NUMA)*CC_PER_NUMA;

    if (a_cc > b_cc)
      return 1;
    else if (a_cc < b_cc)
      return -1;
    else
      return 0;
}



void PhysicalPartitioned_TransactionManager::SortKeys() {

  if (txn_->ReadSetSize() > 0) {
    qsort(txn_->GetReadSet(), txn_->ReadSetSize(), sizeof(TableKey), compare3);
  }
  if (txn_->ReadWriteSetSize() > 0) {
    qsort(txn_->GetReadWriteSet(), txn_->ReadWriteSetSize(), sizeof(TableKey), compare3);
  }

}

void PhysicalPartitioned_TransactionManager::Setup(Txn* txn, const Application* application) {
  txn_ = txn;
  application_ = application;

  SortKeys();


  for(int i = 0; i < LOCK_MANAGER_THREADS; i++) {
    involved_lms[i] = false;
  }

  for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
    uint32_t table_id = txn->GetReadSet(i).table_id;
    uint32_t cc = (table_id%(10-CC_PER_NUMA))%CC_PER_NUMA + table_id/(10-CC_PER_NUMA)*CC_PER_NUMA;
    involved_lms[cc] = true;
  }

  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    uint32_t table_id = txn->GetReadWriteSet(i).table_id;
    uint32_t cc = (table_id%(10-CC_PER_NUMA))%CC_PER_NUMA + table_id/(10-CC_PER_NUMA)*CC_PER_NUMA;
    involved_lms[cc] = true;
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
      uint32_t table_id = txn->GetReadSet(i).table_id;
      uint32_t cc = (table_id%(10-CC_PER_NUMA))%CC_PER_NUMA + table_id/(10-CC_PER_NUMA)*CC_PER_NUMA;
      if (cc != lm) {
        read_key_end = i;
        break;
      }
    }

    for (uint32_t i = write_key_start; i < txn->ReadWriteSetSize(); i++) {
      uint32_t table_id = txn->GetReadWriteSet(i).table_id;
      uint32_t cc = (table_id%(10-CC_PER_NUMA))%CC_PER_NUMA + table_id/(10-CC_PER_NUMA)*CC_PER_NUMA;
      if (cc != lm) {
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

SubTxn* PhysicalPartitioned_TransactionManager::NextLmRequest() {
  if (current_index < sub_txns_cnt) {
    return sub_txns + current_index++;
  } else {
    current_index = 0;
    return NULL;
  }
}

SubTxn* PhysicalPartitioned_TransactionManager::GetFirstLMRequest() {
  return sub_txns;
}

PhysicalPartitioned_TransactionManager::PhysicalPartitioned_TransactionManager() {
}

PhysicalPartitioned_TransactionManager::~PhysicalPartitioned_TransactionManager() {
}

