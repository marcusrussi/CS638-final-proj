
#include "applications/traditional_microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "common/utils.h"

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Traditional_Microbenchmark::GetRandomKeys(vector<uint64_t>* keys, int num_keys, uint64_t key_start, uint64_t  key_limit) const{
  set<uint64_t> keys_set;
  for (int i = 0; i < num_keys; i++) {
    // Find a key not already in '*keys'.
    uint64_t key;
    do {
      key = key_start + rand() % (key_limit - key_start);
    } while (keys_set.count(key));
    keys_set.insert(key);
    keys->push_back(key);
  }
}


// Hot keys are located at the beginning
///**
void Traditional_Microbenchmark::MicroTxnSP(Txn* txn, uint64_t txn_id) const{
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  // Insert set of kRWSetSize - 2 random cold keys from specified partition into
  // read/write set.
  vector<uint64_t> keys;
  set<uint64_t> keys_set;

  // Add hot keys to read/write set.
  int hotkey;
  for (int i = 0; i < 1; i++) {
    do {
      hotkey = rand() % hot_records;
    } while (keys_set.count(hotkey));
    keys_set.insert(hotkey);
    keys.push_back(hotkey);
  }

  GetRandomKeys(&keys,
                kRWSetSize - 1,
                hot_records,
                kDBSize);


  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(0, *it);
    //txn->AddReadSet(0, *it);
  }
}
//**/

// Hotkeys are located in the end
/**
void Traditional_Microbenchmark::MicroTxnSP(Txn* txn, uint64_t txn_id) const{
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  // Insert set of kRWSetSize - 2 random cold keys from specified partition into
  // read/write set.
  vector<uint64_t> keys;
  set<uint64_t> keys_set;

  GetRandomKeys(&keys,
                kRWSetSize - 2,
                hot_records,
                kDBSize);


  // Add hot keys to read/write set.
  int hotkey;
  for (int i = 0; i < 2; i++) {
    do {
      hotkey = rand() % hot_records;
    } while (keys_set.count(hotkey));
    keys_set.insert(hotkey);
    keys.push_back(hotkey);
  }

  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(0, *it);
    //txn->AddReadSet(0, *it);
  }
}
**/

void Traditional_Microbenchmark::NewTxn(Txn* txn, uint64_t txn_id) const {
    MicroTxnSP(txn, txn_id);
}

uint32_t Traditional_Microbenchmark::GetTableNum() const {
  return 1;
}

int Traditional_Microbenchmark::Execute(Txn* txn) const {
  // Read all elements of 'txn->read_set()', add one to each, write them all
  // back out.
uint64_t count_writer = 0;
uint64_t count_reader = 0;
  for (uint32_t i = 0; i < txn->read_write_cnt; i++) {
    TableKey table_key = txn->GetReadWriteSet(i);
    char* val = (char*)storage_->ReadRecord(table_key.table_id, table_key.key);   
    for (int j = 0; j < 8; j++) {
      *(uint64_t*)&val[j*8] = *(uint64_t*)&val[j*8] + count_writer;
      count_writer = count_writer/10 + (*(uint64_t*)&val[j*8])/10;
    }
  }

  for (uint32_t i = 0; i < txn->read_cnt; i++) {
    TableKey table_key = txn->GetReadSet(i);
    char* val = (char*)storage_->ReadRecord(table_key.table_id, table_key.key);   
    for (int j = 0; j < 8; j++) {
      count_reader = count_reader + *(uint64_t*)&val[j*8];
      count_reader = count_reader/10 + (*(uint64_t*)&val[j*8])/10;
    }
  }

  return 0;
}

int Traditional_Microbenchmark::Execute2(LockUnit* lock_unit) const {
  uint64_t count_writer = 0;
  uint64_t count_reader = 0;
  if (lock_unit->mode == WRITE) {
    char* val = (char*)storage_->ReadRecord(lock_unit->table_id, lock_unit->key);   
    for (int j = 0; j < 8; j++) {
      *(uint64_t*)&val[j*8] = *(uint64_t*)&val[j*8] + count_writer;
      count_writer = count_writer/10 + (*(uint64_t*)&val[j*8])/10;
    }
  } else {
    char* val = (char*)storage_->ReadRecord(lock_unit->table_id, lock_unit->key);   
    for (int j = 0; j < 8; j++) {
      count_reader = count_reader + *(uint64_t*)&val[j*8];
      count_reader = count_reader/10 + (*(uint64_t*)&val[j*8])/10;
    }
  }
//std::cout<<lock_unit->txn->GetTxnId()<<": Execute2.\n"<<std::flush;
  return 0;
}

int Traditional_Microbenchmark::Rollback(LockUnit* lock_unit) const {
  uint64_t count_writer = 0;

  if (lock_unit->mode == READ) {
    return 0; 
  }
  
  Txn* txn = lock_unit->txn;
  for (uint32_t i = 0; i < txn->read_write_cnt; i++) {
    TableKey table_key = txn->GetReadWriteSet(i);
    if (table_key.table_id == lock_unit->table_id && table_key.key == lock_unit->key) {
      break;
    }
    char* val = (char*)storage_->ReadRecord(table_key.table_id, table_key.key);   
    for (int j = 0; j < 8; j++) {
      *(uint64_t*)&val[j*8] = *(uint64_t*)&val[j*8] - count_writer;
      count_writer = count_writer/10 + (*(uint64_t*)&val[j*8])/10;
    }
  }

  return 0;
}

uint32_t Traditional_Microbenchmark::LookupPartition(const uint64_t& key) const {
  return 0;
}

void Traditional_Microbenchmark::InitializeStorage() const {
  char* int_buffer = (char *)malloc(sizeof(char)*kValueSize);
  uint64_t* big_int = (uint64_t*)int_buffer;
  for (int j = 0; j < kValueSize / 8; j++) {
    big_int[j] = (uint64_t)rand(); 
  }
  
  storage_->NewTable(0, kDBSize, kDBSize + 1, kValueSize);

  for (uint64_t i = 0; i < kDBSize; i++) {
    storage_->PutRecord(0, i, (void*)int_buffer);
  }
}

void Traditional_Microbenchmark::InitializeTable(uint32_t table_id) const {  
}



