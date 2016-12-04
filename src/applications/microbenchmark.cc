// Microbenchmark/YCSB for Orthrus

#include "applications/microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "common/utils.h"

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Microbenchmark::GetRandomKeys(set<uint64_t>* keys, int num_keys, uint64_t key_start,
                                   uint64_t  key_limit, int part) const{
  keys->clear();
  for (int i = 0; i < num_keys; i++) {
    // Find a key not already in '*keys'.
    int key;
    do {
      key = key_start + part +
            lm_count_ * (rand() % ((key_limit - key_start)/lm_count_));
    } while (keys->count(key));
    keys->insert(key);
  }
}


// Create a non-dependent single-partition transaction
void Microbenchmark::MicroTxnSP(Txn* txn, uint64_t txn_id, int part) const{

  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  set<uint64_t> keys;
  GetRandomKeys(&keys,
                kRWSetSize - 2,
                hot_records,
                kDBSize,
                part);

  // Add hot keys to read/write set. Each txn accesses 2 hot records
  int hotkey;
  for (int i = 0; i < 2; i++) {
    do {
      hotkey = part + lm_count_ * (rand() % hot_records_per_lm);
    } while (hotkey >= hot_records || keys.count(hotkey));
    keys.insert(hotkey);
  }

  for (set<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(0, *it);
    //txn->AddReadSet(0, *it);
  }
}


// Create a non-dependent multi-partition transaction
void Microbenchmark::MicroTxnMltiple(Txn* txn, uint64_t txn_id, int mp) const {

  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_MP);
  txn->SetTxnStatus(ACTIVE);

  if (mp > LOCK_MANAGER_THREADS) {
    mp = LOCK_MANAGER_THREADS;
  }

  int per_partition = kRWSetSize / mp;
  int left = kRWSetSize - per_partition*mp;

  set<uint64_t> partitions;
  

  int size;
  int current_partition;
  for (int i = 0; i < mp; i++) {

    size = per_partition;
    if (left > 0) {
      size++;
      left--;
    }

    do {
      current_partition = rand() % LOCK_MANAGER_THREADS;
    } while (partitions.count(current_partition));
    partitions.insert(current_partition);

    // Add hot keys
    if (i <= 1) {
      uint64_t hot_key = current_partition + lm_count_ * (rand() % hot_records_per_lm);
      txn->AddReadWriteSet(0, hot_key);
      //txn->AddReadSet(0, hot_key);
      size--;
    }

    if (size > 0) {
      set<uint64_t> keys;
      GetRandomKeys(&keys,
                    size,
                    hot_records,
                    kDBSize,
                    current_partition);

      for (set<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
        txn->AddReadWriteSet(0, *it);
        //txn->AddReadSet(0, *it);
      }
    }
  }

}


// Create a txn that random accesses records from different lms
void Microbenchmark::MicroTxnRandom(Txn* txn, uint64_t txn_id, int part) const{

  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_MP);
  txn->SetTxnStatus(ACTIVE);

  set<uint64_t> keys;
  for (int i = 0; i < kRWSetSize - 2; i++) {
    int key;
    do {
      key = rand() % (kDBSize - hot_records) + hot_records;
    } while (keys.count(key));
    keys.insert(key);
  }

  // Add hot keys to read/write set.
  set<uint64_t> hot_keys;
  int hotkey;
  for (int i = 0; i < 2; i++) {
    do {
      hotkey = rand() % hot_records;
    } while (hot_keys.count(hotkey));
    hot_keys.insert(hotkey);
  }

  for (set<uint64_t>::iterator it = hot_keys.begin(); it != hot_keys.end(); ++it)
    txn->AddReadWriteSet(0, *it);
    //txn->AddReadSet(0, *it);

  for (set<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it)
    txn->AddReadWriteSet(0, *it);
    //txn->AddReadSet(0, *it);
}

// Create a new txn
void Microbenchmark::NewTxn(Txn* txn, uint64_t txn_id) const {
    if (percent_mp_ == 1) {
      int lm_id = rand() % LOCK_MANAGER_THREADS;
      MicroTxnSP(txn, txn_id, lm_id);
    } else if (percent_mp_ >= 2 && percent_mp_ <= 10) {
      assert(lm_count_ >= percent_mp_);
      MicroTxnMltiple(txn, txn_id, percent_mp_);
    } else if (percent_mp_ == 11) {
      int lm_id = rand() % LOCK_MANAGER_THREADS;
      MicroTxnRandom(txn, txn_id, lm_id);
    }
}

int Microbenchmark::Execute(Txn* txn) const {
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

uint32_t Microbenchmark::LookupPartition(const uint64_t& key) const {
  return key % lm_count_;
}

uint32_t Microbenchmark::GetTableNum() const {
  return 1;
}

void Microbenchmark::InitializeStorage() const {
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

int Microbenchmark::Execute2(LockUnit* lock_unit) const {
  return 0;
}
int Microbenchmark::Rollback(LockUnit* lock_unit) const {
  return 0;
}

void Microbenchmark::InitializeTable(uint32_t table_id) const {  
}


