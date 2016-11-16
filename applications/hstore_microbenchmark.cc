
#include "applications/hstore_microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "common/utils.h"

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Hstore_Microbenchmark::GetRandomKeys(vector<uint64_t>* keys, int num_keys, uint64_t key_start, uint64_t  key_limit) const{
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


// Create a non-dependent single-partition transaction
/**void Hstore_Microbenchmark::MicroTxnSP(Txn* txn, uint64_t txn_id) const{
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
  for (int i = 0; i < 2; i++) {
    do {
      hotkey = rand() % hot_per_worker[txn_id%WORKER_THREADS];
    } while (keys_set.count(hotkey));
    keys_set.insert(hotkey);
    keys.push_back(hotkey);
  }


  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(0, *it);
  }

  keys.clear();

  GetRandomKeys(&keys,
                kRWSetSize-2,
                hot_per_worker[txn_id%WORKER_THREADS],
                kDBSize/WORKER_THREADS);


  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(txn_id%WORKER_THREADS, *it);
  }
}**/

// Create a non-dependent single-partition transaction
void Hstore_Microbenchmark::MicroTxnSP(Txn* txn, uint64_t txn_id) const{
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
  for (int i = 0; i < 2; i++) {
    do {
      hotkey = rand() % hot_per_worker[txn_id%WORKER_THREADS];
    } while (keys_set.count(hotkey));
    keys_set.insert(hotkey);
    keys.push_back(hotkey);
  }

  GetRandomKeys(&keys,
                kRWSetSize-2,
                hot_per_worker[txn_id%WORKER_THREADS],
                kDBSize/WORKER_THREADS);


  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(txn_id%WORKER_THREADS, *it);
  }
}

// Create a non-dependent single-partition transaction
void Hstore_Microbenchmark::MicroTxnMP(Txn* txn, uint64_t txn_id, int mp) const{
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  if (mp > WORKER_THREADS) {
    mp = WORKER_THREADS;
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
      if (i == 0) {
        current_partition = txn_id % WORKER_THREADS;
      } else {
        current_partition = rand() % WORKER_THREADS;
      }
    } while (partitions.count(current_partition));
    partitions.insert(current_partition);

    // Add hot keys
    if (i <= 1) {
      uint64_t hot_key = rand() % hot_per_worker[current_partition];
      txn->AddReadWriteSet(current_partition, hot_key);
      size--;
    }
    
    vector<uint64_t> keys;

    GetRandomKeys(&keys,
                size,
                hot_per_worker[current_partition],
                kDBSize/WORKER_THREADS);


    for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
      txn->AddReadWriteSet(current_partition, *it);
    }
    
  }

}


// Create a non-dependent single-partition transaction
void Hstore_Microbenchmark::MicroTxnRandom(Txn* txn, uint64_t txn_id) const{
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  // Insert set of kRWSetSize - 2 random cold keys from specified partition into
  // read/write set.

  uint64_t hot_key = rand() % hot_per_worker[txn_id%WORKER_THREADS];
  txn->AddReadWriteSet(txn_id%WORKER_THREADS, hot_key);

  hot_key = rand() % hot_per_worker[txn_id%WORKER_THREADS];
  txn->AddReadWriteSet(rand()%WORKER_THREADS, hot_key);

  vector<uint64_t> keys;

  GetRandomKeys(&keys,
                kRWSetSize-2,
                hot_per_worker[txn_id%WORKER_THREADS],
                kDBSize/WORKER_THREADS);


  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    if (it == keys.begin()) {
      txn->AddReadWriteSet(txn_id%WORKER_THREADS, *it); 
    } else {
      txn->AddReadWriteSet(rand()%WORKER_THREADS, *it);
    }
  }
}

void Hstore_Microbenchmark::NewTxn(Txn* txn, uint64_t txn_id) const {
  if (percent_mp_ == 0) {
    MicroTxnSP(txn, txn_id);
  } else if (percent_mp_ >= 2 && percent_mp_ <= 10) {   
    MicroTxnMP(txn, txn_id, percent_mp_);
  } else if (percent_mp_ == 11) {
    MicroTxnRandom(txn, txn_id);
  }

  /**if (rand()%100 < percent_mp_) {
    MicroTxnMP(txn, txn_id, 2);
  } else {
    MicroTxnSP(txn, txn_id);
  }**/

}

uint32_t Hstore_Microbenchmark::GetTableNum() const {
  return 1;
}

int Hstore_Microbenchmark::Execute(Txn* txn) const {
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

uint32_t Hstore_Microbenchmark::LookupPartition(const uint64_t& key) const {
  return 0;
}

void Hstore_Microbenchmark::InitializeStorage() const {

}

void Hstore_Microbenchmark::InitializeTable(uint32_t table_id) const {
  char* int_buffer = (char *)malloc(sizeof(char)*kValueSize);
  uint64_t* big_int = (uint64_t*)int_buffer;
  for (int j = 0; j < kValueSize / 8; j++) {
    big_int[j] = (uint64_t)rand(); 
  }
  
  storage_->NewTable(table_id, kDBSize/WORKER_THREADS + 1, kDBSize/WORKER_THREADS + 1, kValueSize);

  for (uint64_t i = 0; i < kDBSize/WORKER_THREADS; i++) {
    storage_->PutRecord(table_id, i, (void*)int_buffer);
  }
}

int Hstore_Microbenchmark::Execute2(LockUnit* lock_unit) const {
  return 0;
}
int Hstore_Microbenchmark::Rollback(LockUnit* lock_unit) const {
  return 0;
}

