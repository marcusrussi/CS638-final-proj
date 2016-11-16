
#include "applications/physical_traditional_microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "common/utils.h"

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void PhysicalTraditional_Microbenchmark::GetRandomKeys(vector<uint64_t>* keys, int num_keys, uint64_t key_start, uint64_t  key_limit) const{
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
void PhysicalTraditional_Microbenchmark::MicroTxnSP(Txn* txn, uint64_t txn_id) const{
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  int physical_partitions = WORKER_THREADS;

  // Insert set of kRWSetSize - 2 random cold keys from specified partition into
  // read/write set.
  vector<uint64_t> keys;
  set<uint64_t> keys_set;

  // Add hot keys to read/write set.
  int hotkey;
  for (int i = 0; i < 2; i++) {
    do {
      hotkey = rand() % (hot_records/ physical_partitions);
    } while (keys_set.count(hotkey));
    keys_set.insert(hotkey);
    keys.push_back(hotkey);
  }

  GetRandomKeys(&keys,
                kRWSetSize-2,
                hot_records/ physical_partitions,
                kDBSize/WORKER_THREADS -1);

  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(txn_id % WORKER_THREADS, *it);
    //txn->AddReadSet(0, *it);
  }
}


// Create a non-dependent single-partition transaction
void PhysicalTraditional_Microbenchmark::MicroTxnMP(Txn* txn, uint64_t txn_id, int mp) const{
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  int physical_partitions = WORKER_THREADS;

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
        current_partition = rand() % physical_partitions;
      }
    } while (partitions.count(current_partition));
    partitions.insert(current_partition);

    // Add hot keys
    if (i <= 1) {
      uint64_t hot_key = rand() % (hot_records/ physical_partitions);
      txn->AddReadWriteSet(current_partition, hot_key);
      size--;
    }

    if (size > 0) {
      vector<uint64_t> keys;
      GetRandomKeys(&keys,
                  size,
                  hot_records/ physical_partitions,
                  kDBSize/WORKER_THREADS -1);


      for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
        txn->AddReadWriteSet(current_partition, *it);
        //txn->AddReadSet(0, *it);
      }
    }
  }
}


// Create a non-dependent single-partition transaction
void PhysicalTraditional_Microbenchmark::MicroTxnRandom(Txn* txn, uint64_t txn_id) const{
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(MICROTXN_SP);
  txn->SetTxnStatus(ACTIVE);

  int physical_partitions = WORKER_THREADS;

  int first = txn_id % WORKER_THREADS;
  int second;
  do {
    second = rand() % physical_partitions;
  } while (second == first);

  // Insert set of kRWSetSize - 2 random cold keys from specified partition into
  // read/write set.
  vector<uint64_t> keys;
  set<uint64_t> keys_set;

  // Add hot keys to read/write set.
  int hotkey;
  for (int i = 0; i < 1; i++) {
    do {
      hotkey = rand() % (hot_records/ physical_partitions);
    } while (keys_set.count(hotkey));
    keys_set.insert(hotkey);
    keys.push_back(hotkey);
  }

  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(first, *it);
    //txn->AddReadSet(0, *it);
  }

  keys.clear();
  keys_set.clear();

  for (int i = 0; i < 1; i++) {
    do {
      hotkey = rand() % (hot_records/ physical_partitions);
    } while (keys_set.count(hotkey));
    keys_set.insert(hotkey);
    keys.push_back(hotkey);
  }

  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(second, *it);
    //txn->AddReadSet(0, *it);
  }

  keys.clear();
  keys_set.clear();

  GetRandomKeys(&keys,
                kRWSetSize-2,
                hot_records/ physical_partitions,
                kDBSize/WORKER_THREADS -1);


  for (vector<uint64_t>::iterator it = keys.begin(); it != keys.end(); ++it) {
    txn->AddReadWriteSet(rand()%physical_partitions, *it);
    //txn->AddReadSet(0, *it);
  }

}

void PhysicalTraditional_Microbenchmark::NewTxn(Txn* txn, uint64_t txn_id) const {
    if (percent_mp_ == 0) {
      MicroTxnSP(txn, txn_id);
    } else if (percent_mp_ >= 2 && percent_mp_ <=10) {
      MicroTxnMP(txn, txn_id, percent_mp_);
    } else if (percent_mp_ == 11) {
      MicroTxnRandom(txn, txn_id);
    }

    /**if (rand() % 100 < percent_mp_) {
      MicroTxnMP(txn, txn_id, 2);
    } else {
      MicroTxnSP(txn, txn_id);
    }**/
}

uint32_t PhysicalTraditional_Microbenchmark::GetTableNum() const {
  return WORKER_THREADS;
}

int PhysicalTraditional_Microbenchmark::Execute(Txn* txn) const {
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

int PhysicalTraditional_Microbenchmark::Execute2(LockUnit* lock_unit) const {
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

int PhysicalTraditional_Microbenchmark::Rollback(LockUnit* lock_unit) const {
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
//std::cout<<lock_unit->txn->GetTxnId()<<": Rollback\n"<<std::flush;
  return 0;
}

uint32_t PhysicalTraditional_Microbenchmark::LookupPartition(const uint64_t& key) const {
  return 0;
}

void PhysicalTraditional_Microbenchmark::InitializeStorage() const {

}

void PhysicalTraditional_Microbenchmark::InitializeTable(uint32_t table_id) const {
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



