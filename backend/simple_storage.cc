//
// A simple implementation of the storage interface.

#include "backend/simple_storage.h"


SimpleStorage::SimpleStorage() {
  table_cnt_ = 0;

  tables_ = (Table**)malloc(sizeof(Table*)*MAX_TABLE_CNT);
  memset(tables_, 0x0, MAX_TABLE_CNT * sizeof(Table*));
  for (uint32_t i = 0; i < MAX_TABLE_CNT; i++) {
    tables_[i] = NULL;
  }
  
  // For TPC-C
  if (MAX_TABLE_CNT == 9) {
    NewOrder_tables_ = (Table**)malloc(sizeof(Table*)*WORKER_THREADS);
    memset(NewOrder_tables_, 0x0, WORKER_THREADS * sizeof(Table*));
    for (uint32_t i = 0; i < WORKER_THREADS; i++) {
      NewOrder_tables_[i] = NULL;
    }

    Order_tables_ = (Table**)malloc(sizeof(Table*)*WORKER_THREADS);
    memset(Order_tables_, 0x0, WORKER_THREADS * sizeof(Table*));
    for (uint32_t i = 0; i < WORKER_THREADS; i++) {
      Order_tables_[i] = NULL;
    }

    OrderLine_tables_ = (Table**)malloc(sizeof(Table*)*WORKER_THREADS);
    memset(OrderLine_tables_, 0x0, WORKER_THREADS * sizeof(Table*));
    for (uint32_t i = 0; i < WORKER_THREADS; i++) {
      OrderLine_tables_[i] = NULL;
    }

    History_tables_ = (Table**)malloc(sizeof(Table*)*WORKER_THREADS);
    memset(History_tables_, 0x0, WORKER_THREADS * sizeof(Table*));
    for (uint32_t i = 0; i < WORKER_THREADS; i++) {
      History_tables_[i] = NULL;
    }
  }

}

void SimpleStorage::NewTable(uint64_t table_id, uint64_t bucket_cnt, uint64_t freeList_cnt, uint64_t value_size) {
  assert(tables_[table_id] == NULL);
  table_cnt_++;
  assert(table_cnt_ <= MAX_TABLE_CNT);
 
  // For TPC-C
  if (MAX_TABLE_CNT == 9) {
    if (table_id == 3) {
      for (int i = 0; i < WORKER_THREADS; i++) {
        History_tables_[i] = new Table(table_id, bucket_cnt, freeList_cnt, value_size);
      }
    } else if (table_id == 4) {
      for (int i = 0; i < WORKER_THREADS; i++) {
        NewOrder_tables_[i] = new Table(table_id, bucket_cnt, freeList_cnt, value_size);
      }
    } else if (table_id == 5) {
      for (int i = 0; i < WORKER_THREADS; i++) {
        Order_tables_[i] = new Table(table_id, bucket_cnt, freeList_cnt, value_size);
      }
    } else if (table_id == 6) {
      for (int i = 0; i < WORKER_THREADS; i++) {
        OrderLine_tables_[i] = new Table(table_id, bucket_cnt, freeList_cnt, value_size);
      }
    } else {
      tables_[table_id] = new Table(table_id, bucket_cnt, freeList_cnt, value_size);
    }
  } else {
    tables_[table_id] = new Table(table_id, bucket_cnt, freeList_cnt, value_size); 
  }
}

void* SimpleStorage::ReadRecord(const uint32_t table_id, const uint64_t key) {
  assert(tables_[table_id] != NULL);
  return tables_[table_id]->Get(key);
}

void SimpleStorage::PutRecord(const uint32_t table_id, const uint64_t key, void* value) {
  assert(tables_[table_id] != NULL);
  tables_[table_id]->Put(key, value); 
}

void SimpleStorage::DeleteRecord(const uint32_t table_id, const uint64_t key) {
  assert(tables_[table_id] != NULL);
  tables_[table_id]->Delete(key);
}

void SimpleStorage::InsertRecord(const uint32_t worker_id, const uint32_t table_id, const uint64_t key, void* value) {
  /**if (MAX_TABLE_CNT == 9) {
    if (table_id == 3) {
      History_tables_[worker_id]->Put(key, value);
    } else if (table_id == 4) {
      NewOrder_tables_[worker_id]->Put(key, value);
    } else if (table_id == 5) {
      Order_tables_[worker_id]->Put(key, value);
    } else if (table_id == 6) {
      OrderLine_tables_[worker_id]->Put(key, value);
    }
  } **/
  // Do something to simulate the insert
      int x = 1;
      for(int i = 0; i < 1100; i++) {
        x = x*x+1;
        x = x+10;
        x = x/10-2;
      }
}
