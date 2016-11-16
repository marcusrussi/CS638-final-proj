
#include "scheduler/waitforgraph_lock_manager.h"


Waitforgraph_LockManager::Waitforgraph_LockManager(uint32_t table_num) {
  table_num_ = table_num;
  
  if (table_num_ == 1) {
    // For microbenchmark
    table_buckets[0] = BUCKET_SIZE;
    table_sum_buckets[0] = 0;

    lock_table_ = (Traditional_Bucket*)malloc(sizeof(Traditional_Bucket)*BUCKET_SIZE*table_num_);
    memset(lock_table_, 0x00, sizeof(Traditional_Bucket)*BUCKET_SIZE*table_num_);
    for (uint32_t i = 0; i < BUCKET_SIZE*table_num_; i++) {
      lock_table_[i].head = NULL;
      pthread_mutex_init(&(lock_table_[i].latch), NULL);
    }
  } else if (table_num_ == 9){
    // For tpcc
    table_buckets[0] = WAREHOUSE_BUCKET;
    table_buckets[1] = DISTRICT_BUCKET;
    table_buckets[2] = CUSTOMER_BUCKET;
    table_buckets[3] = HISTORY_BUCKET;
    table_buckets[4] = NEWORDER_BUCKET;
    table_buckets[5] = ORDER_BUCKET;
    table_buckets[6] = ORDERLINE_BUCKET;
    table_buckets[7] = ITEM_BUCKET;
    table_buckets[8] = STOCK_BUCKET;

    for (int i = 0; i < 9; i++) {
      if (i == 0) {
        table_sum_buckets[0] = 0;
      } else {
        table_sum_buckets[i] = table_sum_buckets[i-1] + table_buckets[i-1];
      }
    }

    lock_table_ = (Traditional_Bucket*)malloc(sizeof(Traditional_Bucket)*(table_sum_buckets[8] + table_buckets[8]));
    memset(lock_table_, 0x00, sizeof(Traditional_Bucket)*(table_sum_buckets[8] + table_buckets[8]));
    for (uint32_t i = 0; i < (table_sum_buckets[8] + table_buckets[8]); i++) {
      lock_table_[i].head = NULL;
      pthread_mutex_init(&(lock_table_[i].latch), NULL);
    }
  } else {
    for (uint32_t i = 0; i < table_num_; i++) {
      table_buckets[i] = BUCKET_SIZE;
    }

    for (uint32_t i = 0; i < table_num_; i++) {
      if (i == 0) {
        table_sum_buckets[0] = 0;
      } else {
        table_sum_buckets[i] = table_sum_buckets[i-1] + table_buckets[i-1];
      }
    }

    lock_table_ = (Traditional_Bucket*)malloc(sizeof(Traditional_Bucket)*BUCKET_SIZE*table_num_);
    memset(lock_table_, 0x00, sizeof(Traditional_Bucket)*BUCKET_SIZE*table_num_);
    for (uint32_t i = 0; i < BUCKET_SIZE*table_num_; i++) {
      lock_table_[i].head = NULL;
      pthread_mutex_init(&(lock_table_[i].latch), NULL);
    }
  }

  pthread_mutex_init(&waitforgraph_latch, NULL);
  wait_for_graph = new HashMap_Waitforgraph();
}

void Waitforgraph_LockManager::Setup(int worker_id) {
  keys_freelist[worker_id] = new Keys_Freelist();
  lockrequest_freelist[worker_id] = new Lockrequest_Freelist();
}

// Return 0 while acquired the lock
// Return 1 while can not acquire the lock
// Return -1 while found deadlocks
int Waitforgraph_LockManager::Lock(LockUnit* lock_unit) {
  Txn* txn = lock_unit->txn;
  uint64_t table_id = lock_unit->table_id;
  uint64_t key = lock_unit->key;
  LockMode mode = lock_unit->mode;
  int acquired;
  
  Traditional_Bucket* bucket = lock_table_ + Hash(key) % table_buckets[table_id] + table_sum_buckets[table_id];
  pthread_mutex_lock(&(bucket->latch));

  KeysList* key_list;
  if (bucket->head == NULL) {
    key_list = keys_freelist[txn->GetWorkerId()]->Get();
    key_list->key = key;
    bucket->head = key_list;
  } else {
    key_list = bucket->head;
    bool found = false;
    KeysList* previous;
    do {
      if (key_list->key == key) {
        found = true;
        break;
      }
      previous = key_list;
      key_list = key_list->next;
    }while (key_list != NULL);
   
    if (found == false) {
      key_list = keys_freelist[txn->GetWorkerId()]->Get();
      key_list->key = key;
      key_list->prev = previous;
      previous->next = key_list;
    }
  }

  // Already got the key_list
  LockRequest* lock_request = lockrequest_freelist[txn->GetWorkerId()]->Get();
  if (mode == WRITE) {
    if (key_list->head == NULL) {
      lock_request->txn = txn;
      lock_request->mode = WRITE;
      key_list->head = lock_request;
      key_list->tail = lock_request;
      acquired = 0;
    } else {
      key_list->tail->next = lock_request; 
      lock_request->prev = key_list->tail;
      key_list->tail = lock_request;
      lock_request->txn = txn;
      lock_request->mode = WRITE;

  pthread_mutex_lock(&waitforgraph_latch);
      AddToWaitforgraph(txn->GetTxnId(), lock_request->prev->txn->GetTxnId());
      bool deadlock = CheckDeadlock(txn->GetTxnId());
      if (deadlock == true) {
        acquired = -1;
 /** pthread_mutex_lock(&test);
std::cout<<txn->GetWorkerId()<<":####Txn: "<<txn->GetTxnId()<<". key: "<<key<<". by:"<<lock_request->prev->txn->GetTxnId()<<".\n"<<std::flush;
  pthread_mutex_unlock(&test);  **/      
        RemoveToWaitforgraph(txn->GetTxnId());

        key_list->tail = lock_request->prev;
        key_list->tail->next = NULL;
        lockrequest_freelist[txn->GetWorkerId()]->Put(lock_request);
      } else {
 /** pthread_mutex_lock(&test);
std::cout<<txn->GetWorkerId()<<":!!!Txn: "<<txn->GetTxnId()<<". key: "<<key<<". by:"<<lock_request->prev->txn->GetTxnId()<<".\n"<<std::flush;
  pthread_mutex_unlock(&test);**/
        acquired = 1;
      }

  pthread_mutex_unlock(&waitforgraph_latch);

    }
  } else {
    if (key_list->head == NULL) {
      lock_request->txn = txn;
      lock_request->mode = READ;
      key_list->head = lock_request;
      key_list->tail = lock_request;
      acquired = 0;
    } else {
      key_list->tail->next = lock_request; 
      lock_request->prev = key_list->tail;
      key_list->tail = lock_request;
      lock_request->txn = txn;
      lock_request->mode = READ;

      LockRequest* travel = key_list->head;
      LockRequest* last_write;
      acquired = 0;
      do {
        if (travel->mode == WRITE) {
          last_write = travel;
          acquired = 1;
        }
        travel = travel->next;
      }while(travel != NULL);
  
   if (acquired == 1) {
   pthread_mutex_lock(&waitforgraph_latch);
      AddToWaitforgraph(txn->GetTxnId(), last_write->txn->GetTxnId());
      bool deadlock = CheckDeadlock(txn->GetTxnId());
      if (deadlock == true) {
        acquired = -1;
        RemoveToWaitforgraph(txn->GetTxnId());

        key_list->tail = lock_request->prev;
        key_list->tail->next = NULL;
        lockrequest_freelist[txn->GetWorkerId()]->Put(lock_request);
      }
  pthread_mutex_unlock(&waitforgraph_latch);
  }


    }
  }

  pthread_mutex_unlock(&(bucket->latch));
  return acquired;
}


void Waitforgraph_LockManager::Release(Txn* txn) {
  for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
    Release(txn->GetReadSet(i), txn);
  }

  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    Release(txn->GetReadWriteSet(i), txn);
  }
}


void Waitforgraph_LockManager::Release(const TableKey table_key, Txn* txn) {
  uint64_t key = table_key.key;
  uint64_t table_id = table_key.table_id;
  Traditional_Bucket* bucket = lock_table_ + Hash(key) % table_buckets[table_id] + table_sum_buckets[table_id];
  pthread_mutex_lock(&(bucket->latch));

  KeysList* key_list = bucket->head;
  //assert(key_list != NULL);

  do {
    if (key_list->key == key) {
      break;
    }
    key_list = key_list->next;
  }while (key_list != NULL);
  
  assert(key_list != NULL);

  LockRequest* target = key_list->head;
//assert(target != NULL);

  bool write_requests_precede_target = false;
  do {
    if (target->txn == txn) {
      break;
    }
    if (target->mode == WRITE) {
      write_requests_precede_target = true; 
    }
    target = target->next;
  } while(target != NULL);
//if (target == NULL)
//std::cout<<"******txn id is: "<<txn->GetTxnId()<<"********\n"<<std::flush;
assert(target != NULL);  
  LockRequest* following_locks = target->next;

  if (following_locks != NULL) {
    // Grant subsequent request(s) if:
    //  (a) The canceled request held a write lock.
    //  (b) The canceled request held a read lock ALONE.
    //  (c) The canceled request was a write request preceded only by read
    //      requests and followed by one or more read requests.
    if (target == key_list->head && (target->mode == WRITE || (target->mode == READ && following_locks->mode == WRITE))) {  // (a) or (b)
      // If a write lock request follows, grant it.
      if (following_locks->mode == WRITE) {
  pthread_mutex_lock(&waitforgraph_latch);
      RemoveToWaitforgraph(following_locks->txn->GetTxnId());
  pthread_mutex_unlock(&waitforgraph_latch);
        int not_full;
        do {
          not_full = lm_messages[txn->GetWorkerId()][following_locks->txn->GetWorkerId()]->Push(following_locks->txn);
        } while (not_full == false);


      }

      // If a sequence of read lock requests follows, grant all of them.
      for (; following_locks != NULL && following_locks->mode == READ; following_locks = following_locks->next) {
  pthread_mutex_lock(&waitforgraph_latch);
      RemoveToWaitforgraph(following_locks->txn->GetTxnId());
  pthread_mutex_unlock(&waitforgraph_latch);
        int not_full;
        do {
          not_full = lm_messages[txn->GetWorkerId()][following_locks->txn->GetWorkerId()]->Push(following_locks->txn);
        } while (not_full == false);
      }

    } else if (!write_requests_precede_target && target->mode == WRITE && following_locks->mode == READ) {  // (c)
      // If a sequence of read lock requests follows, grant all of them.
      for (; following_locks != NULL && following_locks->mode == READ; following_locks = following_locks->next) {
  pthread_mutex_lock(&waitforgraph_latch);
      RemoveToWaitforgraph(following_locks->txn->GetTxnId());
  pthread_mutex_unlock(&waitforgraph_latch);
        int not_full;
        do {
          not_full = lm_messages[txn->GetWorkerId()][following_locks->txn->GetWorkerId()]->Push(following_locks->txn);
        } while (not_full == false);
      }
    } // end "else if"
  } // end "if"

  if (target->prev == NULL && target->next == NULL) {
  // Need to delete the key_list
    lockrequest_freelist[txn->GetWorkerId()]->Put(target);
    if (key_list->prev != NULL) {
      key_list->prev->next = key_list->next;
    } else {
      bucket->head = key_list->next;
    }
    if (key_list->next != NULL) {
      key_list->next->prev = key_list->prev;
    }
    keys_freelist[txn->GetWorkerId()]->Put(key_list);
  } else if (target->prev == NULL && target->next != NULL){
    key_list->head = target->next;
    target->next->prev = NULL;
    lockrequest_freelist[txn->GetWorkerId()]->Put(target);
  } else if (target->prev != NULL && target->next == NULL) {
    key_list->tail = target->prev;
    target->prev->next = NULL;
    lockrequest_freelist[txn->GetWorkerId()]->Put(target);
  } else if (target->prev != NULL && target->next != NULL) {
    target->prev->next = target->next;
    target->next->prev = target->prev;
    lockrequest_freelist[txn->GetWorkerId()]->Put(target);
  }

  pthread_mutex_unlock(&(bucket->latch));
}

void Waitforgraph_LockManager::DeadlockRelease(LockUnit* lock_unit) {
  Txn* txn = lock_unit->txn;
  if (lock_unit->mode == READ) {
    for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
      TableKey table_key = txn->GetReadSet(i);
      if (table_key.table_id == lock_unit->table_id && table_key.key == lock_unit->key) {
        break;
      }
      Release(table_key, txn);
    }
  } else {
    for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
      TableKey table_key = txn->GetReadSet(i);
      Release(table_key, txn);
    }

    for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
      TableKey table_key = txn->GetReadWriteSet(i);
      if (table_key.table_id == lock_unit->table_id && table_key.key == lock_unit->key) {
        break;
      }
      Release(table_key, txn);
    }    
  }
}

void Waitforgraph_LockManager::AddToWaitforgraph(uint64_t txn1, uint64_t txn2) {
//assert(wait_for_graph->Exist(txn1) == false);
  wait_for_graph->Put(txn1, txn2);
}

void Waitforgraph_LockManager::RemoveToWaitforgraph(uint64_t txn1) {
  wait_for_graph->Erase(txn1);
}

bool Waitforgraph_LockManager::CheckDeadlock(uint64_t txn1) {
  bool deadlock = false;
  uint64_t next_txn = wait_for_graph->Get(txn1);
  while (true) {
    if (wait_for_graph->Exist(next_txn) == false) {
      deadlock = false;
      break;
    } else {
      next_txn = wait_for_graph->Get(next_txn);
      if (next_txn == txn1) {
        deadlock = true;
        break;
      }
    }
  }
  return deadlock;
}

void Waitforgraph_LockManager::RemoveToWaitforgraph(uint32_t worker, uint64_t txn1) {
}
