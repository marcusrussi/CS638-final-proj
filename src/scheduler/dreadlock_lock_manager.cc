
#include "scheduler/dreadlock_lock_manager.h"


Dreadlock_LockManager::Dreadlock_LockManager(uint32_t table_num) {
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
  } else if (table_num_ == 9) {
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


  pthread_mutex_init(&test, NULL);
}


void Dreadlock_LockManager::Setup(int worker_id) {
  thread_digests[worker_id] = new Digest(worker_id);
  keys_freelist[worker_id] = new Keys_Freelist();
  lockrequest_freelist[worker_id] = new Lockrequest_Freelist();
}

// Return 0 while acquired the lock
// Return 1 while can not acquire the lock
// Return -1 while found deadlocks
int Dreadlock_LockManager::Lock(LockUnit* lock_unit) {

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

  thread_digests[txn->GetWorkerId()]->SetSingle();

  // Already got the key_list
  LockRequest* lock_request = lockrequest_freelist[txn->GetWorkerId()]->Get();
  LockRequest* head_request = NULL;
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
      
      head_request = key_list->head;
      acquired = 1;

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
      acquired = 0;
      do {
        if (travel->mode == WRITE) {
          acquired = 1;
          break;
        }
        travel = travel->next;
      }while(travel != NULL);
      head_request = key_list->head;
    }
  }

  if (acquired == 0) {
  pthread_mutex_unlock(&(bucket->latch));
    return acquired;
  } else {
//pthread_mutex_lock(&test);
//std::cout<<txn->GetTxnId()<<": Can not got the lock.key:"<<key<<".\n"<<std::flush;
//pthread_mutex_unlock(&test);

//pthread_mutex_lock(&test);
//assert(head_request->txn != NULL);
//std::cout<<txn->GetTxnId()<<": Need spin,prev:"<<head_request->txn->GetTxnId()<<".\n"<<std::flush;
//pthread_mutex_unlock(&test);
    // Need to spin
    int worker_id = txn->GetWorkerId();
    assert(head_request != NULL);

    Digest* head_digest = thread_digests[head_request->txn->GetWorkerId()];
    Digest* me_digest = thread_digests[worker_id];
    me_digest->SetUnion(head_digest->GetDigest());
  pthread_mutex_unlock(&(bucket->latch));

//double begin_time = GetTime();
 // pthread_mutex_lock(&test);
//std::cout<<txn->GetTxnId()<<":I am here.\n"<<std::flush;
//  pthread_mutex_unlock(&test);
assert(key_list->head != NULL);
    while (key_list->head != lock_request) {

if (key_list->head->txn != NULL) {
head_digest = thread_digests[key_list->head->txn->GetWorkerId()];
}

/**if (GetTime() - begin_time > 0.1) {
  pthread_mutex_lock(&test);
std::cout<<txn->GetTxnId()<<":^^^^Always running^^^^, worker:"<<txn->GetWorkerId()<<".\n"<<std::flush;
assert(key_list->head != NULL);
assert(key_list->head->txn != NULL);
  pthread_mutex_unlock(&test);
}**/
      if (head_digest->Contains(worker_id)) {

        // Deadlocks
  pthread_mutex_lock(&(bucket->latch));
if (key_list->head == lock_request) {
  me_digest->ClearAndSetSingle();
  pthread_mutex_unlock(&(bucket->latch));
  return 0;
}

//assert(key_list->head->txn != NULL);

//  pthread_mutex_lock(&test);
//std::cout<<txn->GetTxnId()<<":------deadlock.-----\n"<<std::flush;
//  pthread_mutex_unlock(&test);
me_digest->ClearAndSetSingle();
//assert(!(lock_request-> prev == NULL && lock_request->next == NULL));
        if (lock_request->prev != NULL) {
          lock_request->prev->next = lock_request->next;
        }

        if (key_list->tail == lock_request) {
          key_list->tail = lock_request->prev;
          if (key_list->tail != NULL)
            key_list->tail->next = NULL;      
        }
        
        if (key_list->head == lock_request) {
          key_list->head = lock_request->next;       
        }

        if (lock_request->next != NULL) {
          lock_request->next->prev = lock_request->prev;
        }

        lockrequest_freelist[txn->GetWorkerId()]->Put(lock_request);
 // pthread_mutex_lock(&test);
//std::cout<<txn->GetTxnId()<<":------Finish a deadlock.-----\n"<<std::flush;
//  pthread_mutex_unlock(&test);

  pthread_mutex_unlock(&(bucket->latch));
        return -1;
      }

if (key_list->head->txn != NULL) {
head_digest = thread_digests[key_list->head->txn->GetWorkerId()];
}

      if (key_list->head != lock_request) {
        me_digest->SetUnion(head_digest->GetDigest());
      } else {
        break;
      }
    }
  
  assert(key_list->head == lock_request);
  // Now Got the lock
  me_digest->ClearAndSetSingle();

//pthread_mutex_lock(&test);
//std::cout<<txn->GetTxnId()<<":Now lock.\n"<<std::flush;
//pthread_mutex_unlock(&test);

    return 0;
  
  }
}


void Dreadlock_LockManager::Release(Txn* txn) {
  thread_digests[txn->GetWorkerId()]->Clear();
  for (uint32_t i = 0; i < txn->ReadSetSize(); i++) {
    Release(txn->GetReadSet(i), txn);
  }

  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    Release(txn->GetReadWriteSet(i), txn);
  }
}


void Dreadlock_LockManager::Release(const TableKey table_key, Txn* txn) {
  uint64_t key = table_key.key;
  uint64_t table_id = table_key.table_id;


  Traditional_Bucket* bucket = lock_table_ + Hash(key) % table_buckets[table_id] + table_sum_buckets[table_id];
  pthread_mutex_lock(&(bucket->latch));

//pthread_mutex_lock(&test);
//std::cout<<txn->GetTxnId()<<": Begin releasing lock.\n"<<std::flush;
//pthread_mutex_unlock(&test);


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
//pthread_mutex_lock(&test);
//std::cout<<txn->GetTxnId()<<": Finish releasing, pass to:"<<following_locks->txn->GetTxnId()<<".\n"<<std::flush;
//pthread_mutex_unlock(&test);

      }

      // If a sequence of read lock requests follows, grant all of them.
      for (; following_locks != NULL && following_locks->mode == READ; following_locks = following_locks->next) {

      }

    } else if (!write_requests_precede_target && target->mode == WRITE && following_locks->mode == READ) {  // (c)
      // If a sequence of read lock requests follows, grant all of them.
      for (; following_locks != NULL && following_locks->mode == READ; following_locks = following_locks->next) {
      }
    } // end "else if"
  } 

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

void Dreadlock_LockManager::DeadlockRelease(LockUnit* lock_unit) {
  Txn* txn = lock_unit->txn;
thread_digests[txn->GetWorkerId()]->Clear();
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

//std::cout<<txn->GetTxnId()<<": Finish DeadlockRelease.\n"<<std::flush;
}


void Dreadlock_LockManager::RemoveToWaitforgraph(uint32_t worker, uint64_t txn1) {
}
