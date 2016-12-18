// Orthrus lock manager implementation
//

#include "scheduler/partitioned_lock_manager.h"


LockManager::LockManager(int lm_id, uint32_t table_num) {
  lm_id_ = lm_id;
  table_num_ = table_num;
  for (int i = 0; i < WORKER_THREADS; i++) {
    request_locks_queue_[i] = new LatchFreeQueue<SubTxn*>();
    acquired_locks_queue_[i] = new LatchFreeQueue<uint64_t>();
    release_locks_queue_[i] = new LatchFreeQueue<SubTxn*>();
  }

  for (int i = 0; i < LOCK_MANAGER_THREADS; i++) {
    if (i != lm_id_) {
      communication_receive_queue_[i] = new LatchFreeQueue<SubTxn*>(LM_QUEUE_SIZE);
    }
  }

  if (table_num_ == 1) {
    // For microbenchmark(YCSB)
    table_buckets[0] = BUCKET_SIZE;
    table_sum_buckets[0] = 0;

    // If this lock manager thread is the master thread for its partition,
    // initialize the lock table and associated mutexes
    if (lm_id % LM_THREADS_PER_PARTITION == 0) {
      lock_table_ = (Traditional_Bucket*)malloc(sizeof(Traditional_Bucket)*BUCKET_SIZE*table_num_);
      memset(lock_table_, 0x00, sizeof(Traditional_Bucket)*BUCKET_SIZE*table_num_);
      for (uint32_t i = 0; i < BUCKET_SIZE*table_num_; i++) {
        lock_table_[i].head = NULL;
        pthread_mutex_init(&(lock_table_[i].latch), NULL);
      }
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
    }

  }

  keys_freelist = new Keys_Freelist();
  lockrequest_freelist = new Lockrequest_Freelist();

  txn_wait = new HashMap_Lm();
  progressed_subtxns = new SetArray_txn();
}

void LockManager::Setup(PartitionedExecutor* scheduler) {
  for (int i = 0; i < LOCK_MANAGER_THREADS; i++) {
    if (i != lm_id_) {
      communication_send_queue_[i] = scheduler->lock_manager_[i]->communication_receive_queue_[lm_id_];
    }
  }

  int partition_master_id = lm_id_ - (lm_id_ % LM_THREADS_PER_PARTITION);
  if (partition_master_id != lm_id_) {
    lock_table_ = scheduler->lock_manager_[partition_master_id]->lock_table_;
  }
}

void LockManager::Lock(SubTxn* sub_txn) {
  Lock(sub_txn, false, 0, 0);
}

void LockManager::Lock(SubTxn* sub_txn, bool optimize, int start_tableid, int start_key) {
  uint32_t not_acquired = 0;
  bool acquire_locks = !optimize; // Whether or not we have already jumped ahead towards the first lock
                             // we know to not have acquired.
  Txn* txn = sub_txn->txn;
  // Handle read/write lock requests.

  for (uint32_t i = sub_txn->write_key_start; i < sub_txn->write_key_end; i++) {
    TableKey table_key = txn->GetReadWriteSet(i);
    uint64_t key = table_key.key;
    uint32_t table_id = table_key.table_id;

    if (!acquire_locks && table_id == start_tableid && key == start_key) {
      acquire_locks = true;
      continue;
    }

    if (!acquire_locks)
      continue;

    Traditional_Bucket* bucket =  lock_table_ + Hash(key) % table_buckets[table_id] + table_sum_buckets[table_id];
    pthread_mutex_lock(&(bucket->latch));
    // printf("txnid %d: locked bucket\n", txn->txn_id);
    KeysList* key_list;

    if (bucket->head == NULL) {

      key_list = keys_freelist->Get();
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

      } while (key_list != NULL);

      if (found == false) {
        key_list = keys_freelist->Get();
        key_list->key = key;
        key_list->prev = previous;
        previous->next = key_list;
      }
    }

    // Already got the key_list
    LockRequest* lock_request = lockrequest_freelist->Get();
    if (key_list->head == NULL) { // SUCCESS, nobody has the lock
      lock_request->txn = txn;
      lock_request->mode = WRITE;
      lock_request->next_sub_txn = sub_txn->next_sub_txn;
      lock_request->current_sub_txn = sub_txn;
      key_list->head = lock_request;
      key_list->tail = lock_request;
    } else { // someone has the lock
      if (key_list->head->txn == txn) {
        lockrequest_freelist->Put(lock_request);
      } else { // it's actually someone else, so do the usual
        key_list->tail->next = lock_request;
        lock_request->prev = key_list->tail;
        key_list->tail = lock_request;
        lock_request->txn = txn;
        lock_request->mode = WRITE;
        lock_request->next_sub_txn = sub_txn->next_sub_txn;
        lock_request->current_sub_txn = sub_txn;
        not_acquired++;

        // Since we weren't able to get a lock, we have to exit
        // so that things get acquired in order.
        pthread_mutex_unlock(&(bucket->latch));
        return;
      }
    }

    pthread_mutex_unlock(&(bucket->latch));
    // printf("txnid %d: unlocked bucket\n", txn->txn_id);
  }


   // Handle read lock requests.
  for (uint32_t i = sub_txn->read_key_start; i < sub_txn->read_key_end; i++) {
    TableKey table_key = txn->GetReadSet(i);
    uint64_t key = table_key.key;
    uint32_t table_id = table_key.table_id;

    if (!acquire_locks && table_id == start_tableid && key == start_key) {
      acquire_locks = true;
      continue;
    }

    if (!acquire_locks)
      continue;

    Traditional_Bucket* bucket =  lock_table_ + Hash(key) % table_buckets[table_id] + table_sum_buckets[table_id];
    pthread_mutex_lock(&(bucket->latch));
    // printf("txnid %d: locked bucket\n", txn->txn_id);
    KeysList* key_list;

    if (bucket->head == NULL) {
      key_list = keys_freelist->Get();
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
        key_list = keys_freelist->Get();
        key_list->key = key;
        key_list->prev = previous;
        previous->next = key_list;
      }
    }

    // Already got the key_list
    LockRequest* lock_request = lockrequest_freelist->Get();
    if (key_list->head == NULL) {
      lock_request->txn = txn;
      lock_request->mode = READ;
      lock_request->next_sub_txn = sub_txn->next_sub_txn;
      lock_request->current_sub_txn = sub_txn;
      key_list->head = lock_request;
      key_list->tail = lock_request;
    } else { // Some transaction, or group of transactions, have a lock
      key_list->tail->next = lock_request;
      lock_request->prev = key_list->tail;
      key_list->tail = lock_request;
      lock_request->txn = txn;
      lock_request->mode = READ;
      lock_request->next_sub_txn = sub_txn->next_sub_txn;
      lock_request->current_sub_txn = sub_txn;

      lock_request = key_list->head;
      do {
        if (lock_request->mode == WRITE) {
          not_acquired++;

          // break;
          // New: Must immediately exit to prevent any additional locks
          // from being acquired
          pthread_mutex_unlock(&(bucket->latch));
          return;
        }
        if (lock_request->txn == txn) {
          // remove ourselves from the tail if this isn't the tail
          if (key_list->tail != lock_request) { // We are not at the tail
            // Erase the tail
            lock_request = key_list->tail;
            key_list->tail = lock_request->prev;
            key_list->tail->next = NULL;

            // Recycle the lock request
            lockrequest_freelist->Put(lock_request);
            break;
          }
        }
        lock_request = lock_request->next;
      }while(lock_request != NULL);
    }

    pthread_mutex_unlock(&(bucket->latch));
    // printf("txnid %d: unlocked bucket\n", txn->txn_id);
  }

  // printf("txnid %d: acquired all locks\n", txn->txn_id);

  // Record and return the number of locks that the txn is blocked on.
  // if (not_acquired > 0) {
    // txn_wait->Put(txn->GetTxnId(), not_acquired);
  // } else {
    if (sub_txn->next_sub_txn == NULL) {
      bool not_full;
      do {
        not_full = acquired_locks_queue_[txn->GetWorkerId()]->Push(txn->GetTxnId());
      } while (not_full == false);
    } else {
      // pass the txn to the next lock manager that is involved in this txn
      //communication_send_queue_[sub_txn->next_sub_txn->lm_id]->Push(sub_txn->next_sub_txn);
      communication_send_queue_[sub_txn->next_lm_id]->Push(sub_txn->next_sub_txn);
    }
  // }
}


void LockManager::Release(SubTxn* sub_txn) {
  Txn* txn = sub_txn->txn;
//std::cout<<"Beginning of Release method. Txn id is: "<<txn->GetTxnId()<<"\n"<<std::flush;
  for (uint32_t i = sub_txn->write_key_start; i < sub_txn->write_key_end; i++) {
    Release(txn->GetReadWriteSet(i), txn);
  }

  for (uint32_t i = sub_txn->read_key_start; i < sub_txn->read_key_end; i++) {
    Release(txn->GetReadSet(i), txn);
  }

  SetArray_Element progressing_subtxn = NULL;
  while ((progressing_subtxn = progressed_subtxns->Pop()) != NULL)
    Lock(progressing_subtxn.subtxn, true, progressing_subtxn.start_table_id, progressing_subtxn.start_key);
}


void LockManager::Release(const TableKey table_key, Txn* txn) {
  uint64_t key = table_key.key;
  uint32_t table_id = table_key.table_id;

  Traditional_Bucket* bucket =  lock_table_ + Hash(key) % table_buckets[table_id] + table_sum_buckets[table_id];
  pthread_mutex_lock(&(bucket->latch));
  // printf("txnid %d: locked bucket\n", txn->txn_id);
  KeysList* key_list = bucket->head;

  assert(key_list != NULL);

  do {
    if (key_list->key == key) {
      break;
    }
    key_list = key_list->next;
  }while (key_list != NULL);

  LockRequest* target = key_list->head;
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

  LockRequest* following_locks = target->next;

//if (target == key_list->head)
//std::cout<<"This lock is the head.\n"<<std::flush;
//if (target->next == NULL)
//std::cout<<"This lock is the only one.\n"<<std::flush;
  if (following_locks != NULL) {
    // Grant subsequent request(s) if:
    //  (a) The canceled request held a write lock.
    //  (b) The canceled request held a read lock ALONE.
    //  (c) The canceled request was a write request preceded only by read
    //      requests and followed by one or more read requests.
    if (target == key_list->head && (target->mode == WRITE || (target->mode == READ && following_locks->mode == WRITE))) {  // (a) or (b)

      // If a write lock request follows, grant it.
      if (following_locks->mode == WRITE) {
        progressed_subtxns->Add(following_locks->current_sub_txn, table_id, key);
        // if (txn_wait->DecreaseAndIfZero(following_locks->txn->GetTxnId()) == true) {
        //   if (following_locks->next_sub_txn == NULL) {
        //     bool not_full;
        //     do {
        //       not_full = acquired_locks_queue_[following_locks->txn->GetWorkerId()]->Push(following_locks->txn->GetTxnId());
        //     } while (not_full == false);
        //   } else {
        //     communication_send_queue_[following_locks->next_sub_txn->lm_id]->Push(following_locks->next_sub_txn);
        //   }
        // }

      }

      // If a sequence of read lock requests follows, grant all of them.
      for (; following_locks != NULL && following_locks->mode == READ; following_locks = following_locks->next) {
        progressed_subtxns->Add(following_locks->current_sub_txn, table_id, key);
        // if (txn_wait->DecreaseAndIfZero(following_locks->txn->GetTxnId()) ) {
        //   if (following_locks->next_sub_txn == NULL) {
        //     bool not_full;
        //     do {
        //       not_full = acquired_locks_queue_[following_locks->txn->GetWorkerId()]->Push(following_locks->txn->GetTxnId());
        //     } while (not_full == false);
        //   } else {
        //     communication_send_queue_[following_locks->next_sub_txn->lm_id]->Push(following_locks->next_sub_txn);
        //   }
        // }
      }

    } else if (!write_requests_precede_target && target->mode == WRITE && following_locks->mode == READ) {  // (c)
      // If a sequence of read lock requests follows, grant all of them.
      for (; following_locks != NULL && following_locks->mode == READ; following_locks = following_locks->next) {
        progressed_subtxns->Add(following_locks->current_sub_txn, table_id, key);
        // if (txn_wait->DecreaseAndIfZero(following_locks->txn->GetTxnId()) ) {
        //   if (following_locks->next_sub_txn == NULL) {
        //     bool not_full;
        //     do {
        //       not_full = acquired_locks_queue_[following_locks->txn->GetWorkerId()]->Push(following_locks->txn->GetTxnId());
        //     } while (not_full == false);
        //   } else {
        //     communication_send_queue_[following_locks->next_sub_txn->lm_id]->Push(following_locks->next_sub_txn);
        //   }
        // }
      }
    } // end "else if"
  } // end "if"

  if (target->prev == NULL && target->next == NULL) {
  // Need to delete the key_list
    lockrequest_freelist->Put(target);
    if (key_list->prev != NULL) {
      key_list->prev->next = key_list->next;
    } else {
      bucket->head = key_list->next;
    }
    if (key_list->next != NULL) {
      key_list->next->prev = key_list->prev;
    }
    keys_freelist->Put(key_list);
  } else if (target->prev == NULL && target->next != NULL){
    key_list->head = target->next;
    target->next->prev = NULL;
    lockrequest_freelist->Put(target);
  } else if (target->prev != NULL && target->next == NULL) {
    key_list->tail = target->prev;
    target->prev->next = NULL;
    lockrequest_freelist->Put(target);
  } else if (target->prev != NULL && target->next != NULL) {
    target->prev->next = target->next;
    target->next->prev = target->prev;
    lockrequest_freelist->Put(target);
  }

  pthread_mutex_unlock(&(bucket->latch));
  // printf("txnid %d: unlocked bucket\n", txn->txn_id);
}

