// Data structure for lock manager
//

#ifndef _DB_SCHEDULER_LOCK_MANAGER_H_
#define _DB_SCHEDULER_LOCK_MANAGER_H_

#include "common/utils.h"
#include "applications/application.h"

extern LatchFreeQueue<Txn*>* lm_messages[WORKER_THREADS][WORKER_THREADS];

enum LockMode {
  UNLOCKED = 0,
  READ = 1,
  WRITE = 2,
};

struct LockUnit {
  Txn* txn;
  uint64_t table_id;
  uint64_t key;
  LockMode mode;
};

  struct LockRequest {
    Txn* txn;  // Pointer to txn requesting the lock.
    LockMode mode;  // Specifies whether this is a read or write lock request.
    LockRequest* next;
    LockRequest* prev;
    SubTxn* next_sub_txn;
    SubTxn* current_sub_txn;
    //char pad[CACHE_LINE - sizeof(void *)*3 - sizeof(LockMode)];
  };

  struct KeysList {
    uint64_t key;
    KeysList* next;
    KeysList* prev;
    LockRequest* head;
    LockRequest* tail;

    //char pad[CACHE_LINE - sizeof(void *)*4 - sizeof(uint64_t)];
  };

class Keys_Freelist {
  private:
    KeysList* freelist;
    KeysList* tail;
    //Mutex* latch;
  public:

   Keys_Freelist() {
     freelist = (KeysList*)malloc(sizeof(KeysList)*KEYS_FREELIST_CNT );
     memset(freelist, 0x00, sizeof(KeysList)*KEYS_FREELIST_CNT );

     for (uint64_t i = 0; i < KEYS_FREELIST_CNT  - 1; i++) {
       freelist[i].next = &freelist[i+1];
     }
     freelist[KEYS_FREELIST_CNT  - 1].next = NULL;
     tail = &freelist[KEYS_FREELIST_CNT - 1];

   }

   KeysList* Get() {
  //pthread_mutex_lock(&(latch->mutex_));
     assert(freelist != NULL);
     KeysList* ret = freelist;
     freelist = freelist->next;
     ret->next = NULL;
     ret->prev = NULL;
     ret->head = NULL;
     ret->tail = NULL;
  //pthread_mutex_unlock(&(latch->mutex_));
     return ret;
   }

   void Put(KeysList* keys_list) {
  //pthread_mutex_lock(&(latch->mutex_));
     tail->next = keys_list;
     tail = keys_list;
     keys_list->next = NULL;
     keys_list->prev = NULL;
     keys_list->head = NULL;
     keys_list->tail = NULL;
  //pthread_mutex_unlock(&(latch->mutex_));
   }
};

class Lockrequest_Freelist {
  private:
    LockRequest* freelist;
    LockRequest* tail;
    //Mutex* latch;

  public:

   Lockrequest_Freelist() {
     freelist = (LockRequest*)malloc(sizeof(LockRequest)*LOCKREQUEST_FREELIST_CNT);
     memset(freelist, 0x00, sizeof(LockRequest)*LOCKREQUEST_FREELIST_CNT);

     for (uint64_t i = 0; i < LOCKREQUEST_FREELIST_CNT - 1; i++) {
       freelist[i].next = &freelist[i+1];
     }
     freelist[LOCKREQUEST_FREELIST_CNT - 1].next = NULL;
     tail = &freelist[LOCKREQUEST_FREELIST_CNT - 1];
   }

   LockRequest* Get() {
  //pthread_mutex_lock(&(latch->mutex_));
     assert(freelist != NULL);
     LockRequest* ret = freelist;
     freelist = freelist->next;
     ret->next = NULL;
     ret->prev = NULL;
     ret->txn = NULL;
     ret->next_sub_txn = NULL;
  //pthread_mutex_unlock(&(latch->mutex_));
     return ret;
   }

   void Put(LockRequest* lock_list) {
  //pthread_mutex_lock(&(latch->mutex_));
     tail->next = lock_list;
     tail = lock_list;
     lock_list->next = NULL;
     lock_list->prev = NULL;
     lock_list->txn = NULL;
     lock_list->next_sub_txn = NULL;
  //pthread_mutex_unlock(&(latch->mutex_));
   }
};

  struct Bucket {
    KeysList* head;
    //char pad[CACHE_LINE - sizeof(void *)];
  };

  struct Traditional_Bucket {
    KeysList* head;
    pthread_mutex_t latch;
    Keys_Freelist* keys_freelist;
    //char pad[CACHE_LINE - 2*sizeof(void *) - sizeof(latch)];
  };



class HashEntry_Lm {
 public:
   uint64_t key;
   uint32_t value;
   HashEntry_Lm(uint64_t key, uint32_t value) {
     this->key = key;
     this->value = value;
   }

   uint64_t GetKey() {
     return key;
   }

   uint32_t getValue() {
     return value;
   }
};


class HashMap_Lm {
 private:
   HashEntry_Lm* table;
   uint32_t size;

 public:
   HashMap_Lm() {
     table = (HashEntry_Lm*)malloc(sizeof(HashEntry_Lm) * HASH_MAP_LM_SIZE);
     for (int i = 0; i < HASH_MAP_LM_SIZE; i++) {
       table[i].key = MAX_UINT64;
       table[i].value = 0;
     }
     size = 0;
   }

   uint64_t Get(uint64_t key) {
     int hash = Hash(key) % HASH_MAP_LM_SIZE;
     while (table[hash].key != key) {
       hash = (hash + 1) % HASH_MAP_LM_SIZE;
     }

     return table[hash].value;
   }

   void Put(uint64_t key, uint32_t value) {
     int hash = Hash(key) % HASH_MAP_LM_SIZE;
     while (table[hash].key != MAX_UINT64) {
       hash = (hash + 1) % HASH_MAP_LM_SIZE;
     }
     table[hash].key = key;
     table[hash].value = value;
     size++;
   }

   void Erase(uint64_t key) {
     int hash = Hash(key) % HASH_MAP_LM_SIZE;
     while (table[hash].key != key) {
       hash = (hash + 1) % HASH_MAP_LM_SIZE;
     }
     table[hash].key = MAX_UINT64;
     table[hash].value = 0;
     size--;
   }

   uint32_t Size() {
     return size;
   }

   bool DecreaseAndIfZero(uint64_t key) {
//std::cout<<"Begin of DecreaseAndIfZero.\n"<<std::flush;
     int hash = Hash(key) % HASH_MAP_LM_SIZE;
     while (table[hash].key != key) {
       hash = (hash + 1) % HASH_MAP_LM_SIZE;
     }
     //assert(table[hash].key == key);
     table[hash].value--;
//std::cout<<"Value is: "<<table[hash].value<<".\n"<<std::flush;
     if (table[hash].value == 0) {
       table[hash].key = MAX_UINT64;
       table[hash].value = 0;
       size--;
       return true;
     } else {
       return false;
     }
   }

   ~HashMap_Lm() {
     delete  table;
   }
};

class HashEntry_Waitforgraph {
 public:
   uint64_t key;
   uint64_t value;
   HashEntry_Waitforgraph* next;

   HashEntry_Waitforgraph(uint64_t key, uint64_t value) {
     this->key = key;
     this->value = value;
     next = NULL;
   }

   uint64_t GetKey() {
     return key;
   }

   uint64_t getValue() {
     return value;
   }
};

class HashEntry_FreeList {
  private:
   HashEntry_Waitforgraph* freeList_;
   HashEntry_Waitforgraph* tail_;

  public:
   HashEntry_FreeList(uint64_t freeList_cnt) {
     char* data = (char*)malloc(freeList_cnt * sizeof(HashEntry_Waitforgraph));
     memset(data, 0x0, freeList_cnt * sizeof(HashEntry_Waitforgraph));

     for(uint64_t i = 0; i < freeList_cnt; i++) {
       ((HashEntry_Waitforgraph*)(data + i * sizeof(HashEntry_Waitforgraph)))->next = (HashEntry_Waitforgraph*)(data + (i + 1) * sizeof(HashEntry_Waitforgraph));
     }
     ((HashEntry_Waitforgraph*)(data + (freeList_cnt - 1) * sizeof(HashEntry_Waitforgraph)))->next = NULL;
     freeList_ = (HashEntry_Waitforgraph*)data;
     tail_ = (HashEntry_Waitforgraph*)(data + (freeList_cnt - 1) * sizeof(HashEntry_Waitforgraph));
   }

   HashEntry_Waitforgraph* GetHashEntry() {
     assert(freeList_ != NULL);
     HashEntry_Waitforgraph* rec = freeList_;
     freeList_ = freeList_->next;
     rec->next = NULL;
     return rec;
   }

   void PutHashEntry(HashEntry_Waitforgraph* rec) {
     tail_->next = rec;
     tail_ = rec;
     rec->next = NULL;
   }
};

class HashMap_Waitforgraph {
  private:
   HashEntry_Waitforgraph** buckets_;
   HashEntry_FreeList* freeList_;
   uint32_t size;

  public:
   HashMap_Waitforgraph() {
     buckets_ = (HashEntry_Waitforgraph**)malloc(sizeof(HashEntry_Waitforgraph*)* HASH_MAP_WAITFORGRAPH);
     memset(buckets_, 0x0, HASH_MAP_WAITFORGRAPH * sizeof(HashEntry_Waitforgraph*));

     freeList_ = new HashEntry_FreeList(HASH_MAP_WAITFORGRAPH);
  }

  virtual void Put(uint64_t key, uint64_t value) {
     uint64_t index = Hash(key) % HASH_MAP_WAITFORGRAPH;

     HashEntry_Waitforgraph* rec = freeList_->GetHashEntry();
     rec->next = buckets_[index];
     rec->key = key;
     rec->value = value;
     buckets_[index] = rec;
  }

   virtual uint64_t Get(uint64_t key) {
     uint64_t index = Hash(key) % HASH_MAP_WAITFORGRAPH;

     HashEntry_Waitforgraph* rec = buckets_[index];
     while (rec != NULL && rec->key != key) {
       rec = rec->next;
     }
if (rec == NULL) {
spin_lock(&print_lock);
std::cout<<"----wrong, key:"<<key<<".\n"<<std::flush;
spin_unlock(&print_lock);
}
     assert(rec != NULL);
     return rec->value;
   }

   virtual bool Exist(uint64_t key) {
     uint64_t index = Hash(key) % HASH_MAP_WAITFORGRAPH;

     HashEntry_Waitforgraph* rec = buckets_[index];
     while (rec != NULL && rec->key != key) {
       rec = rec->next;
     }
     if (rec == NULL) {
       return false;
     } else {
       return true;
     }
   }

   virtual void Erase(uint64_t key) {
     uint64_t index = Hash(key) % HASH_MAP_WAITFORGRAPH;

     HashEntry_Waitforgraph* rec = buckets_[index];
     HashEntry_Waitforgraph* prev = NULL;

     while (rec != NULL && rec->key != key) {
       prev = rec;
       rec = rec->next;
     }
     assert(rec != NULL);
     if (rec == buckets_[index]) {
       buckets_[index] = rec->next;
     } else {
       prev->next = rec->next;
     }
     freeList_->PutHashEntry(rec);
   }
};


class Deadlock_LockManager {
 public:
  virtual ~Deadlock_LockManager() {}
  virtual int Lock(LockUnit* lock_unit) = 0;
  virtual void Release(Txn* txn) = 0;
  virtual void Release(const TableKey table_key, Txn* txn) = 0;
  virtual void DeadlockRelease(LockUnit* lock_unit) = 0;
  virtual void RemoveToWaitforgraph(uint32_t worker, uint64_t txn1) = 0;

  virtual void Setup(int worker_id) = 0;
};

struct SetArray_Element {
  SubTxn* subtxn;
  uint32_t start_table_id;
  uint64_t start_key;
};

class SetArray_txn {
 private:
   SetArray_Element* table;
   int size;

 public:
   SetArray_txn() {
     table = (SetArray_Element*)malloc(sizeof(SetArray_Element) * SET_ARRAY_MAX_SIZE);
     for (int i = 0; i < SET_ARRAY_MAX_SIZE; i++) {
       table[i].subtxn = NULL;
       table[i].start_table_id = 0;
       table[i].start_key = 0;
     }
     size = 0;
   }

   SetArray_Element* Pop(void) {
    return (size > 0) ? &table[--size] : NULL;
   }

   void Add(SubTxn* subtxn, uint32_t start_table_id_, uint64_t start_key_) {
    int i = 0;
    for (; i < size && i < SET_ARRAY_MAX_SIZE; i++) {
      if (table[i].subtxn == subtxn)
        return;
    }

    if (size == SET_ARRAY_MAX_SIZE)
      exit(1);

    table[i].subtxn = subtxn;
    table[i].start_table_id = start_table_id_;
    table[i].start_key = start_key_;

    size++;

    return;
   }

   ~SetArray_txn() {
     delete table;
   }
};


#endif  // _DB_SCHEDULER_LOCK_MANAGER_H_
