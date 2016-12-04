
#ifndef _DB_BACKEND_TABLE_H_
#define _DB_BACKEND_TABLE_H_

#include "common/types.h"
#include "common/utils.h"
#include <city.h>

struct TableRecord {
  struct TableRecord* next;
  uint64_t key;
  char value[0];
};

class TableRecord_FreeList {
  private:
   TableRecord* freeList_;
   TableRecord* tail_;
  
  public: 
   TableRecord_FreeList(uint64_t freeList_cnt, uint64_t record_size) {
     char* data = (char*)malloc(freeList_cnt * record_size);
     memset(data, 0x0, freeList_cnt * record_size);
     
     for(uint64_t i = 0; i < freeList_cnt; i++) {
       ((TableRecord*)(data + i * record_size))->next = (TableRecord*)(data + (i + 1) * record_size);
     }
     ((TableRecord*)(data + (freeList_cnt - 1) * record_size))->next = NULL;
     freeList_ = (TableRecord*)data;
     tail_ = (TableRecord*)(data + (freeList_cnt - 1) * record_size);
   }

   TableRecord* GetRecord() {
     assert(freeList_ != NULL);
     TableRecord* rec = freeList_;
     freeList_ = freeList_->next;
     rec->next = NULL;
     return rec;
   }

   void PutRecord(TableRecord* rec) {
     tail_->next = rec;
     tail_ = rec;
     rec->next = NULL;
   }
};

class Table {
 public:
   TableRecord** buckets_;
   TableRecord_FreeList* freeList_;

   uint64_t table_id_;
   uint64_t bucket_cnt_;
   uint64_t value_size_;
   uint64_t record_size_;

    
   Table() {}

   Table(uint64_t table_id, uint64_t bucket_cnt, uint64_t freeList_cnt, uint64_t value_size) {
     this->table_id_ = table_id;
     this->bucket_cnt_ = bucket_cnt;
     this->value_size_ = value_size;

     buckets_ = (TableRecord**)malloc(bucket_cnt_ * sizeof(TableRecord*));
     memset(buckets_, 0x0, bucket_cnt_ * sizeof(TableRecord*));

     uint64_t record_size_ = sizeof(TableRecord) + value_size_;
     freeList_ = new TableRecord_FreeList(freeList_cnt, record_size_);     

   }

   virtual void Put(uint64_t key, void* value) {
     uint64_t index = Hash(key) % bucket_cnt_;
     TableRecord* rec = freeList_->GetRecord();
     rec->next = buckets_[index];
     rec->key = key;
     memcpy(rec->value, value, value_size_);
     buckets_[index] = rec;
   }

   virtual void* Get(uint64_t key) {
     uint64_t index = Hash(key) % bucket_cnt_;
     TableRecord* rec = buckets_[index];
     while (rec != NULL && rec->key != key) {
       rec = rec->next;
     }
if (rec == NULL) {
  std::cout<<"Table id is: "<<table_id_<<"  key is:"<<key<<".\n"<<std::flush;
}
     assert(rec != NULL);
     return (void*)(rec->value);
   }

   virtual void Delete(uint64_t key) {
     uint64_t index = Hash(key) % bucket_cnt_;
     TableRecord* rec = buckets_[index];
     TableRecord* prev = NULL;

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
     freeList_->PutRecord(rec);
   }

};

class LatchTable : public Table{
  public:
   TableRecord** buckets_;
   TableRecord_FreeList* freeList_;

   uint64_t table_id_;
   uint64_t bucket_cnt_;
   uint64_t value_size_;
   uint64_t record_size_;
  
  pthread_mutex_t mutex_;

    
   LatchTable(uint64_t table_id, uint64_t bucket_cnt, uint64_t freeList_cnt, uint64_t value_size) {
     this->table_id_ = table_id;
     this->bucket_cnt_ = bucket_cnt;
     this->value_size_ = value_size;

     buckets_ = (TableRecord**)malloc(bucket_cnt_ * sizeof(TableRecord*));
     memset(buckets_, 0x0, bucket_cnt_ * sizeof(TableRecord*));

     uint64_t record_size_ = sizeof(TableRecord) + value_size_;
     freeList_ = new TableRecord_FreeList(freeList_cnt, record_size_);  
    
     pthread_mutex_init(&mutex_, NULL);   

   }

   virtual void Put(uint64_t key, void* value) {
    pthread_mutex_lock(&mutex_);

     uint64_t index = Hash(key) % bucket_cnt_;
     TableRecord* rec = freeList_->GetRecord();
     rec->next = buckets_[index];
     rec->key = key;
     memcpy(rec->value, value, value_size_);
     buckets_[index] = rec;

    pthread_mutex_unlock(&mutex_);
   }

   virtual void* Get(uint64_t key) {
    pthread_mutex_lock(&mutex_);

     uint64_t index = Hash(key) % bucket_cnt_;
     TableRecord* rec = buckets_[index];
     while (rec != NULL && rec->key != key) {
       rec = rec->next;
     }
     assert(rec != NULL);
    pthread_mutex_unlock(&mutex_);
     return (void*)(rec->value);
   }

   virtual void Delete(uint64_t key) {
    pthread_mutex_lock(&mutex_);

     uint64_t index = Hash(key) % bucket_cnt_;
     TableRecord* rec = buckets_[index];
     TableRecord* prev = NULL;

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
     freeList_->PutRecord(rec);

    pthread_mutex_unlock(&mutex_);
   }

};


#endif  // _DB_TABLE_STORAGE_H_

