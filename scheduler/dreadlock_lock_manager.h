// Dreadlock implementation

#ifndef _DB_SCHEDULER_DREADLOCK_LOCK_MANAGER_H_
#define _DB_SCHEDULER_DREADLOCK_LOCK_MANAGER_H_

#include "scheduler/lock_manager.h"

inline int div64(int x)         { return x >> 6; }
inline int mod64(int x)         { return x & 63; }


class Digest {
 public:
  uint64_t* bit_vector;
  uint64_t* single_bit;
  uint32_t worker_id;
  uint32_t bit_cnt;
  uint32_t word_cnt;

  Digest(uint32_t id) {
    bit_vector = (uint64_t*)malloc(2*sizeof(uint64_t)*(div64(WORKER_THREADS - 1) + 1));

    worker_id = id;
    word_cnt = div64(WORKER_THREADS - 1) + 1;
    bit_cnt = word_cnt*sizeof(uint64_t)*8;
    single_bit = bit_vector + word_cnt;
//std::cout<<"word_cnt:"<<word_cnt<<". bit_cnt:"<<bit_cnt<<".\n"<<std::flush;
    Setup();
  }
  
  void Setup() {
barrier();
    for (uint32_t i = 0; i < word_cnt; i++) {
      bit_vector[i] = 0;
    }
    //SetBit(bit_vector, worker_id);
    
    for (uint32_t i = 0; i < word_cnt; i++) {
      single_bit[i] = 0;
    }
    SetBit(single_bit, worker_id);
//BitPrint(bit_vector);
barrier();    
  }

  void SetBit(uint64_t* bm, int offset){
    bm[div64(offset)] |= (1 << mod64(offset));
  }

  void SetSingle() {
//std::cout<<"At SetSingle method.\n"<<std::flush;
barrier();
    SetBit(bit_vector, worker_id);
//BitPrint(bit_vector);
barrier();
  }

  void ClearAndSetSingle() {
//std::cout<<"At SetSingle method.\n"<<std::flush;
barrier();
    for (uint32_t i = 0; i < word_cnt; i++) {
      bit_vector[i] = 0;
    }
    SetBit(bit_vector, worker_id);
//BitPrint(bit_vector);
barrier();
  }

  bool Contains(uint32_t offset) {
barrier();
//BitPrint(bit_vector);
//std::cout<<"Offset check:"<<offset<<".\n"<<std::flush;
    return (bit_vector[div64(offset)] & (1 << mod64(offset))) != 0;
  }
  
  void SetUnion(uint64_t* bm) {
//std::cout<<"In SetUnion method.\n"<<std::flush;
barrier();
//BitPrint(bit_vector);
//BitPrint(bm);
    for (uint32_t i = 0; i < word_cnt; i++) {
      bit_vector[i] = single_bit[i] | bm[i];
    }
//BitPrint(bit_vector);
barrier();
  }

  uint64_t* GetDigest() {
    return bit_vector;  
  }


  bool IsSet(uint64_t* bm, int offset) {
barrier();
    return (bm[div64(offset)] & (1 << mod64(offset))) != 0;
barrier();
  }

  void Clear() {
barrier();
    //bit_vector[div64(worker_id)] &= (~(1 << mod64(worker_id)));
    bit_vector[div64(worker_id)] = 0;
barrier();
  }

  void Clear1() {
//std::cout<<"At SetSingle method.\n"<<std::flush;
barrier();
    for (uint32_t i = 0; i < word_cnt; i++) {
      bit_vector[i] = 0;
    }
//BitPrint(bit_vector);
barrier();
  }

  void BitPrint(uint64_t* bm) {
    for (uint32_t i = 0; i < bit_cnt; i++)  {
      if (IsSet(bm, i) == true)
        std::cout <<"1 "<<std::flush;
      else
        std::cout <<"0 "<<std::flush;
    }
    std::cout<<"\n"<<std::flush;
  }

};


class Dreadlock_LockManager : public Deadlock_LockManager {
 public:
  Dreadlock_LockManager(uint32_t table_num);
  virtual ~Dreadlock_LockManager() {}
  virtual int Lock(LockUnit* lock_unit) ;
  virtual void Release(Txn* txn) ;
  virtual void Release(const TableKey table_key, Txn* txn) ;
  virtual void DeadlockRelease(LockUnit* lock_unit) ;
  virtual void RemoveToWaitforgraph(uint32_t worker, uint64_t txn1);

  virtual void Setup(int worker_id);

 private:
  Traditional_Bucket* lock_table_;
  Keys_Freelist* keys_freelist[WORKER_THREADS];
  Lockrequest_Freelist* lockrequest_freelist[WORKER_THREADS];
  uint32_t table_num_;

  uint32_t table_buckets[9];
  uint32_t table_sum_buckets[9];

  Digest* thread_digests[WORKER_THREADS];

  pthread_mutex_t test;
  
};

#endif  // _DB_SCHEDULER_DREADLOCK_LOCK_MANAGER_H_
