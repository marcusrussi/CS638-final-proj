//
// A simple implementation of the storage interface using an stl map.

#ifndef _DB_BACKEND_SIMPLE_STORAGE_H_
#define _DB_BACKEND_SIMPLE_STORAGE_H_

#include "backend/storage.h"
#include "common/types.h"

class SimpleStorage : public Storage {
 public:
  SimpleStorage();
  virtual void* ReadRecord(const uint32_t table_id, const uint64_t key);
  virtual void PutRecord(const uint32_t table_id, const uint64_t key, void* value);
  virtual void DeleteRecord(const uint32_t table_id, const uint64_t key);
  virtual void InsertRecord(const uint32_t worker_id, const uint32_t table_id, const uint64_t key, void* value);
  virtual void NewTable(uint64_t table_id, uint64_t bucket_cnt, uint64_t freeList_cnt, uint64_t value_size);

 private:
  uint32_t table_cnt_;
  Table** tables_;

  Table** NewOrder_tables_;
  Table** Order_tables_;
  Table** OrderLine_tables_;
  Table** History_tables_;

};
#endif  // _DB_BACKEND_SIMPLE_STORAGE_H_

