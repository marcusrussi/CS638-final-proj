//
// The Storage class provides an interface for writing and accessing data
// objects stored by the system.

#ifndef _DB_BACKEND_STORAGE_H_
#define _DB_BACKEND_STORAGE_H_

#include "backend/table.h"

class Storage {
 public:

  // If the object specified by 'key' exists, copies the object into '*result'
  // and returns true. If the object does not exist, false is returned.
  virtual void* ReadRecord(const uint32_t table_id, const uint64_t key) = 0;

  // Sets the object specified by 'key' equal to 'value'. Any previous version
  // of the object is replaced. Returns true if the write succeeds, or false if
  // it fails for any reason.
  virtual void PutRecord(const uint32_t table_id, const uint64_t key, void* value) = 0;

  virtual void InsertRecord(const uint32_t worker_id, const uint32_t table_id, const uint64_t key, void* value) = 0;

  // Removes the object specified by 'key' if there is one. Returns true if the
  // deletion succeeds (or if no object is found with the specified key), or
  // false if it fails for any reason.
  virtual void DeleteRecord(const uint32_t table_id, const uint64_t key) = 0;

  virtual void NewTable(uint64_t table_id, uint64_t bucket_cnt, uint64_t freeList_cnt, uint64_t value_size) = 0;

 private:
  uint32_t table_cnt_;
  Table** tables_;

  Table** NewOrder_tables_;
  Table** Order_tables_;
  Table** OrderLine_tables_;
  Table** History_tables_;
};

#endif  // _DB_BACKEND_STORAGE_H_

