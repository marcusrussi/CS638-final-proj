//
// The Application abstract class
//
// Application execution logic in the system is coded into

#ifndef _DB_APPLICATIONS_APPLICATION_H_
#define _DB_APPLICATIONS_APPLICATION_H_

#include "common/types.h"
#include "scheduler/lock_manager.h"

class Storage;
class Txn;
struct LockUnit;

class Application {
 public:
  virtual ~Application() {}

  // Load generation.
  virtual void NewTxn(Txn* txn, uint64_t txn_id) const = 0;

  // Execute a transaction's application logic given the input 'txn'.
  virtual int Execute(Txn* txn) const = 0;

  // Storage initialization method.
  virtual void InitializeStorage() const = 0;

  // Storage initialization method.
  virtual void InitializeTable(uint32_t table_id) const = 0;

  virtual uint32_t LookupPartition(const uint64_t& key) const = 0;

  virtual uint32_t GetTableNum() const = 0;

  virtual int Execute2(LockUnit* lock_unit) const = 0;
  virtual int Rollback(LockUnit* lock_unit) const = 0;
};

#endif  // _DB_APPLICATIONS_APPLICATION_H_
