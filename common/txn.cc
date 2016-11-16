
#include "common/txn.h"

Txn::Txn() {
}

void Txn::SetupTxn() {
  //read_set = (TableKey*)malloc(sizeof(TableKey)*MAX_READ_CNT);
  //read_write_set = (TableKey*)malloc(sizeof(TableKey)*MAX_WRITE_CNT);
  read_cnt = 0;
  read_write_cnt = 0;
}

void Txn::SetTxnId(uint32_t id) {
  txn_id = id;
}

void Txn::SetTxnType(uint32_t type) {
  txn_type = type;
}

void Txn::SetWorkerId(uint32_t worker_id) {
  txn_worker_id = worker_id;
}

void Txn::SetTxnStatus(TxnStatus status) {
  txn_status = status;
}

uint32_t Txn::GetTxnId() {
  return txn_id;
}

uint32_t Txn::GetTxnType() {
  return txn_type;
}

uint32_t Txn::GetWorkerId() {
  return txn_worker_id;
}

TxnStatus Txn::GetTxnStatus() {
  return txn_status;
}

void Txn::AddReadSet(uint64_t table, uint64_t key) {
  read_set[read_cnt].table_id = table;
  read_set[read_cnt].key = key;
  read_cnt++;
}

void Txn::AddReadWriteSet(uint64_t table, uint64_t key) {
  read_write_set[read_write_cnt].table_id = table;
  read_write_set[read_write_cnt].key = key;
  read_write_cnt++;
}

TableKey Txn::GetReadSet(uint32_t index) {
  return read_set[index];
}

TableKey Txn::GetReadWriteSet(uint32_t index) {
  return read_write_set[index];
}

TableKey* Txn::GetReadSet() {
  return read_set;
}

TableKey* Txn::GetReadWriteSet() {
  return read_write_set;
}

uint32_t Txn::ReadSetSize() {
  return read_cnt;
}

uint32_t Txn::ReadWriteSetSize() {
  return read_write_cnt;
}

uint64_t Txn::GetTimestamp() {
  return timestamp;
}

void Txn::SetTimestamp(uint64_t ts) {
  timestamp = ts;
}

void Txn::ClearTimestamp() {
  timestamp = 0;
}
