
// Txn class, and most parameters defined here
#include <algorithm>
#include <stdint.h>
#include <stdlib.h>

//#define PROFILER


// TPCC
/**#define MAX_READ_CNT   2
#define MAX_WRITE_CNT  11
#define MAX_TABLE_CNT  9**/

// YCSB, MAX_TABLE_CNT  1, current setting is for 10RMW
#define MAX_READ_CNT   0   // Number of Reads per txn
#define MAX_WRITE_CNT  10  // Number of Writes per txn
#define MAX_TABLE_CNT  1   // Number of tables per txn

#define MAX_SUB_TXNS  10  // Max lock managers that the txn needs to access

#define CC_PER_NUMA 1

#define HOT 100000                 // Number of hot records
#define WORKER_THREADS  60     // Number of worker threads
#define LOCK_MANAGER_THREADS  20  // Number of lock manager threads. This is only for partitioned_lm(Orthrus)
#define LM_THREADS_PER_PARTITION 4 // Number of lock manager threads per partition
#define QUEUE_SIZE 64
#define LM_QUEUE_SIZE  512
#define MAX_ACTIVE_TRANSACTIONS 1  // Number of max active transactions per worker thread (Should set it bigger, say 5 for low conention&random setting, should set smaller for high contention, say 1.)

#define TRANSACTIONS_GENERATED  2000000  // million transactions created(will repeatly use the same transactions)
#define BUCKET_SIZE 1000     // Bucket size for each lock manager
#define LOCKREQUEST_FREELIST_CNT  2000
#define KEYS_FREELIST_CNT    2000
#define HASH_MAP_SIZE  200
#define HASH_MAP_LM_SIZE 200
#define HASH_MAP_WAITFORGRAPH 10000
#define SET_ARRAY_MAX_SIZE 200
#define MAX_UINT64 0xFFFFFFFFFFFFFFFF
#define CACHE_LINE 64

// TPCC
/**#define TXN_MALLOC_START      20
#define STORAGE_MALLOC_START  30
#define EXECUTION_MALLOC_START  70**/

#define TXN_MALLOC_START      50  // Allocate txn/txn manager memory in the fifth NUMA node
#define STORAGE_MALLOC_START  60  // Allocate storage memory in the sixth NUMA node
#define EXECUTION_MALLOC_START 70


// For tpcc
#define WAREHOUSE_CNT  8
#define NUMBER_ITEMS 100000
#define DISTRICT_PER_WH 10
#define CUSTOMER_PER_DIST 3000

#define WAREHOUSE_BUCKET 10
#define DISTRICT_BUCKET 100
#define CUSTOMER_BUCKET 10000
#define HISTORY_BUCKET  100
#define NEWORDER_BUCKET 100
#define ORDER_BUCKET 100
#define ORDERLINE_BUCKET 100
#define ITEM_BUCKET 100
#define STOCK_BUCKET 10000


enum TxnStatus {
    NEW = 0,
    ACTIVE = 1,
    COMMITTED = 2,
    ABORTED = 3,
    BLOCKED = 4,
};

struct TableKey {
  TableKey() {}
  TableKey (uint64_t id, uint64_t k) {
    table_id = id;
    key = k;
  }
  uint64_t table_id;
  uint64_t key;
};


class Txn {
public:
  Txn();

  void SetupTxn();

  void SetTxnId(uint32_t id);
  void SetTxnType(uint32_t type);
  void SetWorkerId(uint32_t worker_id);
  void SetTxnStatus(TxnStatus status);

  uint32_t GetTxnId();
  uint32_t GetTxnType();
  uint32_t GetWorkerId();
  TxnStatus GetTxnStatus();

  void AddReadSet(uint64_t table, uint64_t key);
  void AddReadWriteSet(uint64_t table, uint64_t key);

  TableKey GetReadSet(uint32_t index);
  TableKey GetReadWriteSet(uint32_t index);

  TableKey* GetReadSet();
  TableKey* GetReadWriteSet();

  uint32_t ReadSetSize();
  uint32_t ReadWriteSetSize();

  uint64_t GetTimestamp();
  void SetTimestamp(uint64_t ts);
  void ClearTimestamp();

  TableKey read_set[MAX_READ_CNT];
  TableKey read_write_set[MAX_WRITE_CNT];

  uint32_t read_cnt;
  uint32_t read_write_cnt;

  uint32_t txn_id;
  uint32_t txn_type;
  uint32_t txn_worker_id;
  TxnStatus txn_status;

  uint64_t timestamp;
  //Tpcc_Args tpcc_args;
};

struct SubTxn {
  SubTxn() {

  }
  SubTxn(Txn* t) {
    txn = t;
  }
  uint32_t read_key_start;
  uint32_t read_key_end;
  uint32_t write_key_start;
  uint32_t write_key_end;

  uint32_t lm_id;
  Txn* txn;
  SubTxn* next_sub_txn;
uint32_t next_lm_id;
  //char pad[CACHE_LINE - sizeof(void*) - sizeof(uint32_t)*5];
};
