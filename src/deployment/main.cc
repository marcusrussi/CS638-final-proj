// Main invokation of a single node in the system.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
//#include <gperftools/profiler.h>

#include "backend/simple_storage.h"
#include "common/utils.h"
#include "applications/microbenchmark.h"
#include "applications/physical_microbenchmark.h"
#include "applications/tpcc.h"
#include "applications/traditional_tpcc.h"
#include "scheduler/partitioned_executor.h"
#include "scheduler/physical_partitioned_executor.h"
#include "scheduler/partitioned_transaction_manager.h"
#include "scheduler/physical_partitioned_transaction_manager.h"
#include "applications/traditional_microbenchmark.h"
#include "applications/physical_traditional_microbenchmark.h"
#include "scheduler/traditional_executor.h"
#include "scheduler/physical_traditional_executor.h"
#include "scheduler/traditional_transaction_manager.h"
#include "scheduler/traditional_deadlock_executor.h"
#include "scheduler/hstore_executor.h"
#include "scheduler/hstore_transaction_manager.h"
#include "applications/hstore_microbenchmark.h"

Txn* transactions_input_queues[WORKER_THREADS];
Partitioned_TransactionManager* partitioned_transactions_managers[WORKER_THREADS];
PhysicalPartitioned_TransactionManager* physical_partitioned_transactions_managers[WORKER_THREADS];
Traditional_TransactionManager* traditional_transactions_managers[WORKER_THREADS];
Hstore_TransactionManager* hstore_transactions_managers[WORKER_THREADS];
LatchFreeQueue<Txn*>* lm_messages[WORKER_THREADS][WORKER_THREADS];

char spin_locks[CACHE_LINE*WORKER_THREADS*8];  // For partitioned store

volatile uint64_t print_lock;

void stop(int sig) {
  exit(sig);
}

//
//  8: Orthrus
//  1: Traditional deadlock-free
//  2: Traditional waitfor-graph
//  3: Wait-die
//  4: Dreadlock
//  5: optimized waitfor-graph

int main(int argc, char** argv) {
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <|1|2|3|4|5|7|8> <m[icro]|t[pcc]> <percent_mp>\n", argv[0]);
    exit(1);
  }

  print_lock = 0;

  // Catch ^C and kill signals and exit gracefully (for profiling).
  signal(SIGINT, &stop);
  signal(SIGTERM, &stop);


  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(TXN_MALLOC_START, &cs);
  int ret = sched_setaffinity(0, sizeof(cs), &cs);
  if (ret) {
    perror("sched_setaffinity");
    assert(false);
  }

  uint64_t txns_per_thread = TRANSACTIONS_GENERATED/WORKER_THREADS + 1;
  // Create txns and txn manager in advance
  for (int i = 0; i < WORKER_THREADS; i++) {
    transactions_input_queues[i] = (Txn*)malloc(sizeof(Txn) * txns_per_thread);
    if (StringToInt(argv[1]) == 8) {
      partitioned_transactions_managers[i] = (Partitioned_TransactionManager*)malloc(sizeof(Partitioned_TransactionManager) * txns_per_thread);
    } else if (StringToInt(argv[1]) == 1 || StringToInt(argv[1]) == 2 || StringToInt(argv[1]) == 3 || StringToInt(argv[1]) == 4 || StringToInt(argv[1]) == 5 || StringToInt(argv[1]) == 10){
      traditional_transactions_managers[i] = (Traditional_TransactionManager*)malloc(sizeof(Traditional_TransactionManager) * txns_per_thread);
    } else if (StringToInt(argv[1]) == 6) {
      hstore_transactions_managers[i] = (Hstore_TransactionManager*)malloc(sizeof(Hstore_TransactionManager) * txns_per_thread);
    } else if (StringToInt(argv[1]) == 9) {
      physical_partitioned_transactions_managers[i] = (PhysicalPartitioned_TransactionManager*)malloc(sizeof(PhysicalPartitioned_TransactionManager) * txns_per_thread);
    }
  }

  // This is only for partitioned LM(Orthrus), number of lock managers that each txn needs to access
  int lm_cnt_per_txn = StringToInt(argv[3]);

  if (StringToInt(argv[1]) == 1 || StringToInt(argv[1]) == 2 || StringToInt(argv[1]) == 3 || StringToInt(argv[1]) == 4 || StringToInt(argv[1]) == 5 ||StringToInt(argv[1]) == 10) {
    for(int i = 0; i < WORKER_THREADS; i++) {
      for (int j = 0; j < WORKER_THREADS; j++) {
        lm_messages[i][j] = new LatchFreeQueue<Txn*>();
      }
    }
  }

  Storage* storage = new SimpleStorage();

  Application* application;

  if (StringToInt(argv[1]) == 8) {
    if (argv[2][0] == 'm') {
      application = new Microbenchmark(LOCK_MANAGER_THREADS, HOT, lm_cnt_per_txn, storage);
    } else if (argv[2][0] == 't'){
      // For TPCC
      application = new TPCC(LOCK_MANAGER_THREADS, storage);
    } else {
      fprintf(stderr, "Usage: %s <|1|2|3|4|5|7|8> <m[icro]|t[pcc]> <percent_mp>\n", argv[0]);
      exit(1);
    }
  } else if (StringToInt(argv[1]) == 1 || StringToInt(argv[1]) == 2 || StringToInt(argv[1]) == 3 || StringToInt(argv[1]) == 4 || StringToInt(argv[1]) == 5){
    if (argv[2][0] == 'm') {
      application = new Traditional_Microbenchmark(HOT, storage);
    } else if (argv[2][0] == 't') {
      // For TPCC
     application = new Traditional_TPCC(storage);
    } else {
      fprintf(stderr, "Usage: %s <|1|2|3|7|8> <m[icro]|t[pcc]> <percent_mp>\n", argv[0]);
      exit(1);
    }
  } else if (StringToInt(argv[1]) == 6) {
    application = new Hstore_Microbenchmark(HOT, lm_cnt_per_txn, storage);
  } else if (StringToInt(argv[1]) == 9) {
    application = new Physical_Microbenchmark(LOCK_MANAGER_THREADS, HOT, lm_cnt_per_txn, storage);
  } else if (StringToInt(argv[1]) == 10) {
    application = new PhysicalTraditional_Microbenchmark(lm_cnt_per_txn, HOT, storage);
  } else {
      fprintf(stderr, "Usage: %s <|1|2|3|4|5|7|8> <m[icro]|t[pcc]> <percent_mp>\n", argv[0]);
      exit(1);
  }

  std::cout<<"-----------Begin generating transactions.-----------\n"<<std::flush;

  for (uint64_t i = 0; i < txns_per_thread * WORKER_THREADS; i++) {
    uint32_t worker_id = i % WORKER_THREADS;
    Txn* txn = transactions_input_queues[worker_id] + i/WORKER_THREADS;
    txn->SetupTxn();
    txn->SetWorkerId(worker_id);

    application->NewTxn(txn, i);

    if (StringToInt(argv[1]) == 8) {
      Partitioned_TransactionManager* manager = partitioned_transactions_managers[worker_id] + i/WORKER_THREADS;
      manager->Setup(txn, application);
    } else if(StringToInt(argv[1]) == 1 || StringToInt(argv[1]) == 10) {
      Traditional_TransactionManager* manager = traditional_transactions_managers[worker_id] + i/WORKER_THREADS;
      manager->Setup(txn, true);
    } else if(StringToInt(argv[1]) == 2 || StringToInt(argv[1]) == 3 || StringToInt(argv[1]) == 4 || StringToInt(argv[1]) == 5){
      Traditional_TransactionManager* manager = traditional_transactions_managers[worker_id] + i/WORKER_THREADS;
      manager->Setup(txn, false);
    } else if(StringToInt(argv[1]) == 6){
      Hstore_TransactionManager* manager = hstore_transactions_managers[worker_id] + i/WORKER_THREADS;
      manager->Setup(txn);
    } else if (StringToInt(argv[1]) == 9) {
      PhysicalPartitioned_TransactionManager* manager = physical_partitioned_transactions_managers[worker_id] + i/WORKER_THREADS;

      manager->Setup(txn, application);

    } else {
      fprintf(stderr, "Usage: %s <|1|2|3|4|5|7|8> <m[icro]|t[pcc]> <percent_mp>\n", argv[0]);
      exit(1);
    }

    if (i % 1000000 == 0 && i != 0) {
      std::cout<<"Generated "<<i / 1000000 <<" million transactions.\n"<< std::flush;
    }
  }
  std::cout<<"-----------Done generating transactions.-----------\n"<<std::flush;

  std::cout<<"-----------Begin initializing storage.----------\n"<<std::flush;

  CPU_ZERO(&cs);
  CPU_SET(STORAGE_MALLOC_START, &cs);
  ret = sched_setaffinity(0, sizeof(cs), &cs);
  if (ret) {
    perror("sched_setaffinity");
    assert(false);
  }

  application->InitializeStorage();
  std::cout<<"-----------Finish initializing storage.-----------\n"<<std::flush;


//  barrier();

  Spin(2);

  CPU_ZERO(&cs);
  CPU_SET(EXECUTION_MALLOC_START, &cs);
  ret = sched_setaffinity(0, sizeof(cs), &cs);
  if (ret) {
    perror("sched_setaffinity");
    assert(false);
  }


  // Create work threads to execute transactions
  if (StringToInt(argv[1]) == 8){
    PartitionedExecutor transaction_executor(application);
  } else if (StringToInt(argv[1]) == 1){
    TraditionalExecutor transaction_executor(application);
  } else if (StringToInt(argv[1]) == 2 || StringToInt(argv[1]) == 3 || StringToInt(argv[1]) == 4 || StringToInt(argv[1]) == 5){
    TraditionalDeadlockExecutor transaction_executor(application, StringToInt(argv[1]));
  } else if (StringToInt(argv[1]) == 6){
    HstoreExecutor transaction_executor(application);
  } else if (StringToInt(argv[1]) == 9){
    PhysicalPartitionedExecutor transaction_executor(application);
  } else if (StringToInt(argv[1]) == 10){
    PhysicalTraditionalExecutor transaction_executor(application);
  } else {
    fprintf(stderr, "Usage: %s <|1|2|3|7|8> <m[icro]|t[pcc]> <percent_mp>\n", argv[0]);
    exit(1);
  }

  Spin(1200);

  return 0;
}

