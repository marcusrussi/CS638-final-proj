
#include "scheduler/physical_traditional_executor.h"
#include "scheduler/traditional_executor.h"

using std::pair;

PhysicalTraditionalExecutor::PhysicalTraditionalExecutor (const Application* application) {

  application_ = application;

  g_ctr1.store(WORKER_THREADS);

  print_word = 0;

  lock_manager_ = new PhysicalTraditional_LockManager(application->GetTableNum());
//  barrier();

  for (int i = 0; i < WORKER_THREADS; i ++) {
    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(worker_threads_[i]), &attr, RunWorkerThread,
                   reinterpret_cast<void*>(
                   new pair<int, PhysicalTraditionalExecutor*>(i, this)));
  }

}

PhysicalTraditionalExecutor::~PhysicalTraditionalExecutor() {
}

void* PhysicalTraditionalExecutor::RunWorkerThread(void* arg) {
  int worker_id = reinterpret_cast<pair<int, PhysicalTraditionalExecutor*>*>(arg)->first;
  PhysicalTraditionalExecutor* scheduler = reinterpret_cast<pair<int, PhysicalTraditionalExecutor*>*>(arg)->second;

  const Application* application = scheduler->application_;

  Txn* transactions_input = transactions_input_queues[worker_id];
  Traditional_TransactionManager* transactions_manager = traditional_transactions_managers[worker_id];

  HashMap_Worker2* active_txns = new HashMap_Worker2();

  Txn* txn;
  int throughput = 0;
  uint64_t input_index = 0;
  double time = GetTime();
  bool acquired;
  LockUnit* lock_unit;
  Traditional_TransactionManager* manager;
  uint64_t total_input = TRANSACTIONS_GENERATED / WORKER_THREADS;



  application->InitializeTable(worker_id);


  PhysicalTraditional_LockManager* lock_manager = scheduler->lock_manager_;

  --scheduler->g_ctr1;
  while (scheduler->g_ctr1.load())
    ;
  lock_manager->Setup(worker_id);

std::cout<<"~~~~~~~~~~~~~~~I am in RunWorkerThread thread: "<<worker_id<<" .~~~~~~~~~~~~~~~\n"<<std::flush;

  while (true) {
    for (int i = 0; i < WORKER_THREADS; i++) {
      while (lm_messages[i][worker_id]->Pop(&txn) == true) {

        manager = active_txns->Get(txn->GetTxnId());

        do {
          lock_unit = manager->NextLockUnit();
          if (lock_unit != NULL) {
            acquired = lock_manager->Lock(lock_unit);
          } else {
            acquired = false;
          }
        } while (acquired == true);

        if (lock_unit == NULL) {
          application->Execute(txn);
//std::cout<<"##"<<worker_id<<": Before release txn: "<<txn->GetTxnId()<<"\n"<<std::flush;
          lock_manager->Release(txn);
//std::cout<<"##"<<worker_id<<": After release txn: "<<txn->GetTxnId()<<"\n"<<std::flush;
          active_txns->Erase(txn->GetTxnId());
          throughput++;
        }

      }
    }

    if (active_txns->Size() < MAX_ACTIVE_TRANSACTIONS) {
      if (input_index == total_input-1) {
        input_index = 0;
      }

      // Get a new txn and execute it
      txn = transactions_input + input_index;

      manager = transactions_manager + input_index;
      input_index ++;
//std::cout<<"@@"<<worker_id<<": Get txn: "<<txn->GetTxnId()<<"\n"<<std::flush;
      do {
//std::cout<<"Before NextLockUnit.\n"<<std::flush;
        lock_unit = manager->NextLockUnit();
        if (lock_unit != NULL) {
//std::cout<<"Before Lock.\n"<<std::flush;
          acquired = lock_manager->Lock(lock_unit);
//std::cout<<"After Lock.\n"<<std::flush;
        } else {
          acquired = false;
        }
      } while (acquired == true);

      if (lock_unit == NULL) {
        application->Execute(txn);
//std::cout<<"@@"<<worker_id<<": Before release txn: "<<txn->GetTxnId()<<"\n"<<std::flush;
        lock_manager->Release(txn);
//std::cout<<"@@"<<worker_id<<": After release txn: "<<txn->GetTxnId()<<"\n"<<std::flush;
        throughput++;
      } else {
        active_txns->Put(txn->GetTxnId(), manager);
      }

    }

    // Report throughput.
    if (GetTime() > time + 2) {
      double total_time = GetTime() - time;
      spin_lock(&print_lock);
      std::cout << "Worker thread: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec.  "<< active_txns->Size() <<"\n"<< std::flush;
      spin_unlock(&print_lock);
      // Reset txn count.
      time = GetTime();
      throughput = 0;
    }
  }
  return NULL;
}

