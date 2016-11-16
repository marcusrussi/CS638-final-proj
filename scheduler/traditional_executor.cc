
#include "scheduler/traditional_executor.h"


using std::pair;

TraditionalExecutor::TraditionalExecutor (const Application* application) {
  
  application_ = application;

  lock_manager_ = new Traditional_LockManager(application_->GetTableNum()); 
  
  g_ctr1.store(WORKER_THREADS);

  print_word = 0;

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
                   new pair<int, TraditionalExecutor*>(i, this)));
  }
  
}

TraditionalExecutor::~TraditionalExecutor() {
}


// First acquired all locks, then execute the txn, used while comparing with Orthrus and TPC-C
void* TraditionalExecutor::RunWorkerThread(void* arg) {
  int worker_id = reinterpret_cast<pair<int, TraditionalExecutor*>*>(arg)->first;
  TraditionalExecutor* scheduler = reinterpret_cast<pair<int, TraditionalExecutor*>*>(arg)->second;
  
  const Application* application = scheduler->application_;
  Traditional_LockManager* lock_manager = scheduler->lock_manager_;
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
  uint64_t total_input = TRANSACTIONS_GENERATED*1000000 / WORKER_THREADS;

  lock_manager->Setup(worker_id);

  --scheduler->g_ctr1;
  while (scheduler->g_ctr1.load())
    ;
std::cout<<"~~~~~~~~~~~~~~~I am in RunWorkerThread thread: "<<worker_id<<" .~~~~~~~~~~~~~~~\n"<<std::flush;

#ifdef PROFILER
uint64_t total_duration = 0, mgr_time = 0, exec_time = 0;
uint64_t total_start, total_end, mgr_start, mgr_end, exec_start, exec_end;
total_start = rdtsc();
#endif

  while (true) {  
    for (int i = 0; i < WORKER_THREADS; i++) {
      while (lm_messages[i][worker_id]->Pop(&txn) == true) {

        manager = active_txns->Get(txn->GetTxnId());

        do {
          lock_unit = manager->NextLockUnit();
          if (lock_unit != NULL) {
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
#endif
            acquired = lock_manager->Lock(lock_unit);
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif
          } else {
            acquired = false;
          }
        } while (acquired == true);
     
        if (lock_unit == NULL) {
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
          application->Execute(txn);
#ifdef PROFILER
exec_end = rdtsc(); // end exec/start release
mgr_start = exec_end;
exec_time += exec_end - exec_start;
#endif
          lock_manager->Release(txn);
#ifdef PROFILER
mgr_end = rdtsc(); // end release
mgr_time += mgr_end - mgr_start;
#endif
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
      do {
        lock_unit = manager->NextLockUnit();
        if (lock_unit != NULL) {
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
#endif
          acquired = lock_manager->Lock(lock_unit);
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif
        } else {
          acquired = false;
        }
      } while (acquired == true);
     
      if (lock_unit == NULL) {
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
        application->Execute(txn);
#ifdef PROFILER
exec_end = rdtsc(); // end exec/start release
mgr_start = exec_end;
exec_time += exec_end - exec_start;
#endif
        lock_manager->Release(txn);
#ifdef PROFILER
mgr_end = rdtsc(); // end release
mgr_time += mgr_end - mgr_start;
#endif
        throughput++;
      } else {
        active_txns->Put(txn->GetTxnId(), manager);
      }

    }

    // Report throughput.
    if (GetTime() > time + 2) {
      double total_time = GetTime() - time;
      spin_lock(&print_lock);
#ifdef PROFILER
total_end = rdtsc(); // end loop
total_duration = total_end - total_start;
      std::cout << "Worker thread: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec.  "<< active_txns->Size() <<". exec_time/Total_duration:"<<static_cast<double>(exec_time)/total_duration<<". mgr_time/Total_duration:"<<static_cast<double>(mgr_time)/total_duration<<"\n"<< std::flush;
total_start = rdtsc(); // end loop
mgr_time = 0;
exec_time = 0;
#else
      std::cout << "Worker thread: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec.  "<< active_txns->Size() <<"\n"<< std::flush;
#endif

      spin_unlock(&print_lock);

      // Reset txn count.
      time = GetTime();
      throughput = 0;
    }
  }  
  return NULL;
}
