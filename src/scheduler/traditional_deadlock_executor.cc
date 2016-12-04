
#include "scheduler/traditional_deadlock_executor.h"
#include "scheduler/traditional_executor.h"

using std::pair;
using std::make_pair;

TraditionalDeadlockExecutor::TraditionalDeadlockExecutor (const Application* application, int deadlock_method) {
  deadlock_method_ = deadlock_method;
  application_ = application;

  g_ctr1.store(WORKER_THREADS);

  print_word = 0;
 

//pthread_mutex_init(&global_, NULL);
  if (deadlock_method_ == 2) {
    deadlock_lockmanager_ = new Waitforgraph_LockManager(application_->GetTableNum()); 
  } else if (deadlock_method_ == 3)  {
    deadlock_lockmanager_ = new Waitdie_LockManager(application_->GetTableNum());
  } else if (deadlock_method_ == 4)  {
    deadlock_lockmanager_ = new Dreadlock_LockManager(application_->GetTableNum());
  } else if (deadlock_method_ == 5) {
    deadlock_lockmanager_ = new LocalWaitforgraph_LockManager(application_->GetTableNum());
  }

  //Spin(1);

  barrier();

  for (int i = 0; i < WORKER_THREADS; i ++) {
    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(worker_threads_[i]), &attr, RunWorkerThread,
                   reinterpret_cast<void*>(
                   new pair<int, TraditionalDeadlockExecutor*>(i, this)));
  }
  
}

TraditionalDeadlockExecutor::~TraditionalDeadlockExecutor() {
}

void* TraditionalDeadlockExecutor::RunWorkerThread(void* arg) {
  int worker_id = reinterpret_cast<pair<int, TraditionalDeadlockExecutor*>*>(arg)->first;
  TraditionalDeadlockExecutor* scheduler = reinterpret_cast<pair<int, TraditionalDeadlockExecutor*>*>(arg)->second;
  
  const Application* application = scheduler->application_;
  
  Deadlock_LockManager* lock_manager = scheduler->deadlock_lockmanager_;

  Txn* transactions_input = transactions_input_queues[worker_id];
  Traditional_TransactionManager* transactions_manager = traditional_transactions_managers[worker_id];

  HashMap_Worker2* active_txns = new HashMap_Worker2();
  LatchFreeQueue<TxnManager>* abort_txns = new LatchFreeQueue<TxnManager>();
  TxnManager txn_manager;
  bool not_full;
   
  Txn* txn;
  int throughput = 0;
  uint64_t input_index = 0;
  double time = GetTime();
  int acquired;
  LockUnit* lock_unit;
  Traditional_TransactionManager* manager;
  uint64_t total_input = TRANSACTIONS_GENERATED*1000000 / WORKER_THREADS;
int abort_cnt = 0;

   lock_manager->Setup(worker_id);

   uint64_t timestamp_;

  --scheduler->g_ctr1;
  while (scheduler->g_ctr1.load())
    ;
//std::cout<<"~~~~~~~~~~~~~~~I am in RunWorkerThread thread: "<<worker_id<<" .~~~~~~~~~~~~~~~\n"<<std::flush;

#ifdef PROFILER
uint64_t total_duration = 0, mgr_time = 0, exec_time = 0, abort_time = 0, timestamp_time = 0;
uint64_t total_start, total_end, mgr_start, mgr_end, exec_start, exec_end, abort_start, abort_end, timestamp_start, timestamp_end;
total_start = rdtsc();
#endif

  while (true) {  
    for (int i = 0; i < WORKER_THREADS; i++) {
      while (lm_messages[i][worker_id]->Pop(&txn) == true) {
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
#endif
        if (scheduler->deadlock_method_ == 5) {
          lock_manager->RemoveToWaitforgraph(worker_id, txn->GetTxnId());
        }
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif         
        manager = active_txns->Get(txn->GetTxnId());
        lock_unit = manager->CurrentLMRequest();
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
        application->Execute2(lock_unit);
#ifdef PROFILER
exec_end = rdtsc(); // end exec/start release
exec_time += exec_end - exec_start;
#endif
        while (true) {
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
            if (acquired == 0) {
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
              application->Execute2(lock_unit);
#ifdef PROFILER
exec_end = rdtsc(); // end exec/start release
exec_time += exec_end - exec_start;
#endif
              continue;
            } else if (acquired == 1) {
              break;
            } else {
#ifdef PROFILER
abort_start = rdtsc(); // Start abort
#endif
abort_cnt++;
              application->Rollback(lock_unit);  // Rollback not include the current lock_unit
              lock_manager->DeadlockRelease(lock_unit);  // Release not include the current lock_unit
              manager->Reset();
              txn_manager.txn = txn;
              txn_manager.manager = manager;
if (scheduler->deadlock_method_ == 3) {
  txn->ClearTimestamp();
}
              do {
                not_full = abort_txns->Push(txn_manager);
              } while (not_full == false);
              active_txns->Erase(txn->GetTxnId());
#ifdef PROFILER
abort_end = rdtsc(); // end exec/start release
abort_time += abort_end - abort_start;
#endif
              break;
            }
          } else {
//application->Execute(txn);
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
#endif
            lock_manager->Release(txn);
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif   
            active_txns->Erase(txn->GetTxnId());
            throughput++;
            break;
        }
      }

      }
    }

    if (active_txns->Size() < MAX_ACTIVE_TRANSACTIONS) {
      if (input_index == total_input-1) {
        input_index = 0;
      }
     
      if (abort_txns->Pop(&txn_manager) == true) {
        // Get a txn from abort_queue
        txn = txn_manager.txn;
        manager = txn_manager.manager;

      } else {
        // Get a new txn and execute it
        txn = transactions_input + input_index;
        manager = transactions_manager + input_index;
        input_index ++;
      }
#ifdef PROFILER
timestamp_start = rdtsc(); // Start timestamp
#endif        
if (scheduler->deadlock_method_ == 3) {
  timestamp_ = rdtsc();
  txn->SetTimestamp(timestamp_);
}
#ifdef PROFILER
timestamp_end = rdtsc(); // end exec/start release
timestamp_time += timestamp_end - timestamp_start;
#endif
      
      while (true) {
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
          if (acquired == 0) {
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
            application->Execute2(lock_unit);
#ifdef PROFILER
exec_end = rdtsc(); // end exec/start release
exec_time += exec_end - exec_start;
#endif
            continue;
          } else if (acquired == 1) {
            active_txns->Put(txn->GetTxnId(), manager);
            break;
          } else {
#ifdef PROFILER
abort_start = rdtsc(); // Start abort
#endif
abort_cnt++;
            application->Rollback(lock_unit);  // Rollback not include the current lock_unit
            lock_manager->DeadlockRelease(lock_unit);  // Release not include the current lock_unit
            manager->Reset();
            txn_manager.txn = txn;
            txn_manager.manager = manager;
if (scheduler->deadlock_method_ == 3) {
  txn->ClearTimestamp();
}
            do {
              not_full = abort_txns->Push(txn_manager);
            } while (not_full == false);
#ifdef PROFILER
abort_end = rdtsc(); // end exec/start release
abort_time += abort_end - abort_start;
#endif
            break;
          }
        } else {
//application->Execute(txn);
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
#endif
          lock_manager->Release(txn);
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif  
          throughput++;
          break;
        }
      }

    }

    // Report throughput.
    if (GetTime() > time + 2) {
      double total_time = GetTime() - time;
      spin_lock(&print_lock);
#ifdef PROFILER
total_end = rdtsc(); // end loop
total_duration = total_end - total_start;
      std::cout << "Worker: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec. "<< active_txns->Size() <<"  "<<(static_cast<double>(abort_cnt) / total_time)<<". exec_:"<<static_cast<double>(exec_time)/total_duration<<". mgr_:"<<static_cast<double>(mgr_time)/total_duration<<". abort_:"<<static_cast<double>(abort_time)/total_duration<<". Timestamp:"<<static_cast<double>(timestamp_time)/total_duration<<"\n"<< std::flush;
total_start = rdtsc(); // end loop
mgr_time = 0;
exec_time = 0;
abort_time = 0;
timestamp_time = 0;
#else
      std::cout << "Worker thread: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec.  "<< active_txns->Size() <<"  "<<(static_cast<double>(abort_cnt) / total_time)<<"\n"<< std::flush;
#endif
      spin_unlock(&print_lock);
      // Reset txn count.
      time = GetTime();
      throughput = 0;
abort_cnt = 0;
    }
  }  
  return NULL;
}

// This is for TPC-C
/**void* TraditionalDeadlockExecutor::RunWorkerThread(void* arg) {
  int worker_id = reinterpret_cast<pair<int, TraditionalDeadlockExecutor*>*>(arg)->first;
  TraditionalDeadlockExecutor* scheduler = reinterpret_cast<pair<int, TraditionalDeadlockExecutor*>*>(arg)->second;
  
  const Application* application = scheduler->application_;
  
  Deadlock_LockManager* lock_manager = scheduler->deadlock_lockmanager_;

  Txn* transactions_input = transactions_input_queues[worker_id];
  Traditional_TransactionManager* transactions_manager = traditional_transactions_managers[worker_id];

  HashMap_Worker2* active_txns = new HashMap_Worker2();
  LatchFreeQueue<TxnManager>* abort_txns = new LatchFreeQueue<TxnManager>();
  TxnManager txn_manager;
  bool not_full;
   
  Txn* txn;
  int throughput = 0;
  uint64_t input_index = 0;
  double time = GetTime();
  int acquired;
  LockUnit* lock_unit;
  Traditional_TransactionManager* manager;
  uint64_t total_input = TRANSACTIONS_GENERATED*1000000 / WORKER_THREADS;
int abort_cnt = 0;

   lock_manager->Setup(worker_id);

   uint64_t timestamp_;

  --scheduler->g_ctr1;
  while (scheduler->g_ctr1.load())
    ;
//std::cout<<"~~~~~~~~~~~~~~~I am in RunWorkerThread thread: "<<worker_id<<" .~~~~~~~~~~~~~~~\n"<<std::flush;

#ifdef PROFILER
uint64_t total_duration = 0, mgr_time = 0, exec_time = 0, abort_time = 0, timestamp_time = 0;
uint64_t total_start, total_end, mgr_start, mgr_end, exec_start, exec_end, abort_start, abort_end, timestamp_start, timestamp_end;
total_start = rdtsc();
#endif

  while (true) {  
    for (int i = 0; i < WORKER_THREADS; i++) {
      while (lm_messages[i][worker_id]->Pop(&txn) == true) {
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
#endif
        if (scheduler->deadlock_method_ == 5) {
          lock_manager->RemoveToWaitforgraph(worker_id, txn->GetTxnId());
        }
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif         
        manager = active_txns->Get(txn->GetTxnId());
        lock_unit = manager->CurrentLMRequest();

        //application->Execute2(lock_unit);

        while (true) {
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
            if (acquired == 0) {
             // application->Execute2(lock_unit);
              continue;
            } else if (acquired == 1) {
              break;
            } else {
#ifdef PROFILER
abort_start = rdtsc(); // Start abort
#endif
abort_cnt++;
              application->Rollback(lock_unit);  // Rollback not include the current lock_unit
              lock_manager->DeadlockRelease(lock_unit);  // Release not include the current lock_unit
              manager->Reset();
              txn_manager.txn = txn;
              txn_manager.manager = manager;
if (scheduler->deadlock_method_ == 3) {
  txn->ClearTimestamp();
}
              do {
                not_full = abort_txns->Push(txn_manager);
              } while (not_full == false);
              active_txns->Erase(txn->GetTxnId());
#ifdef PROFILER
abort_end = rdtsc(); // end exec/start release
abort_time += abort_end - abort_start;
#endif
              break;
            }
          } else {
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
application->Execute(txn);
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
exec_end = mgr_start;
exec_time += exec_end - exec_start;
#endif
            lock_manager->Release(txn);
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif   
            active_txns->Erase(txn->GetTxnId());
            throughput++;
            break;
        }
      }

      }
    }

    if (active_txns->Size() < MAX_ACTIVE_TRANSACTIONS) {
      if (input_index == total_input-1) {
        input_index = 0;
      }
     
      if (abort_txns->Pop(&txn_manager) == true) {
        // Get a txn from abort_queue
        txn = txn_manager.txn;
        manager = txn_manager.manager;

      } else {
        // Get a new txn and execute it
        txn = transactions_input + input_index;
        manager = transactions_manager + input_index;
        input_index ++;
      }
#ifdef PROFILER
timestamp_start = rdtsc(); // Start timestamp
#endif        
if (scheduler->deadlock_method_ == 3) {
  timestamp_ = rdtsc();
  txn->SetTimestamp(timestamp_);
}
#ifdef PROFILER
timestamp_end = rdtsc(); // end exec/start release
timestamp_time += timestamp_end - timestamp_start;
#endif
      
      while (true) {
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
          if (acquired == 0) {
            //application->Execute2(lock_unit);
            continue;
          } else if (acquired == 1) {
            active_txns->Put(txn->GetTxnId(), manager);
            break;
          } else {
#ifdef PROFILER
abort_start = rdtsc(); // Start abort
#endif
abort_cnt++;
            application->Rollback(lock_unit);  // Rollback not include the current lock_unit
            lock_manager->DeadlockRelease(lock_unit);  // Release not include the current lock_unit
            manager->Reset();
            txn_manager.txn = txn;
            txn_manager.manager = manager;
if (scheduler->deadlock_method_ == 3) {
  txn->ClearTimestamp();
}
            do {
              not_full = abort_txns->Push(txn_manager);
            } while (not_full == false);
#ifdef PROFILER
abort_end = rdtsc(); // end exec/start release
abort_time += abort_end - abort_start;
#endif
            break;
          }
        } else {
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
application->Execute(txn);
#ifdef PROFILER
mgr_start = rdtsc(); // Start lock
exec_end = mgr_start;
exec_time += exec_end - exec_start;
#endif
          lock_manager->Release(txn);
#ifdef PROFILER
mgr_end = rdtsc(); // End lock
mgr_time += mgr_end - mgr_start;
#endif  
          throughput++;
          break;
        }
      }

    }

    // Report throughput.
    if (GetTime() > time + 2) {
      double total_time = GetTime() - time;
      spin_lock(&print_lock);
#ifdef PROFILER
total_end = rdtsc(); // end loop
total_duration = total_end - total_start;
      std::cout << "Worker: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec. "<< active_txns->Size() <<"  "<<(static_cast<double>(abort_cnt) / total_time)<<". exec_:"<<static_cast<double>(exec_time)/total_duration<<". mgr_:"<<static_cast<double>(mgr_time)/total_duration<<". abort_:"<<static_cast<double>(abort_time)/total_duration<<". Timestamp:"<<static_cast<double>(timestamp_time)/total_duration<<"\n"<< std::flush;
total_start = rdtsc(); // end loop
mgr_time = 0;
exec_time = 0;
abort_time = 0;
timestamp_time = 0;
#else
      std::cout << "Worker thread: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec.  "<< active_txns->Size() <<"  "<<(static_cast<double>(abort_cnt) / total_time)<<"\n"<< std::flush;
#endif
      spin_unlock(&print_lock);
      // Reset txn count.
      time = GetTime();
      throughput = 0;
abort_cnt = 0;
    }
  }  
  return NULL;
}**/

