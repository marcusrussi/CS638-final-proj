// Executor for Orthrus
//

#include "scheduler/partitioned_executor.h"
#include <cstdlib>
#include <iostream>

using std::pair;

PartitionedExecutor::PartitionedExecutor (const Application* application) {
  
  application_ = application;

  g_ctr1.store(LOCK_MANAGER_THREADS);
  g_ctr2.store(LOCK_MANAGER_THREADS);
  g_ctr3.store(WORKER_THREADS);

  print_word = 0;
//  barrier();

  for (int i = 0; i < LOCK_MANAGER_THREADS; i++) {
    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);

    CPU_SET(i, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(lm_threads_[i]), &attr, LockManagerThread,
                   reinterpret_cast<void*>(
                   new pair<int, PartitionedExecutor*>(i, this)));

  }

  //Spin(1);

  for (int i = 0; i < WORKER_THREADS; i ++) {
    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    CPU_SET(LOCK_MANAGER_THREADS + i, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(worker_threads_[i]), &attr, RunWorkerThread,
                   reinterpret_cast<void*>(
                   new pair<int, PartitionedExecutor*>(i, this)));
  }
  
}

PartitionedExecutor::~PartitionedExecutor() {
}

void* PartitionedExecutor::RunWorkerThread(void* arg) {
  int worker_id = reinterpret_cast<pair<int, PartitionedExecutor*>*>(arg)->first;
  PartitionedExecutor* scheduler = reinterpret_cast<pair<int, PartitionedExecutor*>*>(arg)->second;
  
  const Application* application = scheduler->application_;
  Txn* transactions_input = transactions_input_queues[worker_id];
  Partitioned_TransactionManager* transactions_manager = partitioned_transactions_managers[worker_id];
  HashMap_Worker* active_txns = new HashMap_Worker();
  
  Txn* txn;
  SubTxn* sub_txn;
  uint64_t txn_id;

  bool not_full;
  int throughput = 0;
  uint64_t input_index = 0;
  double time = GetTime();
  Partitioned_TransactionManager* manager;
  uint64_t total_input = (TRANSACTIONS_GENERATED*1000000) / WORKER_THREADS;

  LatchFreeQueue<SubTxn*>* request_locks_queue[LOCK_MANAGER_THREADS];
  LatchFreeQueue<uint64_t>* acquired_locks_queue[LOCK_MANAGER_THREADS];
  LatchFreeQueue<SubTxn*>* release_locks_queue[LOCK_MANAGER_THREADS];

  while (scheduler->g_ctr2.load())
    ;

  for (int i = 0; i < LOCK_MANAGER_THREADS; i++) {
    request_locks_queue[i] = scheduler->lock_manager_[i]->request_locks_queue_[worker_id];
    acquired_locks_queue[i] = scheduler->lock_manager_[i]->acquired_locks_queue_[worker_id];
    release_locks_queue[i] = scheduler->lock_manager_[i]->release_locks_queue_[worker_id];
  }

  // Waiting for all worker threads to finish initialization 
  --scheduler->g_ctr3;
  while (scheduler->g_ctr3.load())
    ;

//std::cout<<"~~~~~~~~~~~~~~~I am in RunWorkerThread thread: "<<worker_id<<" .~~~~~~~~~~~~~~~\n"<<std::flush;

#ifdef PROFILER
uint64_t total_duration;
uint64_t exec_time = 0;
uint64_t total_start, total_end, exec_start, exec_end;
total_start = rdtsc();
#endif

  while (true) {  
    // Check whether some locks  are acquired
    for (int i = 0; i < LOCK_MANAGER_THREADS; i++) {
      while (acquired_locks_queue[i]->Pop(&txn_id) == true) {
         manager = active_txns->Get(txn_id);
#ifdef PROFILER
exec_start = rdtsc(); // Start exec
#endif
         application->Execute(manager->txn_);
#ifdef PROFILER
exec_end = rdtsc(); // end exec/start release
exec_time += exec_end - exec_start;
#endif
         // Release all locks
         sub_txn = manager->NextLmRequest();
         do {
           do {
             not_full = release_locks_queue[sub_txn->lm_id]->Push(sub_txn);
           } while(not_full == false);

           sub_txn = manager->NextLmRequest();
         } while (sub_txn != NULL); 
         active_txns->Erase(txn_id);
         throughput++;
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

      sub_txn = manager->GetFirstLMRequest();
      do {
        not_full = request_locks_queue[sub_txn->lm_id]->Push(sub_txn);
      } while (not_full == false);
      active_txns->Put(txn->GetTxnId(), manager);

    } 
  
    // Report throughput.
    if (GetTime() > time + 2) {
      double total_time = GetTime() - time;
      spin_lock(&print_lock);
#ifdef PROFILER
total_end = rdtsc(); // end loop
total_duration = total_end - total_start;
      std::cout << "Worker thread: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec.  "<< active_txns->Size() <<". exec_time/Total_duration:"<<static_cast<double>(exec_time)/total_duration<<"\n"<< std::flush;
total_start = rdtsc(); // end loop
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


void* PartitionedExecutor::LockManagerThread(void* arg) {
  int lm_id = reinterpret_cast<pair<int, PartitionedExecutor*>*>(arg)->first;
  PartitionedExecutor* scheduler = reinterpret_cast<pair<int, PartitionedExecutor*>*>(arg)->second;
  SubTxn* sub_txn;
  scheduler->lock_manager_[lm_id] = new LockManager(lm_id, scheduler->application_->GetTableNum());

  --scheduler->g_ctr1;
  while (scheduler->g_ctr1.load())
    ;

  LockManager* lock_manager = scheduler->lock_manager_[lm_id];
  lock_manager->Setup(scheduler);

  --scheduler->g_ctr2;
  while (scheduler->g_ctr2.load())
    ;

  // Waiting for all worker threads to finish initialization 

//std::cout<<"-----------I am in LockManagerThread thread: "<<lm_id<<"  .-----------\n"<<std::flush;

  while (true) {
    // First check whether there are transactions in the request_locks_queue_
    for (int i = 0; i < WORKER_THREADS; i++) {
      if (lock_manager->request_locks_queue_[i]->Pop(&sub_txn) == true) {
        lock_manager->Lock(sub_txn);
      }
    }

    // Then check whether there are transactions in the release_locks_queue_
    for (int i = 0; i < WORKER_THREADS; i++) {
      while (lock_manager->release_locks_queue_[i]->Pop(&sub_txn) == true) {
        lock_manager->Release(sub_txn);
      }
    }

    for (int i = 0; i < LOCK_MANAGER_THREADS; i++) {
      if (i != lm_id) {
        while (lock_manager->communication_receive_queue_[i]->Pop(&sub_txn) == true) {
          lock_manager->Lock(sub_txn);
        }
      }
    }
  }

  return NULL;
}



