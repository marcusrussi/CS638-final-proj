// HStore implementation

#include "scheduler/hstore_executor.h"


using std::pair;

HstoreExecutor::HstoreExecutor (const Application* application) {

  application_ = application;

  g_ctr1.store(WORKER_THREADS);

  print_word = 0;

  //spin_locks = (char*)malloc(CACHE_LINE*WORKER_THREADS);
  memset(spin_locks, 0x0, CACHE_LINE*WORKER_THREADS*8);

  for (int i = 0; i < WORKER_THREADS; i ++) {
    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(worker_threads_[i]), &attr, RunWorkerThread,
                   reinterpret_cast<void*>(
                   new pair<int, HstoreExecutor*>(i, this)));
  }

}

HstoreExecutor::~HstoreExecutor() {
}

volatile uint64_t * get_lock(uint32_t partition)
{
  uint32_t index = CACHE_LINE*8*partition;
  return (volatile uint64_t *)(&spin_locks[index]);
}

void lock_partition(uint32_t partition)
{
  volatile uint64_t* lock_word = get_lock(partition);
  spin_lock(lock_word);
}

void unlock_partition(uint32_t partition)
{
  volatile uint64_t* lock_word = get_lock(partition);
  spin_unlock(lock_word);
}

void* HstoreExecutor::RunWorkerThread(void* arg) {
  int worker_id = reinterpret_cast<pair<int, HstoreExecutor*>*>(arg)->first;
  HstoreExecutor* scheduler = reinterpret_cast<pair<int, HstoreExecutor*>*>(arg)->second;

  const Application* application = scheduler->application_;

  Txn* transactions_input = transactions_input_queues[worker_id];
  Hstore_TransactionManager* transactions_manager = hstore_transactions_managers[worker_id];

  Txn* txn;
  int throughput = 0;
  uint64_t input_index = 0;
  double time = GetTime();
  int lock_index;
  Hstore_TransactionManager* manager;
  uint64_t total_input = TRANSACTIONS_GENERATED / WORKER_THREADS;

  application->InitializeTable(worker_id);

  --scheduler->g_ctr1;
  while (scheduler->g_ctr1.load())
    ;
std::cout<<"~~~~~~~~~~~~~~~I am in RunWorkerThread thread: "<<worker_id<<" .~~~~~~~~~~~~~~~\n"<<std::flush;

  while (true) {

      if (input_index == total_input-1) {
        input_index = 0;
      }

      // Get a new txn and execute it
      txn = transactions_input + input_index;

      manager = transactions_manager + input_index;
      input_index ++;

      do {
        lock_index = manager->NextLock();
        if (lock_index != -1) {
          lock_partition(lock_index);
        }
      } while (lock_index != -1);


      application->Execute(txn);

      do {
        lock_index = manager->NextLock();
        if (lock_index != -1) {
          unlock_partition(lock_index);
        }
      } while (lock_index != -1);

      throughput++;



    // Report throughput.
    if (GetTime() > time + 2) {
      double total_time = GetTime() - time;

      spin_lock(&print_lock);
      std::cout << "Worker thread: "<<worker_id<<" Completed " << (static_cast<double>(throughput) / total_time)<< " txns/sec.  "<<"\n"<< std::flush;
      spin_unlock(&print_lock);
      // Reset txn count.
      time = GetTime();
      throughput = 0;
    }
  }
  return NULL;
}

