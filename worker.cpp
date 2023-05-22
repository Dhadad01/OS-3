#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>

#include "job.hpp"
#include "pdebug.hpp"
#include "worker.hpp"

// #include "user_types.hpp"

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

#include <cstdlib>

static void
exit_on_error(int status, const std::string& text);

static void
map_phase(Worker* worker);

// static void
// sort_phase(Worker* worker);

static void
shuffle_phase(Worker* worker);

static void
reduce_phase(Worker* worker);

static void*
worker_entry_point(void* arg);

static void
move_stage(Worker* worker, stage_t stage);

Worker::Worker(int thread_id,
               const MapReduceClient& client,
               const InputVec& inputVec,
               pthread_barrier_t* shuffle_barrier,
               std::atomic<std::size_t>* counter,
               std::atomic<std::size_t>* outputs_counter,
               Job* job)
  : m_id(thread_id)
  , m_client(client)
  , m_inputs(inputVec)
  , m_shuffle_barrier(shuffle_barrier)
  , m_intermediates_counter(counter)
  , m_outputs_counter(outputs_counter)
  , m_job(job)
{
  //  pdebug ("Worker (address %p) thread id = %d, m_id = %d\n",
  //          (void *) this,
  //          thread_id,
  //          m_id);
  int status = pthread_create(
    &m_thread_handle, NULL, worker_entry_point, static_cast<void*>(this));
  exit_on_error(status, "pthread_create failed");
}

Worker::~Worker()
{
  pdebug("%s was called for worker #%d\n", __FUNCTION__, m_id);

  //  for (size_t i = 0; i < m_intermediates.size(); i++) {
  //    K2* k = m_intermediates[i].first;
  //    V2* v = m_intermediates[i].second;
  //
  //    m_intermediates[i].first = nullptr;
  //    m_intermediates[i].second = nullptr;
  //
  //    //    delete k;
  //    UNUSED(k);
  //    delete v;
  //  }
}

void*
worker_entry_point(void* arg)
{
  Worker* worker = static_cast<Worker*>(arg);
  int status;

  //    printf("%s - worker is %p, worker #%d\n", __FUNCTION__, arg,
  //    worker->m_id); fflush(stdout);
  map_phase(worker);

  // sort_phase(worker);

  /*
   * wait for all workers to finish the sort phase.
   */
  status = pthread_barrier_wait(&worker->m_job->m_shuffle_barrier);
  if (status != 0 and status != PTHREAD_BARRIER_SERIAL_THREAD) {
    printf("system error: [worker #%d] pthread_barrier_wait failed with error "
           "%s (%d), status is: %d\n",
           worker->m_id,
           std::strerror(errno),
           errno,
           status);
    std::exit(1);
  }
  pdebug("thread #%d passed the barrier\n", worker->m_id);

  status = pthread_mutex_lock(&worker->m_job->m_procede_to_reduce_mutex);

  if (worker->m_id != 0) {
    if (not worker->m_job->m_procede_to_reduce) {
      status = pthread_cond_wait(&worker->m_job->m_reduce_condition,
                                 &worker->m_job->m_procede_to_reduce_mutex);
      exit_on_error(status, "pthread_cond_wait failed");
    }
  } else {
    /*
     * worker 0 - continue to shuffle phase.
     */
    shuffle_phase(worker);

    //      printf("shuffle is done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
    /*
     * signal to all workers that they can proceed to the reduce phase.
     */
    worker->m_job->m_outputs_counter->store(0);
    worker->m_job->m_procede_to_reduce = true;
    //    worker->m_job->m_stage = REDUCE_STAGE;
    //    (void)worker->m_job->m_progress->store(0);
    move_stage(worker, REDUCE_STAGE);
    status = pthread_cond_broadcast(&worker->m_job->m_reduce_condition);
    exit_on_error(status, "pthread_cond_broadcast failed");
  }

  status = pthread_mutex_unlock(&worker->m_job->m_procede_to_reduce_mutex);
  exit_on_error(status, "pthread_mutex_unlock failed");

  reduce_phase(worker);

  return NULL;
}

void
map_phase(Worker* worker)
{
  pdebug("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  //  worker->m_job->m_stage = MAP_STAGE;
  //    move_stage(worker, MAP_STAGE);

  std::size_t pair_index = worker->m_job->m_pair_counter->fetch_add(1);

  while (pair_index < worker->m_job->m_inputs.size()) {
    const InputPair& p = worker->m_job->m_inputs.at(pair_index);
    worker->m_job->m_client.map(p.first, p.second, static_cast<void*>(worker));

    pair_index = worker->m_job->m_pair_counter->fetch_add(1);
    (void)worker->m_job->m_progress->fetch_add(1);
  }
}

static bool
operator<(const IntermediatePair& a, const IntermediatePair& b)
{
  //    bool result = *a.first < *b.first;
  //    pdebug("%s: a=%s, b=%s, a < b = %d\n", __FUNCTION__,
  //    std::string(*a.first).c_str(), std::string(*b.first).c_str(), result);
  //    return result;
  return *a.first < *b.first;
}

// void
// sort_phase(Worker* worker)
//{
//   pdebug("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
//   //
//   //  std::sort(worker->m_intermediates.begin(),
//   worker->m_intermediates.end()); worker->m_job->m_progress++;
//   // sort and map are the same stage
// }

// static bool
// operator==(const K2& a, const K2& b)
//{
//   return (not(a < b)) and (not(b < a));
// }

// int
// is_key_in_vector(const IntermediateVec& vec, const K2& key)
//{
//   for (size_t i = 0; i < vec.size(); i++) {
//     if (key == *(vec[i].first)) {
//       return (int)i;
//     }
//   }
//
//   return -1;
// }

static bool
not_equal(const K2* a, const K2* b)
{
  return (*b < *a) or (*a < *b);
}

void
shuffle_phase(Worker* worker)
{

  pdebug("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  //  worker->m_job->m_progress->store(0);
  //  worker->m_job->m_stage = SHUFFLE_STAGE;
  move_stage(worker, SHUFFLE_STAGE);

  IntermediateVec& shuffled = worker->m_job->m_shuffled;
  //  std::vector<size_t>& index_vec = worker->m_job->m_index_vec;
  std::vector<size_t> index_vec;
  std::vector<IntermediateVec>& splits = worker->m_job->m_intermediate_splits;

  for (const Worker* w : worker->m_job->m_workers) {
    std::copy(w->m_intermediates.begin(),
              w->m_intermediates.end(),
              std::back_inserter(shuffled));
  }
  std::sort(shuffled.begin(), shuffled.end());

  K2* curkey_monster = shuffled[0].first;
  index_vec.push_back(0);
  for (size_t i = 0; i < shuffled.size(); i++) {
    if (not_equal(shuffled[i].first, curkey_monster)) {
      index_vec.push_back(i);
      curkey_monster = shuffled[i].first;
    }
  }

  size_t start = 0;
  size_t end = start + 1;
  while (end < index_vec.size()) {
    IntermediateVec slice;
    slice.insert(
      slice.end(), &shuffled[index_vec[start]], &shuffled[index_vec[end]]);
    splits.push_back(slice); // important - this must be a copy.
    start++;
    end++;
  }
  /*
   * handle the last key.
   */
  if (end == index_vec.size()) {
    IntermediateVec slice;
    slice.insert(
      slice.end(), shuffled.begin() + index_vec[start], shuffled.end());
    splits.push_back(slice); // important - this must be a copy.
  }
}

void
reduce_phase(Worker* worker)
{
  pdebug("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  //  worker->m_job->m_stage = REDUCE_STAGE;

  std::atomic<size_t>* vec_index = worker->m_job->m_outputs_counter;
  std::vector<IntermediateVec>& splits = worker->m_job->m_intermediate_splits;

  size_t i;

  for (i = vec_index->fetch_add(1); i < splits.size();
       i = vec_index->fetch_add(1)) {
    pdebug("%s: [worker #%d] i = %lu (atomic); splits size is %lu\n",
           __FUNCTION__,
           worker->m_id,
           i,
           splits.size());
    pdebug("%s: [worker #%d] atomic before reduce %lu\n",
           __FUNCTION__,
           worker->m_id,
           vec_index->load());
    worker->m_job->m_client.reduce(&splits[i], static_cast<void*>(worker));
    pdebug("%s: [worker #%d] atomic after reduce %lu\n",
           __FUNCTION__,
           worker->m_id,
           vec_index->load());

    // increment progress counter.
    (void)worker->m_job->m_progress->fetch_add(splits[i].size());
  }

  vec_index = nullptr;
}

void
exit_on_error(int status, const std::string& text)
{
  if (status != 0) {
    printf("system error: %s with error: %s (%d), status code: %d\n",
           text.c_str(),
           std::strerror(errno),
           errno,
           status);
    std::exit(1);
  }
}

void
move_stage(Worker* worker, stage_t stage)
{
  /*
   * set stage & reset the counter as a single atomic operation.
   */
  size_t calc = ((size_t)stage) << ((sizeof(size_t) * 8) - 2);
  worker->m_job->m_progress->store(calc);
}

