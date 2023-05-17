#include <algorithm>
#include <string>

#include "job.hpp"
#include "pdebug.hpp"
#include "worker.hpp"

#include "user_types.hpp"

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

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
  (void)pthread_create(
    &m_thread_handle, NULL, worker_entry_point, static_cast<void*>(this));
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

  //    printf("%s - worker is %p, worker #%d\n", __FUNCTION__, arg,
  //    worker->m_id); fflush(stdout);
  map_phase(worker);

  // sort_phase(worker);

  /*
   * wait for all workers to finish the sort phase.
   */
  pthread_barrier_wait(&worker->m_job->m_shuffle_barrier);
  pdebug("thread #%d passed the barrier\n", worker->m_id);

  pthread_mutex_lock(&worker->m_job->m_procede_to_reduce_mutex);

  if (worker->m_id != 0) {
    if (not worker->m_job->m_procede_to_reduce) {
      pthread_cond_wait(&worker->m_job->m_reduce_condition,
                        &worker->m_job->m_procede_to_reduce_mutex);
    }
  } else {
    /*
     * worker 0 - continue to shuffle phase.
     */
    shuffle_phase(worker);

    /*
     * signal to all workers that they can proceed to the reduce phase.
     */
    worker->m_job->m_outputs_counter->store(0);
    worker->m_job->m_procede_to_reduce = true;
    pthread_cond_broadcast(&worker->m_job->m_reduce_condition);
  }

  pthread_mutex_unlock(&worker->m_job->m_procede_to_reduce_mutex);

  reduce_phase(worker);

  return NULL;
}

void
map_phase(Worker* worker)
{
  pdebug("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  worker->m_job->m_stage = MAP_STAGE;

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
  worker->m_job->m_progress->store(0);
  worker->m_job->m_stage = SHUFFLE_STAGE;

  IntermediateVec& shuffled = worker->m_job->m_shuffled;
  std::vector<size_t>& index_vec = worker->m_job->m_index_vec;

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
}

void
reduce_phase(Worker* worker)
{
  pdebug("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  worker->m_job->m_stage = REDUCE_STAGE;

  IntermediateVec intermediate_vec;
  IntermediateVec& shuffled = worker->m_job->m_shuffled;
  std::vector<size_t>& index_vec = worker->m_job->m_index_vec;

  std::atomic<size_t> idx(0);
  UNUSED(idx);

  size_t start = worker->m_job->m_outputs_counter->fetch_add(1);
  // start = start / 2;
  size_t end = start + 1;

  /*
   * the size of the m_index_vec might be greater than the number of workers.
   * therefor each & every worker shall try to reduce as many pairs as possible.
   * using an atomic variable for the distribution of pairs eliminates the risk
   * of distributing the same pair to more than 1 worker.
   *
   * since every element in m_index_vec is both a start & end for different keys
   * we shall "double" it's size.
   * by integer dividing start with 2 each element of index_vec is picked as
   * both start & at the next iteration as end.
   */
  while (end < index_vec.size()) {
#ifdef DEBUG
    pdebug("%s (worker #%d) proccessing intermidiates[%d:%d]\n",
           __FUNCTION__,
           worker->m_id,
           start,
           end);
#endif
    intermediate_vec.insert(intermediate_vec.end(),
                            &shuffled[index_vec[start]],
                            &shuffled[index_vec[end]]);
    worker->m_job->m_client.reduce(&intermediate_vec,
                                   static_cast<void*>(worker));

#ifdef DEBUG
    size_t tmp = idx.fetch_add(1);
    pdebug("%s outputs <%c (%d)|%d> \n",
           __FUNCTION__,
           ((KChar*)worker->m_job->m_outputs[tmp].first)->c,
           (int)((KChar*)worker->m_job->m_outputs[tmp].first)->c,
           ((VCount*)worker->m_job->m_outputs[tmp].second)->count);
#endif

    start = worker->m_job->m_outputs_counter->fetch_add(1);
    // start = start / 2;
    end = start + 1;
    intermediate_vec.clear();
    (void)worker->m_job->m_progress->fetch_add(1);
  }

  /*
   * handle the last key.
   */
  if (end == index_vec.size()) {
    intermediate_vec.clear();
    intermediate_vec.insert(intermediate_vec.end(),
                            shuffled.begin() + index_vec[start],
                            shuffled.end());
    worker->m_job->m_client.reduce(&intermediate_vec,
                                   static_cast<void*>(worker));
#ifdef DEBUG
    size_t tmp = idx.fetch_add(1);
    pdebug("%s outputs <%c (%d)|%d> \n",
           __FUNCTION__,
           ((KChar*)worker->m_job->m_outputs[tmp].first)->c,
           (int)((KChar*)worker->m_job->m_outputs[tmp].first)->c,
           ((VCount*)worker->m_job->m_outputs[tmp].second)->count);
#endif

    (void)worker->m_job->m_progress->fetch_add(1);
  }
}
