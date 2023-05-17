#include <algorithm>

#include "job.hpp"
#include "pdebug.hpp"
#include "worker.hpp"

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

static void
map_phase (Worker *worker);

static void
sort_phase (Worker *worker);

static void
shuffle_phase (Worker *worker);

static void
reduce_phase (Worker *worker);

static void *
worker_entry_point (void *arg);

Worker::Worker (int thread_id,
                const MapReduceClient &client,
                const InputVec &inputVec,
                pthread_barrier_t *shuffle_barrier,
                std::atomic<std::size_t> *counter,
                std::atomic<std::size_t> *outputs_counter,
                Job *job)
    : m_id (thread_id), m_client (client), m_inputs (inputVec),
    m_shuffle_barrier (shuffle_barrier), m_intermediates_counter (counter),
    m_outputs_counter (outputs_counter), m_job (job)
{
//  pdebug ("Worker (address %p) thread id = %d, m_id = %d\n",
//          (void *) this,
//          thread_id,
//          m_id);
  (void) pthread_create (
      &m_thread_handle, NULL, worker_entry_point, static_cast<void *>(this));
}

void *
worker_entry_point (void *arg)
{
  Worker *worker = static_cast<Worker *>(arg);

  //    printf("%s - worker is %p, worker #%d\n", __FUNCTION__, arg,
  //    worker->m_id); fflush(stdout);
  map_phase (worker);

  sort_phase (worker);

  /*
   * wait for all workers to finish the sort phase.
   */
  pthread_barrier_wait (&worker->m_job->m_shuffle_barrier);
  pdebug ("thread #%d passed the barrier\n", worker->m_id);

  pthread_mutex_lock (&worker->m_job->m_procede_to_reduce_mutex);

  if (worker->m_id != 0)
  {
    if (not worker->m_job->m_procede_to_reduce)
    {
      pthread_cond_wait (&worker->m_job->m_reduce_condition,
                         &worker->m_job->m_procede_to_reduce_mutex);
    }
  }
  else
  {
    /*
     * worker 0 - continue to shuffle phase.
     */
    shuffle_phase (worker);

    /*
     * signal to all workers that they can proceed to the reduce phase.
     */
    worker->m_job->m_procede_to_reduce = true;
    pthread_cond_broadcast (&worker->m_job->m_reduce_condition);
  }

  pthread_mutex_unlock (&worker->m_job->m_procede_to_reduce_mutex);

  reduce_phase (worker);

  return NULL;
}

void
map_phase (Worker *worker)
{
  pdebug ("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  worker->m_job->m_stage = MAP_STAGE;

  std::size_t pair_index = worker->m_job->m_pair_counter->fetch_add (1);

  if (pair_index >= worker->m_job->m_inputs.size ())
  {
    /*
     * index out-of-range.
     */
    return;
  }

  const InputPair &p = worker->m_job->m_inputs.at (pair_index);
  worker->m_job->m_client.map (p.first, p.second, static_cast<void *>(worker));
}

static bool
operator< (const IntermediatePair &a, const IntermediatePair &b)
{
  return a.first < b.first;
}

void
sort_phase (Worker *worker)
{
  pdebug ("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
//
//  std::sort(worker->m_intermediates.begin(), worker->m_intermediates.end());
  worker->m_job->m_progress++;
  //sort and map are the same stage
}

static bool
operator== (const K2 &a, const K2 &b)
{
  return (not(a < b)) and (not(b < a));
}

int
is_key_in_vector (const IntermediateVec &vec, const K2 &key)
{
  for (size_t i = 0; i < vec.size (); i++)
  {
    if (key == *(vec[i].first))
    {
      return (int) i;
    }
  }

  return -1;
}

void
shuffle_phase (Worker *worker)
{

  pdebug ("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  *(worker->m_job->m_progress) = 0;
  worker->m_job->m_stage = SHUFFLE_STAGE;

  IntermediateVec shuffled;
  std::vector<size_t> index_vec;
  for (const Worker *w: worker->m_job->m_workers)
  {
    shuffled.insert (shuffled.end (), w->m_intermediates.begin (),
                     w->m_intermediates.begin ());
  }
  std::sort (shuffled.begin (), shuffled.end ());

  K2 *curkey_monster = shuffled[0].first;
  index_vec.push_back (0);
  for (size_t i = 0; i < shuffled.size (); i++)
  {
    if (shuffled[i].first != curkey_monster)
    {
      index_vec.push_back (i);
      curkey_monster = shuffled[i].first;
    }
  }
}

void
reduce_phase (Worker *worker)
{
  pdebug ("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__);
  worker->m_job->m_stage = REDUCE_STAGE;
  worker->m_job->m_progress++;

  size_t start = worker->m_job->m_outputs_counter->fetch_add (1);

  IntermediateVec intermediate_vec;
  if (start<worker->m_job->m_index_vec.size()){
    for (size_t i = worker->m_job->m_index_vec[start]; i <
    worker->m_job->m_index_vec[start+1]; ++i)
    {
      intermediate_vec.push_back (worker->m_job->m_shuffled[i]);
    }
    //const shit
    worker->m_job->m_client.reduce(intermediate_vec,static_cast<void *>
    (worker));
  }


}
