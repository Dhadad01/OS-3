#include "job.hpp"
#include "pdebug.hpp"

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

Job::Job(const MapReduceClient& client,
         const InputVec& inputVec,
         OutputVec& outputVec,
         int multiThreadLevel)
  : m_client(client)
  , m_inputs(inputVec)
  , m_outputs(outputVec)
  , m_exited(false)
  , m_procede_to_reduce(false)
{
  m_intermediates_counter = new std::atomic<std::size_t>(0);
  m_outputs_counter = new std::atomic<std::size_t>(0);
  m_pair_counter = new std::atomic<std::size_t>(0);

  m_progress = new std::atomic<std::size_t>(0);

  (void)pthread_mutex_init(&m_exit_run_join_mutex, NULL);
  (void)pthread_cond_init(&m_exit_condition, NULL);

  (void)pthread_barrier_init(&m_shuffle_barrier, NULL, multiThreadLevel);

  (void)pthread_mutex_init(&m_procede_to_reduce_mutex, NULL);
  (void)pthread_cond_init(&m_reduce_condition, NULL);

  (void)pthread_mutex_init(&m_push_to_outputs_mutex, NULL);

  size_t calc = ((size_t)MAP_STAGE) << ((sizeof(size_t) * 8) - 2);
  m_progress->store(calc);

  /*
   * lastly create the threads so all the locks, condition variables, etc are
   * ready to use.
   */
  for (int id = 0; id < multiThreadLevel; id++) {
    /*
     * create & dispatch workers.
     */
    m_workers.push_back(new Worker(id,
                                   m_client,
                                   m_inputs,
                                   &m_shuffle_barrier,
                                   m_intermediates_counter,
                                   m_outputs_counter,
                                   this));
  }
}

Job::~Job()
{
  pdebug("%s: destructor called for job at %p\n", __FUNCTION__, this);

  /*
   * delete all workers.
   */
  for (size_t i = 0; i < m_workers.size(); i++) {
    Worker* w = m_workers[i];
    m_workers[i] = nullptr;
    delete w;
  }

  (void)pthread_mutex_destroy(&m_push_to_outputs_mutex);
  (void)pthread_mutex_destroy(&m_exit_run_join_mutex);
  (void)pthread_cond_destroy(&m_exit_condition);

  (void)pthread_barrier_destroy(&m_shuffle_barrier);

  delete m_intermediates_counter;
  m_intermediates_counter = nullptr;

  delete m_outputs_counter;
  m_outputs_counter = nullptr;

  delete m_pair_counter;
  m_pair_counter = nullptr;

  delete m_progress;
  m_progress = nullptr;
}

// void Job::print(void) const
//{
//     printf("%s: stage: %d, progress: %lu\n", __FUNCTION__, m_stage,
//     m_progress->load());
// }

void
Job::save_state_to(JobState* state)
{
  size_t progress_and_state = m_progress->load();
  state->stage = (stage_t)((progress_and_state >> 62) & 0x3);
  // state->stage = m_stage;
  float progress = float((progress_and_state << 2) >> 2);

  //  state->percentage = float(m_progress->load()) / float(m_workers.size());
  switch (state->stage) {
    case MAP_STAGE:
      state->percentage = (100.0 * float(progress)) / float(m_inputs.size());
      break;
    case SHUFFLE_STAGE:
      state->percentage = 0;
      break;
    case REDUCE_STAGE:
      state->percentage = (100.0 * float(progress)) / float(m_shuffled.size());
      break;
    default:
      break;
  }
}
