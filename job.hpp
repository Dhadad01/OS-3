#ifndef __JOB_HPP_INCLUDED
#define __JOB_HPP_INCLUDED

#include <atomic>
#include <pthread.h>
#include <vector>

#include "MapReduceClient.h"
#include "MapReduceFramework.h"

#include "worker.hpp"

class Job
{
public:
  Job(const MapReduceClient& client,
      const InputVec& inputVec,
      OutputVec& outputVec,
      int multiThreadLevel);

  ~Job();

  void save_state_to(JobState* state);

  //  void print(void) const;

  const MapReduceClient& m_client;
  const InputVec& m_inputs;
  OutputVec& m_outputs;
  stage_t m_stage;
  std::vector<Worker*> m_workers;

  pthread_barrier_t m_shuffle_barrier;
  pthread_cond_t m_exit_condition;
  pthread_mutex_t m_exit_run_join_mutex;
  pthread_mutex_t m_push_to_outputs_mutex;

  std::atomic<std::size_t>* m_started;
  std::atomic<std::size_t>* m_intermediates_counter;
  std::atomic<std::size_t>* m_outputs_counter;
  std::atomic<std::size_t>* m_pair_counter;
  std::atomic<std::size_t>* m_progress;
  IntermediateVec m_shuffled;
  std::vector<IntermediateVec> m_intermediate_splits;
  //  std::vector<size_t> m_index_vec;

  pthread_cond_t m_reduce_condition;
  pthread_mutex_t m_procede_to_reduce_mutex;

  bool m_exited;
  bool m_procede_to_reduce;
};

#endif // __JOB_HPP_INCLUDED
