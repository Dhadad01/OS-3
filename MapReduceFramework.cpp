//
// Created by roeey on 5/10/23.
//

#include <algorithm>
#include <atomic>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h> // for usleep
#include <vector>

#include "MapReduceClient.h"
#include "MapReduceFramework.h"

#include "job.hpp"
#include "pdebug.hpp"

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

void
getJobState(JobHandle job, JobState* state)
{
  Job* j = static_cast<Job*>(job);

  if (!j and !state) {
    j->save_state_to(state);
  }
}

JobHandle
startMapReduceJob(const MapReduceClient& client,
                  const InputVec& inputVec,
                  OutputVec& outputVec,
                  int multiThreadLevel)
{
  Job* job = nullptr;

  /*
   * TODO: return nullptr on error instead of throwing exception.
   */
  job = new Job(client, inputVec, outputVec, multiThreadLevel);

  return static_cast<void*>(job);
}

void
closeJobHandle(JobHandle job)
{
  Job* j = static_cast<Job*>(job);

  pdebug("%s: closing job (at %p)\n", __FUNCTION__, j);

  delete j;
}

void
waitForJob(JobHandle job)
{
  static const int LOCKED = 0;
  Job* j = static_cast<Job*>(job);

  if (j != nullptr) {
    if (not j->m_exited) {
      if (pthread_mutex_trylock(&j->m_exit_run_join_mutex) == LOCKED) {

        // calling pthread_join is critical section.V
        for (const Worker* w : j->m_workers) {
          (void)pthread_join(w->m_thread_handle, NULL);
        }
        j->m_exited = true;
        (void)pthread_cond_broadcast(&j->m_exit_condition);

        pthread_mutex_unlock(&j->m_exit_run_join_mutex);
      } else {
        (void)pthread_cond_wait(&j->m_exit_condition,
                                &j->m_exit_run_join_mutex);
      }
    }
  }

  /*
   * Unite all outputs.
   */
}

void
emit2(K2* key, V2* value, void* context)
{
  Worker* worker = static_cast<Worker*>(context);

  worker->m_intermediates.push_back(IntermediatePair(key, value));

  /*
   * increment the atomic counter using it's operator++.
   */
  size_t prev_value = worker->m_intermediates_counter->fetch_add(1);
  UNUSED(prev_value);
}

void
emit3(K3* key, V3* value, void* context)
{
  Worker* worker = static_cast<Worker*>(context);

  (void)pthread_mutex_lock(&worker->m_job->m_push_to_outputs_mutex);
  worker->m_job->m_outputs.push_back(OutputPair(key, value));
  (void)pthread_mutex_unlock(&worker->m_job->m_push_to_outputs_mutex);

  /*
   * increment the atomic counter using it's operator++.
   */
  size_t prev_value = worker->m_outputs_counter->fetch_add(1);
  UNUSED(prev_value);
}
