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

static void exit_on_error(int status, const std::string &text);

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
  int status;
  Job* j = static_cast<Job*>(job);

  if (j != nullptr) {
    if (not j->m_exited) {
      if (pthread_mutex_trylock(&j->m_exit_run_join_mutex) == LOCKED) {

        // calling pthread_join is critical section.V
        for (const Worker* w : j->m_workers) {
          status = pthread_join(w->m_thread_handle, NULL);
            exit_on_error(status, "phread_join failed");
        }
        j->m_exited = true;
          status = pthread_cond_broadcast(&j->m_exit_condition);
          exit_on_error(status, "pthread_cond_broadcast failed");

          status = pthread_mutex_unlock(&j->m_exit_run_join_mutex);
          exit_on_error(status, "pthread_mutex_unlock failed");
      } else {
          status = pthread_cond_wait(&j->m_exit_condition,
                                &j->m_exit_run_join_mutex);
          exit_on_error(status, "pthread_cond_wait failed");
      }
    }

#ifdef DEBUG
#include <algorithm>

    std::sort(j->m_outputs.begin(), j->m_outputs.end());
#endif // DEBUG
  }
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
  int status;

    status = pthread_mutex_lock(&worker->m_job->m_push_to_outputs_mutex);
    exit_on_error(status, "pthread_mutex_lock failed");
  worker->m_job->m_outputs.push_back(OutputPair(key, value));
    status = pthread_mutex_unlock(&worker->m_job->m_push_to_outputs_mutex);
    exit_on_error(status, "pthread_mutex_unlock failed");

  /*
   * increment the atomic counter using it's operator++.
   */
  //  size_t prev_value = worker->m_outputs_counter->fetch_add(1);
  // UNUSED(prev_value);
}

void exit_on_error(int status, const std::string &text) {
    if (status != 0) {
        printf("system error: %s\n", text.c_str());
        std::exit(1);
    }
}
