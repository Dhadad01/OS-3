#ifndef __WORKER_HPP_INCLUDED
#define __WORKER_HPP_INCLUDED

#include <atomic>
#include <pthread.h>
#include <stdio.h>

#include "MapReduceClient.h"
#include "MapReduceFramework.h"

#include "job.hpp"

class Job;

class Worker {
public:
    Worker(int thread_id,
           const MapReduceClient &client,
           const InputVec &inputVec,
           pthread_barrier_t *shuffle_barrier,
           std::atomic<std::size_t> *counter,
           std::atomic<std::size_t> *outputs_counter,
           Job *job);

    //Worker(Worker &) = delete;

    int m_id;
    pthread_t m_thread_handle;

    const MapReduceClient &m_client;
    const InputVec &m_inputs;

    IntermediateVec m_intermediates;
    OutputVec m_outputs;
    pthread_barrier_t *m_shuffle_barrier;
    std::atomic<std::size_t> *m_intermediates_counter;
    std::atomic<std::size_t> *m_outputs_counter;

    Job *m_job;
};

#endif // __WORKER_HPP_INCLUDED
