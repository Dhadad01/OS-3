//
// Created by roeey on 5/10/23.
//

#include <pthread.h>
#include <vector>
#include <atomic>
#include <stdio.h>
#include <unistd.h> // for usleep

#include "MapReduceFramework.h"
#include "MapReduceClient.h"

#define UNUSED(x) ((void)x)

void *foo(void *arg) {
    int *id = (int *) arg;

    printf("thread #%d going to sleep\n", *id);
    usleep(3000000);
    printf("thread #%d woke up\n", *id);

    return NULL;
}

class Worker {
public:
    Worker(int thread_id,
           std::atomic<std::size_t> *counter,
           std::atomic<std::size_t> *outputs_counter)
            : m_id(thread_id), m_intermediates_counter(counter), m_outputs_counter(outputs_counter) {
        (void) pthread_create(&m_thread_handle, NULL, foo, &m_id);
    }

    int m_id;
    pthread_t m_thread_handle;
    IntermediateVec m_intermediates;
    OutputVec m_outputs;
    std::atomic<std::size_t> *m_intermediates_counter;
    std::atomic<std::size_t> *m_outputs_counter;
};

#include "MapReduceFramework.h"

class Job {
public:
//    Job(const MapReduceClient& client,
//        const InputVec& inputVec, OutputVec& outputVec,
//        int multiThreadLevel);
    Job();

    ~Job();

    void save_state_to(JobState* state);


    JobState m_state;
    std::vector<Worker> m_workers;
    pthread_barrier_t m_shuffle_barrier;
    pthread_cond_t m_exit_condition;
    pthread_mutex_t m_exit_run_join_mutex;
    bool m_exited;
    std::atomic<std::size_t> *m_intermediates_counter;
    std::atomic<std::size_t> *m_outputs_counter;
};

//Job::Job(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
Job::Job() {
    m_intermediates_counter = new std::atomic<std::size_t>(0);
    m_outputs_counter = new std::atomic<std::size_t>(0);
    for (int id = 0; id < 4; id++) {
        //for (int id = 0; id < multiThreadLevel; id++) {

        /*
         * create & dispatch threads.
         */
        m_workers.emplace_back(Worker(id, m_intermediates_counter, m_outputs_counter));
    }

    (void) pthread_mutex_init(&m_exit_run_join_mutex, NULL);
    (void) pthread_cond_init(&m_exit_condition, NULL);

}

Job::~Job() {
    /*
     * TODO: destroy all semapjores, mutexes, condition variables, etc...
     */
    (void) pthread_mutex_destroy(&m_exit_run_join_mutex);
    (void) pthread_cond_destroy(&m_exit_condition);

    delete m_intermediates_counter;
    m_intermediates_counter = nullptr;

    delete m_outputs_counter;
    m_outputs_counter = nullptr;
}

void Job::save_state_to(JobState* state)
{
    *state = m_state;
}

void getJobState(JobHandle job, JobState* state)
{
    Job *j = static_cast<Job *>(job);

    if (!j and !state) {
        j->save_state_to(state);
    }
}

//JobHandle startMapReduceJob(const MapReduceClient& client,
//                            const InputVec& inputVec, OutputVec& outputVec,
//                            int multiThreadLevel)
JobHandle startMapReduceJob(void) {
    Job *job = nullptr;

    /*
     * TODO: return nullptr on error instead of throwing exception.
     */
    //job = new Job(client, inputVec, outputVec, multiThreadLevel);
    job = new Job();

    return static_cast<void *>(job);
}

void closeJobHandle(JobHandle job) {
    Job *j = static_cast<Job *>(job);

    delete j;
}

void waitForJob(JobHandle job) {
    static const int LOCKED = 0;
    Job *j = static_cast<Job *>(job);

    if (j != nullptr) {
        if (not j->m_exited) {
            if (pthread_mutex_trylock(&j->m_exit_run_join_mutex) == LOCKED) {

                // calling pthread_join is critical section.V
                for (const Worker &t: j->m_workers) {
                    (void) pthread_join(t.m_thread_handle, NULL);
                }
                j->m_exited = true;
                (void) pthread_cond_broadcast(&j->m_exit_condition);

                pthread_mutex_unlock(&j->m_exit_run_join_mutex);
            } else {
                (void) pthread_cond_wait(&j->m_exit_condition, &j->m_exit_run_join_mutex);
            }
        }
    }
}


void emit2(K2 *key, V2 *value, void *context) {
    Worker *worker = static_cast<Worker *>(context);

    worker->m_intermediates.emplace_back(IntermediatePair(key, value));

    /*
     * increment the atomic counter using it's operator++.
     */
    size_t prev_value = worker->m_intermediates_counter->fetch_add(1);
    UNUSED(prev_value);
}

void emit3 (K3* key, V3* value, void* context) {
    Worker *worker = static_cast<Worker *>(context);

    worker->m_outputs.emplace_back(OutputPair(key, value));

    /*
     * increment the atomic counter using it's operator++.
     */
    size_t prev_value = worker->m_outputs_counter->fetch_add(1);
    UNUSED(prev_value);
}
