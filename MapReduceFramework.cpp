//
// Created by roeey on 5/10/23.
//

#include <pthread.h>
#include <vector>
#include <stdio.h>
#include <unistd.h> // for usleep

void* foo(void *arg) {
    int *id = (int *)arg;

    printf("thread #%d going to sleep\n", *id);
    usleep(3000000);
    printf("thread #%d woke up\n", *id);

    return NULL;
}

class Worker
{
public:
    Worker(int thread_id): m_id(thread_id) {
        /*
         * pthread_create(...)
         */
        (void)pthread_create(&m_thread_handle, NULL, foo, &m_id);
    }

    int m_id;
    pthread_t m_thread_handle;
};

#include "MapReduceFramework.h"

class Job
{
public:
//    Job(const MapReduceClient& client,
//        const InputVec& inputVec, OutputVec& outputVec,
//        int multiThreadLevel);
Job();

    ~Job();


    std::vector<Worker> m_workers;
    pthread_barrier_t m_shuffle_barrier;
    pthread_cond_t m_exit_condition;
    pthread_mutex_t m_exit_run_join_mutex;
    bool m_exited;
};

//Job::Job(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
Job::Job()
{
    for (int id = 0; id < 4; id++) {
    //for (int id = 0; id < multiThreadLevel; id++) {

        /*
         * create & dispatch threads.
         */
        m_workers.emplace_back(id);
    }

    (void) pthread_mutex_init(&m_exit_run_join_mutex, NULL);
    (void) pthread_cond_init(&m_exit_condition, NULL);

}

Job::~Job()
{
    /*
     * TODO: destroy all semapjores, mutexes, condition variables, etc...
     */
    (void) pthread_mutex_destroy(&m_exit_run_join_mutex);
    (void) pthread_cond_destroy(&m_exit_condition);
}

//JobHandle startMapReduceJob(const MapReduceClient& client,
//                            const InputVec& inputVec, OutputVec& outputVec,
//                            int multiThreadLevel)
JobHandle startMapReduceJob(void )
{
    Job *job = nullptr;

    /*
     * TODO: return nullptr on error instead of throwing exception.
     */
    //job = new Job(client, inputVec, outputVec, multiThreadLevel);
    job = new Job();

    return static_cast<void *>(job);
}

void closeJobHandle(JobHandle job)
{
    Job *j = static_cast<Job *>(job);

    delete j;
}

void waitForJob(JobHandle job)
{
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
