//
// Created by roeey on 5/10/23.
//

#include <atomic>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h> // for usleep
#include <vector>
#include <algorithm>

#include "MapReduceClient.h"
#include "MapReduceFramework.h"

#define UNUSED(x) ((void)x)

// void *foo(void *arg) {
//     int *id = (int *) arg;
//
//     printf("thread #%d going to sleep\n", *id);
//     usleep(3000000);
//     printf("thread #%d woke up\n", *id);
//
//     return NULL;
// }

class Job;

class Worker;

void map_phase(Worker *worker);

void sort_phase(Worker *worker);

void shuffle_phase(Worker *worker);

void reduce_phase(Worker *worker);

void *
worker_entry_point(void *arg);

class Worker {
public:
    Worker(int thread_id,
           const MapReduceClient &client,
           const InputVec &inputVec,
           pthread_barrier_t *shuffle_barrier,
           std::atomic<std::size_t> *counter,
           std::atomic<std::size_t> *outputs_counter,
           Job *job)
            : m_id(thread_id), m_client(client), m_inputs(inputVec), m_shuffle_barrier(shuffle_barrier),
              m_intermediates_counter(counter), m_outputs_counter(outputs_counter), m_job(job) {
        printf("Worker (address %p) thread id = %d, m_id = %d\n", (void *)this, thread_id, m_id);
        fflush(stdout);
        (void) pthread_create(&m_thread_handle, NULL, worker_entry_point, static_cast<void *>(this));
    }

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


class Job {
public:
    Job(const MapReduceClient &client,
        const InputVec &inputVec,
        OutputVec &outputVec,
        int multiThreadLevel);

    ~Job();

    void save_state_to(JobState *state);

    const MapReduceClient &m_client;
    const InputVec &m_inputs;
    OutputVec &m_outputs;
    JobState m_state;
    std::vector<Worker *> m_workers;

    pthread_barrier_t m_shuffle_barrier;
    pthread_cond_t m_exit_condition;
    pthread_mutex_t m_exit_run_join_mutex;
    std::atomic<std::size_t> *m_intermediates_counter;
    std::atomic<std::size_t> *m_outputs_counter;
    std::atomic<std::size_t> *m_pair_counter;

    pthread_cond_t m_reduce_condition;
    pthread_mutex_t m_procede_to_reduce_mutex;

    bool m_exited;
    bool m_procede_to_reduce;
};

Job::Job(const MapReduceClient &client,
         const InputVec &inputVec,
         OutputVec &outputVec,
         int multiThreadLevel)
        : m_client(client), m_inputs(inputVec), m_outputs(outputVec), m_exited(false), m_procede_to_reduce(false) {
    m_intermediates_counter = new std::atomic<std::size_t>(0);
    m_outputs_counter = new std::atomic<std::size_t>(0);
    m_pair_counter = new std::atomic<std::size_t>(0);


    (void) pthread_mutex_init(&m_exit_run_join_mutex, NULL);
    (void) pthread_cond_init(&m_exit_condition, NULL);

    (void) pthread_barrier_init(&m_shuffle_barrier, NULL, multiThreadLevel);

    (void) pthread_mutex_init(&m_procede_to_reduce_mutex, NULL);
    (void) pthread_cond_init(&m_reduce_condition, NULL);

    /*
     * lastly create the threads so all the locks, condition variables, etc are ready to use.
     */
    for (int id = 0; id < multiThreadLevel; id++) {
        /*
         * create & dispatch workers.
         */
        m_workers.push_back(new Worker(
                id, m_client, m_inputs, &m_shuffle_barrier, m_intermediates_counter, m_outputs_counter, this));
    }
}

Job::~Job() {
    /*
     * TODO: destroy all semapjores, mutexes, condition variables, etc...
     */
    (void) pthread_mutex_destroy(&m_exit_run_join_mutex);
    (void) pthread_cond_destroy(&m_exit_condition);

    (void) pthread_barrier_destroy(&m_shuffle_barrier);

    delete m_intermediates_counter;
    m_intermediates_counter = nullptr;

    delete m_outputs_counter;
    m_outputs_counter = nullptr;

    delete m_pair_counter;
    m_pair_counter = nullptr;

    /*
     * delete all workers.
     */
    for (size_t i = 0; i < m_workers.size(); i++) {
        delete m_workers[i];
        m_workers[i] = nullptr;
    }
}

void
Job::save_state_to(JobState *state) {
    *state = m_state;
}

void
getJobState(JobHandle job, JobState *state) {
    Job *j = static_cast<Job *>(job);

    if (!j and !state) {
        j->save_state_to(state);
    }
}

JobHandle
startMapReduceJob(const MapReduceClient &client,
                  const InputVec &inputVec,
                  OutputVec &outputVec,
                  int multiThreadLevel) {
    Job *job = nullptr;

    /*
     * TODO: return nullptr on error instead of throwing exception.
     */
    job = new Job(client, inputVec, outputVec, multiThreadLevel);

    return static_cast<void *>(job);
}

void
closeJobHandle(JobHandle job) {
    Job *j = static_cast<Job *>(job);

    delete j;
}

void
waitForJob(JobHandle job) {
    static const int LOCKED = 0;
    Job *j = static_cast<Job *>(job);

    if (j != nullptr) {
        if (not j->m_exited) {
            if (pthread_mutex_trylock(&j->m_exit_run_join_mutex) == LOCKED) {

                // calling pthread_join is critical section.V
                for (const Worker *w: j->m_workers) {
                    (void) pthread_join(w->m_thread_handle, NULL);
                }
                j->m_exited = true;
                (void) pthread_cond_broadcast(&j->m_exit_condition);

                pthread_mutex_unlock(&j->m_exit_run_join_mutex);
            } else {
                (void) pthread_cond_wait(&j->m_exit_condition,
                                         &j->m_exit_run_join_mutex);
            }
        }
    }

    /*
     * Unite all outputs.
     */
}

void
emit2(K2 *key, V2 *value, void *context) {
    Worker *worker = static_cast<Worker *>(context);

    worker->m_intermediates.push_back(IntermediatePair(key, value));

    /*
     * increment the atomic counter using it's operator++.
     */
    size_t prev_value = worker->m_intermediates_counter->fetch_add(1);
    UNUSED(prev_value);
}

void
emit3(K3 *key, V3 *value, void *context) {
    Worker *worker = static_cast<Worker *>(context);

    worker->m_outputs.push_back(OutputPair(key, value));

    /*
     * increment the atomic counter using it's operator++.
     */
    size_t prev_value = worker->m_outputs_counter->fetch_add(1);
    UNUSED(prev_value);
}

void *
worker_entry_point(void *arg) {
    Worker *worker = static_cast<Worker *>(arg);

//    printf("%s - worker is %p, worker #%d\n", __FUNCTION__, arg, worker->m_id);
//    fflush(stdout);
    map_phase(worker);

    sort_phase(worker);

    /*
     * wait for all workers to finish the sort phase.
     */
    pthread_barrier_wait(&worker->m_job->m_shuffle_barrier);
    printf("thread #%d passed the barrier\n", worker->m_id);
    fflush(stdout);

    pthread_mutex_lock(&worker->m_job->m_procede_to_reduce_mutex);

    if (worker->m_id != 0) {
        if (not worker->m_job->m_procede_to_reduce) {
            pthread_cond_wait(&worker->m_job->m_reduce_condition, &worker->m_job->m_procede_to_reduce_mutex);
        }
    } else {
        /*
         * worker 0 - continue to shuffle phase.
         */
        shuffle_phase(worker);

        /*
         * signal to all workers that they can proceed to the reduce phase.
         */
        worker->m_job->m_procede_to_reduce = true;
        pthread_cond_broadcast(&worker->m_job->m_reduce_condition);
    }

    pthread_mutex_unlock(&worker->m_job->m_procede_to_reduce_mutex);

    reduce_phase(worker);

    return NULL;
}

void map_phase(Worker *worker) {
    printf("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__ );
    fflush(stdout);

    std::size_t pair_index = worker->m_job->m_pair_counter->fetch_add(1);

    if (pair_index >= worker->m_job->m_inputs.size()) {
        /*
         * index out-of-range.
         */
        return;
    }

    const InputPair &p = worker->m_job->m_inputs.at(pair_index);
    worker->m_job->m_client.map(p.first, p.second, static_cast<void *>(worker));
}

static bool operator<(const IntermediatePair &a, const IntermediatePair &b)
{
    return a.first < b.first;
}

void sort_phase(Worker *worker) {
    printf("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__ );
    fflush(stdout);

    std::sort(worker->m_intermediates.begin(), worker->m_intermediates.end());
}

static bool operator==(const K2 &a, const K2 &b)
{
    return (not (a < b)) and (not (b < a));
}

int is_key_in_vector(const IntermediateVec &vec, const K2 &key)
{
    for (size_t i = 0; i < vec.size(); i++) {
        if (key == *(vec[i].first)) {
            return (int)i;
        }
    }

    return -1;
}

void shuffle_phase(Worker *worker) {
    printf("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__ );
    fflush(stdout);

    IntermediateVec shuffled;

    for (const Worker *w: worker->m_job->m_workers) {
        for (const IntermediatePair &p: w->m_intermediates) {
            int i = is_key_in_vector(w->m_intermediates, *(p.first));
            if (-1 == i) {
                /*
                 *  couldn't find the current key in the unified vector.
                 */

            } else {

            }
        }
    }
}

void reduce_phase(Worker *worker) {
    printf("thread #%d reached phase %s\n", worker->m_id, __FUNCTION__ );
    fflush(stdout);

}