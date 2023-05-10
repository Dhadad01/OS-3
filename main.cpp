#include <iostream>
#include "MapReduceFramework.h"
#include <pthread.h>

pthread_barrier_t barrier;

void *shit1(void *arg) {
    JobHandle j = arg;

    pthread_barrier_wait(&barrier);

    std::cout << "shit1 - waitForJob called" << std::endl;
    waitForJob(j);
    std::cout << "shit1 - job is done." << std::endl;

    return NULL;
}

void *shit2(void *arg) {
    JobHandle j = arg;

    pthread_barrier_wait(&barrier);

    std::cout << "shit2 - waitForJob called" << std::endl;
    waitForJob(j);
    std::cout << "shit2 - job is done." << std::endl;

    return NULL;
}

int main() {
    pthread_t t1, t2;
    std::cout << "Hello, World!" << std::endl;
    JobHandle j = startMapReduceJob();

    pthread_barrier_init(&barrier, NULL, 2);

    pthread_create(&t1, NULL, shit1, j);
    pthread_create(&t2, NULL, shit2, j);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    pthread_barrier_destroy(&barrier);

    return 0;
}
