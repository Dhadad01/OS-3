#include "MapReduceFramework.h"
#include <iostream>
#include <pthread.h>

#include <array>
#include <cstdio>
#include <string>
#include <unistd.h>

#include "job.hpp"

#include "user_types.hpp"

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED


// pthread_barrier_t barrier;
// 
// void*
// shit1(void* arg)
// {
//   JobHandle j = arg;
// 
//   pthread_barrier_wait(&barrier);
// 
//   std::cout << "shit1 - waitForJob called" << std::endl;
//   waitForJob(j);
//   std::cout << "shit1 - job is done." << std::endl;
// 
//   return NULL;
// }
// 
// void*
// shit2(void* arg)
// {
//   JobHandle j = arg;
// 
//   pthread_barrier_wait(&barrier);
// 
//   std::cout << "shit2 - waitForJob called" << std::endl;
//   waitForJob(j);
//   std::cout << "shit2 - job is done." << std::endl;
// 
//   return NULL;
// }

#define NUM_OF_THREADS 2

int
main()
{
  pthread_t t1, t2;
  (void)t1;
  (void)t2;
  std::cout << "Hello, World!" << std::endl;

  CounterClient client;
  InputVec inputVec;
  OutputVec outputVec;
  VString s1("This string is full of characters");
  VString s2("Multithreading is awesome");
  VString s3("race conditions are bad");
  inputVec.push_back({ nullptr, &s1 });
  inputVec.push_back({ nullptr, &s2 });
  inputVec.push_back({ nullptr, &s3 });

  JobHandle job = startMapReduceJob(client, inputVec, outputVec, NUM_OF_THREADS);

  waitForJob(job);

  //    JobHandle j = startMapReduceJob();
  //
  //    pthread_barrier_init(&barrier, NULL, 2);
  //
  //    pthread_create(&t1, NULL, shit1, j);
  //    pthread_create(&t2, NULL, shit2, j);
  //
  //    pthread_join(t1, NULL);
  //    pthread_join(t2, NULL);
  //
  //    pthread_barrier_destroy(&barrier);

  std::cout << __FUNCTION__ << ": closing job handle" << std::endl;
  Job *j = static_cast<Job *>(job);
    (void)j;
  closeJobHandle(job);
  j = nullptr;

  std::cout << __FUNCTION__ << ": about to print the output of map-reduce" << std::endl;
  for (const OutputPair& p : outputVec) {
    std::cout << __FUNCTION__ << ": "
              << "output pair: < " << static_cast<KChar*>(p.first)->c << ", "
              << static_cast<VCount*>(p.second)->count << ">" << std::endl;
    delete p.first;
    delete p.second;
  }

  return 0;
}
