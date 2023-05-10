#include "MapReduceFramework.h"
#include <iostream>
#include <pthread.h>

#include <cstdio>
#include <string>
#include <array>
#include <unistd.h>

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    std::string content;
};

class KChar : public K2, public K3{
public:
    KChar(char c) : c(c) { }
    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    char c;
};

class VCount : public V2, public V3{
public:
    VCount(int count) : count(count) { }
    int count;
};


class CounterClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* value, void* context) const {
        std::array<int, 256> counts;
        counts.fill(0);
        for(const char& c : static_cast<const VString*>(value)->content) {
            counts[(unsigned char) c]++;
        }

        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0)
                continue;

            KChar* k2 = new KChar(i);
            VCount* v2 = new VCount(counts[i]);
            usleep(150000);
            emit2(k2, v2, context);
        }
        UNUSED(key);
        UNUSED(value);
        UNUSED(context);
    }

    virtual void reduce(const IntermediateVec* pairs,
                        void* context) const {
        /*
        const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
        int count = 0;
        for(const IntermediatePair& pair: *pairs) {
            count += static_cast<const VCount*>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        KChar* k3 = new KChar(c);
        VCount* v3 = new VCount(count);
        usleep(150000);
        emit3(k3, v3, context);
         */
        UNUSED(pairs);
        UNUSED(context);
    }
};

pthread_barrier_t barrier;

void *
shit1(void *arg) {
    JobHandle j = arg;

    pthread_barrier_wait(&barrier);

    std::cout << "shit1 - waitForJob called" << std::endl;
    waitForJob(j);
    std::cout << "shit1 - job is done." << std::endl;

    return NULL;
}

void *
shit2(void *arg) {
    JobHandle j = arg;

    pthread_barrier_wait(&barrier);

    std::cout << "shit2 - waitForJob called" << std::endl;
    waitForJob(j);
    std::cout << "shit2 - job is done." << std::endl;

    return NULL;
}

int
main() {
    pthread_t t1, t2;
    (void) t1;
    (void) t2;
    std::cout << "Hello, World!" << std::endl;

    CounterClient client;
    InputVec inputVec;
    OutputVec outputVec;
    VString s1("This string is full of characters");
    VString s2("Multithreading is awesome");
    VString s3("race conditions are bad");
    inputVec.push_back({nullptr, &s1});
    inputVec.push_back({nullptr, &s2});
    inputVec.push_back({nullptr, &s3});

    JobHandle job = startMapReduceJob(client, inputVec, outputVec, 4);

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

    return 0;
}
