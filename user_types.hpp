#include "MapReduceFramework.h"
#include <iostream>
#include <pthread.h>

#include <array>
#include <cstdio>
#include <string>
#include <unistd.h>

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

class VString : public V1
{
public:
  VString(std::string content)
    : content(content)
  {
  }
  std::string content;
};

class KChar
  : public K2
  , public K3
{
public:
  KChar(char c)
    : c(c)
  {
  }
  virtual bool operator<(const K2& other) const
  {
      return c < static_cast<const KChar&>(other).c;
  }
  virtual bool operator<(const K3& other) const
  {
      return c < static_cast<const KChar&>(other).c;
  }
    virtual  operator std::string() const
    {
      return std::string(1, c);
    }
  char c;
};

class VCount
  : public V2
  , public V3
{
public:
  VCount(int count)
    : count(count)
  {
  }
  int count;
};

class CounterClient : public MapReduceClient
{
public:
  void map(const K1* key, const V1* value, void* context) const
  {
    std::array<int, 256> counts;
    counts.fill(0);
    for (const char& c : static_cast<const VString*>(value)->content) {
      counts[(unsigned char)c]++;
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

  virtual void reduce(const IntermediateVec* pairs, void* context) const
  {
    const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
    int count = 0;
    for (const IntermediatePair& pair : *pairs) {
      count += static_cast<const VCount*>(pair.second)->count;
//      printf("reduce (worker #%d): deleting the pair <%c (%d)|%d>\n",
//             ((Worker *)context)->m_id,
//             ((KChar *)pair.first)->c,
//             (int)((KChar *)pair.first)->c,
//             ((VCount *)pair.second)->count);
      delete pair.first;
      delete pair.second;
    }
    KChar* k3 = new KChar(c);
    VCount* v3 = new VCount(count);
    usleep(150000);
    emit3(k3, v3, context);

    UNUSED(pairs);
    UNUSED(context);
  }
};
