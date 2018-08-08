#pragma once
#include <fstream>
#include "Relation.hpp"
#include "MemoryPool.hpp"

// Utils macro, maybe faster than static method?

#define LIKELY(x) __builtin_expect((x),1)
#define UNLIKELY(x) __builtin_expect((x),0)

#define CNT_PARTITIONS(WHOLE,PART) (((WHOLE)+((PART)-1))/(PART))
//#define ROUND_DOWN(value, bit) ((value)&(~((1<<bit)-1))
#define RADIX_HASH(value, base) ((value)&(base-1))
//---------------------------------------------------------------------------
// per thread variables
extern MemoryPool** localMemPool;
extern thread_local int tid;
extern int nextTid;
//---------------------------------------------------------------------------
extern unsigned cnt;
class Utils {
public:
    /// Create a dummy relation
    static Relation createRelation(uint64_t size,uint64_t numColumns);

    /// Store a relation in all formats
    static void storeRelation(std::ofstream& out,Relation& r,unsigned i);

    /// flooring
    static int log2(unsigned v);
};
//---------------------------------------------------------------------------
