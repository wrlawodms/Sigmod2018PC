#pragma once
#include <fstream>
#include "Relation.hpp"

// Utils macro, maybe faster than static method?
#define CNT_PARTITIONS(WHOLE,PART) (((WHOLE)+((PART)-1))/(PART))

//---------------------------------------------------------------------------
class Utils {
public:
    /// Create a dummy relation
    static Relation createRelation(uint64_t size,uint64_t numColumns);

    /// Store a relation in all formats
    static void storeRelation(std::ofstream& out,Relation& r,unsigned i);

};
//---------------------------------------------------------------------------
