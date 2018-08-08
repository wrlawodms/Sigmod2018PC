#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include "Relation.hpp"
#include "Config.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Relation::storeRelation(const string& fileName)
// Stores a relation into a binary file
{
    ofstream outFile;
    outFile.open(fileName,ios::out|ios::binary);
    outFile.write((char*)&size,sizeof(size));
    auto numColumns=columns.size();
    outFile.write((char*)&numColumns,sizeof(size_t));
    for (auto c : columns) {
        outFile.write((char*)c,size*sizeof(uint64_t));
    }
    outFile.close();
}
//---------------------------------------------------------------------------
void Relation::storeRelationCSV(const string& fileName)
// Stores a relation into a file (csv), e.g., for loading/testing it with a DBMS
{
    ofstream outFile;
    outFile.open(fileName+".tbl",ios::out);
    for (uint64_t i=0;i<size;++i) {
        for (auto& c : columns) {
            outFile << c[i] << '|';
        }
        outFile << "\n";
    }
}
//---------------------------------------------------------------------------
void Relation::dumpSQL(const string& fileName,unsigned relationId)
// Dump SQL: Create and load table (PostgreSQL)
{
    ofstream outFile;
    outFile.open(fileName+".sql",ios::out);
    // Create table statement
    outFile << "CREATE TABLE r" << relationId << " (";
    for (unsigned cId=0;cId<columns.size();++cId) {
        outFile << "c" << cId << " bigint" << (cId<columns.size()-1?",":"");
    }
    outFile << ");\n";
    // Load from csv statement
    outFile << "copy r" << relationId << " from 'r" << relationId << ".tbl' delimiter '|';\n";
}
//---------------------------------------------------------------------------
void Relation::loadRelation(const char* fileName)
{
    int fd = open(fileName, O_RDONLY);
    if (fd==-1) {
        cerr << "cannot open " << fileName << endl;
        throw;
    }

    // Obtain file size
    struct stat sb;
    if (fstat(fd,&sb)==-1)
        cerr << "fstat\n";

    auto length=sb.st_size;
    
    char* addr=static_cast<char*>(mmap(nullptr,length,PROT_READ,MAP_PRIVATE,fd,0u));
    //madvise(addr, length, MADV_SEQUENTIAL);
    if (addr==MAP_FAILED) {
        cerr << "cannot mmap " << fileName << " of length " << length << endl;
        throw;
    }

    if (length<16) {
        cerr << "relation file " << fileName << " does not contain a valid header" << endl;
        throw;
    }

    this->size=*reinterpret_cast<uint64_t*>(addr);
    addr+=sizeof(size);
    auto numColumns=*reinterpret_cast<size_t*>(addr);
    addr+=sizeof(size_t);
    for (unsigned i=0;i<numColumns;++i) {
        this->columns.push_back(reinterpret_cast<uint64_t*>(addr));
        addr+=size*sizeof(uint64_t);
    }
    needCount.resize(numColumns, -1);
    counted.resize(numColumns);
}
//---------------------------------------------------------------------------
Relation::Relation(const char* fileName) : ownsMemory(false)
// Constructor that loads relation from disk
{
    loadRelation(fileName);
}
//---------------------------------------------------------------------------
Relation::~Relation()
// Destructor
{
    if (ownsMemory) {
        for (auto c : columns)
            delete[] c;
    }
}
//---------------------------------------------------------------------------
void Relation::loadStat(unsigned colId)
{
    auto &c(columns[colId]);
    unordered_set<uint64_t> cntSet;
    const unsigned stat_sample = size/SAMPLING_CNT == 0 ? 1 : size/SAMPLING_CNT ;
    for (unsigned i = 0; i < size; i+=stat_sample){
        cntSet.insert(c[i]);
    }
    bool res = ((size / stat_sample / cntSet.size()) >= COUNT_THRESHOLD);
    if (res){
        unordered_map<uint64_t, uint64_t> cntMap;
        for (unsigned i = 0; i < size; ++i){
            ++cntMap[c[i]];
        }
        uint64_t sizep = 0;
        uint64_t *vals= new uint64_t[cntMap.size() * 2];
        uint64_t *cnts= vals + cntMap.size();
        for (auto &it: cntMap){
            vals[sizep] = it.first;
            cnts[sizep++] = it.second;
        }
        counted[colId].emplace_back(vals);
        counted[colId].emplace_back(cnts);
    }
    needCount[colId] = res;
}
