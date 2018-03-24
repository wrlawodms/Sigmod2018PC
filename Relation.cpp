#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <algorithm>
#include "Config.hpp"
#include "Relation.hpp"
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

    struct Comparer{
        bool operator()(const pair<uint64_t, uint64_t> &a, pair<uint64_t, uint64_t> &b){
            return a.first < b.first;
        }
    } comparer;
    for (unsigned i=0;i<numColumns;++i) {
        vector<pair<uint64_t,uint64_t>> idx;
        //idx.reserve(size);
        for (uint64_t j=0;j<size;++j){
            idx.emplace_back(columns[i][j], j);
        }
        sort(idx.begin(), idx.end(), comparer); 
        vector<uint64_t*> res;
        //res.reserve(numColumns);
        uint64_t *area = (uint64_t*)aligned_alloc(CACHE_LINE_SIZE, numColumns * size * sizeof(uint64_t));
        for (unsigned j=0;j<numColumns;++j){
            res.emplace_back(&area[j*size]);
        }
        for (unsigned j=0;j<numColumns;++j){
            for (uint64_t k=0;k<size;k++){
                res[j][k]=columns[j][idx[k].second];
            }
        }
        sorted.emplace_back(res);
    }
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
