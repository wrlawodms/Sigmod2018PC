#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <set>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <mutex>
#include "Relation.hpp"
#include "Parser.hpp"
#include "Config.hpp"
#include "Column.hpp"
//#include "Joiner.hpp"

class Joiner;

//---------------------------------------------------------------------------
namespace std {
/// Simple hash function to enable use with unordered_map

template<> struct hash<SelectInfo> {
    std::size_t operator()(SelectInfo const& s) const noexcept { return s.binding ^ (s.colId << 5); }
};
/*
struct myHash {
    std::size_t operator()(uint64_t const& s) const noexcept { 
        uint64_t x = s;
        x = ((x >> 32) ^ x) * 0x45d9f3b;
        x = ((x >> 32) ^ x) * 0x45d9f3b;
        x = (x >> 32) ^ x;
        return x; 
    }
};
*/

};
//---------------------------------------------------------------------------
class Operator {
/// Operators materialize their entire result
friend Joiner; // for monitoring
protected:
    /// parent Operator
    std::weak_ptr<Operator> parent;
    /// Mapping from select info to data
    std::unordered_map<SelectInfo,unsigned> select2ResultColId;
    /// The materialized results
    std::vector<Column<uint64_t>> results;
	//std::vector<uint64_t*> resultColumns;
    /// mutex for local ops to global
//    std::mutex localMt;
    /// if 0, all asyncrunning input oeraotr finish.
    int pendingAsyncOperator=-1;
    virtual void finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync=false); 

public:
    int counted = 0; // 0 - no counting, 1 - make new counting, 2 - follow child op'counting
    bool isStopped = false; // if ture, children can stop
#ifdef VERBOSE
    unsigned operatorIndex;
    unsigned queryIndex;
    void setQeuryIndex(unsigned qI) { queryIndex = qI; }
    void setOperatorIndex(unsigned oI) { operatorIndex = oI; }
#endif
    /// Require a column and add it to results
    virtual bool require(SelectInfo info) = 0;
    /// Resolves a column
    unsigned resolve(SelectInfo info) { assert(select2ResultColId.find(info)!=select2ResultColId.end()); return select2ResultColId[info]; }
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) = 0;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) { throw; }
    /// SetParnet
    void setParent(std::shared_ptr<Operator> parent) { this->parent = parent; }
    /// Get  materialized results
    virtual std::vector<Column<uint64_t>>& getResults();
    /// Get materialized results size in bytes
    virtual uint64_t getResultsSize();
	/// stop all children 
    virtual void stop() {isStopped = true; __sync_synchronize();}
    // Print async info
	virtual void printAsyncInfo() = 0;
    /// The result size
    uint64_t resultSize=0;
    ///he destructor
    virtual ~Operator() {};
};
//---------------------------------------------------------------------------
class Scan : public Operator {
protected:
    /// The relation
    Relation& relation;
    /// The name of the relation in the query
    unsigned relationBinding;
    /// required info
    std::vector<SelectInfo> infos;

public:
    /// The constructor
    Scan(Relation& r,unsigned relationBinding) : relation(r), relationBinding(relationBinding) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    virtual uint64_t getResultsSize() override;
	// Print async info
	virtual void printAsyncInfo() override;
};
//---------------------------------------------------------------------------
class FilterScan : public Scan {
    /// The filter info
    std::vector<FilterInfo> filters;
    /// The input data
    std::vector<uint64_t*> inputData;
    /// tmpResults
	std::vector<std::vector<std::vector<uint64_t>>> tmpResults; // [partition][col][tuple]
    /// Apply filter
    bool applyFilter(uint64_t id,FilterInfo& f);
    /// Copy tuple to result
    void copy2Result(uint64_t id);
    /// for parallel
    int pendingTask = -1;
    
    unsigned minTuplesPerTask = 1000;

    void filterTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length);
public:
    /// The constructor
    FilterScan(Relation& r,std::vector<FilterInfo> filters) : Scan(r,filters[0].filterColumn.binding), filters(filters)  {};
    /// The constructor
    FilterScan(Relation& r,FilterInfo& filterInfo) : FilterScan(r,std::vector<FilterInfo>{filterInfo}) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
    /// create sync test
    virtual uint64_t getResultsSize() override { return Operator::getResultsSize(); }
	// Print async info
	virtual void printAsyncInfo() override;
    /// The result size
};
//---------------------------------------------------------------------------
class Join : public Operator {
    /// The input operators
    std::shared_ptr<Operator> left, right;
    /// The join predicate info
    PredicateInfo pInfo;
    /// tmpResults
	std::vector<std::vector<std::vector<std::vector<uint64_t>>>> tmpResults; // [partition][probingTaskIndex][col][tuple]
    /// Copy tuple to result
    void copy2Result(uint64_t leftId,uint64_t rightId);
    /// Create mapping for bindings
    void createMappingForBindings();

    char pad1[CACHE_LINE_SIZE];
    int pendingMakingHistogram[2*CACHE_LINE_SIZE]; // bug
    int pendingScattering[2*CACHE_LINE_SIZE];// bug, CACHE_LINE_SIZE/4 enough
    int pendingPartitioning = -1;
    char pad2[CACHE_LINE_SIZE];
    int pendingBuilding = -1;
    char pad3[CACHE_LINE_SIZE];
    int pendingProbing = 0;
    char pad4[CACHE_LINE_SIZE];

    // sequentially aloocated address for partitions, will be freed after materializing the result
    uint64_t* partitionTable[2] = {NULL, NULL};
    int allocTid = -1;
    const uint64_t partitionSize = L2_SIZE/8;
    uint64_t cntPartition;

    const unsigned hashThreshold = 4; //left size가 넘으면 해쉬사용 안넘으면 단순 비교 


    // variablse per partitions
    std::vector<unsigned> cntProbing; // determined in histogramTask
    std::vector<uint64_t> lengthProbing; 
    std::vector<unsigned> restProbing;
    std::vector<unsigned> resultIndex;
    // std::vector<std::unordered_multimap<uint64_t, uint64_t>> hashTables;


    uint64_t taskLength[2];
    uint64_t taskRest[2];
    const unsigned minTuplesPerTask = 100000; // minimum part table size
//    const unsigned minTuplesPerProbing = 1000; // minimum part table size

    std::vector<std::vector<uint64_t*>> partition[2]; // just pointing partitionTable[], it is built after histogram, 각 파티션별 컬럼들의 위치를 포인팅  [LR][partition][column][tuple] P|C1sC2sC3s|P|C1sC2sC3s|...
    std::vector<std::vector<uint64_t>> histograms[2]; // [LR][taskIndex][partitionIndex], 각 파티션에 대한 벡터는 heap에 allocate되나? 안그럼 invalidate storㅇ이 일어날거 같은데
    std::vector<uint64_t> partitionLength[2]; // #tuples per each partition

    std::vector<std::unordered_multimap<uint64_t, uint64_t>*> hashTablesIndices; // for using thread local storage 
    std::vector<std::unordered_map<uint64_t, uint64_t>*> hashTablesCnt; // for using thread local storage 
    bool cntBuilding = false;

    void histogramTask(boost::asio::io_service* ioService, int cntTask, int taskIndex, int leftOrRight, uint64_t start, uint64_t length);
    void scatteringTask(boost::asio::io_service* ioService, int taskIndex, int leftOrRight, uint64_t start, uint64_t length); 
    // for cache, partition must be allocated sequentially 
    // void subJoinTask(boost::asio::io_service* ioService, int taskIndex, std::vector<uint64_t*> left, uint64_t leftLimit, std::vector<uint64_t*> right, uint64_t rightLimit);  
    void buildingTask(boost::asio::io_service* ioService, int taskIndex, std::vector<uint64_t*> left, uint64_t leftLimit, std::vector<uint64_t*> right, uint64_t rightLimit);  
    void probingTask(boost::asio::io_service* ioService, int partIndex, int taskIndex, std::vector<uint64_t*> left, uint64_t leftLength, std::vector<uint64_t*> right, uint64_t start, uint64_t length);  
    
    /// Columns that have to be materialized
    std::unordered_set<SelectInfo> requestedColumns;
    /// Left/right columns that have been requested
    std::vector<SelectInfo> requestedColumnsLeft,requestedColumnsRight;


    /// The entire input data of left and right
    std::vector<Column<uint64_t>> leftInputData,rightInputData;
    /// The input data that has to be copied
    //std::vector<uint64_t*> copyLeftData,copyRightData;
    /// key colums
    unsigned leftColId, rightColId;

public:
    /// The constructor
    Join(std::shared_ptr<Operator>& left,std::shared_ptr<Operator>& right,PredicateInfo pInfo) : left(left), right(right), pInfo(pInfo) {};
    ~Join() {

//        free(partitionTable[0]);
//        free(partitionTable[1]);
//        if (!hashTables.size())
//            return;
/*
        for (unsigned i=0; i<cntPartition; i++) {
            if(hashTables[i] != NULL)
                delete hashTables[i];
        }
        */
    }
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
	// Print async info
	virtual void printAsyncInfo() override;
	/// stop all children 
    virtual void stop() {
        isStopped = true;
        __sync_synchronize();
        if (!left->isStopped)
            left->stop();
        if (!right->isStopped)
            right->stop(); 
    }
};
//---------------------------------------------------------------------------
class SelfJoin : public Operator {
    /// The input operators
    std::shared_ptr<Operator> input;
    /// The join predicate info
    PredicateInfo pInfo;
    /// tmpResults
	std::vector<std::vector<std::vector<uint64_t>>> tmpResults; // [partition][col][tuple]
    /// Copy tuple to result
    void copy2Result(uint64_t id);
    /// The required IUs
    std::set<SelectInfo> requiredIUs;

    /// The input data that has to be copied
    std::vector<Column<uint64_t>*> copyData;
    int pendingTask = -1;
    unsigned minTuplesPerTask = 1000;
    void selfJoinTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length);

public:
    /// The constructor
    SelfJoin(std::shared_ptr<Operator>& input,PredicateInfo pInfo) : input(input), pInfo(pInfo) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
	// Print async info
	virtual void printAsyncInfo() override;
};
//---------------------------------------------------------------------------
class Checksum : public Operator {
    Joiner& joiner;
    /// The input operator
    std::shared_ptr<Operator> input;
    /// The join predicate info
    std::vector<SelectInfo> colInfo;
    /// Query Index
    int queryIndex;
    
    int pendingTask = -1;
    unsigned minTuplesPerTask = 1000;
    void checksumTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length);

public:
    std::vector<uint64_t> checkSums;
    /// The constructor
    Checksum(Joiner& joiner, std::shared_ptr<Operator>& input,std::vector<SelectInfo> colInfo) : joiner(joiner), input(input), colInfo(colInfo) {};
    /// Request a column and add it to results
    bool require(SelectInfo info) override { throw; /* check sum is always on the highest level and thus should never request anything */ }
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService, int queryIndex);
    virtual void asyncRun(boost::asio::io_service& ioService) {}
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
    /// root node register result value to joiner
    virtual void finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync=false) override;
	// Print async info
	virtual void printAsyncInfo() override;
};
//---------------------------------------------------------------------------
