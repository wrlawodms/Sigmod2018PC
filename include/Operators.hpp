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
#include "tbb/concurrent_unordered_map.h"
#include "Relation.hpp"
#include "Parser.hpp"
#include "Config.hpp"
//#include "Joiner.hpp"

class Joiner;

//---------------------------------------------------------------------------
namespace std {
/// Simple hash function to enable use with unordered_map
template<> struct hash<SelectInfo> {
    std::size_t operator()(SelectInfo const& s) const noexcept { return s.binding ^ (s.colId << 5); }
};
};
//---------------------------------------------------------------------------
class Operator {
/// Operators materialize their entire result

protected:
    /// parent Operator
    std::weak_ptr<Operator> parent;
    /// Mapping from select info to data
    std::unordered_map<SelectInfo,unsigned> select2ResultColId;
    /// The materialized results
    std::vector<uint64_t*> resultColumns;
    /// The tmp results
    std::vector<std::vector<uint64_t>> tmpResults;
    /// mutex for local ops to global
    std::mutex localMt;
    /// if 0, all asyncrunning input oeraotr finish.
    int pendingAsyncOperator=-1;
    virtual void finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync=false); 

public:
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
    /// Run
    virtual void run() = 0;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) = 0;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) { throw; }
    /// SetParnet
    void setParent(std::shared_ptr<Operator> parent) { this->parent = parent; }
    /// Get  materialized results
    virtual std::vector<uint64_t*> getResults();
    /// Get materialized results size in bytes
    virtual unsigned getResultsSize();
    // Get one tuple size of the materialized result in bytes
    virtual unsigned getResultTupleSize();
    virtual unsigned getResultColCnt();
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

public:
    /// The constructor
    Scan(Relation& r,unsigned relationBinding) : relation(r), relationBinding(relationBinding) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    void run() override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// Get  materialized results
    virtual std::vector<uint64_t*> getResults() override;
    virtual unsigned getResultsSize() override;
    virtual unsigned getResultTupleSize() override;
    virtual unsigned getResultColCnt() override;
};
//---------------------------------------------------------------------------
class FilterScan : public Scan {
    /// The filter info
    std::vector<FilterInfo> filters;
    /// The input data
    std::vector<uint64_t*> inputData;
    /// Apply filter
    bool applyFilter(uint64_t id,FilterInfo& f);
    /// Copy tuple to result
    void copy2Result(uint64_t id);

    /// for parallel
    int pendingTask = -1;
public:
    /// The constructor
    FilterScan(Relation& r,std::vector<FilterInfo> filters) : Scan(r,filters[0].filterColumn.binding), filters(filters)  {};
    /// The constructor
    FilterScan(Relation& r,FilterInfo& filterInfo) : FilterScan(r,std::vector<FilterInfo>{filterInfo}) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    void run() override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
    /// create sync test
    void filterTask(boost::asio::io_service* ioService, unsigned start, unsigned length);
    /// Get  materialized results
    virtual std::vector<uint64_t*> getResults() override { return Operator::getResults(); }
    virtual unsigned getResultsSize() override { return Operator::getResultsSize(); }
    virtual unsigned getResultTupleSize() override { return Operator::getResultTupleSize(); }
    virtual unsigned getResultColCnt() override { return Operator::getResultColCnt(); }
    /// The result size
};
//---------------------------------------------------------------------------
class Join : public Operator {
    /// The input operators
    std::shared_ptr<Operator> left, right;
    /// The join predicate info
    PredicateInfo pInfo;
    /// Copy tuple to result
    void copy2Result(uint64_t leftId,uint64_t rightId);
    /// Create mapping for bindings
    void createMappingForBindings();

    using HT=std::unordered_multimap<uint64_t,uint64_t>;
    //using HT=tbb::concurrent_unordered_multimap<uint64_t,uint64_t>;

    int pendingMakingHistogram[2] = {-1,1};
    int pendingScattering[2] = {-1,1};
    int pendingPartitioning = -1;
    int pendingSubjoin = -1;

    // sequentially aloocated address for partitions, will be freed after materializing the result
    uint64_t* partitionTable[2];
    std::vector<std::vector<uint64_t*>> partition[2]; // just pointing partitionTable[], it is built after histogram, 각 파티션별 컬럼들의 위치를 포인팅  [LR][partition][column][tuple] P|C1sC2sC3s|P|C1sC2sC3s|...
    const unsigned partitionSize = L2_SIZE/2;
    unsigned cntPartition;
    std::vector<std::vector<unsigned>> histograms[2]; // [LR][taskIndex][partitionIndex]
    std::vector<unsigned> partitionLength[2]; // #tuples per each partition

    void histogramTask(boost::asio::io_service* ioService, int cntTask, int taskIndex, int leftOrRight, unsigned start, unsigned length);
    void scatteringTask(boost::asio::io_service* ioService, int cntTask, int taskIndex, int leftOrRight, unsigned start, unsigned length); 
    // for cache, partition must be allocated sequentially 
    void subJoinTask(boost::asio::io_service* ioService, std::vector<uint64_t*> left, unsigned leftLimit, std::vector<uint64_t*> right, unsigned rightLimit);  
    
    /// The hash table for the join
    HT hashTable; // is not used in async version
    /// Columns that have to be materialized
    std::unordered_set<SelectInfo> requestedColumns;
    /// Left/right columns that have been requested
    std::vector<SelectInfo> requestedColumnsLeft,requestedColumnsRight;


    /// The entire input data of left and right
    std::vector<uint64_t*> leftInputData,rightInputData;
    /// The input data that has to be copied
    //std::vector<uint64_t*> copyLeftData,copyRightData;
    /// key colums
    unsigned leftColId, rightColId;

public:
    /// The constructor
    Join(std::shared_ptr<Operator>& left,std::shared_ptr<Operator>& right,PredicateInfo pInfo) : left(left), right(right), pInfo(pInfo) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    void run() override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
};
//---------------------------------------------------------------------------
/*class PartitioningJoin : public Operaotor {


public:
    /// The constructor
    Join(std::shared_ptr<Operator>&& left,std::shared_ptr<Operator>&& right,PredicateInfo& pInfo) : left(std::move(left)), right(std::move(right)), pInfo(pInfo) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    void run() override;

}*/
//---------------------------------------------------------------------------
class SelfJoin : public Operator {
    /// The input operators
    std::shared_ptr<Operator> input;
    /// The join predicate info
    PredicateInfo pInfo;
    /// Copy tuple to result
    void copy2Result(uint64_t id);
    /// The required IUs
    std::set<SelectInfo> requiredIUs;

    /// The entire input data
    std::vector<uint64_t*> inputData;
    /// The input data that has to be copied
    std::vector<uint64_t*> copyData;

public:
    /// The constructor
    SelfJoin(std::shared_ptr<Operator>& input,PredicateInfo pInfo) : input(input), pInfo(pInfo) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    void run() override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
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

public:
    std::vector<uint64_t> checkSums;
    /// The constructor
    Checksum(Joiner& joiner, std::shared_ptr<Operator>& input,std::vector<SelectInfo> colInfo) : joiner(joiner), input(input), colInfo(colInfo) {};
    /// Request a column and add it to results
    bool require(SelectInfo info) override { throw; /* check sum is always on the highest level and thus should never request anything */ }
    /// Run
    void run() override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService, int queryIndex);
    virtual void asyncRun(boost::asio::io_service& ioService) {}
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
    /// root node register result value to joiner
    virtual void finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync=false) override;
};
//---------------------------------------------------------------------------
