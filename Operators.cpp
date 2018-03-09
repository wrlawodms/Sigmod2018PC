#include "Operators.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <mutex>
//---------------------------------------------------------------------------
using namespace std;

void Operator::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    if (auto p = parent.lock()) {
        int pending = __sync_sub_and_fetch(&p->pendingAsyncOperator, 1);
        if (pending == 0 && startParentAsync) 
            p->createAsyncTasks(ioService);
    } else {
        // root node
    }
    
}

//---------------------------------------------------------------------------
bool Scan::require(SelectInfo info)
// Require a column and add it to results
{
    if (info.binding!=relationBinding)
        return false;
    assert(info.colId<relation.columns.size());
    resultColumns.push_back(relation.columns[info.colId]);
    select2ResultColId[info]=resultColumns.size()-1;
    return true;
}
//---------------------------------------------------------------------------
void Scan::run()
// Run
{
// Nothing to do
    resultSize=relation.size;
}
//---------------------------------------------------------------------------
void Scan::asyncRun(boost::asio::io_service& ioService) {
    pendingAsyncOperator = 1;
    run();
    finishAsyncRun(ioService, true);
}
//---------------------------------------------------------------------------
vector<uint64_t*> Scan::getResults()
// Get materialized results
{
    return resultColumns;
}
//---------------------------------------------------------------------------
bool FilterScan::require(SelectInfo info)
// Require a column and add it to results
{
    if (info.binding!=relationBinding)
        return false;
    assert(info.colId<relation.columns.size());
    if (select2ResultColId.find(info)==select2ResultColId.end()) {
        // Add to results
        inputData.push_back(relation.columns[info.colId]);
        tmpResults.emplace_back();
        unsigned colId=tmpResults.size()-1;
        select2ResultColId[info]=colId;
    }
    return true;
}
//---------------------------------------------------------------------------
void FilterScan::copy2Result(uint64_t id)
// Copy to result
{
    for (unsigned cId=0;cId<inputData.size();++cId)
        tmpResults[cId].push_back(inputData[cId][id]);
    ++resultSize;
}
//---------------------------------------------------------------------------
bool FilterScan::applyFilter(uint64_t i,FilterInfo& f)
// Apply filter
{
    auto compareCol=relation.columns[f.filterColumn.colId];
    auto constant=f.constant;
    switch (f.comparison) {
        case FilterInfo::Comparison::Equal:
            return compareCol[i]==constant;
        case FilterInfo::Comparison::Greater:
            return compareCol[i]>constant;
        case FilterInfo::Comparison::Less:
            return compareCol[i]<constant;
    };
    return false;
}
//---------------------------------------------------------------------------
void FilterScan::run()
// Run
{
    for (uint64_t i=0;i<relation.size;++i) {
        bool pass=true;
        for (auto& f : filters) {
            pass&=applyFilter(i,f);
        }
        if (pass)
            copy2Result(i);
    }
}
//---------------------------------------------------------------------------
void FilterScan::asyncRun(boost::asio::io_service& ioService) {
    pendingAsyncOperator = 1;
    __sync_synchronize();
    createAsyncTasks(ioService);  
}

//---------------------------------------------------------------------------
void FilterScan::createAsyncTasks(boost::asio::io_service& ioService) {
    // can be parallize
    ioService.post([&]() {
        run();
        finishAsyncRun(ioService, true);
    });        
}

//---------------------------------------------------------------------------
vector<uint64_t*> Operator::getResults()
// Get materialized results
{
    vector<uint64_t*> resultVector;
    for (auto& c : tmpResults) {
        resultVector.push_back(c.data());
    }
    return resultVector;
}
//---------------------------------------------------------------------------
bool Join::require(SelectInfo info)
// Require a column and add it to results
{
    if (requestedColumns.count(info)==0) {
        bool success=false;
        if(left->require(info)) {
            requestedColumnsLeft.emplace_back(info);
            success=true;
        } else if (right->require(info)) {
            success=true;
            requestedColumnsRight.emplace_back(info);
        }
        if (!success)
            return false;

        tmpResults.emplace_back();
        requestedColumns.emplace(info);
    }
    return true;
}
//---------------------------------------------------------------------------
void Join::copy2Result(uint64_t leftId,uint64_t rightId)
// Copy to result
{
    //if array, cache util is better, becaouse now, each columns in tmpResults are seperated
    unsigned relColId=0;
    for (unsigned cId=0;cId<copyLeftData.size();++cId)
        tmpResults[relColId++].push_back(copyLeftData[cId][leftId]);

    for (unsigned cId=0;cId<copyRightData.size();++cId)
        tmpResults[relColId++].push_back(copyRightData[cId][rightId]);
    ++resultSize;
}
//---------------------------------------------------------------------------
void Join::run()
// Run
{
    /*
    left->require(pInfo.left);
    right->require(pInfo.right);
    left->run();
    right->run();
*/

    // Use smaller input for build
    if (left->resultSize>right->resultSize) {
        swap(left,right);
        swap(pInfo.left,pInfo.right);
        swap(requestedColumnsLeft,requestedColumnsRight);
    }

    auto leftInputData=left->getResults();
    auto rightInputData=right->getResults();

    // Resolve the input columns
    unsigned resColId=0;
    for (auto& info : requestedColumnsLeft) {
        copyLeftData.push_back(leftInputData[left->resolve(info)]);
        select2ResultColId[info]=resColId++;
    }
    for (auto& info : requestedColumnsRight) {
        copyRightData.push_back(rightInputData[right->resolve(info)]);
        select2ResultColId[info]=resColId++;
    }

    auto leftColId=left->resolve(pInfo.left);
    auto rightColId=right->resolve(pInfo.right);

    // Build phase
    auto leftKeyColumn=leftInputData[leftColId];
    //hashTable.reserve(left->resultSize*2);
   
    vector<thread> workers;
    const int thread_num = 40;
    uint64_t limit = left->resultSize;
    

    for (int i=0; i<thread_num; i++) {
        int length = limit/(thread_num-1);
        int start = i*length;
        if ( i == thread_num-1) {
            length = limit%(thread_num-1);
        }
        if (length == 0)
            continue;
        workers.push_back(thread([&, start, length](){ 
            for (uint64_t i=start,limit=start+length;i!=limit;++i) {        
                hashTable.insert(make_pair(leftKeyColumn[i],i));
            }
        }));
    }
    for (auto& worker : workers) {
        worker.join();
    }

    // Probe phase
    auto rightKeyColumn=rightInputData[rightColId];

    limit = right->resultSize;    
    vector<thread> workers2;
    
    mutex mt;
    for (int i=0; i<thread_num; i++) {
        int length = limit/(thread_num-1);
        int start = i*length;
        if (i == thread_num-1) {
            length = limit%(thread_num-1);
        }
        if (length == 0)
            continue;
        workers2.push_back(thread([&, start, length](){ 
            vector<vector<uint64_t>> localResults;
            for (unsigned i=0; i<requestedColumns.size(); i++)
                localResults.emplace_back();
            for (uint64_t i=start,limit=start+length;i!=limit;++i) {        
                auto rightKey=rightKeyColumn[i];
                auto range=hashTable.equal_range(rightKey);
                for (auto iter=range.first;iter!=range.second;++iter) {
                    //copy2Result(iter->second,i);
                    unsigned relColId=0;
                    
                    for (unsigned cId=0;cId<copyLeftData.size();++cId)
                        localResults[relColId++].push_back(copyLeftData[cId][iter->second]);

                    for (unsigned cId=0;cId<copyRightData.size();++cId)
                        localResults[relColId++].push_back(copyRightData[cId][i]);
                }
            }
            
            mt.lock();
            for (unsigned i=0; i<requestedColumns.size(); i++)  {
                tmpResults[i].insert(tmpResults[i].end(), localResults[i].begin(), localResults[i].end());
            }
            resultSize += localResults[0].size();
            mt.unlock();
            
        }));
    }
  
    for (auto& worker : workers2) {
        worker.join();
    }
    
}
//---------------------------------------------------------------------------
void Join::asyncRun(boost::asio::io_service& ioService) {
    pendingAsyncOperator = 2;
    __sync_synchronize();
    left->require(pInfo.left);
    right->require(pInfo.right);
    left->asyncRun(ioService);
    right->asyncRun(ioService);
}

//---------------------------------------------------------------------------
void Join::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
    // can be parallize
    ioService.post([&]() {
        run(); // call parallized task 
        finishAsyncRun(ioService, true);
    });        

            
}
//---------------------------------------------------------------------------
void SelfJoin::copy2Result(uint64_t id)
// Copy to result
{
    for (unsigned cId=0;cId<copyData.size();++cId)
        tmpResults[cId].push_back(copyData[cId][id]);
    ++resultSize;
}
//---------------------------------------------------------------------------
bool SelfJoin::require(SelectInfo info)
// Require a column and add it to results
{
    if (requiredIUs.count(info))
        return true;
    if(input->require(info)) {
        tmpResults.emplace_back();
        requiredIUs.emplace(info);
        return true;
    }
    return false;
}
//---------------------------------------------------------------------------
void SelfJoin::run()
// Run
{
    /*
    input->require(pInfo.left);
    input->require(pInfo.right);
    input->run();
    */
    inputData=input->getResults();

    for (auto& iu : requiredIUs) {
        auto id=input->resolve(iu);
        copyData.emplace_back(inputData[id]);
        select2ResultColId.emplace(iu,copyData.size()-1);
    }

    auto leftColId=input->resolve(pInfo.left);
    auto rightColId=input->resolve(pInfo.right);

    auto leftCol=inputData[leftColId];
    auto rightCol=inputData[rightColId];
    for (uint64_t i=0;i<input->resultSize;++i) {
        if (leftCol[i]==rightCol[i])
            copy2Result(i);
    }
}
//---------------------------------------------------------------------------
void SelfJoin::asyncRun(boost::asio::io_service& ioService) {
    pendingAsyncOperator = 1;
    __sync_synchronize();
    input->require(pInfo.left);
    input->require(pInfo.right);
    input->asyncRun(ioService);
}

//---------------------------------------------------------------------------
void SelfJoin::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
    // can be parallize
    ioService.post([&]() {
        run(); // call parallized task 
        finishAsyncRun(ioService, true);
    });        
            
}
//---------------------------------------------------------------------------
void Checksum::run()
// Run
{
    /*
    for (auto& sInfo : colInfo) {
        input->require(sInfo);
    }
    input->run();
    */
    auto results=input->getResults();

    for (auto& sInfo : colInfo) {
        auto colId=input->resolve(sInfo);
        auto resultCol=results[colId];
        uint64_t sum=0;
        resultSize=input->resultSize;
        for (auto iter=resultCol,limit=iter+input->resultSize;iter!=limit;++iter)
            sum+=*iter;
        checkSums.push_back(sum);
    }
}
//---------------------------------------------------------------------------
void Checksum::asyncRun(boost::asio::io_service& ioService, int queryIndex) {
    this->queryIndex = queryIndex;
    pendingAsyncOperator = 1;
    __sync_synchronize();
    for (auto& sInfo : colInfo) {
        input->require(sInfo);
    }
    input->asyncRun(ioService);
}

//---------------------------------------------------------------------------
void Checksum::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
    // can be parallize
    ioService.post([&]() {
        run(); // call parallized task 
        finishAsyncRun(ioService, false);
    });        
            
}
void Checksum::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    joiner.asyncResults[queryIndex] = std::move(checkSums);
    int pending = __sync_sub_and_fetch(&joiner.pendingAsyncJoin, 1);
    if (pending == 0)
        joiner.cvAsync.notify_one();
}
/*
//---------------------------------------------------------------------------
void PartitioningJoin::require(SelectInfo info) {
}
//---------------------------------------------------------------------------
void PartitioningJoin::run() {
}
*/
//---------------------------------------------------------------------------
