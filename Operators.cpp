#include "Operators.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <mutex>
#include "Joiner.hpp"
#include "Config.hpp"
#include "Utils.hpp"

//---------------------------------------------------------------------------
using namespace std;

void Operator::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    if (auto p = parent.lock()) {
        int pending = __sync_sub_and_fetch(&p->pendingAsyncOperator, 1);
#ifdef VERBOSE
    cout << "Operator("<< queryIndex << "," << operatorIndex <<")::finishAsyncRun parent's pending: " << pending << endl;
#endif
        if (pending == 0 && startParentAsync) 
            p->createAsyncTasks(ioService);
    } else {
#ifdef VERBOSE
    cout << "Operator("<< queryIndex << "," << operatorIndex <<")::finishAsyncRun has no parent" << endl;
#endif
        // root node
    }
    
}

//---------------------------------------------------------------------------
bool Scan::require(SelectInfo info)
// Require a column and add it to results
{
    if (info.binding!=relationBinding)  {
        return false;
    }
    assert(info.colId<relation.columns.size());
    if (select2ResultColId.find(info)==select2ResultColId.end()) {
        resultColumns.push_back(relation.columns[info.colId]);
        select2ResultColId[info]=resultColumns.size()-1;
    }
    return true;
}
//---------------------------------------------------------------------------
unsigned Scan::getResultsSize() {
    return resultColumns.size()*relation.size*8; 
}
//---------------------------------------------------------------------------
unsigned Scan::getResultTupleSize() {
    return resultColumns.size()*8; 
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
#ifdef VERBOSE
    cout << "Scan("<< queryIndex << "," << operatorIndex <<")::asyncRun, Task" << endl;
#endif
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
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 1;
    __sync_synchronize();
    createAsyncTasks(ioService);  
//    cout << "FilterScan::asyncRun" << endl;
}

//---------------------------------------------------------------------------
void FilterScan::createAsyncTasks(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif
    // can be parallize
   
     
    const unsigned partitionSize = L2_SIZE/2;
    const unsigned taskNum = CNT_PARTITIONS(relation.size*relation.columns.size()*8, partitionSize);
    pendingTask = taskNum;
    __sync_synchronize(); 
    unsigned length = partitionSize/(relation.columns.size()*8); 
    for (unsigned i=0; i<taskNum; i++) {
        unsigned start = i*length;
        if ( i == taskNum-1) {
            length = relation.size%length;
        }
        ioService.post(bind(&FilterScan::filterTask, this, &ioService, start, length)); 
        /*
        ioService.post([&, start, legnth]() {
#ifdef VERBOSE
            cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::Task start: " << start << " length: " << length << endl;
#endif
            
            finishAsyncRun(ioService, true);
        });  
        */      
    }
}
//---------------------------------------------------------------------------
void FilterScan::filterTask(boost::asio::io_service* ioService, unsigned start, unsigned length) {
     
    vector<vector<uint64_t>> localResults;
    for (unsigned i=0; i<inputData.size(); i++) {
        localResults.emplace_back();
    }
    for (unsigned i=start;i<start+length;++i) {
        bool pass=true;
        for (auto& f : filters) {
            pass&=applyFilter(i,f);
        }
        if (pass) {
            for (unsigned cId=0;cId<inputData.size();++cId)
                localResults[cId].push_back(inputData[cId][i]);
        }
    }
    
    localMt.lock();
    for (unsigned cId=0;cId<inputData.size();++cId) {
        tmpResults[cId].insert(tmpResults[cId].end(), localResults[cId].begin(), localResults[cId].end()); 
    }
    resultSize += localResults[0].size();
    localMt.unlock();

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0) {
        finishAsyncRun(*ioService, true);
    }
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
unsigned Operator::getResultsSize() {
    return resultSize*tmpResults.size()*8;
}
//---------------------------------------------------------------------------
unsigned Operator::getResultTupleSize() {
    return tmpResults.size()*8;
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
/*
void Join::run()
  // Run
{
    */
    /*
  if (runInput) {
     left->require(pInfo.left);
     right->require(pInfo.right);
     left->run();
     right->run();
   }
*/
  // Use smaller input for build
  /*
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
  for (uint64_t i=0,limit=i+left->resultSize;i!=limit;++i) {
    hashTable.emplace(leftKeyColumn[i],i);
  }
  // Probe phase
  auto rightKeyColumn=rightInputData[rightColId];
  for (uint64_t i=0,limit=i+right->resultSize;i!=limit;++i) {
    auto rightKey=rightKeyColumn[i];
    auto range=hashTable.equal_range(rightKey);
    for (auto iter=range.first;iter!=range.second;++iter) {
      copy2Result(iter->second,i);
    }
  }
}*/

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
    const int thread_num = 20;
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
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 2;
    assert(left->require(pInfo.left));
    assert(right->require(pInfo.right));
    __sync_synchronize();
    left->asyncRun(ioService);
    right->asyncRun(ioService);
}

//---------------------------------------------------------------------------
void Join::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);

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
    //hashTable.reserve(left->resultSize*2);
   
    uint64_t limit = left->resultSize;
    const unsigned partitionSize = L2_SIZE/2;
    //const unsigned taskNum = (left->getResultsSize()+(partitionSize-1))/(partitionSize); // just roundup
    const unsigned taskNum = CNT_PARTITIONS(left->getResultsSize(), partitionSize);
        // @TODO cachemiis may occur when a tuple is int the middle of partitioning point
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks left_table_size(" << left->operatorIndex <<  "): "<< limit << " tuples " <<  left->getResultsSize()/1024.0 << "KB" << endl << "-> create #buildingTask: "<< taskNum << " partitionSize: " << partitionSize/left->getResultTupleSize() << " tuples(size:" << left->getResultTupleSize() << ")" << endl;
#endif

    pendingBuildingHashtable = taskNum;
   // __sync_synchronize(); // need it?
    uint64_t* leftKeyColumn=leftInputData[leftColId];
    uint64_t* rightKeyColumn=rightInputData[rightColId];
    vector<uint64_t*> lrKeys;
    lrKeys.push_back(leftKeyColumn);
    lrKeys.push_back(rightKeyColumn);
    if (taskNum == 0) {
        finishAsyncRun(ioService, true);
    }
    // int length = limit/(taskNum);
    int length = partitionSize/left->getResultTupleSize();
    for (unsigned i=0; i<taskNum; i++) {
        int start = i*length;
        if (i == taskNum-1) {
            length = limit%length;
        }
        ioService.post(bind(&Join::buildingTask, this, &ioService, lrKeys, start, length));
    }
  
  /*  
    ioService.post([&]() {
        run(); // call parallized task 
        finishAsyncRun(ioService, true);
    }); 
    */ 
}

//---------------------------------------------------------------------------
// lrKeys : 0 : left, 1: right 
void Join::buildingTask(boost::asio::io_service* ioService, vector<uint64_t*> lrKeys, unsigned start, unsigned length) {
    for (uint64_t i=start,limit=start+length;i!=limit;++i) {        
        hashTable.insert(make_pair(lrKeys[0][i],i));
    }
    int remainder = __sync_sub_and_fetch(&pendingBuildingHashtable, 1);
    if (remainder == 0) { 
        unsigned limit = right->resultSize;
        const unsigned partitionSize = L2_SIZE/2;
        const unsigned taskNum = CNT_PARTITIONS(right->getResultsSize(), partitionSize);
        //const unsigned taskNum = ((right->getResultsSize())+(L2_SIZE/2-1))/(L2_SIZE/2); 
        // @TODO cachemiis may occur when a tuple is int the middle of partitioning point
        pendingProbingHashtable = taskNum;
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks right_table_size(" << right->operatorIndex <<  "): "<< limit << " tuples " <<  right->getResultsSize()/1024.0 << "KB" << endl << "-> create #probingTask: "<< taskNum << " partitionSize: " << (partitionSize/right->getResultTupleSize()) << " tuples(size:" << right->getResultTupleSize() << ")" << endl;
#endif
        __sync_synchronize(); // for hash table
        //int length_p = limit/(taskNum);
        int length_p = partitionSize/right->getResultTupleSize();
        for (unsigned i=0; i<taskNum; i++) {
            int start_p = i*length_p;
            if (i == taskNum-1) { //  
                length_p = limit%length_p;
            }
            ioService->post(bind(&Join::probingTask, this, ioService, lrKeys, start_p, length_p));
        }
        
    }
    
}
void Join::probingTask(boost::asio::io_service* ioService, vector<uint64_t*> lrKeys, unsigned start, unsigned length) { 
    vector<vector<uint64_t>> localResults;
    unsigned rightKeyChecksum = 0;
    for (unsigned i=0; i<requestedColumns.size(); i++)
        localResults.emplace_back();
    for (uint64_t i=start,limit=start+length;i!=limit;++i) {        
        auto rightKey=lrKeys[1][i];
        rightKeyChecksum += rightKey;
        auto range=hashTable.equal_range(rightKey);
        for (auto iter=range.first;iter!=range.second;++iter) {
            unsigned relColId=0; 
            for (unsigned cId=0;cId<copyLeftData.size();++cId)
                localResults[relColId++].push_back(copyLeftData[cId][iter->second]);

            for (unsigned cId=0;cId<copyRightData.size();++cId)
                localResults[relColId++].push_back(copyRightData[cId][i]);
        }
    } 
    localMt.lock(); 
    for (unsigned i=0; i<requestedColumns.size(); i++)  {
        tmpResults[i].insert(tmpResults[i].end(), localResults[i].begin(), localResults[i].end());
    }
    resultSize += localResults[0].size();
    localMt.unlock();

    int remainder = __sync_sub_and_fetch(&pendingProbingHashtable, 1);
    if (remainder == 0) {
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::asyncRun end result_table: " << resultSize << " tuples "<< "used_hash_size: " << hashTable.size() << endl;
#endif
        finishAsyncRun(*ioService, true);
    }
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
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 1;
    input->require(pInfo.left);
    input->require(pInfo.right);
    __sync_synchronize();
    input->asyncRun(ioService);
//    cout << "SelfJoin::asyncRun" << endl;
}

//---------------------------------------------------------------------------
void SelfJoin::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif
    // can be parallize
    ioService.post([&]() {
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<"):: run task" << endl;
#endif
//        cout << "SelfJoin::Task" << endl;
        run(); // call parallized task 
        finishAsyncRun(ioService, true);
    });        
            
}
//---------------------------------------------------------------------------
void Checksum::run()
// Run
{
    /*
    for (auto& sInfo : yncOperatorolInfo) {
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
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex << ")::asyncRun()" << endl;
#endif
    this->queryIndex = queryIndex;
    pendingAsyncOperator = 1;
    for (auto& sInfo : colInfo) {
        assert(input->require(sInfo));
    }
    __sync_synchronize();
    input->asyncRun(ioService);
//    cout << "Checksum::asyncRun" << endl;
}

//---------------------------------------------------------------------------
void Checksum::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ")::createAsyncTasks" << endl;
#endif
    // can be parallize
    ioService.post([&]() {
#ifdef VERBOSE
    cout << "Checksum("<< queryIndex << "," << operatorIndex <<"):: run task" << endl;
#endif
        run(); // call parallized task 
        finishAsyncRun(ioService, false);
//        cout << "Checksum::Task" << endl;
    });        
            
}
void Checksum::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    joiner.asyncResults[queryIndex] = std::move(checkSums);
    int pending = __sync_sub_and_fetch(&joiner.pendingAsyncJoin, 1);
    if (pending == 0) {
#ifdef VERBOSE
        cout << "Checksum: finish query index: " << queryIndex << endl;
#endif
        unique_lock<mutex> lk(joiner.cvAsyncMt); // guard for missing notification
        joiner.cvAsync.notify_one();
    }
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
