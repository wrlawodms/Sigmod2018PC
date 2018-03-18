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
    //run();
    //finishAsyncRun(ioService, true);
//    cout << "FilterScan::asyncRun" << endl;
}

//---------------------------------------------------------------------------
void FilterScan::createAsyncTasks(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif  
    //const unsigned partitionSize = L2_SIZE/2;
    //const unsigned taskNum = CNT_PARTITIONS(relation.size*relation.columns.size()*8, partitionSize);
    const unsigned taskNum = THREAD_NUM;
    pendingTask = taskNum;
    __sync_synchronize(); 
    // unsigned length = partitionSize/(relation.columns.size()*8); 
    unsigned length = (relation.size+taskNum-1)/taskNum;
    for (unsigned i=0; i<taskNum; i++) {
        unsigned start = i*length;
        if (i == taskNum-1 && relation.size%length) {
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
    throw;
    //if array, cache util is better, becaouse now, each columns in tmpResults are seperated
    /*
    unsigned relColId=0;
    for (unsigned cId=0;cId<copyLeftData.size();++cId)
        tmpResults[relColId++].push_back(copyLeftData[cId][leftId]);

    for (unsigned cId=0;cId<copyRightData.size();++cId)
        tmpResults[relColId++].push_back(copyRightData[cId][rightId]);
    ++resultSize;
    */
}
//---------------------------------------------------------------------------

void Join::run()
  // Run
{
    throw;
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
*/
}

//void Join::run()
// Run
//{
    /*
    left->require(pInfo.left);
    right->require(pInfo.right);
    left->run();
    right->run();
*/
/*
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
*/
//---------------------------------------------------------------------------
void Join::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 2;
    left->require(pInfo.left);
    right->require(pInfo.right);
    __sync_synchronize();
    left->asyncRun(ioService);
    right->asyncRun(ioService);
}

//---------------------------------------------------------------------------
void Join::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif

    if (left->resultSize>right->resultSize) {
        swap(left,right);
        swap(pInfo.left,pInfo.right);
        swap(requestedColumnsLeft,requestedColumnsRight);
    }

    auto leftInputData=left->getResults();
    auto rightInputData=right->getResults(); 

    unsigned resColId = 0;
    for (auto& info : requestedColumnsLeft) {
        select2ResultColId[info]=resColId++;
    }
    for (auto& info : requestedColumnsRight) {
        select2ResultColId[info]=resColId++;
    }
    
    if (left->resultSize == 0) { // no reuslts
        finishAsyncRun(ioService, true);
        return;
    }

    leftColId=left->resolve(pInfo.left);
    rightColId=right->resolve(pInfo.right);

    // cntPartition = CNT_PARTITIONS(right->getResultsSize(), partitionSize); 
	cntPartition = CNT_PARTITIONS(left->getResultsSize(), partitionSize); 
    cntPartition = 1<<(Utils::log2(cntPartition-1)+1); // round up, power of 2 for hashing
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<") Right table size: " << right->getResultsSize() << " cnt_tuple: " << right->resultSize << " Left table size: " << left->getResultsSize() << " cnt_tuple: " << left->resultSize << " cntPartition: " << cntPartition << endl;
#endif
    if (cntPartition == 1) {
        pendingSubjoin = 1;
        __sync_synchronize();
        ioService.post(bind(&Join::subJoinTask, this, &ioService, leftInputData, left->resultSize, rightInputData, right->resultSize));
        return;
    }
    
    pendingPartitioning = 2;
    // must use jemalloc
    partitionTable[0] = (uint64_t*)malloc(left->getResultsSize());
    partitionTable[1] = (uint64_t*)malloc(right->getResultsSize());
    
    
    for (unsigned i=0; i<cntPartition; i++) {
        partition[0].emplace_back();
        partition[1].emplace_back();
        for (unsigned j=0; j<leftInputData.size(); j++) {
            partition[0][i].emplace_back();
        }
        for (unsigned j=0; j<rightInputData.size(); j++) {
            partition[1][i].emplace_back();
        }
    }
    
        // @TODO cachemiis may occur when a tuple is int the middle of partitioning point
/*
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks left_table_size(" << left->operatorIndex <<  "): "<< limit << " tuples " <<  left->getResultsSize()/1024.0 << "KB" << endl << "-> create #buildingTask: "<< taskNum << " partitionSize: " << partitionSize/left->getResultTupleSize() << " tuples(size:" << left->getResultTupleSize() << ")" << endl;
#endif
*/
    int cntTaskLeft = THREAD_NUM;
    int cntTaskRight = THREAD_NUM;
    taskLength[0] = (left->resultSize+cntTaskLeft-1)/cntTaskLeft;
	taskLength[1] = (right->resultSize+cntTaskRight-1)/cntTaskRight;

	// 각 튜플이 모두 다른 파티션에 들어간다고 했을떄, 한 튜플은 CACHE_LINE_SIZE*#COL 만큼의 메모리를 쓰게 된다. 이때, 한 태스크내에서의 모든 튜플들이 캐시라인에 들어가게 하는 튜플수가 maxLength
	//unsigned maxLengthLeft = L2_SIZE/(CACHE_LINE_SIZE*leftInputData.size());
	unsigned maxLengthLeft = L2_SIZE/(CACHE_LINE_SIZE*leftInputData.size());
	//unsigned maxLengthRight = L2_SIZE/(CACHE_LINE_SIZE*rightInputData.size());
	unsigned maxLengthRight = L2_SIZE/(CACHE_LINE_SIZE*rightInputData.size());
	
	if (taskLength[0] > maxLengthLeft) {
		// int preLength = taskLength[0];
		taskLength[0] = maxLengthLeft;
		cntTaskLeft = (left->resultSize+maxLengthLeft-1)/maxLengthLeft;	
		// cout << "Left too big length: " << preLength << " to " << maxLengthLeft << " " << cntTaskLeft << "tasks" <<  endl;
	}
	if (taskLength[1] > maxLengthRight) {
		// int preLength = taskLength[1];
		taskLength[1] = maxLengthRight;
		cntTaskRight = (right->resultSize+maxLengthRight-1)/maxLengthRight;	
		// cout << "Right too big length: " << preLength << " to " << maxLengthRight << " " << cntTaskRight << "tasks" << endl;
	}
	/*
    if (left->resultSize/cntTaskLeft < minTuplesPerTask) {
        cntTaskLeft = (left->resultSize+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength[0] = minTuplesPerTask;
    }
    if (right->resultSize/cntTaskRight < minTuplesPerTask) {
        cntTaskRight = (right->resultSize+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength[1] = minTuplesPerTask;
    }
	*/
	histograms[0].reserve(cntTaskLeft);
	histograms[1].reserve(cntTaskRight);
    for (int i=0; i<cntTaskLeft; i++) {
		histograms[0].emplace_back();
		histograms[0][i].reserve(CACHE_LINE_SIZE); // for preventing false sharing
        for (unsigned j=0; j<cntPartition; j++) {
            histograms[0][i].emplace_back();
        }
    }
    for (int i=0; i<cntTaskRight; i++) {
        histograms[1].emplace_back();
		histograms[1][i].reserve(CACHE_LINE_SIZE);
        for (unsigned j=0; j<cntPartition; j++) {
            histograms[1][i].emplace_back();
        }
    }
    pendingMakingHistogram[0] = cntTaskLeft;
    pendingMakingHistogram[1] = cntTaskRight;

    __sync_synchronize();    
    // int length = limit/(taskNum);
   
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<") make histogramTasks: cntTasks(L,R): " << cntTaskLeft << "," << cntTaskRight << " Length(L,R): " << taskLength[0] << ", " << taskLength[1] << endl;
#endif
    unsigned lengthLeft = taskLength[0];
    for (int i=0; i<cntTaskLeft; i++) {
        unsigned startLeft = i*lengthLeft;
        if (i == cntTaskLeft-1) {
            if (left->resultSize%lengthLeft) {
                lengthLeft = left->resultSize%lengthLeft;
            }
        }
        // cout << "Left len: (" << i <<")" << lengthLeft << endl;
        ioService.post(bind(&Join::histogramTask, this, &ioService, cntTaskLeft, i, 0, startLeft, lengthLeft)); // for left
    }
    unsigned lengthRight = taskLength[1];
    for (int i=0; i<cntTaskRight; i++) {
        unsigned startRight = i*lengthRight;
        if (i == cntTaskRight-1) {
            if (right->resultSize%lengthRight) {
                lengthRight = right->resultSize%lengthRight;
            }
        }
        // cout << "Right len: ("<<i<<")" << lengthRight << endl;
        ioService.post(bind(&Join::histogramTask, this, &ioService, cntTaskRight, i, 1, startRight, lengthRight)); // for right
    }
    
}

//---------------------------------------------------------------------------
void Join::histogramTask(boost::asio::io_service* ioService, int cntTask, int taskIndex, int leftOrRight, unsigned start, unsigned length) {
    uint64_t* keyColumn;
    vector<uint64_t*> inputData = left->getResults();
    if (!leftOrRight) { 
        inputData = left->getResults();
        keyColumn = inputData[leftColId];
    } else {
        inputData = right->getResults();
        keyColumn = inputData[rightColId];
    }
    
    for (unsigned i=start,limit=start+length; i<limit; i++) {
        histograms[leftOrRight][taskIndex][RADIX_HASH(keyColumn[i], cntPartition)]++; // cntPartition으로 나뉠 수 있도록하는 HASH
    }
    int remainder = __sync_sub_and_fetch(&pendingMakingHistogram[leftOrRight], 1);
    if (remainder == 0) { // gogo scattering
        for (int i=0; i<cntPartition; i++) {
            partitionLength[leftOrRight].push_back(0);
            for (int j=0; j<cntTask; j++) {
                partitionLength[leftOrRight][i] += histograms[leftOrRight][j][i];
                if (j != 0) { // make histogram to containprefix-sum
                    histograms[leftOrRight][j][i] += histograms[leftOrRight][j-1][i];
                }
            }
        }
        auto cntColumns = inputData.size();//requestedColumns.size();
        uint64_t* partAddress = partitionTable[leftOrRight];
        // partition[leftOrRight][0][0] = partitionTable[leftOrRight];
        for (unsigned i=0; i<cntPartition;i ++) {
            unsigned cntTuples = partitionLength[leftOrRight][i];// histograms[leftOrRight][cntTask-1][i];
            for (unsigned j=0; j<cntColumns; j++) {
                partition[leftOrRight][i][j] = partAddress + j*cntTuples;
            }
            partAddress += cntTuples*cntColumns;
        }
        pendingScattering[leftOrRight] = cntTask;
        __sync_synchronize(); //for histograms
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<") " << (!leftOrRight?"left":"right") << " histogram tasks are done." << endl << "create scatteringTasks " <<endl;
#endif
        unsigned length = taskLength[leftOrRight];
        for (int i=0; i<cntTask; i++) {
            unsigned start = i*length;
            if (i == cntTask-1) {
                if (!leftOrRight) {
                    if (left->resultSize%length) {
                        length = left->resultSize%length;
                    }
                } else {
                    if (right->resultSize%length) {
                        length = right->resultSize%length;
                    }
                }
            }
            // :cout << "scatter : "<< leftOrRight <<" len: ("<<i<<")" << length << endl;
            ioService->post(bind(&Join::scatteringTask, this, ioService, i, leftOrRight, start, length)); // for left
        }
    }
}
//---------------------------------------------------------------------------
void Join::scatteringTask(boost::asio::io_service* ioService, int taskIndex, int leftOrRight, unsigned start, unsigned length) {
    uint64_t* keyColumn;
    vector<uint64_t*> inputData;
    if (!leftOrRight) { 
        inputData = left->getResults();
        keyColumn = inputData[leftColId];
    } else {
        inputData = right->getResults();
        keyColumn = inputData[rightColId];
    }
    // copy histogram for cache
    vector<unsigned> insertOffs;
    for (int i=0; i<cntPartition; i++) { 
        insertOffs.push_back(0);
    }
    //copyHist.insert(copyHist.end(), histograms[leftOrRight][taskIndex].begin(), histograms[leftOrRight][taskIndex].end());
    for (unsigned i=start, limit=start+length; i<limit; i++) {
        unsigned hashResult = RADIX_HASH(keyColumn[i], cntPartition);
        unsigned insertBase;
        unsigned insertOff = insertOffs[hashResult];
        
        if (taskIndex == 0)
            insertBase = 0;
        else { 
            insertBase = histograms[leftOrRight][taskIndex-1][hashResult];
        }
        for (unsigned j=0; j<inputData.size(); j++) {
            // unsigned& tupleIdx = histograms[leftOrRight][taskIndex][hashResult];
            // << it may cause hard invalication storm
            partition[leftOrRight][hashResult][j][insertBase+insertOff] = inputData[j][i];
            // tupleIdx++; 
            // cout << pthread_self() << " " <<queryIndex<< " " << operatorIndex << " "<< taskIndex << " task "<< leftOrRight << " lr "<< j << " column " << insertBase+insertOff << " tuple: " << inputData[j][i] << "to part " << hashResult << endl;
        }
        insertOffs[hashResult]++;
    }
    int remainder = __sync_sub_and_fetch(&pendingScattering[leftOrRight], 1);
    if (remainder == 0) { // gogo scattering
        int remPart = __sync_sub_and_fetch(&pendingPartitioning, 1);
        if (remPart == 0) {
            pendingSubjoin = cntPartition;
            __sync_synchronize();
#ifdef VERBOSE
            cout << "Join("<< queryIndex << "," << operatorIndex <<") All partitioning are done. " << endl << "create subJoinTasks " <<endl;
#endif
            int taskNum = cntPartition;
            for (int i=0; i<taskNum; i++) {
                ioService->post(bind(&Join::subJoinTask, this, ioService, partition[0][i], partitionLength[0][i], partition[1][i], partitionLength[1][i]));
                 // cout << "Join("<< queryIndex << "," << operatorIndex <<")" << " Part Size(L,R) (" << i << "): " << partitionLength[0][i] << ", " << partitionLength[1][i] << endl;
            }
        }
    }
}

void Join::subJoinTask(boost::asio::io_service* ioService, vector<uint64_t*> localLeft, unsigned limitLeft, vector<uint64_t*> localRight, unsigned limitRight) {
    unordered_multimap<uint64_t, uint64_t> hashTable;
    // Resolve the partitioned columns
    std::vector<uint64_t*> copyLeftData,copyRightData;
    vector<vector<uint64_t>> localResults;
    uint64_t* leftKeyColumn = localLeft[leftColId];
    uint64_t* rightKeyColumn = localRight[rightColId];
    std::set<unsigned> leftSet;
    std::set<unsigned> rightSet;
    
    if (limitLeft == 0 || limitRight == 0) {
        goto sub_join_finish;
    }

    for (auto& info : requestedColumnsLeft) {
        copyLeftData.push_back(localLeft[left->resolve(info)]);
    }
    for (auto& info : requestedColumnsRight) {
        copyRightData.push_back(localRight[right->resolve(info)]);
    }
    // building
    //hashTable.reserve(limitLeft);
    hashTable.reserve(limitLeft*2);
    for (uint64_t i=0; i<limitLeft; i++) {
        hashTable.emplace(make_pair(leftKeyColumn[i],i));
    }
    for (unsigned i=0; i<requestedColumns.size(); i++) {
        localResults.emplace_back();
    }
    // probing
    for (uint64_t i=0; i<limitRight; i++) {
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
#ifdef VERBOSE
        //cout << "Join("<< queryIndex << "," << operatorIndex <<") subjoin finish. local result size: " << localResults[0].size() << endl;
#endif
    if (localResults[0].size() < 0) 
        goto sub_join_finish;
    localMt.lock();
    for (unsigned i=0; i<requestedColumns.size(); i++)  {
        tmpResults[i].insert(tmpResults[i].end(), localResults[i].begin(), localResults[i].end());
    }
    resultSize += localResults[0].size();
    localMt.unlock();

sub_join_finish:  
    int remainder = __sync_sub_and_fetch(&pendingSubjoin, 1);
    if (remainder == 0) {
#ifdef VERBOSE
        cout << "Join("<< queryIndex << "," << operatorIndex <<") join finish. result size: " << resultSize << endl;
#endif
        finishAsyncRun(*ioService, true); 
        if (cntPartition != 1) { // if 1, no partitioning
            free(partitionTable[0]);
            free(partitionTable[1]);
        }
    }
    //일단은 그냥 left로 building하자. 나중에 최적화된 방법으로 ㄲ
    
}

//---------------------------------------------------------------------------
// lrKeys : 0 : left, 1: right
/* 
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
*/
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
        input->require(sInfo);
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
//---------------------------------------------------------------------------
void Checksum::printAsyncInfo() {
	input->printAsyncInfo();
}

void SelfJoin::printAsyncInfo() {
	input->printAsyncInfo();
}
void Join::printAsyncInfo() {
	left->printAsyncInfo();
	right->printAsyncInfo();
	__sync_synchronize();
	cout << "pendingMakingHistogram[0] : " << pendingMakingHistogram[0] << endl;
	cout << "pendingMakingHistogram[1] : " << pendingMakingHistogram[1] << endl;
	cout << "pendingScattering[0] : " << pendingScattering[0] << endl;
	cout << "pendingScattering[1] : " << pendingScattering[1] << endl;
	cout << "pendingSubjoin : " << pendingSubjoin << endl;
	
}
void FilterScan::printAsyncInfo() {
	cout << "pendingFilterScan : " << pendingTask << endl;
}
void Scan::printAsyncInfo() {
}
