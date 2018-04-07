#include "Operators.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <mutex>
#include "Joiner.hpp"
#include "Config.hpp"
#include "Utils.hpp"
#include <sys/mman.h>

//---------------------------------------------------------------------------
using namespace std;

#ifdef ANALYZE
extern uint64_t cntCounted;
#endif
void Operator::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    if (auto p = parent.lock()) {
        int pending = __sync_sub_and_fetch(&p->pendingAsyncOperator, 1);
        assert(pending>=0);
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
//        resultColumns.push_back(relation.columns[info.colId]);
        results.emplace_back(1);
        infos.push_back(info);
		select2ResultColId[info]=results.size()-1;
    }
    return true;
}
//---------------------------------------------------------------------------
uint64_t Scan::getResultsSize() {
    return results.size()*relation.size*8; 
}
//---------------------------------------------------------------------------
void Scan::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "Scan("<< queryIndex << "," << operatorIndex <<")::asyncRun, Task" << endl;
#endif
    pendingAsyncOperator = 0;
    if (infos.size() == 1 && !relation.counted[infos[0].colId].empty()){
        resultSize = relation.counted[infos[0].colId][1] - relation.counted[infos[0].colId][0];
        results[0].addTuples(0, relation.counted[infos[0].colId][0], resultSize); //Count Column
        results[0].fix();
        results.emplace_back(1);
        results[1].addTuples(0, relation.counted[infos[0].colId][1], resultSize); //Count Column
        results[1].fix();
        counted = 2;
    }
    else{
        for (int i=0; i<infos.size(); i++) {
            results[i].addTuples(0, relation.columns[infos[i].colId], relation.size);
            results[i].fix();
        }
        resultSize=relation.size; 
    }
    finishAsyncRun(ioService, true);
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
        infos.push_back(info);
//        tmpResults.emplace_back();
        unsigned colId=infos.size()-1;
        select2ResultColId[info]=colId;
    }
    return true;
}
//---------------------------------------------------------------------------
bool FilterScan::applyFilter(uint64_t i,FilterInfo& f)
// Apply filter
{
    auto compareCol(counted == 2 ? relation.counted[f.filterColumn.colId][0] :
            relation.columns[f.filterColumn.colId]);
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
void FilterScan::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 0;
    __sync_synchronize();
    createAsyncTasks(ioService);  
}

//---------------------------------------------------------------------------
void FilterScan::createAsyncTasks(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif  
    //const uint64_t partitionSize = L2_SIZE/2;
    //const unsigned taskNum = CNT_PARTITIONS(relation.size*relation.columns.size()*8, partitionSize);
    int cntTask = THREAD_NUM;
    uint64_t size = relation.size;
    if (infos.size() == 1){
        bool pass = true;
        for (auto &f:filters){
            if (f.filterColumn.colId != infos[0].colId){
                pass = false;
                break;
            }
        }
        if (pass && !relation.counted[infos[0].colId].empty()){
            //Use pre-processed count Column
            counted = 2;
            size = relation.counted[infos[0].colId][1] - relation.counted[infos[0].colId][0];
#ifdef ANALYZE
            __sync_fetch_and_add(&cntCounted, 1);
#endif
        }
        if (!counted && relation.needCount[infos[0].colId]){
            counted = 1;
#ifdef ANALYZE
            __sync_fetch_and_add(&cntCounted, 1);
#endif
        }
    }

    uint64_t taskLength = size/cntTask;
    uint64_t rest = size%cntTask;
    
    if (taskLength < minTuplesPerTask) {
        cntTask = size/minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = size/cntTask;
        rest = size%cntTask;
    }
    
    pendingTask = cntTask;
    
    if (counted == 2){
        inputData.emplace_back(relation.counted[infos[0].colId][0]);
        inputData.emplace_back(relation.counted[infos[0].colId][1]);
        results.emplace_back(cntTask);
        results.emplace_back(cntTask);
    }
    else{
        for (auto &sInfo : infos) {
            inputData.emplace_back(relation.columns[sInfo.colId]);
            results.emplace_back(cntTask);
        }
        if (counted){
            results.emplace_back(cntTask);
        }
    }
	for (int i=0; i<cntTask; i++) {
		tmpResults.emplace_back();
	}
	
    __sync_synchronize(); 
    // uint64_t length = partitionSize/(relation.columns.size()*8); 
    uint64_t start = 0;
    for (unsigned i=0; i<cntTask; i++) {
        uint64_t length = taskLength;
        if (rest) {
            length++;
            rest--;
        }
        ioService.post(bind(&FilterScan::filterTask, this, &ioService, i, start, length)); 
        start += length;
    }
}
//---------------------------------------------------------------------------
void FilterScan::filterTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length) {
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    
    for (int j=0; j<inputData.size(); j++) {
        localResults.emplace_back();
    }
    if (counted){
        localResults.emplace_back(); // For Count Column
    }
    /*
    for (unsigned cId=0;cId<inputData.size();++cId) {
        localResults[cId].reserve(length);
    }*/

    unsigned colSize = inputData.size();
    unordered_map<uint64_t, unsigned> cntMap;
    for (uint64_t i=start;i<start+length;++i) {
        bool pass=true;
        for (auto& f : filters) {
            if(!(pass=applyFilter(i,f)))
                break;
        }
        if (pass) {
            if (counted == 1){ // Only one Column will be used
                auto iter = cntMap.find(inputData[0][i]);
                if (iter != cntMap.end()){
                    ++localResults[1][iter->second];
                }
                else{
                    localResults[0].push_back(inputData[0][i]);
                    localResults[1].push_back(1);
                    cntMap.insert(iter, pair<uint64_t, unsigned>(inputData[0][i], localResults[1].size()-1));
                }
            }
            else{
                // If count == 2, colSize already contains count column
                for (unsigned cId=0;cId<colSize;++cId)
                    localResults[cId].push_back(inputData[cId][i]);
            }
        }
    }

    for (unsigned cId=0;cId<colSize;++cId) {
		results[cId].addTuples(taskIndex, localResults[cId].data(), localResults[cId].size());
    }
    if (counted == 1){// Use calculated count column
        results[1].addTuples(taskIndex, localResults[1].data(), localResults[1].size());
    }
    //resultSize += localResults[0].size();
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0) {
        for (unsigned cId=0;cId<colSize;++cId) {
            results[cId].fix();
        }
        if (counted == 1){
            results[1].fix();
        }
        finishAsyncRun(*ioService, true);
    }
}

//---------------------------------------------------------------------------
vector<Column<uint64_t>>& Operator::getResults()
// Get materialized results
{
//    vector<uint64_t*> resultVector;
//    for (auto& c : tmpResults) {
//        resultVector.push_back(c.data());
//    }
    return results;
}
//---------------------------------------------------------------------------
uint64_t Operator::getResultsSize() {
    return resultSize*results.size()*8;
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

        requestedColumns.emplace(info);
    }
    return true;
}
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

    auto& leftInputData=left->getResults();
    auto& rightInputData=right->getResults(); 

    unsigned resColId = 0;
    for (auto& info : requestedColumnsLeft) {
        select2ResultColId[info]=resColId++;
    }
    for (auto& info : requestedColumnsRight) {
        select2ResultColId[info]=resColId++;
    }
    
    if (requestedColumnsLeft.size() == 0 ||
        (requestedColumnsLeft.size() == 1 && requestedColumnsLeft[0] == pInfo.left)) {
        cntBuilding = true;
    }

    
    if (left->resultSize == 0) { // no reuslts
        finishAsyncRun(ioService, true);
        //left = nullptr;
        //right = nullptr;
        return;
    }

    if (requestedColumnsLeft.size() + requestedColumnsRight.size() == 1){
        auto &sInfo(requestedColumnsLeft.size() == 1 ?
                requestedColumnsLeft[0] : requestedColumnsRight[0]);
        counted = Joiner::relations[sInfo.relId].needCount[sInfo.colId];
#ifdef ANALYZE
        if (counted)
            __sync_fetch_and_add(&cntCounted, 1);
#endif
    }
    if ((left->counted || right->counted || cntBuilding) && !counted ){
        counted = 2;
#ifdef ANALYZE
        __sync_fetch_and_add(&cntCounted, 1);
#endif
    }

    leftColId=left->resolve(pInfo.left);
    rightColId=right->resolve(pInfo.right);

    //cntPartition = CNT_PARTITIONS(right->getResultsSize(), partitionSize); 
	cntPartition = CNT_PARTITIONS(left->resultSize*8*2, partitionSize); // uint64*2(key, value)
    //CNT_PARTITIONS(left->getResultsSize(), partitionSize); 
    if (cntPartition < 32) 
        cntPartition = 32; // < left->getResultsSize() ? 32 : left->getResultsSize();
    cntPartition = 1<<(Utils::log2(cntPartition-1)+1); // round up, power of 2 for hashing
    	
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<") Right table size: " << right->getResultsSize() << " cnt_tuple: " << right->resultSize << " Left table size: " << left->getResultsSize() << " cnt_tuple: " << left->resultSize << " cntPartition: " << cntPartition << endl;
#endif
    
    /*
    if (cntPartition == 1) {
        pendingSubjoin = 1;
        __sync_synchronize();
        ioService.post(bind(&Join::subJoinTask, this, &ioService, leftInputData, left->resultSize, rightInputData, right->resultSize));
        return;
    }*/
    /*
    for (int i=0; i<requestedColumns.size(); i++) {
        results.emplace_back(cntPartition);
    }
    */
    
    // bloom filter
    /*
    bloomArgs.projected_element_count = left->resultSize/cntPartition/2;
    bloomArgs.false_positive_probability = 0.1; 
    assert(bloomArgs);
    bloomArgs.compute_optimal_parameters();
    */
    pendingPartitioning = 2;
//    partitionTable[0] = (uint64_t*)malloc(left->getResultsSize());
//    partitionTable[1] = (uint64_t*)malloc(right->getResultsSize());
//    partitionTable[0] = (uint64_t*)aligned_alloc(CACHE_LINE_SIZE, left->getResultsSize());
//    partitionTable[1] = (uint64_t*)aligned_alloc(CACHE_LINE_SIZE, right->getResultsSize());
    partitionTable[0] = (uint64_t*)localMemPool[tid]->alloc(left->getResultsSize());
    partitionTable[1] = (uint64_t*)localMemPool[tid]->alloc(right->getResultsSize());
    allocTid = tid; 
/*
    if (left->getResultsSize() > 2*1024*1024) 
        madvise(partitionTable[0], left->getResultsSize(), MADV_HUGEPAGE);    
    if (right->getResultsSize() > 2*1024*1024) 
        madvise(partitionTable[1], right->getResultsSize(), MADV_HUGEPAGE);    
  */  
    for (uint64_t i=0; i<cntPartition; i++) {
        partition[0].emplace_back();
        partition[1].emplace_back();
        for (unsigned j=0; j<leftInputData.size(); j++) {
            partition[0][i].emplace_back();
        }
        for (unsigned j=0; j<rightInputData.size(); j++) {
            partition[1][i].emplace_back();
        }
        
		tmpResults.emplace_back();
    }
    
        // @TODO cachemiis may occur when a tuple is int the middle of partitioning point
/*
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks left_table_size(" << left->operatorIndex <<  "): "<< limit << " tuples " <<  left->getResultsSize()/1024.0 << "KB" << endl << "-> create #buildingTask: "<< taskNum << " partitionSize: " << partitionSize/left->getResultTupleSize() << " tuples(size:" << left->getResultTupleSize() << ")" << endl;
#endif
*/
    int cntTaskLeft = THREAD_NUM;
    int cntTaskRight = THREAD_NUM;
    taskLength[0] = left->resultSize/cntTaskLeft;
	taskLength[1] = right->resultSize/cntTaskRight;
    taskRest[0] = left->resultSize%cntTaskLeft; 
    taskRest[1] = right->resultSize%cntTaskRight; 

	// 각 튜플이 모두 다른 파티션에 들어간다고 했을떄, 한 튜플은 CACHE_LINE_SIZE*#COL 만큼의 메모리를 쓰게 된다. 이때, 한 태스크내에서의 모든 튜플들이 캐시라인에 들어가게 하는 튜플수가 maxLength
	//unsigned maxLengthLeft = L2_SIZE/(CACHE_LINE_SIZE*leftInputData.size());
/*
	unsigned maxLengthLeft = L2_SIZE/(CACHE_LINE_SIZE*leftInputData.size());
	//unsigned maxLengthRight = L2_SIZE/(CACHE_LINE_SIZE*rightInputData.size());
	unsigned maxLengthRight = L2_SIZE/(CACHE_LINE_SIZE*rightInputData.size());
	
	if (taskLength[0] > maxLengthLeft) {
		taskLength[0] = maxLengthLeft;
		cntTaskLeft = (left->resultSize+maxLengthLeft-1)/maxLengthLeft;	
	}
	if (taskLength[1] > maxLengthRight) {
		taskLength[1] = maxLengthRight;
		cntTaskRight = (right->resultSize+maxLengthRight-1)/maxLengthRight;	
	}
	*/
    if (taskLength[0] < minTuplesPerTask) {
        cntTaskLeft = left->resultSize/minTuplesPerTask;
        if (cntTaskLeft == 0 ) cntTaskLeft = 1;
        taskLength[0] = left->resultSize/cntTaskLeft;
        taskRest[0] = left->resultSize%cntTaskLeft;
    }
    if (taskLength[1] < minTuplesPerTask) {
        cntTaskRight = right->resultSize/minTuplesPerTask;
        if (cntTaskRight == 0 ) cntTaskRight = 1;
        taskLength[1] = right->resultSize/cntTaskRight;
        taskRest[1] = right->resultSize%cntTaskRight;
    }
	histograms[0].reserve(cntTaskLeft);
	histograms[1].reserve(cntTaskRight);
    for (int i=0; i<cntTaskLeft; i++) {
		histograms[0].emplace_back();
    }
    for (int i=0; i<cntTaskRight; i++) {
        histograms[1].emplace_back();
    }
    pendingMakingHistogram[0] = cntTaskLeft;
    pendingMakingHistogram[1*CACHE_LINE_SIZE] = cntTaskRight;

    __sync_synchronize();    
    // int length = limit/(taskNum);
   
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<") make histogramTasks: cntTasks(L,R): " << cntTaskLeft << "," << cntTaskRight << " Length(L,R): " << taskLength[0] << ", " << taskLength[1] << endl;
#endif
    uint64_t startLeft = 0;
    uint64_t restLeft = taskRest[0];
    for (int i=0; i<cntTaskLeft; i++) {
        uint64_t lengthLeft = taskLength[0];
        if (restLeft) {
            lengthLeft++;
            restLeft--;
        }
        // cout << "Left len: (" << i <<")" << lengthLeft << endl;
        ioService.post(bind(&Join::histogramTask, this, &ioService, cntTaskLeft, i, 0, startLeft, lengthLeft)); // for left
        startLeft += lengthLeft;
    }
    uint64_t startRight = 0;
    uint64_t restRight = taskRest[1];
    for (int i=0; i<cntTaskRight; i++) {
        uint64_t lengthRight = taskLength[1];
        if (restRight) {
            lengthRight++;
            restRight--;
        }
        // cout << "Right len: ("<<i<<")" << lengthRight << endl;
        ioService.post(bind(&Join::histogramTask, this, &ioService, cntTaskRight, i, 1, startRight, lengthRight)); // for right
        startRight += lengthRight;
    }
    
}

//---------------------------------------------------------------------------
void Join::histogramTask(boost::asio::io_service* ioService, int cntTask, int taskIndex, int leftOrRight, uint64_t start, uint64_t length) {
    vector<Column<uint64_t>>& inputData = !leftOrRight ? left->getResults() : right->getResults();
    Column<uint64_t>& keyColumn = !leftOrRight ? inputData[leftColId] : inputData[rightColId];
    /*
	if (!leftOrRight) { 
        inputData = left->getResults();
        keyColumn = inputData[leftColId];
    } else {
        inputData = right->getResults();
        keyColumn = inputData[rightColId];
    }
    */
    histograms[leftOrRight][taskIndex].reserve(CACHE_LINE_SIZE); // for preventing false sharing
    for (uint64_t j=0; j<cntPartition; j++) {
        histograms[leftOrRight][taskIndex].emplace_back();
    }
	auto it = keyColumn.begin(start);
    for (uint64_t i=start,limit=start+length; i<limit; i++, ++it) {
        histograms[leftOrRight][taskIndex][RADIX_HASH(*it, cntPartition)]++; // cntPartition으로 나뉠 수 있도록하는 HASH
    }
    int remainder = __sync_sub_and_fetch(&pendingMakingHistogram[leftOrRight*CACHE_LINE_SIZE], 1);
    
    if (UNLIKELY(remainder == 0)) { // gogo scattering
        for (int i=0; i<cntPartition; i++) {
            partitionLength[leftOrRight].push_back(0);
            for (int j=0; j<cntTask; j++) {
                partitionLength[leftOrRight][i] += histograms[leftOrRight][j][i];
                if (j != 0) { // make histogram to containprefix-sum
                    histograms[leftOrRight][j][i] += histograms[leftOrRight][j-1][i];
                }
            }

        }
        if ( leftOrRight == 1 ) { // variables for probingTask
            resultIndex.push_back(0);
            for (int i=0; i<cntPartition; i++) {
                uint64_t limitRight = partitionLength[1][i];
                unsigned cntTask = THREAD_NUM;
                uint64_t taskLength = limitRight/cntTask;
                unsigned rest = limitRight%cntTask;

                if (taskLength < minTuplesPerTask) {
                    cntTask = limitRight/minTuplesPerTask;
                    if (cntTask == 0)
                        cntTask = 1;
                    taskLength = limitRight/cntTask;
                    rest = limitRight%cntTask;
                }
                cntProbing.push_back(cntTask);
                lengthProbing.push_back(taskLength);
                restProbing.push_back(rest);
                resultIndex.push_back(cntTask);
                resultIndex[i+1] += resultIndex[i];

                //hashTables.emplace_back();
            }
            unsigned probingResultSize = resultIndex[cntPartition];
            for (int i=0; i<requestedColumns.size(); i++) {
                results.emplace_back(probingResultSize);
            }
            if (counted){
                results.emplace_back(probingResultSize); // For Count Column
            }
        }
        

        auto cntColumns = inputData.size();//requestedColumns.size();
        uint64_t* partAddress = partitionTable[leftOrRight];
        // partition[leftOrRight][0][0] = partitionTable[leftOrRight];
        for (uint64_t i=0; i<cntPartition;i ++) {
            uint64_t cntTuples = partitionLength[leftOrRight][i];// histograms[leftOrRight][cntTask-1][i];
            for (unsigned j=0; j<cntColumns; j++) {
                partition[leftOrRight][i][j] = partAddress + j*cntTuples;
            }
            partAddress += cntTuples*cntColumns;
        }
        pendingScattering[leftOrRight*CACHE_LINE_SIZE] = cntTask;
        __sync_synchronize(); 
#ifdef VERBOSE
    cout << "Join("<< queryIndex << "," << operatorIndex <<") " << (!leftOrRight?"left":"right") << " histogram tasks are done." << endl << "create scatteringTasks " <<endl;
#endif
        uint64_t start = 0;
        uint64_t rest = taskRest[leftOrRight];
        
        for (int i=0; i<cntTask; i++) {
            uint64_t length = taskLength[leftOrRight];
            if (rest) {
                length++;
                rest--;
            }
            ioService->post(bind(&Join::scatteringTask, this, ioService, i, leftOrRight, start, length));
            start += length;
        }
    }
}
//---------------------------------------------------------------------------
void Join::scatteringTask(boost::asio::io_service* ioService, int taskIndex, int leftOrRight, uint64_t start, uint64_t length) {
    vector<Column<uint64_t>>& inputData = !leftOrRight ? left->getResults() : right->getResults();
    Column<uint64_t>& keyColumn = !leftOrRight ? inputData[leftColId] : inputData[rightColId];
    /*
	uint64_t* keyColumn;
    vector<uint64_t*> inputData;
    if (!leftOrRight) { 
        inputData = left->getResults();
        keyColumn = inputData[leftColId];
    } else {
        inputData = right->getResults();
        keyColumn = inputData[rightColId];
    }*/
    // copy histogram for cache
    vector<uint64_t> insertOffs;
    for (int i=0; i<cntPartition; i++) { 
        insertOffs.push_back(0);
    }
    //copyHist.insert(copyHist.end(), histograms[leftOrRight][taskIndex].begin(), histograms[leftOrRight][taskIndex].end());
    
	auto keyIt = keyColumn.begin(start);
	vector<Column<uint64_t>::Iterator> colIt;
	
	for (unsigned i=0; i<inputData.size(); i++) {
		colIt.push_back(inputData[i].begin(start));
	}
	for (uint64_t i=start, limit=start+length; i<limit; i++, ++keyIt) {
        uint64_t hashResult = RADIX_HASH(*keyIt, cntPartition);
        uint64_t insertBase;
        uint64_t insertOff = insertOffs[hashResult]++;
//        insertOffs[hashResult]++;
        
        if (UNLIKELY(taskIndex == 0))
            insertBase = 0;
        else { 
            insertBase = histograms[leftOrRight][taskIndex-1][hashResult];
        }
        for (unsigned j=0; j<inputData.size(); j++) {
            partition[leftOrRight][hashResult][j][insertBase+insertOff] = *(colIt[j]);
			++(colIt[j]);
        }
    }
    int remainder = __sync_sub_and_fetch(&pendingScattering[leftOrRight*CACHE_LINE_SIZE], 1);
    if (UNLIKELY(remainder == 0)) { 
        int remPart = __sync_sub_and_fetch(&pendingPartitioning, 1);
        if (remPart == 0) {
            vector<unsigned> subJoinTarget;
            for (int i=0; i<cntPartition; i++) {
                if (partitionLength[0][i] != 0 && partitionLength[1][i] != 0) {
                    subJoinTarget.push_back(i);
                }
            }
            if(subJoinTarget.size() == 0) { // left !=0 이지만 양쪽이 서로 다른 파티션으로만 나뉜경우 
                for (unsigned cId=0;cId<requestedColumns.size();++cId) {
                    results[cId].fix();
                }
                if (counted){
                    results[requestedColumns.size()].fix();//For Count Column
                }
                /*
                if (allocTid == 10) {
                    std::cerr << "Mempool(" << allocTid << ") free requested by " << tid << " addr: " << partitionTable[0] << ", " << partitionTable[1] << std::endl;
                }
                */
//                free(partitionTable[0]);
//                free(partitionTable[1]);
                localMemPool[allocTid]->requestFree(partitionTable[0]);
                localMemPool[allocTid]->requestFree(partitionTable[1]);
                finishAsyncRun(*ioService, true); 
                //left = nullptr;
                //right = nullptr; 
                return;         
            }
            for (int i=0; i<cntPartition; i++) {
                if (cntBuilding)
                    hashTablesCnt.emplace_back();
                else
                    hashTablesIndices.emplace_back();
            }
            for (int i=0; i<cntPartition; i++) {
                if (cntBuilding)
                    hashTablesCnt[i] = NULL;
                else
                    hashTablesIndices[i] = NULL;
            }
            pendingBuilding = subJoinTarget.size();
            unsigned taskNum = pendingBuilding;
            __sync_synchronize();
#ifdef VERBOSE
            cout << "Join("<< queryIndex << "," << operatorIndex <<") All partitioning are done. " << endl << "create buildingTasks " <<endl;
#endif
    
//            for (int i=0; i<taskNum; i++) {
            for (auto& s : subJoinTarget) {
                ioService->post(bind(&Join::buildingTask, this, ioService, s, partition[0][s], partitionLength[0][s], partition[1][s], partitionLength[1][s]));
                 // cout << "Join("<< queryIndex << "," << operatorIndex <<")" << " Part Size(L,R) (" << i << "): " << partitionLength[0][i] << ", " << partitionLength[1][i] << endl;
            }
        }
    }
}

void Join::buildingTask(boost::asio::io_service* ioService, int taskIndex, vector<uint64_t*> localLeft, uint64_t limitLeft, vector<uint64_t*> localRight, uint64_t limitRight) {
    // Resolve the partitioned columns
    // unordered_multimap<uint64_t, uint64_t>& hashTable = hashTables[taskIndex];
//    shared_ptr<unordered_multimap<uint64_t, uint64_t>> hashTable = make_shared<unordered_multimap<uint64_t, uint64_t>>();

    if (limitLeft > limitRight*2){
#if 0
        __sync_fetch_and_add(&cnt, 1);
#endif
         /*
        cerr << "Warning : left partition > right partition - " << limitLeft << " > "<< limitRight << endl;
        cerr << "totalLeft: " << left->resultSize << " taotalRight: " << right->resultSize << " " << cntPartition << endl;
        uint64_t min = -1;
        uint64_t max = 0;
        for (int i=0; i<cntPartition; i++) {
            if (partitionLength[0][i] < min)
                min = partitionLength[0][i];
            if (partitionLength[0][i] > max)
                max = partitionLength[0][i];
        }
        cerr << "left min: " << min << " max: " << max << endl;
        min = -1;
        max = 0;
        for (int i=0; i<cntPartition; i++) {
            if (partitionLength[1][i] < min)
                min = partitionLength[1][i];
            if (partitionLength[1][i] > max)
                max = partitionLength[1][i];
        }
        cerr << "right min: " << min << " max: " << max << endl;
        */
    }
    if (limitLeft > hashThreshold) {
        uint64_t* leftKeyColumn = localLeft[leftColId];
        if (cntBuilding) {
            hashTablesCnt[taskIndex] = new unordered_map<uint64_t, uint64_t>();
            unordered_map<uint64_t, uint64_t>* hashTable = hashTablesCnt[taskIndex];

            hashTable->reserve(limitLeft*2);
            for (uint64_t i=0; i<limitLeft; i++) {
                if (left->counted)
                    (*hashTable)[leftKeyColumn[i]] += localLeft.back()[i]; 
                else  
                    (*hashTable)[leftKeyColumn[i]]++; 
            }
        } else {
            hashTablesIndices[taskIndex] = new unordered_multimap<uint64_t, uint64_t>();
            unordered_multimap<uint64_t, uint64_t>* hashTable = hashTablesIndices[taskIndex];
            //vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
            
            hashTable->reserve(limitLeft*2);
            for (uint64_t i=0; i<limitLeft; i++) {
                hashTable->emplace(make_pair(leftKeyColumn[i],i));
            //    bloomFilter.insert(leftKeyColumn[i]);
            }
        }
    }
    unsigned cntTask = cntProbing[taskIndex]; 
    uint64_t taskLength = lengthProbing[taskIndex];
    unsigned rest = restProbing[taskIndex];

    for (int i=0; i<cntTask; i++) {
        tmpResults[taskIndex].emplace_back();
    }

    __sync_fetch_and_add(&pendingProbing, cntTask);
    __sync_sub_and_fetch(&pendingBuilding, 1);
    //cerr << "building task " << taskIndex << " end : ";
    //cerr << " PendingProbing: " << pendingProbing;
    //cerr << " PendingBuilding: " << pendingBuilding << endl;
    uint64_t start = 0;
    for (int i=0; i<cntTask-1; i++) {
        uint64_t length = taskLength;
        if (rest) {
            length++;
            rest--;
        }
        ioService->post(bind(&Join::probingTask, this, ioService, taskIndex, i, localLeft, limitLeft, localRight, start, length)); 
        start += length;
    }
    uint64_t length = taskLength; 
    if (rest) {
        length++;
    }
    probingTask(ioService, taskIndex, cntTask-1, localLeft, limitLeft, localRight, start, length); 
}

 
void Join::probingTask(boost::asio::io_service* ioService, int partIndex, int taskIndex, vector<uint64_t*> localLeft, uint64_t leftLength, vector<uint64_t*> localRight, uint64_t start, uint64_t length) {
    // unordered_multimap<uint64_t, uint64_t>& hashTable = hashTables[partIndex];
    uint64_t* rightKeyColumn = localRight[rightColId];
    uint64_t* leftKeyColumn = localLeft[leftColId];
    vector<uint64_t*> copyLeftData, copyRightData;
    vector<vector<uint64_t>>& localResults = tmpResults[partIndex][taskIndex];
    uint64_t limit = start+length;
    unsigned leftColSize = requestedColumnsLeft.size();
    unsigned rightColSize = requestedColumnsRight.size();
    unsigned resultColSize = requestedColumns.size(); 
    unordered_map<uint64_t, uint64_t>* hashTableCnt = NULL;
    unordered_multimap<uint64_t, uint64_t>* hashTableIndices = NULL;
    unordered_map<uint64_t, uint64_t> cntMap;
    
    if (leftLength == 0 || length == 0)
        goto probing_finish;
    
    if (cntBuilding) { 
        hashTableCnt = hashTablesCnt[partIndex];
    }else {
        hashTableIndices = hashTablesIndices[partIndex];
    }
    
    for (unsigned j=0; j<requestedColumns.size(); j++) {
        localResults.emplace_back();
    }
    if (counted){
        localResults.emplace_back(); // For Count Column
    }
    for (auto& info : requestedColumnsLeft) {
        copyLeftData.push_back(localLeft[left->resolve(info)]);
    }
    if (left->counted){
        copyLeftData.push_back(localLeft.back()); // Count Column for left
    }
    for (auto& info : requestedColumnsRight) {
        copyRightData.push_back(localRight[right->resolve(info)]);
    }
    if (right->counted){
        copyRightData.push_back(localRight.back()); // Count Column for right
    }
    
    if (leftLength > hashThreshold) {   
        // probing
        if (cntBuilding) {
            for (uint64_t i=start; i<limit; i++) {
                auto rightKey=rightKeyColumn[i];
                if (hashTableCnt->find(rightKey) == hashTableCnt->end())
                    continue;
                uint64_t leftCnt = hashTableCnt->at(rightKey);
                uint64_t rightCnt= right->counted ? copyRightData[rightColSize][i] : 1;
                if (counted == 1){
                    auto data = (leftColSize == 1) ? rightKey : copyRightData[0][i];
                    localResults[0].push_back(data);
                    localResults[1].push_back(leftCnt*rightCnt);
                
                } else {
                    unsigned relColId=0;
                    for (unsigned cId=0;cId<leftColSize;++cId) // if exist
                        localResults[relColId++].push_back(rightKey);
                    for (unsigned cId=0;cId<rightColSize;++cId)
                        localResults[relColId++].push_back(copyRightData[cId][i]);
                    if (counted){
                        localResults[relColId].push_back(leftCnt * rightCnt);
                    }
                }
                
            }
        } else {
            for (uint64_t i=start; i<limit; i++) {
                auto rightKey=rightKeyColumn[i];
                /*
                if (!bloomFilter.contains(rightKey))
                    continue;
                    */
                auto range=hashTableIndices->equal_range(rightKey);
                for (auto iter=range.first;iter!=range.second;++iter) {
                    if (counted == 1){
                        auto &copyData((leftColSize == 1) ? copyLeftData[0] : copyRightData[0]);
                        auto rid = (leftColSize == 1) ? iter->second : i;
                        auto dup = cntMap.find(copyData[rid]);
                        uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][iter->second] : 1;
                        uint64_t rightCnt= right->counted ? copyRightData[rightColSize][i] : 1;
                        if (dup != cntMap.end()){
                            localResults[1][dup->second] += leftCnt * rightCnt;
                        }
                        else{
                            localResults[0].push_back(copyData[rid]);
                            localResults[1].push_back(leftCnt * rightCnt);
                            cntMap.insert(dup, pair<uint64_t, uint64_t>(copyData[rid],
                                        localResults[1].size()-1));
                        }
                    }
                    else{
                        unsigned relColId=0;
                        for (unsigned cId=0;cId<leftColSize;++cId)
                            localResults[relColId++].push_back(copyLeftData[cId][iter->second]);
                        for (unsigned cId=0;cId<rightColSize;++cId)
                            localResults[relColId++].push_back(copyRightData[cId][i]);
                        if (counted){
                            uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][iter->second] : 1;
                            uint64_t rightCnt= right->counted ? copyRightData[rightColSize][i] : 1;
                            localResults[relColId].push_back(leftCnt * rightCnt);
                        }
                    }
                }
            }
        }
    } else {
        for (uint64_t i=0; i<leftLength; i++) {
            for (uint64_t j=start; j<limit; j++) {
                if (leftKeyColumn[i] == rightKeyColumn[j]) {
                    if (counted == 1){
                        auto &copyData((leftColSize == 1) ? copyLeftData[0] : copyRightData[0]);
                        auto rid = (leftColSize == 1) ? i : j;
                        auto dup = cntMap.find(copyData[rid]);
                        uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][i] : 1;
                        uint64_t rightCnt= right->counted ? copyRightData[rightColSize][j] : 1;
                        if (dup != cntMap.end()){
                            localResults[1][dup->second] += leftCnt * rightCnt;
                        }
                        else{
                            localResults[0].push_back(copyData[rid]);
                            localResults[1].push_back(leftCnt * rightCnt);
                            cntMap.insert(dup, pair<uint64_t, uint64_t>(copyData[rid],
                                        localResults[1].size()-1));
                        }
                    }
                    else{
                        unsigned relColId=0;
                        for (unsigned cId=0;cId<leftColSize;++cId)
                            localResults[relColId++].push_back(copyLeftData[cId][i]);
                        for (unsigned cId=0;cId<rightColSize;++cId)
                            localResults[relColId++].push_back(copyRightData[cId][j]);
                        if (counted){
                            uint64_t leftCnt = left->counted ? copyLeftData[leftColSize][i] : 1;
                            uint64_t rightCnt= right->counted ? copyRightData[rightColSize][j] : 1;
                            localResults[relColId].push_back(leftCnt * rightCnt);
                        }
                    }
                }
            }
        }
    }
#ifdef VERBOSE
    // cerr << "Join("<< queryIndex << "," << operatorIndex <<") subjoin finish. local result size: " << localResults[0].size() << endl;
#endif
    if (localResults[0].size() == 0) 
        goto probing_finish;
    for (unsigned i=0; i<resultColSize; i++) {
		results[i].addTuples(resultIndex[partIndex]+taskIndex, localResults[i].data(), localResults[i].size());
    }
    if (counted){
        results[resultColSize].addTuples(resultIndex[partIndex]+taskIndex,
                localResults[resultColSize].data(), localResults[resultColSize].size());
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());
//    resultSize += localResults[0].size();

probing_finish:  
    int remainder = __sync_sub_and_fetch(&pendingProbing, 1);
    // pendingProbing을 올리고 pendingBuilding을 낮추므로, 
    // pendingProbing이 0이고 pendingBuilding도 0이면 모두 끝난 것
    if (UNLIKELY(remainder == 0 && pendingBuilding == 0)) {
#ifdef VERBOSE
        cout << "Join("<< queryIndex << "," << operatorIndex <<") join finish. result size: " << resultSize << endl;
#endif
        for (unsigned cId=0;cId<requestedColumns.size();++cId) {
            results[cId].fix();
        }
        if (counted){
            results[requestedColumns.size()].fix();
        }
/*
        vector<unordered_multimap<uint64_t, uint64_t>*> gHashTables = move(hashTables);
        unsigned gCntPartition = cntPartition;
        uint64_t* gPart0 = partitionTable[0];
        uint64_t* gPart1 = partitionTable[1];
 */
        /*
        if (allocTid == 10) {
            std::cerr << "Mempool(" << allocTid << ") free requested by " << tid << " addr: " <<partitionTable[0] << ", " << partitionTable[1] << endl;
        }
        */
//        free(partitionTable[0]);
//        free(partitionTable[1]);
        localMemPool[allocTid]->requestFree(partitionTable[0]);
        localMemPool[allocTid]->requestFree(partitionTable[1]);
        finishAsyncRun(*ioService, true); 
        
        //left = nullptr;
        //right = nullptr;
        
        //free(gPart0);
        //free(gPart1);
        
        /*
        for(unsigned i=0; i<gCntPartition; i++) {
            if(gHashTables[i] != NULL)
                delete gHashTables[i];
        }
        */
        
    }
    //일단은 그냥 left로 building하자. 나중에 최적화된 방법으로 ㄲ
     
}
//---------------------------------------------------------------------------
bool SelfJoin::require(SelectInfo info)
// Require a column and add it to results
{
    if (requiredIUs.count(info))
        return true;
    if(input->require(info)) {
        requiredIUs.emplace(info);
        return true;
    }
    return false;
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
}
//---------------------------------------------------------------------------
void SelfJoin::selfJoinTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length) {
    auto& inputData=input->getResults();
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    auto leftColId=input->resolve(pInfo.left);
    auto rightColId=input->resolve(pInfo.right);

    auto leftColIt=inputData[leftColId].begin(start);
    auto rightColIt=inputData[rightColId].begin(start);

    vector<Column<uint64_t>::Iterator> colIt;

    unsigned colSize = copyData.size();
    
    for (int j=0; j<colSize; j++) {
        localResults.emplace_back();
    }
    if (counted && !input->counted){
        localResults.emplace_back();
    }
    
    for (unsigned i=0; i<colSize; i++) {
        colIt.push_back(copyData[i]->begin(start));
    }
    unordered_map<uint64_t, uint64_t> cntMap;
    for (uint64_t i=start, limit=start+length;i<limit;++i) {
        if (*leftColIt==*rightColIt) {
            if (counted == 1){
                auto dup = cntMap.find(*(colIt[0]));
                uint64_t dupCnt = (input->counted) ? *(colIt[1]) : 1;
                if (dup != cntMap.end()){
                    localResults[1][dup->second] += dupCnt;
                }
                else{
                    localResults[0].push_back(*(colIt[0]));
                    localResults[1].push_back(dupCnt);
                    cntMap.insert(dup, pair<uint64_t, uint64_t>(*(colIt[0]), localResults[1].size()-1));
                }
            }
            else{ // 2 0 / 3 0 / 3 1 / 4 0 / 4 1 ...
                // If counted is true, colSize already contains count Column
                for (unsigned cId=0;cId<colSize;++cId) {
                    localResults[cId].push_back(*(colIt[cId]));
                }
            }
        }
        ++leftColIt;
        ++rightColIt;
        for (unsigned i=0; i<colSize; i++) {
            ++colIt[i];
        }
    }
    //local results가 0일 경우??? ressult, tmp 초기화
    for (int i=0; i<colSize; i++) {
        results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
    }
    if (counted && !input->counted){
        results[colSize].addTuples(taskIndex, localResults[colSize].data(), localResults[colSize].size());
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0)) {
        for (unsigned cId=0;cId<colSize;++cId) {
            results[cId].fix();
        }
        if (counted && !input->counted){
            results[colSize].fix();
        }
        finishAsyncRun(*ioService, true);
        //input = nullptr;
    }
}
//---------------------------------------------------------------------------
void SelfJoin::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif

    if (input->resultSize == 0) {
        finishAsyncRun(ioService, true);
        return;
    }
    
    int cntTask = THREAD_NUM;
    uint64_t taskLength = input->resultSize/cntTask;
    uint64_t rest = input->resultSize%cntTask;
    
    if (taskLength < minTuplesPerTask) {
        cntTask = input->resultSize/minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = input->resultSize/cntTask;
        rest = input->resultSize%cntTask;
    }
    
    auto& inputData=input->getResults();
    
    for (auto& iu : requiredIUs) {
        auto id=input->resolve(iu);
        copyData.emplace_back(&inputData[id]);
        select2ResultColId.emplace(iu,copyData.size()-1);
		results.emplace_back(cntTask);
    }
    if (copyData.size() == 1){
        auto &sInfo(*(requiredIUs.begin()));
        counted = Joiner::relations[sInfo.relId].needCount[sInfo.colId];
    }
    if (input->counted){
        if (!counted){
            counted = 2;
        }
        copyData.emplace_back(&inputData.back());
    }
    if (counted){
#ifdef ANALYZE
        __sync_fetch_and_add(&cntCounted, 1);
#endif
        results.emplace_back(cntTask);
    }

    for (int i=0; i<cntTask; i++) {
        tmpResults.emplace_back();
    } 
    
    pendingTask = cntTask; 
    __sync_synchronize();
    uint64_t start = 0;
    for (int i=0; i<cntTask; i++) {
        uint64_t length = taskLength;
        if (rest) {
            length++;
            rest--;
        }
        ioService.post(bind(&SelfJoin::selfJoinTask, this, &ioService, i, start, length)); 
        start += length;
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

void Checksum::checksumTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length) {
    auto& inputData=input->getResults();
     
    int sumIndex = 0;
    for (auto& sInfo : colInfo) {
        auto colId = input->resolve(sInfo);
        auto inputColIt = inputData[colId].begin(start);
        uint64_t sum=0;
        if (input->counted){
            auto countColIt = inputData.back().begin(start);
            for (int i=0; i<length; i++,++inputColIt,++countColIt){
                sum += (*inputColIt) * (*countColIt);
            }
        }
        else{
            for (int i=0; i<length; i++,++inputColIt){
                sum += (*inputColIt);
            }
        }
        __sync_fetch_and_add(&checkSums[sumIndex++], sum);
    }
    
    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0)) {
        finishAsyncRun(*ioService, false);
        //input = nullptr;
    }
     
}
//---------------------------------------------------------------------------
void Checksum::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ")::createAsyncTasks" << endl;
#endif
    for (auto& sInfo : colInfo) {
        checkSums.push_back(0);
    }
    
    if (input->resultSize == 0) {
        finishAsyncRun(ioService, false);
        return;
    }
    
    int cntTask = THREAD_NUM;
    uint64_t taskLength = input->resultSize/cntTask;
    uint64_t rest = input->resultSize%cntTask;
    
    if (taskLength < minTuplesPerTask) {
        cntTask = input->resultSize/minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = input->resultSize/cntTask;
        rest = input->resultSize%cntTask;
    }
#ifdef VERBOSE 
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ") input size: " << input->resultSize << " cntTask: " << cntTask << " length: " << taskLength << " rest: " << rest <<  endl;
#endif
    pendingTask = cntTask; 
    __sync_synchronize();
    uint64_t start = 0;
    for (int i=0; i<cntTask; i++) {
        uint64_t length = taskLength;
        if (rest) {
            length++;
            rest--;
        }
        ioService.post(bind(&Checksum::checksumTask, this, &ioService, i, start, length)); 
        start += length;
    }
}
void Checksum::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    joiner.asyncResults[queryIndex] = std::move(checkSums);
    int pending = __sync_sub_and_fetch(&joiner.pendingAsyncJoin, 1);
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ") finish query index: " << queryIndex << " rest quries: "<< pending << endl;
#endif
    assert(pending >= 0);
    if (pending == 0) {
#ifdef VERBOSE
        cout << "A query set is done. " << endl;
#endif
        unique_lock<mutex> lk(joiner.cvAsyncMt); // guard for missing notification
        joiner.cvAsync.notify_one();
    }
}
//---------------------------------------------------------------------------
void Checksum::printAsyncInfo() {
	cout << "pendingChecksum : " << pendingTask << endl;
	input->printAsyncInfo();
}

void SelfJoin::printAsyncInfo() {
	cout << "pendingSelfJoin : " << pendingTask << endl;
	input->printAsyncInfo();
}
void Join::printAsyncInfo() {
	cout << "pendingBuilding : " << pendingBuilding << endl;
	cout << "pendingProbing : " << pendingProbing << endl;
	cout << "pendingScattering[0] : " << pendingScattering[0] << endl;
	cout << "pendingScattering[1] : " << pendingScattering[1*CACHE_LINE_SIZE] << endl;
	cout << "pendingMakingHistogram[0] : " << pendingMakingHistogram[0] << endl;
	cout << "pendingMakingHistogram[1] : " << pendingMakingHistogram[1*CACHE_LINE_SIZE] << endl;
	left->printAsyncInfo();
	right->printAsyncInfo();
	
}
void FilterScan::printAsyncInfo() {
	cout << "pendingFilterScan : " << pendingTask << endl;
}
void Scan::printAsyncInfo() {
}
