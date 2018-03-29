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
uint64_t totFilterScan;
uint64_t totJoin;
uint64_t totSelfJoin;
uint64_t totChecksum;
uint64_t totUseIndex;
uint64_t totNotUseIndex;
#define DECL_CNT uint64_t __cnt = 0
#define CNT ++__cnt
#define CNT_FILTERSCAN __sync_fetch_and_add(&totFilterScan, __cnt)
#define CNT_JOIN __sync_fetch_and_add(&totJoin, __cnt)
#define CNT_SELFJOIN __sync_fetch_and_add(&totSelfJoin, __cnt)
#define CNT_CHECKSUM __sync_fetch_and_add(&totChecksum, __cnt)
#else 
#define DECL_CNT 
#define CNT
#define CNT_FILTERSCAN
#define CNT_JOIN
#define CNT_SELFJOIN
#define CNT_CHECKSUM
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
    for (int i=0; i<infos.size(); i++) {
        results[i].addTuples(0, relation.columns[infos[i].colId], relation.size);
        results[i].fix();
    }
    resultSize=relation.size; 
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
        inputData.push_back(relation.columns[info.colId]);
//      tmpResults.emplace_back();
        unsigned colId=inputData.size()-1;
        select2ResultColId[info]=colId;
    }
    return true;
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
    DECL_CNT;
#ifdef VERBOSE
    cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif
    int cntTask = THREAD_NUM;
    for (int i=0; i<inputData.size(); i++) {
		results.emplace_back(cntTask);
        CNT;
    }
    //const uint64_t partitionSize = L2_SIZE/2;
    //const unsigned taskNum = CNT_PARTITIONS(relation.size*relation.columns.size()*8, partitionSize);
    uint64_t taskLength = relation.size/cntTask;
    uint64_t rest = relation.size%cntTask;
    
    if (taskLength < minTuplesPerTask) {
        cntTask = relation.size/minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = relation.size/cntTask;
        rest = relation.size%cntTask;
    }
    
    pendingTask = cntTask;
    
	for (int i=0; i<cntTask; i++) {
		tmpResults.emplace_back();
		for (int j=0; j<inputData.size(); j++) {
            tmpResults[i].emplace_back();
            CNT;
        }
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
        CNT;
        ioService.post(bind(&FilterScan::filterTask, this, &ioService, i, start, length)); 
        start += length;
    }
    CNT_FILTERSCAN;
}
//---------------------------------------------------------------------------
void FilterScan::filterTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length) {
    DECL_CNT;
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    /*
    for (unsigned cId=0;cId<inputData.size();++cId) {
        localResults[cId].reserve(length);
    }*/
    for (uint64_t i=start;i<start+length;++i) {
        bool pass=true;
        for (auto &f:filters){
            if (!(pass&=applyFilter(i,f)))
                break;
            CNT;
        }
        if (pass) {
            for (unsigned cId=0;cId<inputData.size();++cId)
                localResults[cId].push_back(inputData[cId][i]);
        }
    }

    for (unsigned cId=0;cId<inputData.size();++cId) {
		results[cId].addTuples(taskIndex, localResults[cId].data(), localResults[cId].size());
        CNT;
    }
    //resultSize += localResults[0].size();
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0) {
        for (unsigned cId=0;cId<inputData.size();++cId) {
            results[cId].fix();
            CNT;
        }
        finishAsyncRun(*ioService, true);
    }
    CNT_FILTERSCAN;
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
    DECL_CNT;
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
        CNT;
    }
    for (auto& info : requestedColumnsRight) {
        select2ResultColId[info]=resColId++;
        CNT;
    }
    
    if (left->resultSize == 0) { // no reuslts
        finishAsyncRun(ioService, true);
        CNT_JOIN;
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
    if (cntPartition < 32) 
        cntPartition = 32;
    
    /*
    if (cntPartition == 1) {
        pendingSubjoin = 1;
        __sync_synchronize();
        ioService.post(bind(&Join::subJoinTask, this, &ioService, leftInputData, left->resultSize, rightInputData, right->resultSize));
        return;
    }*/
    
    for (int i=0; i<requestedColumns.size(); i++) {
        results.emplace_back(cntPartition);
        CNT;
    }
    
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
    partitionTable[0] = (uint64_t*)aligned_alloc(CACHE_LINE_SIZE, left->getResultsSize());
    partitionTable[1] = (uint64_t*)aligned_alloc(CACHE_LINE_SIZE, right->getResultsSize());

    if (left->getResultsSize() > 2*1024*1024) 
        madvise(partitionTable[0], left->getResultsSize(), MADV_HUGEPAGE);    
    if (right->getResultsSize() > 2*1024*1024) 
        madvise(partitionTable[1], right->getResultsSize(), MADV_HUGEPAGE);    
    
    for (uint64_t i=0; i<cntPartition; i++) {
        partition[0].emplace_back();
        partition[1].emplace_back();
        for (unsigned j=0; j<leftInputData.size(); j++) {
            CNT;
            partition[0][i].emplace_back();
        }
        for (unsigned j=0; j<rightInputData.size(); j++) {
            CNT;
            partition[1][i].emplace_back();
        }
		tmpResults.emplace_back();
		for (unsigned j=0; j<requestedColumns.size(); j++) {
            CNT;
			tmpResults[i].emplace_back();
//            tmpResults[i][tmpResults[i].size()-1].reserve(right->resultSize/cntPartition);
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
		histograms[0][i].reserve(CACHE_LINE_SIZE); // for preventing false sharing
        for (uint64_t j=0; j<cntPartition; j++) {
            CNT;
            histograms[0][i].emplace_back();
        }
    }
    for (int i=0; i<cntTaskRight; i++) {
        histograms[1].emplace_back();
		histograms[1][i].reserve(CACHE_LINE_SIZE);
        for (uint64_t j=0; j<cntPartition; j++) {
            CNT;
            histograms[1][i].emplace_back();
        }
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
        CNT;
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
        CNT;
        // cout << "Right len: ("<<i<<")" << lengthRight << endl;
        ioService.post(bind(&Join::histogramTask, this, &ioService, cntTaskRight, i, 1, startRight, lengthRight)); // for right
        startRight += lengthRight;
    }
    CNT_JOIN;
}

//---------------------------------------------------------------------------
void Join::histogramTask(boost::asio::io_service* ioService, int cntTask, int taskIndex, int leftOrRight, uint64_t start, uint64_t length) {
    DECL_CNT;
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
	auto it = keyColumn.begin(start);
    for (uint64_t i=start,limit=start+length; i<limit; i++, ++it) {
        histograms[leftOrRight][taskIndex][RADIX_HASH(*it, cntPartition)]++; // cntPartition으로 나뉠 수 있도록하는 HASH
        CNT;
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
                CNT;
            }
        }
        auto cntColumns = inputData.size();//requestedColumns.size();
        uint64_t* partAddress = partitionTable[leftOrRight];
        // partition[leftOrRight][0][0] = partitionTable[leftOrRight];
        for (uint64_t i=0; i<cntPartition;i ++) {
            uint64_t cntTuples = partitionLength[leftOrRight][i];// histograms[leftOrRight][cntTask-1][i];
            for (unsigned j=0; j<cntColumns; j++) {
                partition[leftOrRight][i][j] = partAddress + j*cntTuples;
                CNT;
            }
            partAddress += cntTuples*cntColumns;
        }
        pendingScattering[leftOrRight*CACHE_LINE_SIZE] = cntTask;
        __sync_synchronize(); //for histograms
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
            ioService->post(bind(&Join::scatteringTask, this, ioService, i, leftOrRight, start, length)); // for left
            start += length;
            CNT;
        }
    }
    CNT_JOIN;
}
//---------------------------------------------------------------------------
void Join::scatteringTask(boost::asio::io_service* ioService, int taskIndex, int leftOrRight, uint64_t start, uint64_t length) {
    DECL_CNT;
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
        CNT;
        insertOffs.push_back(0);
    }
    //copyHist.insert(copyHist.end(), histograms[leftOrRight][taskIndex].begin(), histograms[leftOrRight][taskIndex].end());
    
	auto keyIt = keyColumn.begin(start);
	vector<Column<uint64_t>::Iterator> colIt;
	
	for (unsigned i=0; i<inputData.size(); i++) {
        CNT;
		colIt.push_back(inputData[i].begin(start));
	}
	for (uint64_t i=start, limit=start+length; i<limit; i++, ++keyIt) {
        uint64_t hashResult = RADIX_HASH(*keyIt, cntPartition);
        uint64_t insertBase;
        uint64_t insertOff = insertOffs[hashResult];
        
        if (UNLIKELY(taskIndex == 0))
            insertBase = 0;
        else { 
            insertBase = histograms[leftOrRight][taskIndex-1][hashResult];
        }
        for (unsigned j=0; j<inputData.size(); j++) {
            CNT;
            partition[leftOrRight][hashResult][j][insertBase+insertOff] = *(colIt[j]);
			++(colIt[j]);
        }
        insertOffs[hashResult]++;
    }
    int remainder = __sync_sub_and_fetch(&pendingScattering[leftOrRight*CACHE_LINE_SIZE], 1);
    if (UNLIKELY(remainder == 0)) { // gogo scattering
        int remPart = __sync_sub_and_fetch(&pendingPartitioning, 1);
        if (remPart == 0) {
            pendingSubjoin = cntPartition;
            __sync_synchronize();
#ifdef VERBOSE
            cout << "Join("<< queryIndex << "," << operatorIndex <<") All partitioning are done. " << endl << "create subJoinTasks " <<endl;
#endif
            int taskNum = cntPartition;
            for (int i=0; i<taskNum; i++) {
                CNT;
                ioService->post(bind(&Join::subJoinTask, this, ioService, i, partition[0][i], partitionLength[0][i], partition[1][i], partitionLength[1][i]));
                 // cout << "Join("<< queryIndex << "," << operatorIndex <<")" << " Part Size(L,R) (" << i << "): " << partitionLength[0][i] << ", " << partitionLength[1][i] << endl;
            }
        }
    }
    CNT_JOIN;
}

void Join::subJoinTask(boost::asio::io_service* ioService, int taskIndex, vector<uint64_t*> localLeft, uint64_t limitLeft, vector<uint64_t*> localRight, uint64_t limitRight) {
    DECL_CNT;
    unordered_multimap<uint64_t, uint64_t> hashTable;
    // Resolve the partitioned columns
    std::vector<uint64_t*> copyLeftData,copyRightData;
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    uint64_t* leftKeyColumn = localLeft[leftColId];
    uint64_t* rightKeyColumn = localRight[rightColId];

    //bloom_filter bloomFilter(bloomArgs);

    if (limitLeft == 0 || limitRight == 0) {
        goto sub_join_finish;
    }

    for (auto& info : requestedColumnsLeft) {
        copyLeftData.push_back(localLeft[left->resolve(info)]);
        CNT;
    }
    for (auto& info : requestedColumnsRight) {
        copyRightData.push_back(localRight[right->resolve(info)]);
        CNT;
    }
    // building
    //hashTable.reserve(limitLeft);
    hashTable.reserve(limitLeft*2);
    for (uint64_t i=0; i<limitLeft; i++) {
        hashTable.emplace(make_pair(leftKeyColumn[i],i));
    //    bloomFilter.insert(leftKeyColumn[i]);
        CNT;
    }
    for (unsigned i=0; i<requestedColumns.size(); i++) {
        localResults.emplace_back();
        CNT;
    }
    // probing
    for (uint64_t i=0; i<limitRight; i++) {
        auto rightKey=rightKeyColumn[i];
        /*
        if (!bloomFilter.contains(rightKey))
            continue;
            */
        auto range=hashTable.equal_range(rightKey);
        for (auto iter=range.first;iter!=range.second;++iter) {
            unsigned relColId=0;
            for (unsigned cId=0;cId<copyLeftData.size();++cId)
                localResults[relColId++].push_back(copyLeftData[cId][iter->second]);

            for (unsigned cId=0;cId<copyRightData.size();++cId)
                localResults[relColId++].push_back(copyRightData[cId][i]);
            CNT;
        }
    }
#ifdef VERBOSE
        //cout << "Join("<< queryIndex << "," << operatorIndex <<") subjoin finish. local result size: " << localResults[0].size() << endl;
#endif
    if (localResults[0].size() < 0) 
        goto sub_join_finish;
    for (unsigned i=0; i<requestedColumns.size(); i++)  {
		results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
        CNT;
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());
//    resultSize += localResults[0].size();

sub_join_finish:  
    int remainder = __sync_sub_and_fetch(&pendingSubjoin, 1);
    if (UNLIKELY(remainder == 0)) {
#ifdef VERBOSE
        cout << "Join("<< queryIndex << "," << operatorIndex <<") join finish. result size: " << resultSize << endl;
#endif
        for (unsigned cId=0;cId<requestedColumns.size();++cId) {
            results[cId].fix();
        }
        if (cntPartition != 1) { // if 1, no partitioning
            free(partitionTable[0]);
            free(partitionTable[1]);
        } //만약 finishAsyncRun하고 free하면, free하려는데 query가 다 끝나서 partitionTable[]이 없을 수 있다.
        finishAsyncRun(*ioService, true); 
    }
    //일단은 그냥 left로 building하자. 나중에 최적화된 방법으로 ㄲ
    CNT_JOIN;
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
    DECL_CNT;
    auto& inputData=input->getResults();
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    auto leftColId=input->resolve(pInfo.left);
    auto rightColId=input->resolve(pInfo.right);

    auto leftColIt=inputData[leftColId].begin(start);
    auto rightColIt=inputData[rightColId].begin(start);

    vector<Column<uint64_t>::Iterator> colIt;
    
    for (unsigned i=0; i<copyData.size(); i++) {
        colIt.push_back(copyData[i]->begin(start));
        CNT;
    }
    for (uint64_t i=start, limit=start+length;i<limit;++i) {
        if (*leftColIt==*rightColIt) {
            for (unsigned cId=0;cId<copyData.size();++cId) {
                localResults[cId].push_back(*(colIt[cId]));
            }
        }
        ++leftColIt;
        ++rightColIt;
        for (unsigned i=0; i<copyData.size(); i++) {
            ++colIt[i];
            CNT;
        }
    }
    //local results가 0일 경우??? ressult, tmp 초기화
    for (int i=0; i<copyData.size(); i++) {
        results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
        CNT;
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0)) {
        for (unsigned cId=0;cId<copyData.size();++cId) {
            results[cId].fix();
            CNT;
        }
        finishAsyncRun(*ioService, true);
    }
    CNT_SELFJOIN;
}
//---------------------------------------------------------------------------
void SelfJoin::createAsyncTasks(boost::asio::io_service& ioService) {
    DECL_CNT;
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif

    if (input->resultSize == 0) {
        finishAsyncRun(ioService, true);
        CNT_SELFJOIN;
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

    for (int i=0; i<cntTask; i++) {
        tmpResults.emplace_back();
        for (int j=0; j<copyData.size(); j++) {
            tmpResults[i].emplace_back();
            CNT;
        }
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
        CNT;
        ioService.post(bind(&SelfJoin::selfJoinTask, this, &ioService, i, start, length)); 
        start += length;
    }
    CNT_SELFJOIN;
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
    DECL_CNT;
    auto& inputData=input->getResults();
     
    int sumIndex = 0;
    for (auto& sInfo : colInfo) {
        auto colId = input->resolve(sInfo);
        auto inputColIt = inputData[colId].begin(start);
        uint64_t sum=0;
        for (int i=0; i<length; i++,++inputColIt){
            sum += *inputColIt;
            CNT;
        }
        __sync_fetch_and_add(&checkSums[sumIndex++], sum);
    }
    
    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0)) {
        finishAsyncRun(*ioService, false);
    }
    CNT_CHECKSUM; 
}
//---------------------------------------------------------------------------
void Checksum::createAsyncTasks(boost::asio::io_service& ioService) {
    DECL_CNT;
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
        CNT;
        ioService.post(bind(&Checksum::checksumTask, this, &ioService, i, start, length)); 
        start += length;
    }
    CNT_CHECKSUM;
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
	cout << "pendingSubjoin : " << pendingSubjoin << endl;
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
