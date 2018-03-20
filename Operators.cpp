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
//        resultColumns.push_back(relation.columns[info.colId]);
        results.emplace_back(1);
        infos.push_back(info);
		select2ResultColId[info]=results.size()-1;
    }
    return true;
}
//---------------------------------------------------------------------------
unsigned Scan::getResultsSize() {
    return results.size()*relation.size*8; 
}
//---------------------------------------------------------------------------
void Scan::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "Scan("<< queryIndex << "," << operatorIndex <<")::asyncRun, Task" << endl;
#endif
    pendingAsyncOperator = 1;
    for (int i=0; i<infos.size(); i++) {
        results[i].addTuples(0, relation.columns[infos[i].colId], relation.size);
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
//        tmpResults.emplace_back();
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
    pendingAsyncOperator = 1;
    __sync_synchronize();
    createAsyncTasks(ioService);  
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
    
    for (int i=0; i<inputData.size(); i++) {
		results.emplace_back(taskNum);
    }
	for (int i=0; i<taskNum; i++) {
		tmpResults.emplace_back();
		for (int j=0; j<inputData.size(); j++) {
			tmpResults[i].emplace_back();
		}
	}
	
    __sync_synchronize(); 
    // unsigned length = partitionSize/(relation.columns.size()*8); 
    unsigned length = (relation.size+taskNum-1)/taskNum;
    for (unsigned i=0; i<taskNum; i++) {
        unsigned start = i*length;
        if (i == taskNum-1 && relation.size%length) {
            length = relation.size%length;
        }
        ioService.post(bind(&FilterScan::filterTask, this, &ioService, i, start, length)); 
    }
}
//---------------------------------------------------------------------------
void FilterScan::filterTask(boost::asio::io_service* ioService, int taskIndex, unsigned start, unsigned length) {
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
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

    for (unsigned cId=0;cId<inputData.size();++cId) {
		results[cId].addTuples(taskIndex, localResults[cId].data(), localResults[cId].size());
    }
    //resultSize += localResults[0].size();
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0) {
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
unsigned Operator::getResultsSize() {
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
	/*
    if (cntPartition == 1) {
        pendingSubjoin = 1;
        __sync_synchronize();
        ioService.post(bind(&Join::subJoinTask, this, &ioService, leftInputData, left->resultSize, rightInputData, right->resultSize));
        return;
    }*/
    
    for (int i=0; i<requestedColumns.size(); i++) {
        results.emplace_back(cntPartition);
    }
    
    pendingPartitioning = 2;
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
		tmpResults.emplace_back();
		for (unsigned j=0; j<requestedColumns.size(); j++) {
			tmpResults[i].emplace_back();
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
    if (left->resultSize/cntTaskLeft < minTuplesPerTask) {
        cntTaskLeft = (left->resultSize+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength[0] = minTuplesPerTask;
    }
    if (right->resultSize/cntTaskRight < minTuplesPerTask) {
        cntTaskRight = (right->resultSize+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength[1] = minTuplesPerTask;
    }
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
    for (unsigned i=start,limit=start+length; i<limit; i++, ++it) {
        histograms[leftOrRight][taskIndex][RADIX_HASH(*it, cntPartition)]++; // cntPartition으로 나뉠 수 있도록하는 HASH
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
    vector<unsigned> insertOffs;
    for (int i=0; i<cntPartition; i++) { 
        insertOffs.push_back(0);
    }
    //copyHist.insert(copyHist.end(), histograms[leftOrRight][taskIndex].begin(), histograms[leftOrRight][taskIndex].end());
    
	auto keyIt = keyColumn.begin(start);
	vector<Column<uint64_t>::Iterator> colIt;
	
	for (unsigned i=0; i<inputData.size(); i++) {
		colIt.push_back(inputData[i].begin(start));
	}
	for (unsigned i=start, limit=start+length; i<limit; i++, ++keyIt) {
        unsigned hashResult = RADIX_HASH(*keyIt, cntPartition);
        unsigned insertBase;
        unsigned insertOff = insertOffs[hashResult];
        
        if (taskIndex == 0)
            insertBase = 0;
        else { 
            insertBase = histograms[leftOrRight][taskIndex-1][hashResult];
        }
        for (unsigned j=0; j<inputData.size(); j++) {
            partition[leftOrRight][hashResult][j][insertBase+insertOff] = *(colIt[j]);
			++(colIt[j]);
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
                ioService->post(bind(&Join::subJoinTask, this, ioService, i, partition[0][i], partitionLength[0][i], partition[1][i], partitionLength[1][i]));
                 // cout << "Join("<< queryIndex << "," << operatorIndex <<")" << " Part Size(L,R) (" << i << "): " << partitionLength[0][i] << ", " << partitionLength[1][i] << endl;
            }
        }
    }
}

void Join::subJoinTask(boost::asio::io_service* ioService, int taskIndex, vector<uint64_t*> localLeft, unsigned limitLeft, vector<uint64_t*> localRight, unsigned limitRight) {
    unordered_multimap<uint64_t, uint64_t> hashTable;
    // Resolve the partitioned columns
    std::vector<uint64_t*> copyLeftData,copyRightData;
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
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
    for (unsigned i=0; i<requestedColumns.size(); i++)  {
		results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());
//    resultSize += localResults[0].size();

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
void SelfJoin::selfJoinTask(boost::asio::io_service* ioService, int taskIndex, unsigned start, unsigned length) {
    auto& inputData=input->getResults();
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    auto leftColId=input->resolve(pInfo.left);
    auto rightColId=input->resolve(pInfo.right);

    auto leftColIt=inputData[leftColId].begin(start);
    auto rightColIt=inputData[rightColId].begin(start);

    vector<Column<uint64_t>::Iterator> colIt;
    
    for (unsigned i=0; i<copyData.size(); i++) {
        colIt.push_back(copyData[i]->begin(start));
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
        }
    }
    //local results가 0일 경우??? ressult, tmp 초기화
    for (int i=0; i<copyData.size(); i++) {
        results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
    }
	__sync_fetch_and_add(&resultSize, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0) {
        finishAsyncRun(*ioService, true);
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
    
    auto& inputData=input->getResults();
    int cntTask = THREAD_NUM;
    unsigned taskLength = (input->resultSize+cntTask-1)/cntTask;
    
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
        }
    } 
    
    if (input->resultSize/cntTask < minTuplesPerTask) {
        cntTask = (input->resultSize+minTuplesPerTask-1)/minTuplesPerTask;
        taskLength = minTuplesPerTask;
    }
    
    pendingTask = cntTask; 
    __sync_synchronize();
    unsigned length = taskLength;
    for (int i=0; i<cntTask; i++) {
        unsigned start = i*length;
        if (i == cntTask-1 && input->resultSize%length) {
            length = input->resultSize%length;
        }
        ioService.post(bind(&SelfJoin::selfJoinTask, this, &ioService, i, start, length)); 
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
        if (input->resultSize == 0) {
            for (auto& sInfo : colInfo) {
                checkSums.push_back(0);
            }
            finishAsyncRun(ioService, false);
            return;
        }
		
        auto& results=input->getResults();

		for (auto& sInfo : colInfo) {
			auto colId=input->resolve(sInfo);
			auto resultColIt=results[colId].begin(0);
			uint64_t sum=0;
			resultSize=input->resultSize;
			for (int i=0;i<input->resultSize;i++, ++resultColIt)
				sum+=*resultColIt;
			checkSums.push_back(sum);
		}
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
	cout << "pendingSelfJoin : " << pendingTask << endl;
}
void Join::printAsyncInfo() {
	left->printAsyncInfo();
	right->printAsyncInfo();
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
