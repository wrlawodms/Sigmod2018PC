#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>
#include <algorithm>
#include "Parser.hpp"
#ifdef ANALYZE
extern map<unsigned, unsigned> numRelation;
extern map<unsigned, unsigned> numPredicate;
extern map<unsigned, unsigned> numFilter;
extern map<SelectInfo, unsigned, SelectInfoComparer> numFilterColumn;
extern map<SelectInfo, unsigned, SelectInfoComparer> numPredicateColumn;
extern map<unsigned, unsigned> numFilterPerRelation;
extern uint64_t numJoin;
extern uint64_t numSelfJoin;
#endif
//#include "Operators.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
void Joiner::addRelation(const char* fileName)
// Loads a relation from disk
{
    relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
void Joiner::printAsyncJoinInfo() {
	__sync_synchronize();
	for (int i=0; i<asyncJoins.size(); i++) {	
		cout << "-----------------Query " << i << "---------------" << endl;
		if (asyncJoins[i] != nullptr && asyncJoins[i]->pendingAsyncOperator != 0) 
			asyncJoins[i]->printAsyncInfo();
	}

}
//---------------------------------------------------------------------------
void Joiner::waitAsyncJoins() {
    unique_lock<mutex> lk(cvAsyncMt);
    if (pendingAsyncJoin > 0)  {
#ifdef VERBOSE
        cout << "Joiner::waitAsyncJoins wait" << endl;
#endif
        cvAsync.wait(lk); 
#ifdef VERBOSE
        cout << "Joiner::waitAsyncJoins wakeup" << endl;
#endif
    }
}
//---------------------------------------------------------------------------
vector<string> Joiner::getAsyncJoinResults() { 
    vector<string> results;

    stringstream out;
    for (auto& queryResult : asyncResults) {
        for (unsigned i=0;i<queryResult.size();++i) {
            out << (queryResult[i]==0?"NULL":to_string(queryResult[i]));
            if (i<queryResult.size()-1)
                out << " ";
        }
        out << "\n";
        results.push_back(out.str());
        out.str("");
    }
#ifdef VERBOSE
    cout << "Joiner::getAsnyJoinResults "<< nextQueryIndex-1 << " queries are processed." << endl;
#endif

    asyncResults.clear();
//    tmp.insert(tmp.end(), asyncJoins.begin(), asyncJoins.end()); 
    ioService.post(bind([](vector<shared_ptr<Checksum>> ops){ }, asyncJoins)); //gc, asynchronous discount shared pointer and release
    asyncJoins.clear();
    nextQueryIndex = 0;
    
    return results;
}
//---------------------------------------------------------------------------
Relation& Joiner::getRelation(unsigned relationId)
// Loads a relation from disk
{
    if (relationId >= relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return relations[relationId];
}
//---------------------------------------------------------------------------
shared_ptr<Operator> Joiner::addScan(SelectInfo& info,QueryInfo& query)
// Add scan to query
{
    vector<FilterInfo> filters;
    for (auto& f : query.filters) {
        if (f.filterColumn.binding==info.binding) {
            filters.emplace_back(f);
        }
    }
    return filters.size()?make_shared<FilterScan>(getRelation(info.relId),filters):make_shared<Scan>(getRelation(info.relId),info.binding);
}
//---------------------------------------------------------------------------
enum QueryGraphProvides {  Left, Right, Both, None };
//---------------------------------------------------------------------------
static QueryGraphProvides analyzeInputOfJoin(set<unsigned>& usedRelations,SelectInfo& leftInfo,SelectInfo& rightInfo)
// Analyzes inputs of join
{
    bool usedLeft=usedRelations.count(leftInfo.binding);
    bool usedRight=usedRelations.count(rightInfo.binding);

    if (usedLeft^usedRight)
        return usedLeft?QueryGraphProvides::Left:QueryGraphProvides::Right;
    if (usedLeft&&usedRight)
        return QueryGraphProvides::Both;
    return QueryGraphProvides::None;
}
//---------------------------------------------------------------------------
//vector<unsigned> idxs;
void Joiner::join(QueryInfo& query, int queryIndex)
// Executes a join query
{
    //cerr << query.dumpText() << endl;
    uint64_t simulSize[4];
    for (unsigned i = 0; i < query.relationIds.size(); i++){
        simulSize[i] = relations[query.relationIds[i]].size;
    }
    for (auto &f : query.filters){
        simulSize[f.filterColumn.binding] = estimateFilterResultSize(f, simulSize[f.filterColumn.binding]);
    }
    for (auto &p : query.predicates){
        p.sel = estimatePredicateSel(p);
    }
    vector<unsigned> idxs;
    idxs.reserve(query.predicates.size());
    for (unsigned i = 0; i < query.predicates.size(); ++i){
        idxs.emplace_back(i);
    }
//    if (!idxs.size()){
//        idxs.reserve(query.predicates.size());
//        for (unsigned i = 0; i < query.predicates.size(); ++i){
//            idxs.emplace_back(i);
//        }
//    }
//    else{
//        next_permutation(idxs.begin(), idxs.end());
//    }
    vector<unsigned> bestIdxs;
    uint64_t bestCost = UINT64_MAX;
    do{
        uint64_t cost = 0;
        uint64_t sizes[4];
        unsigned group[4];
        for (unsigned i = 0; i < query.relationIds.size(); ++i){
            sizes[i] = simulSize[i];
            group[i] = i;
        }
        for (auto x:idxs){
            PredicateInfo &p(query.predicates[x]);
            if (group[p.left.binding] == group[p.right.binding]){
                uint64_t resSize = sizes[p.left.binding] * p.sel;
                cost += sizes[p.left.binding];
                unsigned gid = group[p.left.binding];
                for (unsigned i = 0; i < query.relationIds.size(); ++i){
                    if (group[i] == gid){
                        sizes[i] = resSize;
                    }
                }
            }
            else{
                uint64_t resSize = sizes[p.left.binding] * sizes[p.right.binding] * p.sel;
                cost += 2 * (sizes[p.left.binding] + sizes[p.right.binding]) +
                    min(sizes[p.left.binding], sizes[p.right.binding]) + resSize;
                unsigned lgid = group[p.left.binding];
                unsigned rgid = group[p.right.binding];
                for (unsigned i = 0; i < query.relationIds.size(); ++i){
                    if (group[i] == lgid || group[i] == rgid){
                        group[i] = lgid;
                        sizes[i] = resSize;
                    }
                }
            }
        }
        if (bestCost > cost){
            bestCost = cost;
            bestIdxs = idxs;
        }
        //TEST
        //break;
    }while(next_permutation(idxs.begin(), idxs.end()));
    vector<PredicateInfo> resultPredicates;
    resultPredicates.reserve(query.predicates.size());
    for (auto x:bestIdxs){
        resultPredicates.emplace_back(move(query.predicates[x]));
    }
    query.predicates = move(resultPredicates);
    // We always start with the first join predicate and append the other joins to it (--> left-deep join trees)
    // You might want to choose a smarter join ordering ...
//#ifdef ANALYZE
//    extern uint64_t totFilterScan;
//    extern uint64_t totJoin;
//    extern uint64_t totSelfJoin;
//    extern uint64_t totChecksum;
//    uint64_t tot = totFilterScan + totJoin + totSelfJoin + totChecksum;
//    fprintf(stderr, "%d %d %d %15lu : %15lu \n", bestIdxs[0], bestIdxs[1], bestIdxs[2], bestCost, tot);
//    totFilterScan = 0;
//    totJoin = 0;
//    totSelfJoin = 0;
//    totChecksum = 0;
//#endif
    shared_ptr<Operator> root[4];
#ifdef VERBOSE
    unsigned opIdx = 0;
#endif
    for (unsigned i=0;i<query.predicates.size();++i) {
        auto& pInfo=query.predicates[i];
        auto& leftInfo=pInfo.left; auto& rightInfo=pInfo.right;
        if (root[leftInfo.binding] == nullptr){
            root[leftInfo.binding] = addScan(leftInfo,query);
#ifdef VERBOSE
            left->setOperatorIndex(opIdx++);
#endif
        }
        if (root[rightInfo.binding] == nullptr){
            root[rightInfo.binding] = addScan(rightInfo,query);
#ifdef VERBOSE
            right->setOperatorIndex(opIdx++);
#endif
        }
        shared_ptr<Operator> left(root[leftInfo.binding]), right(root[rightInfo.binding]);
        shared_ptr<Operator> res;
        if (left != right){
            res=make_shared<Join>(left, right,pInfo);
            left->setParent(res);
            right->setParent(res); 
        }
        else{
            res=make_shared<SelfJoin>(left,pInfo);
            left->setParent(res);
        }
#ifdef VERBOSE
        res->setOperatorIndex(opIdx++);
#endif
        for (int i = 0; i < 4; ++i){
            if (root[i] == left || root[i] == right){
                root[i] = res;
            }
        }
    }

    std::shared_ptr<Checksum> checkSum = std::make_shared<Checksum>(*this, root[0], query.selections);
#ifdef VERBOSE
    checkSum->setOperatorIndex(opIdx++);
#endif
    root[0]->setParent(checkSum);
#ifdef VERBOSE
	cout << "Joiner: Query runs asynchrounously: " << queryIndex << endl; 
#endif
	asyncJoins[queryIndex] = checkSum;
	checkSum->asyncRun(ioService, queryIndex); 
}
//---------------------------------------------------------------------------
void Joiner::createAsyncQueryTask(string line)
{
	__sync_fetch_and_add(&pendingAsyncJoin, 1);
    QueryInfo query;
    query.parseQuery(line);
#ifdef ANALYZE
    ++numRelation[query.relationIds.size()];
    ++numPredicate[query.predicates.size()];
    ++numFilter[query.filters.size()];
    for (auto &p:query.predicates){
        ++numPredicateColumn[p.left];
        ++numPredicateColumn[p.right];
    }
    for (auto &f:query.filters){
        ++numFilterColumn[f.filterColumn];
    }
    for (unsigned binding=0;binding<query.relationIds.size();++binding){
        unsigned cnt=0;
        for (auto &f:query.filters){
            cnt+=(binding == f.filterColumn.binding);
        }
        ++numFilterPerRelation[cnt];
    }
    numJoin+=query.relationIds.size()-1;
    numSelfJoin+=query.predicates.size()-(query.relationIds.size()-1);
#endif
    asyncJoins.emplace_back();
    asyncResults.emplace_back();
    
    ioService.post(bind(&Joiner::join, this, query, nextQueryIndex)); 
    __sync_fetch_and_add(&nextQueryIndex, 1);
}
double Joiner::estimatePredicateSel(PredicateInfo &p)
{
//    auto cached = predSels.find(p); 
//    if (cached != predSels.end()){
//        return cached->second;
//    }
    uint64_t res = 0;
    auto &left(relations[p.left.relId].histograms[p.left.colId]);
    auto &right(relations[p.right.relId].histograms[p.right.colId]);
    auto l = left.begin();
    auto r = right.begin();
    while(l != left.end()-1 && r != right.end()-1){
        if (l->first == r->first){
            res += ((l+1)->second - l->second) * ((r+1)->second - r->second);
            ++l;
            ++r;
        }
        else if (l->first < r->first){
            ++l;
        }
        else{
            ++r;
        }
    }
    uint64_t simulSize = res* HISTOGRAM_SAMPLE * HISTOGRAM_SAMPLE / (1 <<HISTOGRAM_SHIFT);
    uint64_t maxSize = relations[p.left.relId].size * relations[p.right.relId].size;
    double sel = 1.0 * simulSize / maxSize;
    //predSels.emplace(p, sel); 
    return sel; 
}
uint64_t Joiner::estimateFilterResultSize(FilterInfo &f, uint64_t inputSize)
{
#define HISTOGRAM_RANGE (1<<HISTOGRAM_SHIFT)
    uint64_t res = 0;
    auto &hist(relations[f.filterColumn.relId].histograms[f.filterColumn.colId]);
    if (f.comparison == FilterInfo::Equal){
        uint64_t v = f.constant>>HISTOGRAM_SHIFT;
        auto iter = lower_bound(hist.begin(), hist.end()-1, pair<uint64_t, uint64_t>(v, 0)); 
        if (iter != hist.end()-1 && iter->first == v){
            res=((iter+1)->second - iter->second) *
                (f.constant - (iter->first<<HISTOGRAM_SHIFT) + 1) / HISTOGRAM_RANGE;
        }
    }
    else if (f.comparison == FilterInfo::Less){
        uint64_t v = f.constant>>HISTOGRAM_SHIFT;
        auto end = lower_bound(hist.begin(), hist.end()-1, pair<uint64_t, uint64_t>(v, 0));
        res = end->second;
        if (end != hist.end()-1){
            uint64_t endVal = end->first<<HISTOGRAM_SHIFT;
            res += ((end+1)->second - end->second) *
                (f.constant <= endVal ? 0 : (f.constant - endVal) / HISTOGRAM_RANGE);
        }
    }
    else{
        uint64_t v = (f.constant+1)>>HISTOGRAM_SHIFT;
        auto start = lower_bound(hist.begin(), hist.end()-1, pair<uint64_t, uint64_t>(v, 0)); 
        if (start != hist.end()-1){
            uint64_t startVal = start->first<<HISTOGRAM_SHIFT;
            res = ((start+1)->second - start->second) *
                (f.constant+1 <= startVal ? 1 :
                 (HISTOGRAM_RANGE - (f.constant + 1 - startVal)) / HISTOGRAM_RANGE);
            res += (hist.end()-1)->second - (start+1)->second;
        }
    }
    uint64_t resSize = res * HISTOGRAM_SAMPLE * inputSize / relations[f.filterColumn.relId].size;
        
    if (resSize > inputSize){
        resSize = inputSize;
    }
    return resSize;
}
void Joiner::loadHistograms(){
    for (auto &r: relations){
        for (unsigned i = 0; i < r.columns.size(); ++i){
            ioService.post(bind(&Relation::loadHistogram, &r, i)); 
        }
    }
    bool done = false;
    while(!done){
        usleep(100000);
        done = true;
        for (auto &r: relations){
            for (unsigned i = 0; i < r.columns.size(); ++i){
                if (!r.histograms[i].size()){
                    done = false;
                    break;
                }
            }
            if (!done){
                break;
            }
        }
    }
}
