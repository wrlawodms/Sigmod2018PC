#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>
#include "Parser.hpp"
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
shared_ptr<Operator> Joiner::addScan(set<unsigned>& usedRelations,SelectInfo& info,QueryInfo& query)
// Add scan to query
{
    usedRelations.emplace(info.binding);
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
void Joiner::join(QueryInfo& query, int queryIndex)
// Executes a join query
{
    //cerr << query.dumpText() << endl;
    set<unsigned> usedRelations;
    uint64_t simulSize[4];
    for (unsigned i = 0; i < query.relationIds.size(); i++){
        simulSize[i] = relations[query.relationIds[i]].size;
    }
    for (auto &f : query.filters){
        simulSize[f.filterColumn.binding] = estimateFilterSelectivity(f, simulSize[f.filterColumn.binding]);
    }
    for (auto &p : query.predicates){
        p.sel = estimatePredicateSelectivity(p, simulSize[p.left.binding], simulSize[p.right.binding]);
    }
    struct PredicateComparer{
        bool operator()(const PredicateInfo &a, const PredicateInfo &b){
            return a.sel < b.sel;
        }
    } predicateComparer;
    sort(query.predicates.begin(), query.predicates.end(), predicateComparer);
    // We always start with the first join predicate and append the other joins to it (--> left-deep join trees)
    // You might want to choose a smarter join ordering ...
    auto& firstJoin=query.predicates[0];
    auto left=addScan(usedRelations,firstJoin.left,query);
    auto right=addScan(usedRelations,firstJoin.right,query);
    
    shared_ptr<Operator> root=make_shared<Join>(left, right, firstJoin);
#ifdef VERBOSE
    unsigned opIdx = 0;
    left->setOperatorIndex(opIdx++);
    right->setOperatorIndex(opIdx++);
    root->setOperatorIndex(opIdx++);
    left->setQeuryIndex(queryIndex);
    right->setQeuryIndex(queryIndex);
    root->setQeuryIndex(queryIndex);
#endif
    left->setParent(root);
    right->setParent(root); 
    unsigned selfJoinCheck = query.predicates.size();

    for (unsigned i=1;i<query.predicates.size();++i) {
        auto& pInfo=query.predicates[i];
        assert(pInfo.left < pInfo.right); 
        auto& leftInfo=pInfo.left; auto& rightInfo=pInfo.right;
        shared_ptr<Operator> left, right;
        auto res = analyzeInputOfJoin(usedRelations,leftInfo,rightInfo);
        if (i < selfJoinCheck){
            if (res == QueryGraphProvides::Both){
                // All relations of this join are already used somewhere else in the query.
                // Thus, we have either a cycle in our join graph or more than one join predicate per join.
                left = root;
                root=make_shared<SelfJoin>(left,pInfo);
#ifdef VERBOSE
                root->setOperatorIndex(opIdx++);
                root->setQeuryIndex(nextQueryIndex);
#endif
                left->setParent(root);
            }
            else{
                query.predicates.push_back(pInfo);
            }
            continue;
        }
        switch(res) {
            case QueryGraphProvides::Left:
                left=root;
                right=addScan(usedRelations,rightInfo,query);
                root=make_shared<Join>(left, right,pInfo);
#ifdef VERBOSE
                right->setOperatorIndex(opIdx++);
                root->setOperatorIndex(opIdx++);
                right->setQeuryIndex(queryIndex);
                root->setQeuryIndex(queryIndex);
#endif
                left->setParent(root);
                right->setParent(root);
                selfJoinCheck = query.predicates.size(); 
                break;
            case QueryGraphProvides::Right:
                left=addScan(usedRelations,leftInfo,query);
                right=root;
                root=make_shared<Join>(left,right,pInfo);
#ifdef VERBOSE
                left->setOperatorIndex(opIdx++);
                root->setOperatorIndex(opIdx++);
                left->setQeuryIndex(queryIndex);
                root->setQeuryIndex(queryIndex);
#endif
                left->setParent(root);
                right->setParent(root);
                selfJoinCheck = query.predicates.size(); 
                break;
            case QueryGraphProvides::None:
                // Process this predicate later when we can connect it to the other joins
                // We never have cross products
                query.predicates.push_back(pInfo);
                break;
        };
    }

    std::shared_ptr<Checksum> checkSum = std::make_shared<Checksum>(*this, root, query.selections);
#ifdef VERBOSE
    checkSum->setOperatorIndex(opIdx++);
#endif
    root->setParent(checkSum);
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
    asyncJoins.emplace_back();
    asyncResults.emplace_back();
    
    ioService.post(bind(&Joiner::join, this, query, nextQueryIndex)); 
    __sync_fetch_and_add(&nextQueryIndex, 1);
}
uint64_t Joiner::estimatePredicateSelectivity(PredicateInfo &p, uint64_t leftSize, uint64_t rightSize)
{
    uint64_t res = 0;
    map<uint64_t, uint64_t> &left(relations[p.left.relId].histograms[p.left.colId]);
    map<uint64_t, uint64_t> &right(relations[p.right.relId].histograms[p.right.colId]);
    auto l = left.begin();
    auto r = right.begin();
    while(l != left.end() && r != right.end()){
        if (l->first == r->first){
            res += l->second * r->second;
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
    //uint64_t buildSize = min(relations[p.left.relId].size, relations[p.right.relId].size);
    //return buildSize + res * HISTOGRAM_SAMPLE * HISTOGRAM_SAMPLE / (1 <<HISTOGRAM_SHIFT);
    return leftSize + rightSize + res * HISTOGRAM_SAMPLE * HISTOGRAM_SAMPLE / (1 <<HISTOGRAM_SHIFT);
}
uint64_t Joiner::estimateFilterSelectivity(FilterInfo &f, uint64_t inputSize)
{
    uint64_t res = 0;
    map<uint64_t, uint64_t> &hist(relations[f.filterColumn.relId].histograms[f.filterColumn.colId]);
    if (f.comparison == FilterInfo::Equal){
        auto iter = hist.find(f.constant>>HISTOGRAM_SHIFT); 
        if (iter != hist.end()){
            res=iter->second*(f.constant - (iter->first<<HISTOGRAM_SHIFT) + 1)/ (1<<HISTOGRAM_SHIFT);
        }
    }
    else if (f.comparison == FilterInfo::Less){
        auto end = hist.lower_bound(f.constant>>HISTOGRAM_SHIFT); 
        for (auto iter = hist.begin(); iter != end; ++iter){
            res+=iter->second;
        }
        if (end != hist.end()){
            res+=end->second*(f.constant < (end->first<<HISTOGRAM_SHIFT) ? 1 : (f.constant - (end->first<<HISTOGRAM_SHIFT))/ (1<<HISTOGRAM_SHIFT));
        }
    }
    else{
        auto start = hist.lower_bound((f.constant+1)>>HISTOGRAM_SHIFT); 
        if (start != hist.end()){
            res+=start->second * ((1<<HISTOGRAM_SHIFT) - (f.constant + 1 - (start->first<<HISTOGRAM_SHIFT)))/(1<<HISTOGRAM_SHIFT);
            for (auto iter = start; iter != hist.end(); ++iter){
                res+=iter->second;
            }
        }
    }
    return res * HISTOGRAM_SAMPLE * inputSize / relations[f.filterColumn.relId].size;
}
