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
		if (asyncJoins[i]->pendingAsyncOperator != 0) 
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
void Joiner::join(QueryInfo& query, int queryIndex)
// Executes a join query
{
    shared_ptr<Operator> root[4];
#ifdef VERBOSE
    unsigned opIdx = 0;
#endif
    for (unsigned i=0;i<query.predicates.size();++i) {
        auto& pInfo=query.predicates[i];
        assert(pInfo.left < pInfo.right); 
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
    asyncJoins.emplace_back();
    asyncResults.emplace_back();
    ioService.post(bind(&Joiner::join, this, query, nextQueryIndex++)); 
}
