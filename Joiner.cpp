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
#include "Operators.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
Joiner::Joiner(int threadNum) {
    boost::asio::io_service::work work(ioService);

    for (int i=0; i<threadNum; i++) {
        threadPool.create_thread(
            boost::bind(&boost::asio::io_service::run, &ioService)
            );
    }
}
//---------------------------------------------------------------------------
void Joiner::addRelation(const char* fileName)
// Loads a relation from disk
{
    relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
void Joiner::waitAsyncJoins() {
    mutex mt;
    unique_lock<mutex> lk(mt); 
    cvAsync.wait(lk); 
}
//---------------------------------------------------------------------------
vector<string> Joiner::getAsyncJoinResults() { 
    vector<string> results;

    
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
string Joiner::join(QueryInfo& query, bool async)
// Executes a join query
{
    //cerr << query.dumpText() << endl;
    set<unsigned> usedRelations;

    // We always start with the first join predicate and append the other joins to it (--> left-deep join trees)
    // You might want to choose a smarter join ordering ...
    auto& firstJoin=query.predicates[0];
    auto left=addScan(usedRelations,firstJoin.left,query);
    auto right=addScan(usedRelations,firstJoin.right,query);
    shared_ptr<Operator> root=make_shared<Join>(left, right,firstJoin);
    left->setParent(root);
    right->setParent(root); 

    for (unsigned i=1;i<query.predicates.size();++i) {
        auto& pInfo=query.predicates[i];
        auto& leftInfo=pInfo.left; auto& rightInfo=pInfo.right;
        shared_ptr<Operator> left, right;
        switch(analyzeInputOfJoin(usedRelations,leftInfo,rightInfo)) {
            case QueryGraphProvides::Left:
                left=root;
                right=addScan(usedRelations,rightInfo,query);
                root=make_shared<Join>(left, right,pInfo);
                left->setParent(root);
                right->setParent(root);
                break;
            case QueryGraphProvides::Right:
                left=addScan(usedRelations,leftInfo,query);
                right=root;
                root=make_shared<Join>(left,right,pInfo);
                left->setParent(root);
                right->setParent(root);
                break;
            case QueryGraphProvides::Both:
                // All relations of this join are already used somewhere else in the query.
                // Thus, we have either a cycle in our join graph or more than one join predicate per join.
                left = root;
                root=make_shared<SelfJoin>(left,pInfo);
                left->setParent(root);
                break;
            case QueryGraphProvides::None:
                // Process this predicate later when we can connect it to the other joins
                // We never have cross products
                query.predicates.push_back(pInfo);
                break;
        };
    }

    Checksum checkSum(*this, root, query.selections);
    if (async) {
        int qurey_index = __sync_fetch_and_add(&pendingAsyncJoin, 1);
        asyncResults.emplace_back();
        checkSum.asyncRun(ioService, query_index);
        return NULL;
    }

    checkSum.run();

    stringstream out;
    auto& results=checkSum.checkSums;
    for (unsigned i=0;i<results.size();++i) {
        out << (checkSum.resultSize==0?"NULL":to_string(results[i]));
        if (i<results.size()-1)
            out << " ";
    }
    out << "\n";
    return out.str();
}
//---------------------------------------------------------------------------
