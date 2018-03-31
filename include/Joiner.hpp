#pragma once
#include <vector>
#include <cstdint>
#include <set>
#include "Operators.hpp"
#include "Relation.hpp"
#include "Parser.hpp"
#include <condition_variable>
#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>
#include <thread>

//---------------------------------------------------------------------------

class Operator;
//class Checksum;
#ifdef ANALYZE
struct SelectInfoComparer{
    bool operator()(const SelectInfo &a, const SelectInfo &b){
        return a.relId < b.relId || (a.relId == b.relId && a.colId < b.colId);
    }
};
#endif

class Joiner {
    friend Checksum;
    /// Add scan to query
    std::shared_ptr<Operator> addScan(std::set<unsigned>& usedRelations,SelectInfo& info,QueryInfo& query);
 
    boost::asio::io_service ioService;
    boost::thread_group threadPool;
    boost::asio::io_service::work work;
    
    int pendingAsyncJoin = 0;
    int nextQueryIndex = 0;
    std::vector<std::vector<uint64_t>> asyncResults; //checksums
    std::vector<std::shared_ptr<Checksum>> asyncJoins;
//    std::vector<std::shared_ptr<Checksum>> tmp;
    std::condition_variable cvAsync;
    std::mutex cvAsyncMt;
    
    char* buf[THREAD_NUM];
    int cntTouch;
    void touchBuf(int i) {
        buf[i] = (char*)malloc(2*1024*1024*1024ll);
        /* 
        for (int j=0; j <1*1024*1024*1024ll/4096; j++) 
            buf[i][j*4096ll] = 1;
          */
        __sync_fetch_and_add(&cntTouch, 1);
    }
    void clearBuf(int i) {
        free(buf[i]);
    }
    public:
    Joiner(int threadNum) : work(ioService) {
        asyncResults.reserve(100);
        asyncJoins.reserve(100);
        for (int i=0; i<threadNum; i++) {
            threadPool.create_thread(
                boost::bind(&boost::asio::io_service::run, &ioService)
                );
        }
        
        cntTouch = 0;
        for (int i=0; i<threadNum; i++) {
            ioService.post(bind(&Joiner::touchBuf, this, i));
        }
        while (cntTouch < threadNum) {
            __sync_synchronize();
        }
        for (int i=0; i<threadNum; i++) { 
            ioService.post(bind(&Joiner::clearBuf, this, i));
        }
        
    }
    /// The relations that might be joined
    std::vector<Relation> relations;
    /// Add relation
    void addRelation(const char* fileName);
    /// Get relation
    Relation& getRelation(unsigned id);
    /// Joins a given set of relations
    void join(QueryInfo& i, int queryIndex);
    /// wait for async joins
    void waitAsyncJoins();
    /// return parsed asyncResults 
    std::vector<std::string> getAsyncJoinResults();
	/// print asyncJoin infos
	void printAsyncJoinInfo();
    uint64_t estimatePredicateSelectivity(PredicateInfo &p, uint64_t leftSize, uint64_t rightSize);
    uint64_t estimateFilterSelectivity(FilterInfo &f, uint64_t inputSize);

    void createAsyncQueryTask(std::string line);
    ~Joiner() {
        ioService.stop();
        threadPool.join_all();
    }
    
};
//---------------------------------------------------------------------------
