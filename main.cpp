#include <iostream>
#include <thread>
#include <map>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "Config.hpp"
#ifdef ANALYZE
#include "Timer.hpp"
#endif

using namespace std;

#ifdef MONITOR_ASYNC_JOIN
	void monitorAsyncJoinThread(Joiner* joiner) {
		while (true) {
			usleep(1000000);
			joiner->printAsyncJoinInfo();
		}
	}
#endif
#ifdef ANALYZE
map<unsigned, unsigned> numRelation;
map<unsigned, unsigned> numPredicate;
map<unsigned, unsigned> numFilter;
map<SelectInfo, unsigned, SelectInfoComparer> numFilterColumn;
map<SelectInfo, unsigned, SelectInfoComparer> numPredicateColumn;
map<unsigned, unsigned> numFilterPerRelation;
uint64_t numJoin = 0;
uint64_t numSelfJoin = 0;
#endif
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    Joiner joiner(THREAD_NUM);
    // Read join relations
    string line;
#ifdef ANALYZE
    Timer t = startTimer();
#endif
    while (getline(cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }
    joiner.loadHistograms();
#ifdef ANALYZE
    cerr << "Prepare Time : " << endTimer(t) << endl;
#endif
    //chrono::steady_clock::time_point begin = chrono::steady_clock::now();

/*    
    unsigned sum;
    for (auto& rel : joiner.relations) {
        for(uint64_t* col : rel.columns) {
            int cnt = rel.size / 4096;
            for (int i=0; i<cnt; i++) {
                sum += col[i*4096];
            }
        }
    }
  */  
    // Preparation phase (not timed)
    // Build histograms, indexes,...
    //
#ifdef MONITOR_ASYNC_JOIN
	thread monitor(monitorAsyncJoinThread, &joiner);
#endif
#ifdef ANALYZE
    t = startTimer();
#endif
    QueryInfo i;
    while (getline(cin, line)) {
        //if (line == "F") continue; // End of a batch
        if (line == "F") { // End of a batch
            joiner.waitAsyncJoins();
            auto results = joiner.getAsyncJoinResults(); // result strings vector
            for (auto& result : results)
                cout << result;
            continue;
        }
        //i.parseQuery(line);
        joiner.createAsyncQueryTask(line);
        //cout << joiner.join(i);
    }
    //cerr << sum;
#ifdef ANALYZE
    extern uint64_t totFilterScan;
    extern uint64_t totJoin;
    extern uint64_t totSelfJoin;
    extern uint64_t totChecksum;
//    extern uint64_t totUseIndex;
//    extern uint64_t totNotUseIndex;
#define PRINT(X) do{\
    fprintf(stderr, "%20s : %20lu\n", #X, X);\
}while(0)
    cerr << "Performance Info" << endl;
    uint64_t tot = totFilterScan + totJoin + totSelfJoin + totChecksum;
    PRINT(totFilterScan);
    PRINT(totJoin);
    PRINT(totSelfJoin);
    PRINT(totChecksum);
//    PRINT(totUseIndex);
//    PRINT(totNotUseIndex);
    cerr << "Total : "<< tot << endl;
    cerr << "Processing Time : " << endTimer(t) << endl;

#define PRINT_MAP(X) do{\
    fprintf(stderr, "%s={\n", #X);\
    for (auto &el : (X)){\
        fprintf(stderr, "  %lu : %5lu\n", el.first, el.second);\
    }\
    fprintf(stderr, "}\n", #X, X);\
}while(0)
#define PRINT_MAP_SELECTINFO(X) do{\
    fprintf(stderr, "%s={\n", #X);\
    for (auto &el : (X)){\
        fprintf(stderr, "  R%lu.%lu : %5lu\n", el.first.relId, el.first.colId, el.second);\
    }\
    fprintf(stderr, "}\n", #X, X);\
}while(0)
//    PRINT_MAP(numRelation);
//    PRINT_MAP(numPredicate);
//    PRINT_MAP(numFilter);
//    PRINT_MAP_SELECTINFO(numFilterColumn);
//    PRINT_MAP_SELECTINFO(numPredicateColumn);
//    PRINT_MAP(numFilterPerRelation);
//    PRINT(numJoin);
//    PRINT(numSelfJoin);
#endif
    return 0;
}
