#include <iostream>
#include <thread>
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
			usleep(1000);
			joiner->printAsyncJoinInfo();
		}
	}
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
    joiner.loadIndexs();
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
        i.parseQuery(line);
        joiner.createAsyncQueryTask(i);
        //cout << joiner.join(i);
    }
    //cerr << sum;
#ifdef ANALYZE
    extern uint64_t totFilterScan;
    extern uint64_t totJoin;
    extern uint64_t totSelfJoin;
    extern uint64_t totChecksum;
    extern uint64_t totUseIndex;
    extern uint64_t totNotUseIndex;
#define PRINT(X) do{\
    fprintf(stderr, "%20s : %20lu\n", #X, X);\
}while(0)
    uint64_t tot = totFilterScan + totJoin + totSelfJoin + totChecksum;
    PRINT(totFilterScan);
    PRINT(totJoin);
    PRINT(totSelfJoin);
    PRINT(totChecksum);
    PRINT(totUseIndex);
    PRINT(totNotUseIndex);
    cerr << "Total : "<< tot << endl;
#endif
#ifdef ANALYZE
    cerr << "Processing Time : " << endTimer(t) << endl;
#endif
    return 0;
}
