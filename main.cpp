#include <iostream>
#include <thread>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "Config.hpp"

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
uint64_t cntCounted;
#endif
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    Joiner joiner(THREAD_NUM);
    // Read join relations
    string line;
    while (getline(cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }
    joiner.loadStat();
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
#ifdef ANALYZE
    cerr << "cntCounted : " << cntCounted << endl; 
#endif
    //cerr << sum;
    return 0;
}
