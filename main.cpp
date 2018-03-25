#include <iostream>
#include <thread>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "Config.hpp"

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
    while (getline(cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }
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
    QueryInfo i;
    while (getline(cin, line)) {
        //if (line == "F") continue; // End of a batch
        if (line == "F") { // End of a batch
            joiner.waitAsyncParsing();
            // 여기서 뭐 각 트리를 이용한 최적화를 하면 된다.
            // 트리 다 만들어짐.
            joiner.runAsyncJoins();           
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
    return 0;
}
