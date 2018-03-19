#include <iostream>
#include <thread>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "Config.hpp"

using namespace std;

#ifdef MONITOR_ASYNC_JOIN
	void monitorAsyncJoinThread(Joiner* joiner) {
		while (true) {
			sleep(1);
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
            joiner.waitAsyncJoins();
            auto results = joiner.getAsyncJoinResults(); // result strings vector
            for (auto& result : results)
                cout << result;
            continue;
        }
        i.parseQuery(line);
        joiner.join(i);
        //cout << joiner.join(i);
    }
    return 0;
}
