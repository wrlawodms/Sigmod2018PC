#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"

#define THREAD_NUM 40

using namespace std;
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
        joiner.join(i, true);
        //cout << joiner.join(i);
    }
    return 0;
}
