#include <chrono>
using namespace std::chrono;

using Timer=high_resolution_clock::time_point;

inline Timer startTimer(){
    return high_resolution_clock::now();
}
inline unsigned long endTimer(Timer t){
    duration<double, std::milli> time_span = high_resolution_clock::now() - t;
    return time_span.count();
}
