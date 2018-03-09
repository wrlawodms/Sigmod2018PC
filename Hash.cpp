#include "Hash.hpp"


RawRadixHashTable::RawRadixHashTable(int num_bits, vector<int> size_per_buckets) {
    assert(2^num_bits == size_per_buckets);
    
}
