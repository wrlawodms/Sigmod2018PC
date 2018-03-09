#pragma once

// using macro for performance.
#define RADIX_HASH(BIT,VALUE) (VALUE&((1<<BIT)-1))

template <class K,V>
class RawRadixHashTable<T> {
    T** buckets;
public:
    /*
        num_bits: a number of bits uing radix hahsing
    */
    RawRadixHashTable(int num_bits, std::vector<int> size_per_buckets);
    
    /*
        insret K,V to hash to the specific pos
        if pos is already used, it'll be overritten.
        
        ret: if pos > allocated size per 
    */
    bool insert(K key, V value, int pos);
    
    /*
        
    */
    int insert(K key, V value);
};
