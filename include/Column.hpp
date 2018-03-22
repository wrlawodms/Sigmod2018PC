#pragma once
#include <iostream>
#include <vector>

using namespace std;

template <typename T> 
class Column {
	vector<T*> tuples;
	vector<unsigned> tupleLength;
	vector<unsigned> baseOffset;
	bool fixed = false;

public:
	//Column(const Column& c) = delete;
	Column(unsigned cntTask) {
        baseOffset.push_back(0);
        for (int i=0; i<cntTask; i++) {
            tuples.emplace_back();
            baseOffset.push_back(0);
            tupleLength.emplace_back();
        }
	}
	void addTuples(unsigned pos, uint64_t* tuples, unsigned length) {
		// if (length <= 0)
		// 	return;
        this->tuples[pos] = tuples;
		this->tupleLength[pos] = length;
        //this->baseOffset[pos+1] = length + baseOffset[pos];
        // this->tuples.push_back(tuples);
		// this->tupleLength.push_back(length);
		// length += baseOffset[baseOffset.size()-1];
		// this->baseOffset.push_back(length);
	}

    void fix() {
        for (int i=1; i<baseOffset.size(); i++) {
            baseOffset[i] = baseOffset[i-1]+tupleLength[i-1];
        }
        fixed = true;
    }

    class Iterator;

	Iterator begin(int index) { 
        assert (fixed);
        return Iterator(*this, index);
     }
	
	class Iterator {
		int localIndex;
		int localOffset; 
		Column<T> col;

	public:
		Iterator(Column<T>& col, int start) : col(col) {
            if (col.tuples.size() == 0)
                return;
			auto it = lower_bound(col.baseOffset.begin(), col.baseOffset.end(),  start);
			localIndex = it - col.baseOffset.begin();
            assert(localIndex < col.baseOffset.size());
			if (col.baseOffset[localIndex] != start)
				localIndex--;
			localOffset = start - col.baseOffset[localIndex];
			while (0 == col.tupleLength[localIndex]) {
                localIndex++;
                localOffset = 0;
                if (localIndex == col.tupleLength.size()) {
                    assert("Column::Iterator: invalud start value");
                    break;
                }
			}
		}
		
		inline T& operator*() {
            assert(localIndex < col.tupleLength.size());
			return col.tuples[localIndex][localOffset];
		}

		inline Iterator& operator++() {
			localOffset++;
			while (localOffset >= col.tupleLength[localIndex]) {
                localIndex++;
                localOffset = 0;
                if (localIndex == col.tupleLength.size()) {
                    break;
                }
			}
			return *this;
		}
				
	};
	//Iterator end() { return Iterator(); }
	
};
