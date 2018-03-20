#pragma once
#include <iostream>
#include <vector>

using namespace std;

template <typename T> 
class Column {
	vector<T*> tuples;
	vector<unsigned> baseOffset;
	vector<unsigned> tupleLength;
	

public:
	//Column(const Column& c) = delete;
	Column(unsigned cntTask) {
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
        for (int i=1; i<baseOffset.size(); i++) {
            baseOffset[i] = baseOffset[i-1]+tupleLength[i-1];
        }
        // this->tuples.push_back(tuples);
		// this->tupleLength.push_back(length);
		// length += baseOffset[baseOffset.size()-1];
		// this->baseOffset.push_back(length);
	}

	
	class Iterator {
		Column<T>& col;
		int globalOffset;
		int localIndex;
		int localOffset; 

	public:
		Iterator(Column<T>& col, int start) : col(col) {
            if (col.tuples.size() == 0)
                return;
			globalOffset = start;
			auto it = lower_bound(col.baseOffset.begin(), col.baseOffset.end(),  globalOffset);
			localIndex = it - col.baseOffset.begin();
			if (col.baseOffset[localIndex] != globalOffset)
				localIndex--;
			localOffset = start - col.baseOffset[localIndex];
			while (0 == col.tupleLength[localIndex] && localIndex < col.tuples.size()-1) {
                localIndex++;
                localOffset = 0;
			}
		}
		
		inline T& operator*() {
			return col.tuples[localIndex][localOffset];
		}

		inline Iterator& operator++() {
			localOffset++;
			while (localOffset >= col.tupleLength[localIndex]) {
                localIndex++;
                localOffset = 0;
			}
			return *this;
		}
				
	};
	Iterator begin(int index) { return Iterator(*this, index); }
	//Iterator end() { return Iterator(); }
	
};
