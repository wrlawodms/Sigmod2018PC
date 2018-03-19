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
	Column() {
		baseOffset.push_back(0);
	}
	void addTuples(uint64_t* tuples, unsigned length) {
		if (length <= 0)
			return;
		this->tuples.push_back(tuples);
		this->tupleLength.push_back(length);
		length += baseOffset[baseOffset.size()-1];
		this->baseOffset.push_back(length);
	}

	
	class Iterator {
		Column<T>& col;
		int globalOffset;
		int localIndex;
		int localOffset; 

	public:
		Iterator(Column<T>& col, int start) : col(col) {
			globalOffset = start;
			auto it = lower_bound(col.baseOffset.begin(), col.baseOffset.end(),  globalOffset);
			localIndex = it - col.baseOffset.begin();
			if (col.baseOffset[localIndex] != globalOffset)
				localIndex--;
			localOffset = start - col.baseOffset[localIndex];
		}
		
		inline T& operator*() {
			return col.tuples[localIndex][localOffset];
		}

		inline Iterator& operator++() {
			localOffset++;
			if (localOffset >= col.tupleLength[localIndex]) {
				if (localIndex < col.tuples.size()-1) {
					localIndex++;
					localOffset = 0;
				}
			}
			return *this;
		}
				
	};
	Iterator begin(int index) { return Iterator(*this, index); }
	//Iterator end() { return Iterator(); }
	
};
