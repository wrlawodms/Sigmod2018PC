#!/bin/bash


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
if [ "$1" == "-g" ]; then
    gdb ${DIR}/build/release/Driver
elif [ "$1" == "-vc" ]; then
	valgrind --tool=cachegrind ${DIR}/build/release/Driver
else
	${DIR}/build/release/Driver
fi
