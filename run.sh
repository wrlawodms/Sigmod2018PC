#!/bin/bash


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
if [ "$1" == "-g" ]; then
    gdb ${DIR}/build/release/Driver
else
    ${DIR}/build/release/Driver
fi
