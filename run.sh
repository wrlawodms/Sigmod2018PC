#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
${DIR}/build/release/Driver
#if [ "$1" == "debug" ]; then
#fi
