#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
mkdir -p build/release
cd build/release

BUILD_TYPE=Release
if [ "$1" == "-g" ]; then
    BUILD_TYPE=Debug
fi
echo compile for $BUILD_TYPE
cmake -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DFORCE_TESTS=OFF ../..
#cmake -DCMAKE_BUILD_TYPE=Debug -DFORCE_TESTS=OFF ../..
make -j8
