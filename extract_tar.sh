#!/bin/bash

tar --exclude=submission.tar.gz --exclude=./workloads --exclude=./build --exclude=.git -czvf submission.tar.gz .
#scp -P 3389 submission.tar.gz 172.17.0.1:.
