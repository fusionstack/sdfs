#!/usr/bin/env bash

NUM=$1

for i in $(seq $NUM)
do
    DATE=$(date +"%Y-%m-%d %T")
    pkill -9 python
    mkdir -p /tmp/sdfs_test
    echo "$DATE begin test $i/$NUM, log is in /tmp/sdfs_test"
    time python2 test.py $2 > /tmp/sdfs_test/test.log 2>&1
    RET=$?
    if [ "$RET" -ne 0 ]; then
        DATE=$(date +"%Y-%m-%d %T")
        echo "$DATE ret $RET"
        exit $RET
    fi
    DATE=$(date +"%Y-%m-%d %T")
    echo "$DATE end $i"

    # check valgrind result
    grep 'llegal\|inappropriate\|inadequate\|verlapping\|emory leak\|overlap\|Invalid read\|Invalid write\|definitely' /tmp/sdfs_test/*.log | grep -v 'definitely lost: 0 bytes'
    if [ $? == 0 ]; then
        echo "==========valgrind check fail==========="
        exit 1
    fi

done
