#!/bin/bash

LOG_FILE="./make_test.log"
for((i=0; i<20; i++)); do
    echo "***********************begin $i times test... ******************************" >> ${LOG_FILE}
    python test.py >> ${LOG_FILE} 2>&1
    ret=`echo $?`
    if [ ${ret} -ne 0 ] ; then
        echo "*********************** $i times test fail ******************************" >> ${LOG_FILE}
        exit 1
    fi
    echo "*********************** $i times test ok ******************************" >> ${LOG_FILE}
done
