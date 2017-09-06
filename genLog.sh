#!/bin/bash

while [ 1 ]; do
    ./sample_web_log.py > test.log

    tmplog = "access.`date +'%s'`.log"
    cp test.log streaming/tmp/$tmplog
    mv streaming/tmp/$tmplog streaming/
    echo "`date +"%F %T"` generating $tmplog succeed"
    sleep 1
done