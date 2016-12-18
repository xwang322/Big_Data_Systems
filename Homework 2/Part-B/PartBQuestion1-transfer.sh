#!/bin/bash
max=1127
for i in `seq 1 $max`
do
    echo "Moving: " $i
    hadoop fs -mv /user/ubuntu/staging/split-dataset/$i.csv /user/ubuntu/monitoring/
    sleep 5
done

hadoop fs -rm monitoring/*
hadoop fs -rm -r staging
hadoop fa -cp staging_backup staging
