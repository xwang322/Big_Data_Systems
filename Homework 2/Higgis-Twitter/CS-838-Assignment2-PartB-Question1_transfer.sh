#!/bin/bash
max=1127
for i in `seq 1 $max`
do 
    echo "Moving: " $in
    hadoop fs -cp /user/ubuntu/staging/split-dataset/$i.csv  /user/ubuntu/monitoring
    sleep 5
done