#!/bin/bash

/home/ubuntu/software/spark-2.0.0-bin-hadoop2.6/bin/spark-submit --class "MyPageRank" target/scala-2.11/assignment3-partb-1_2.11-1.0.jar hdfs://10.254.0.254/user/ubuntu/soc-LiveJournal1.txt 20
