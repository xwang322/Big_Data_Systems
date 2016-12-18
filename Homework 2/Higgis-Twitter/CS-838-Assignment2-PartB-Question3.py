from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pytonFileName.py <hdfs_read_directory> <UserList_file>", file=sys.stderr)
        exit(-1)
    filedir = sys.argv[1]
    UserList = sys.argv[2]
    spark = SparkSession.builder.appName("CS-838-Assignment2-PartB-3").getOrCreate()
     
    UserListSchema = StructType().add("userId", "string")
    UserListDF = spark.read.format('csv').option("sep", ",").schema(UserListSchema).load(UserList)	
     
    StreamSchema = StructType().add("userA", "string").add("userB", "string").add("timestamp", "string").add("interaction", "string")
    StreamDF = spark.readStream.format('csv').option("sep", ",").schema(StreamSchema).load(filedir)

    StreamDF.createOrReplaceTempView("Stream")	
    UserListDF.createOrReplaceTempView("UserList")	
    actionCounts = spark.sql("SELECT userA, count(interaction) FROM Stream, UserList WHERE Stream.userA = UserList.userId GROUP BY userA")
	
    query = actionCounts.writeStream.outputMode('complete').trigger(processingTime='5 seconds').format('console').option('truncate', 'false').start()  
    query.awaitTermination()
# run with bin/spark-submit CS-838-Assignment2-PartB-Question3.py hdfs://10.254.0.254/user/ubuntu/monitoring UserList.csv	
	
