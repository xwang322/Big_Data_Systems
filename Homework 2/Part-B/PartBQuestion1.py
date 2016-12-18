from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pytonFileName.py <hdfs_directory>", file=sys.stderr)
        exit(-1)
    filedir = sys.argv[1]
    spark = SparkSession.builder.appName("CS-838-Assignment2-PartB-Q1").getOrCreate()
    userSchema = StructType().add("userA", "string").add("userB", "string").add("timestamp", "string").add("interaction", "string")
    csvDF = spark.readStream.format('csv').option("sep", ",").schema(userSchema).load(filedir)
    words = csvDF.select("interaction", "timestamp")
    windowedCounts = words.groupBy(window(words.timestamp, '60 minutes', '30 minutes'), words.interaction).count().orderBy('window')
    query = windowedCounts.writeStream.outputMode('complete').format('console').option('truncate', 'false').option('numRows', 200).start()
    query.awaitTermination()
