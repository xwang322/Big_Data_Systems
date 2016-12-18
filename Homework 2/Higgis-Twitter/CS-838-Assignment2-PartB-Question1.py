from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pytonFileName.py <hdfs_directory>", file=sys.stderr)
        exit(-1)
    filedir = sys.argv[1]
    spark = SparkSession.builder.appName("CS-838-Assignment2-PartB-1").getOrCreate()
    userSchema = StructType().add("userA", "string").add("userB", "string").add("timestamp", "string").add("interaction", "string")
    csvDF = spark.readStream.format('csv').option("sep", ",").schema(userSchema).load(filedir)
    words = csvDF.select("interaction", "timestamp")	
    windowedCounts = words.groupBy(window(words.timestamp, '60 minutes', '30 minutes'),words.interaction).count().orderBy('window')
    query = windowedCounts.writeStream.outputMode('complete').format('console').option('truncate', 'false').start()  
    query.awaitTermination()
	
# run with bin/spark-submit CS-838-Assignment2-PartB-Question1.py hdfs://10.254.0.254/user/ubuntu/staging/split-dataset	