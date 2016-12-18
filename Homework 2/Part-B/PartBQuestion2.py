from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ApplicationPython.py <read directory> <write directory>", file=sys.stderr)
        exit(-1)

    readdir = sys.argv[1]
    writedir = sys.argv[2]
    spark = SparkSession.builder.appName("CS-838-Assignment2-PartB-2").getOrCreate()
    userSchema = StructType().add("userA", "string").add("userB", "string").add("timestamp", "string").add("interaction", "string")
    csvDF = spark.readStream.format('csv').option("sep", ",").schema(userSchema).load(readdir)
    words = csvDF.select("userB").where("interaction = 'MT'")
    query = words.writeStream.outputMode('append').trigger(processingTime='10 seconds').format("parquet").option("checkpointLocation", writedir).start(writedir)
    query.awaitTermination()
