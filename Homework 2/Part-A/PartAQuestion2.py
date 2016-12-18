import sys
from operator import add
from pyspark import SparkContext, SparkConf

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def Neighbors(urls):
    parts = urls.strip().split('\t')
    return parts[0], parts[1]

if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("CS-838-Assignment2-PartA-2")
    conf.setMaster("spark://10.254.0.254:7077")
    conf.set("spark.task.cpus","1")
    conf.set("spark.executor.cores","4")
    conf.set("spark.executor.memory","1g")
    conf.set("spark.drive.memory","1g")
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.eventLog.dir","file:///home/ubuntu/logs/apps_spark_master")		
    sc = SparkContext(conf = conf)
    lines = sc.textFile(sys.argv[1], 10)
    links = lines.map(lambda urls: Neighbors(urls)).distinct().groupByKey()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    for iteration in range(int(sys.argv[2])):
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank) + '\n')
    sc.stop()
