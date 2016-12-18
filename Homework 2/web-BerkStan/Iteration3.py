import numpy as np
import math
from pyspark import SparkContext, SparkConf

conf = SparkConf()
conf.setAppName("CS-838-Assignment2-PartA-1")
conf.setMaster("spark://10.254.0.254:7077")
conf.set("spark.task.cpus","1")
conf.set("spark.executor.cores","4")
conf.set("spark.executor.memory","1g")
conf.set("spark.drive.memory","1g")
conf.set("spark.eventLog.enabled","true")
conf.set("spark.eventLog.dir","file:///home/ubuntu/logs/apps_spark_master")
sc = SparkContext(conf = conf)

with open('C:\Academy\CS 838\Fall 2016\Homework2\code\web-BerkStan\web-BerkStan-sample.txt', 'r') as f:
    maxIteration = 10
    nodes = set()
    for line in f:
        if line[0] == '#':
            pass
        else:
            line = line.strip();
            items = line.split('\t')
            for each in items:
                nodes.add(each)
    nodes_ranking = {}
    for each in nodes:
        nodes_ranking[each] = 1
    f.seek(0)
    nodes_connections = {}
    for line in f:
        if line[0] == '#':
            pass
        else:
            line = line.strip();
            items = line.split('\t')
            if len(items) == 2:
                if items[0] not in nodes_connections:
                    nodes_connections.setdefault(items[0], 1)
                else:
                    nodes_connections[items[0]] += 1
    for i in range(maxIteration):
        f.seek(0)        
        nodes_contributions = {}
        for each in nodes:
            nodes_contributions[each] = 0
        for line in f:
            if line[0] == '#':
                pass
            else:
                line = line.strip();
                items = line.split('\t')
                contribution = float(nodes_ranking.get(items[0])) / float(nodes_connections.get(items[0]))
                nodes_contributions[items[1]] += contribution
        for each in nodes_ranking:
            nodes_ranking[each] = 0.15 + 0.85 * nodes_contributions[each] 
f.close()
fout = open('result.txt','w')
for each in nodes_ranking:
    fout.write(str(each) + ':' + str(nodes_ranking[each]) + '\n')
fout.close()








	