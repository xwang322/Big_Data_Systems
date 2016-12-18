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

with open('web-BerkStan.txt', 'r') as f:
    nodes = set()
    for line in f:
        if line[0] == '#':
            pass
        else:
            line = line.strip();
            items = line.split('\t')
            for each in items:
                nodes.add(each)
    Matrix = np.zeros((len(nodes), len(nodes)))
    nodes_ranking = {}
    for each in nodes:
        nodes_ranking[int(each)] = 1
    f.seek(0)
    for line in f:
        if line[0] == '#':
            pass
        else:
            line = line.strip();
            items = line.split('\t')
            if len(items) == 2:
                Matrix[int(items[0])-1][int(items[1])-1] = 1
f.close()
maxIteration = 10
for i in range(maxIteration):
    contributions_matrix = np.zeros(len(nodes))
    row_sum_matrix = Matrix.sum(axis = 1)
    for j in range(len(Matrix)):
        for k in range(len(Matrix[0])):
            if (Matrix[j][k] != 0):
                Matrix[j][k] = float(nodes_ranking.get(j+1))/float(row_sum_matrix[j])
    col_sum_matrix = Matrix.sum(axis = 0)
    for j in range(len(nodes_ranking)):
        nodes_ranking[j+1] = col_sum_matrix[j] * 0.85 + 0.15
    for j in range(len(Matrix)):
        for k in range(len(Matrix[0])):
            if (Matrix[j][k] != 0):
                Matrix[j][k] = 1
fout = open('result.txt','w')
for each in nodes_ranking:
    fout.write(str(each) + ':' + str(nodes_ranking[each]) + '\n')
fout.close()








	
