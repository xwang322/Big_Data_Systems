from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import SparseVector, VectorUDT, Vectors, DenseVector
from pyspark.sql.types import Row, StructType, StructField,DoubleType
from pyspark.mllib.util import MLUtils
#from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import LogisticRegressionWithSGD
import numpy as np



def parse_criteo_line(line):
	items = line.split(",")
	label = float(items[0])
	nnz = len(items) - 1
	indices = np.zeros
	indices = np.zeros(nnz, dtype=np.int32)
	values = np.zeros(nnz)
	for i in range(nnz):
		index, value = items[1 + i].split(":")
		indices[i] = int(index) - 1
		values[i] = float(value)
	return label, indices, values

def load_file (scfile, numFeatures):
	parsed = scfile.map(lambda l : parse_criteo_line(l))
	from pyspark.mllib.regression import LabeledPoint
	if numFeatures <= 0:
		parsed.cache()
		numFeatures = parsed.map(lambda x: -1 if x[1].size == 0 else x[1][-1]).reduce(max) + 1
	return parsed.map(lambda x: LabeledPoint(x[0], SparseVector(numFeatures, x[1], x[2])))

'''
conf = SparkConf()
conf.setAppName("SparkML")
conf.setMaster("spark://10.254.0.254:7077")
conf.set("spark.task.cpus","1")
conf.set("spark.executor.cores","4")
conf.set("spark.executor.memory","16g")
conf.set("spark.drive.memory","1g")
conf.set("spark.eventLog.enabled","true")
conf.set("spark.eventLog.dir","file:///home/ubuntu/logs/apps_spark_master")		
sc = SparkContext(conf = conf)
'''

spark = SparkSession.builder.master("spark://10.254.0.254:7077").appName("SparkML")\
.config("spark.task.cpus","1")\
.config("spark.executor.cores","4")\
.config("spark.executor.memory","10g")\
.config("spark.drive.memory","8g")\
.config("spark.eventLog.enabled","true")\
.config("spark.eventLog.dir","file:///home/ubuntu/logs/apps_spark_master")\
.getOrCreate()

sc = spark.sparkContext

num_features = 33762578

#simpleTrain = sc.textFile("hdfs://10.254.0.254/user/ubuntu/SparkML_data/simple_train.txt")
#simpleTrain = sc.textFile("file:///home/ubuntu/project/Spark/data_src/tcomma.txt")
#simpleTest = sc.textFile("hdfs://10.254.0.254/user/ubuntu/SparkML_data/simple_test.txt")
#tLines = simpleTrain.map(lambda l : l.split(",")).collect() 

sparseData = []
Lines = [['0', '3:0.1', '22:0.2'], ['1','4:0.1','11:0.11'],\
['0', '31:0.1', '122:0.2'], ['1','44:0.1','111:0.11'],\
['1', '83:0.1', '322:0.2'], ['0','54:0.1','311:0.11'],\
['1', '93:0.1', '222:0.2'], ['0','64:0.1','411:0.11'],\
['1', '11193:0.1', '12224:0.2'], ['0','2164:0.1','3411:0.11'],\
['1', '11193:0.1', '12224:0.2'], ['0','2164:0.1','3411:0.11'],\
['1', '11193:0.1', '12224:0.2'], ['0','2164:0.1','3411:0.11'],\
['1', '11193:0.1', '12224:0.2'], ['0','2164:0.1','3411:0.11'],\
['1', '11193:0.1', '12224:0.2'], ['0','2164:0.1','3411:0.11'],\
['1', '11193:0.1', '12224:0.2'], ['0','2164:0.1','3411:0.11'],\
['1', '193:0.1', '1222:0.2'], ['0','1164:0.1','2411:0.11'],\
['0', '103:0.1', '122:0.2'], ['1','74:0.1','511:0.11']]
Lines1 = [['1', '4:0.4', '25:0.21']]
for line in Lines1:
	svec = {}
	for pairs in line[1:]:
		pair = pairs.strip().split(":")
		print pair[0], pair[1]
		svec[int(pair[0])] = float(pair[1])
	sparseData.append(LabeledPoint(float(line[0]), SparseVector(num_features, svec)))
	svec.clear()
temp_rdd = sc.parallelize(sparseData).cache()

sparseData = []
for line in Lines:
	svec = {}
	for pairs in line[1:]:
		pair = pairs.strip().split(":")
		print pair[0], pair[1]
		svec[int(pair[0])] = float(pair[1])
	sparseData.append(LabeledPoint(float(line[0]), SparseVector(num_features, svec)))
	svec.clear()
temp_rdd2 = sc.parallelize(sparseData)


schema = StructType([
	StructField("label", DoubleType(), True),
	StructField("features", VectorUDT(), True)
])

#ttt=spark.read.format("libsvm").load('/home/ubuntu/software/spark-2.0.0-bin-hadoop2.6/data/mllib/sample_libsvm_data.txt')
#line1=spark.read.format("libsvm").load('/home/ubuntu/project/Spark/data_src/line1.txt')
#line2=spark.read.format("libsvm").load('/home/ubuntu/project/Spark/data_src/line1.txt')

#data = load_file(simpleTrain, num_features)
#label = data.map(lambda x: x.label)
#features = data.map(lambda x: x.features)

#df = data.map(lambda r: Row(label=r.label, features=r.features)).toDF()
#df = data.map(lambda r: (r.label, r.features))

lr = LogisticRegressionWithSGD.train(temp_rdd, iterations=1, miniBatchFraction=1.0)

#print str(lr.weights)
#lrModel = lr.train(line1)

#print("coef: " + str(lrModel.coefficients))
