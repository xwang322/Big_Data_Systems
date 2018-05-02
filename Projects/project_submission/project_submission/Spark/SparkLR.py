from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import Row#, SQLContext
from pyspark.ml.feature import *
from pyspark.sql.types import DoubleType
import time

spark = SparkSession.builder.master("spark://10.254.0.254:7077").appName("SparkML")\
.config("spark.task.cpus","1")\
.config("spark.executor.cores","4")\
.config("spark.executor.memory","14g")\
.config("spark.drive.memory","8g")\
.config("spark.eventLog.enabled","true")\
.config("spark.eventLog.dir","file:///home/ubuntu/logs/apps_spark_master")\
.config("spark.executor.extraJavaOptions","-XX:+UseG1GC")\
.getOrCreate()

sc = spark.sparkContext

t_start=time.time()

#sample = 'file:/home/ubuntu/project/data/train.txt'
sample = 'hdfs://10.254.0.254/user/ubuntu/SparkML_data/dac_halfM.txt'
sample_data = sc.textFile(sample, 2).map(lambda x: x.replace('\t', ','))

print "Data size is {}".format(sample_data.count())
sample_data.take(2)

# Parse data and create data frames
def parseData(data, sqlContext):
	parts = data.map(lambda l: l.split(",", -1))
	#from pyspark.sql.types import Row
	features = parts.map(lambda p: Row(label=(float(p[0])), IntFeature1=(p[1]), IntFeature2=(p[2]), IntFeature3=p[3],\
IntFeature4=(p[4]), IntFeature5=(p[5]), IntFeature6=p[6], IntFeature7=p[7],\
IntFeature8=(p[8]), IntFeature9=(p[9]), IntFeature10=p[10], IntFeature11=p[11],\
IntFeature12=(p[12]), IntFeature13=(p[13]), CatFeature1=p[14], CatFeature2=p[15],\
CatFeature3=p[16], CatFeature4=p[17],CatFeature5=p[18], CatFeature6=p[19],\
CatFeature7=p[20], CatFeature8=p[21],CatFeature9=p[22], CatFeature10=p[23],\
CatFeature11=p[24], CatFeature12=p[25],CatFeature13=p[26],\
CatFeature14=p[27], CatFeature15=p[28], CatFeature16=p[29],\
CatFeature17=p[30], CatFeature18=p[31],CatFeature19=p[32],\
CatFeature20=p[33], CatFeature21=p[34], CatFeature22=p[35],\
CatFeature23=p[36], CatFeature24=p[37], CatFeature25=p[38],CatFeature26=p[39]))# Apply the schema to the RDD.
	return sqlContext.createDataFrame(features)
'''	
	features = parts.map(lambda p: Row(label=(long(p[0])), IntFeature1=(long(p[1])), IntFeature2=(long(p[2])), IntFeature3=(long(p[3])),\
IntFeature4=(long(p[4])), IntFeature5=(long(p[5])), IntFeature6=(long(p[6])), IntFeature7=(long(p[7])),\
IntFeature8=(long(p[8])), IntFeature9=(long(p[9])), IntFeature10=(long(p[10])), IntFeature11=(long(p[11])),\
IntFeature12=(long(p[12])), IntFeature13=(long(p[13])), CatFeature1=p[14], CatFeature2=p[15],\
CatFeature3=p[16], CatFeature4=p[17],CatFeature5=p[18], CatFeature6=p[19],\
CatFeature7=p[20], CatFeature8=p[21],CatFeature9=p[22], CatFeature10=p[23],\
CatFeature11=p[24], CatFeature12=p[25],CatFeature13=p[26],\
CatFeature14=p[27], CatFeature15=p[28], CatFeature16=p[29],\
CatFeature17=p[30], CatFeature18=p[31],CatFeature19=p[32],\
CatFeature20=p[33], CatFeature21=p[34], CatFeature22=p[35],\
CatFeature23=p[36], CatFeature24=p[37], CatFeature25=p[38],CatFeature26=p[39]))# Apply the schema to the RDD.
'''

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)

# Register the DataFrame as a table.
schemaClicks = parseData(sample_data, sqlContext).cache()
schemaClicks.registerTempTable("clicks")
schemaClicks.printSchema()

# Cast numeric features to double
df=schemaClicks.withColumn("IntFeature1tmp",schemaClicks.IntFeature1.cast('double'))\
               .drop("IntFeature1")\
               .withColumnRenamed("IntFeature1tmp","IntFeature1")

df = df.withColumn("label", df.label.cast(DoubleType()))

df=schemaClicks.withColumn("IntFeature2tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature2").withColumnRenamed("IntFeature2tmp","IntFeature2")
df=schemaClicks.withColumn("IntFeature3tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature3").withColumnRenamed("IntFeature3tmp","IntFeature3")
df=schemaClicks.withColumn("IntFeature4tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature4").withColumnRenamed("IntFeature4tmp","IntFeature4")
df=schemaClicks.withColumn("IntFeature5tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature5").withColumnRenamed("IntFeature5tmp","IntFeature5")
df=schemaClicks.withColumn("IntFeature6tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature6").withColumnRenamed("IntFeature6tmp","IntFeature6")
df=schemaClicks.withColumn("IntFeature7tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature7").withColumnRenamed("IntFeature7tmp","IntFeature7")
df=schemaClicks.withColumn("IntFeature8tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature8").withColumnRenamed("IntFeature8tmp","IntFeature8")
df=schemaClicks.withColumn("IntFeature9tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature9").withColumnRenamed("IntFeature9tmp","IntFeature9")
df=schemaClicks.withColumn("IntFeature10tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature10").withColumnRenamed("IntFeature10tmp","IntFeature10")
df=schemaClicks.withColumn("IntFeature11tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature11").withColumnRenamed("IntFeature11tmp","IntFeature11")
df=schemaClicks.withColumn("IntFeature12tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature12").withColumnRenamed("IntFeature12tmp","IntFeature12")
df=schemaClicks.withColumn("IntFeature13tmp",schemaClicks.IntFeature1.cast('double')).drop("IntFeature13").withColumnRenamed("IntFeature13tmp","IntFeature13")

# Replace empty values in Categorical features by "NA"
df = df.na.replace('', 'NA', 'CatFeature1')
df = df.na.replace('', 'NA', 'CatFeature2')
df = df.na.replace('', 'NA', 'CatFeature3')
df = df.na.replace('', 'NA', 'CatFeature4')
df = df.na.replace('', 'NA', 'CatFeature5')
df = df.na.replace('', 'NA', 'CatFeature6')
df = df.na.replace('', 'NA', 'CatFeature7')
df = df.na.replace('', 'NA', 'CatFeature8')
df = df.na.replace('', 'NA', 'CatFeature9')
df = df.na.replace('', 'NA', 'CatFeature10')
df = df.na.replace('', 'NA', 'CatFeature11')
df = df.na.replace('', 'NA', 'CatFeature12')
df = df.na.replace('', 'NA', 'CatFeature13')
df = df.na.replace('', 'NA', 'CatFeature14')
df = df.na.replace('', 'NA', 'CatFeature15')
df = df.na.replace('', 'NA', 'CatFeature16')
df = df.na.replace('', 'NA', 'CatFeature17')
df = df.na.replace('', 'NA', 'CatFeature18')
df = df.na.replace('', 'NA', 'CatFeature19')
df = df.na.replace('', 'NA', 'CatFeature20')
df = df.na.replace('', 'NA', 'CatFeature21')
df = df.na.replace('', 'NA', 'CatFeature22')
df = df.na.replace('', 'NA', 'CatFeature23')
df = df.na.replace('', 'NA', 'CatFeature24')
df = df.na.replace('', 'NA', 'CatFeature25')
df = df.na.replace('', 'NA', 'CatFeature26')
# Drop rows containing null values in the DataFrame
df = df.dropna()

schemaClicks.printSchema()

# Use randomSplit with weights and seed to get training, test data sets
#weights = [.8, .2]
weights = [0.5, 0.5]
seed = 42
dfTrain, dfTest = df.randomSplit(weights, seed)

# Use StringIndexer and OneHotEncoder to convert categorical variables to numerical
from pyspark.ml.feature import OneHotEncoder, StringIndexer
cat1Indexer = StringIndexer(inputCol="CatFeature1", outputCol="indexedCat1", handleInvalid="skip")
cat1Encoder = OneHotEncoder(inputCol="indexedCat1", outputCol="CatVector1")
cat2Indexer = StringIndexer(inputCol="CatFeature2", outputCol="indexedCat2", handleInvalid="skip")
cat2Encoder = OneHotEncoder(inputCol="indexedCat2", outputCol="CatVector2")
cat3Indexer = StringIndexer(inputCol="CatFeature3", outputCol="indexedCat3", handleInvalid="skip")
cat3Encoder = OneHotEncoder(inputCol="indexedCat3", outputCol="CatVector3")
cat4Indexer = StringIndexer(inputCol="CatFeature4", outputCol="indexedCat4", handleInvalid="skip")
cat4Encoder = OneHotEncoder(inputCol="indexedCat4", outputCol="CatVector4")
cat5Indexer = StringIndexer(inputCol="CatFeature5", outputCol="indexedCat5", handleInvalid="skip")
cat5Encoder = OneHotEncoder(inputCol="indexedCat5", outputCol="CatVector5")
cat6Indexer = StringIndexer(inputCol="CatFeature6", outputCol="indexedCat6", handleInvalid="skip")
cat6Encoder = OneHotEncoder(inputCol="indexedCat6", outputCol="CatVector6")
cat7Indexer = StringIndexer(inputCol="CatFeature7", outputCol="indexedCat7", handleInvalid="skip")
cat7Encoder = OneHotEncoder(inputCol="indexedCat7", outputCol="CatVector7")
cat8Indexer = StringIndexer(inputCol="CatFeature8", outputCol="indexedCat8", handleInvalid="skip")
cat8Encoder = OneHotEncoder(inputCol="indexedCat8", outputCol="CatVector8")
cat9Indexer = StringIndexer(inputCol="CatFeature9", outputCol="indexedCat9", handleInvalid="skip")
cat9Encoder = OneHotEncoder(inputCol="indexedCat9", outputCol="CatVector9")
cat10Indexer = StringIndexer(inputCol="CatFeature10", outputCol="indexedCat10", handleInvalid="skip")
cat10Encoder = OneHotEncoder(inputCol="indexedCat10", outputCol="CatVector10")
cat11Indexer = StringIndexer(inputCol="CatFeature11", outputCol="indexedCat11", handleInvalid="skip")
cat11Encoder = OneHotEncoder(inputCol="indexedCat11", outputCol="CatVector11")
cat12Indexer = StringIndexer(inputCol="CatFeature12", outputCol="indexedCat12", handleInvalid="skip")
cat12Encoder = OneHotEncoder(inputCol="indexedCat12", outputCol="CatVector12")
cat13Indexer = StringIndexer(inputCol="CatFeature13", outputCol="indexedCat13", handleInvalid="skip")
cat13Encoder = OneHotEncoder(inputCol="indexedCat13", outputCol="CatVector13")
cat14Indexer = StringIndexer(inputCol="CatFeature14", outputCol="indexedCat14", handleInvalid="skip")
cat14Encoder = OneHotEncoder(inputCol="indexedCat14", outputCol="CatVector14")
cat15Indexer = StringIndexer(inputCol="CatFeature15", outputCol="indexedCat15", handleInvalid="skip")
cat15Encoder = OneHotEncoder(inputCol="indexedCat15", outputCol="CatVector15")
cat16Indexer = StringIndexer(inputCol="CatFeature16", outputCol="indexedCat16", handleInvalid="skip")
cat16Encoder = OneHotEncoder(inputCol="indexedCat16", outputCol="CatVector16")
cat17Indexer = StringIndexer(inputCol="CatFeature17", outputCol="indexedCat17", handleInvalid="skip")
cat17Encoder = OneHotEncoder(inputCol="indexedCat17", outputCol="CatVector17")
cat18Indexer = StringIndexer(inputCol="CatFeature18", outputCol="indexedCat18", handleInvalid="skip")
cat18Encoder = OneHotEncoder(inputCol="indexedCat18", outputCol="CatVector18")
cat19Indexer = StringIndexer(inputCol="CatFeature19", outputCol="indexedCat19", handleInvalid="skip")
cat19Encoder = OneHotEncoder(inputCol="indexedCat19", outputCol="CatVector19")
cat20Indexer = StringIndexer(inputCol="CatFeature20", outputCol="indexedCat20", handleInvalid="skip")
cat20Encoder = OneHotEncoder(inputCol="indexedCat20", outputCol="CatVector20")
cat21Indexer = StringIndexer(inputCol="CatFeature21", outputCol="indexedCat21", handleInvalid="skip")
cat21Encoder = OneHotEncoder(inputCol="indexedCat21", outputCol="CatVector21")
cat22Indexer = StringIndexer(inputCol="CatFeature22", outputCol="indexedCat22", handleInvalid="skip")
cat22Encoder = OneHotEncoder(inputCol="indexedCat22", outputCol="CatVector22")
cat23Indexer = StringIndexer(inputCol="CatFeature23", outputCol="indexedCat23", handleInvalid="skip")
cat23Encoder = OneHotEncoder(inputCol="indexedCat23", outputCol="CatVector23")
cat24Indexer = StringIndexer(inputCol="CatFeature24", outputCol="indexedCat24", handleInvalid="skip")
cat24Encoder = OneHotEncoder(inputCol="indexedCat24", outputCol="CatVector24")
cat25Indexer = StringIndexer(inputCol="CatFeature25", outputCol="indexedCat25", handleInvalid="skip")
cat25Encoder = OneHotEncoder(inputCol="indexedCat25", outputCol="CatVector25")
cat26Indexer = StringIndexer(inputCol="CatFeature26", outputCol="indexedCat26", handleInvalid="skip")
cat26Encoder = OneHotEncoder(inputCol="indexedCat26", outputCol="CatVector26")

# Playing with Feature transformation
from pyspark.ml import Pipeline
fAssembler = VectorAssembler(inputCols=["CatVector1"],outputCol="features")
pipelineTmp = Pipeline(stages=[cat1Indexer, cat1Encoder,fAssembler])

modelTmp = pipelineTmp.fit(dfTrain)
tmp = modelTmp.transform(dfTest).select("features")

tmp.show()

# Define the Logistic Regression learner
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=10, regParam=0.01)

fAssembler = VectorAssembler(inputCols=["CatVector1", "CatVector2", "CatVector3", "CatVector4", "CatVector5", "CatVector7",\
"CatVector8","CatVector9","CatVector10","CatVector11","CatVector12","CatVector13",\
"CatVector14","CatVector15","CatVector16","CatVector17","CatVector18","CatVector19",\
"CatVector20","CatVector21","CatVector22","CatVector23","CatVector24","CatVector25","CatVector26"],outputCol="features")

#Construct the pipeline using StringIndexers, OneHotEncoders, VectorAssembler and LogisticRegression in the correct order
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[cat1Indexer, cat2Indexer, cat3Indexer, cat4Indexer, cat5Indexer,cat6Indexer, cat7Indexer,\
cat8Indexer, cat9Indexer, cat10Indexer, cat11Indexer, cat12Indexer, cat13Indexer,\
cat14Indexer, cat15Indexer, cat16Indexer, cat17Indexer, cat18Indexer, cat19Indexer,\
cat20Indexer, cat21Indexer, cat22Indexer, cat23Indexer, cat24Indexer, cat25Indexer, cat26Indexer,\
cat1Encoder, cat2Encoder, cat3Encoder, cat4Encoder, cat5Encoder, cat6Encoder, cat7Encoder,\
cat8Encoder, cat9Encoder, cat10Encoder, cat11Encoder, cat12Encoder, cat13Encoder,\
cat14Encoder, cat15Encoder, cat16Encoder, cat17Encoder, cat18Encoder, cat19Encoder,\
cat20Encoder, cat21Encoder, cat22Encoder, cat23Encoder, cat24Encoder, cat25Encoder, cat26Encoder,\
fAssembler, lr])

# Train model.  This also runs the indexers, encoders and assembler
model = pipeline.fit(dfTrain)

# Transform the test data set to produce predictions and compare predictions to labels to determine accuracy of the model
t_train = time.time()
output = model.transform(dfTest).select("features", "label", "prediction", "rawPrediction", "probability")
prediction = output.select("label", "prediction")
accuracy = prediction.filter(prediction['label'] == prediction['prediction']).count() / float(prediction.count())
print "accuracy: "
print accuracy
t_test=time.time() 
print "train: " 
print (t_train - t_start) 
print "test: " 
print (t_test - t_train)
sc.stop()
