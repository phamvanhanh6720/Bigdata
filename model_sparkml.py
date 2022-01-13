from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import Evaluator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName('PredictionModel')\
    .getOrCreate()

# read data from hdfs
df = spark.read.format("parquet").load("hdfs://namenode:9000/cleaned_data/")

# drop unuseful columns
df = df.drop("Url", "Title", "Unit", "Location", "Province")

# remove nan
cleaned_data = df.dropna()
cleaned_data = cleaned_data.dropDuplicates()


# Index categorical columns
type_stringIdx = StringIndexer(inputCol="Type", outputCol="typeIndex")
district_stringIdx = StringIndexer(inputCol="District", outputCol="districtIndex")

cleaned_data = type_stringIdx.fit(cleaned_data).transform(cleaned_data)
cleaned_data = district_stringIdx.fit(cleaned_data).transform(cleaned_data)

# assemble columns
assembler = VectorAssembler(inputCols=['Area', 'districtIndex', 'typeIndex'], outputCol='features')
cleaned_data = assembler.transform(cleaned_data)
cleaned_data = cleaned_data.withColumnRenamed('Price', 'label')

train, test = cleaned_data.randomSplit([0.8, 0.2], seed=43)

rf = RandomForestRegressor(maxDepth=20, numTrees=30)
evaluator = RegressionEvaluator(labelCol='label', predictionCol='prediction', metricName='rmse')

model = rf.fit(train)
predictions_train = model.transform(train)
predictions_test = model.transform(test)

rmse_train = evaluator.evaluate(predictions_train)
rmse_test = evaluator.evaluate(predictions_test)

print("Root Mean Squared Error (RMSE) on train data = %g" % rmse_train)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse_test)
