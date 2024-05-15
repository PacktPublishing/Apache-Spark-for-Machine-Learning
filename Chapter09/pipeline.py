from pyspark.sql import SparkSession 
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler 
from pyspark.ml.regression import LinearRegression 
from pyspark.ml import Pipeline 


spark = SparkSession.builder.appName("MLPipelineExample").getOrCreate() 
data = spark.read.csv("path/to/your/dataset.csv", header=True, inferSchema=True) 
dest_indexer = StringIndexer(inputCol="destination", outputCol="dest_index") 
pipeline = Pipeline(stages=[dest_indexer, dest_encoder, vec_assembler, lr]) 


dest_encoder = OneHotEncoder(inputCol="dest_index", outputCol="dest_vec") 
vec_assembler = VectorAssembler(inputCols=["feature1", "feature2", "dest_vec"], outputCol="features") 
lr = LinearRegression(featuresCol="features", labelCol="target") 
pipeline = Pipeline(stages=[dest_indexer, dest_encoder, vec_assembler, lr]) 
model = pipeline.fit(data) 
transformed_data = model.transform(data) 
