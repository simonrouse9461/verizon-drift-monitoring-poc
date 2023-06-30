from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestDockerPySpark").getOrCreate()

result = spark.sparkContext.parallelize([1, 2, 3, 4, 5]).collect()
print(result)
