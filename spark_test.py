#%%
from src.metrics import MetricRegistry
from src.dataloader import JDBCDataLoader, BigQueryDataLoader

import os
import dotenv
assert dotenv.load_dotenv(dotenv_path=os.environ.get('DOTENV_PATH', '.env'))

from pyspark.sql import SparkSession


#%%
spark = SparkSession.builder.appName("TestDockerPySpark").getOrCreate()

result = spark.sparkContext.parallelize([1, 2, 3, 4, 5]).collect()
print(result)