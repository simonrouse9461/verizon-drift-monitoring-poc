#%%
import os
import dotenv
assert dotenv.load_dotenv(dotenv_path=os.environ.get('DOTENV_PATH', '.env'))

from src.metrics import MetricRegistry
from src.dataloader import JDBCDataLoader, BigQueryDataLoader

import sys
from functools import partial
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from pyspark.sql import SparkSession
from sqlalchemy.dialects.postgresql import insert


#%%
spark = SparkSession.builder.appName("SparkTest").getOrCreate()

result = spark.sparkContext.parallelize([1, 2, 3, 4, 5]).collect()
print(result)