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
from pyspark import SparkConf
from pyspark.sql import SparkSession
from sqlalchemy.dialects.postgresql import insert

#%%
POSTGRES_ENDPOINT = os.environ['POSTGRES_EXTERNAL_ENDPOINT']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_PSYCOPG_URL = f'postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_ENDPOINT}/'
POSTGRES_JDBC_URL = f'jdbc:postgresql://{POSTGRES_ENDPOINT}/'

#%%
conf = (SparkConf()
        .set('spark.sql.caseSensitive', 'true')
        .set('spark.sql.execution.arrow.pyspark.fallback.enabled', 'true')
        .set('spark.sql.execution.arrow.pyspark.enabled', 'true')
        .set('spark.sql.execution.arrow.pyspark.datetime64.enabled', 'true'))
spark = SparkSession.builder.appName("VerizonDriftMonitoring").config(conf=conf).getOrCreate()

#%%
application_id = sys.argv[1]
metric_id = sys.argv[2]

#%%
metadata_engine = create_engine(POSTGRES_PSYCOPG_URL + 'postgres')
metadata_table = f'metadata.application_{application_id}'
metric_metadata = pd.read_sql(
    sql=f'SELECT * FROM {metadata_table}',
    con=metadata_engine,
    index_col='metric_id'
).loc[metric_id]

#%%
dataloader_dict = {
    'jdbc': JDBCDataLoader(
        spark=spark,
        url=POSTGRES_JDBC_URL + 'postgres',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        schema_name="datasets",
        driver='org.postgresql.Driver'
    ),
    'bigquery': BigQueryDataLoader(
        spark=spark,
        project='healthy-earth-389717',
        dataset_name="datasets",
        temp_bucket='verizon-drift-monitoring'
    )
}

source_data = {
    key: dataloader_dict['bigquery'].from_table(table_name).load()
    for key, table_name in metric_metadata.source_data.items()
}


#%%
def upsert_partition(partition, metadata):
    engine = create_engine(POSTGRES_PSYCOPG_URL + f"application_{application_id}")
    table = Table(metadata["table_name"], MetaData(), autoload_with=engine)
    with engine.begin() as connection:
        for row in partition:
            connection.execute(insert(table).values(row.asDict()).on_conflict_do_update(
                index_elements=["timestamp"],
                set_=row.asDict()
            ))


#%%
metric = MetricRegistry.get(metric_metadata.metric_type)()
df_dict = metric.transform_metric(
    source_data=source_data,
    spark=spark
)
for panel_name, metadata in metric.charts_metadata.items():
    df_dict[panel_name].foreachPartition(partial(upsert_partition, metadata=metadata))
