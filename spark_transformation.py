#%%
from metrics import MetricRegistry

import dotenv
dotenv.load_dotenv()

import os
import sys
import yaml

from pyspark.sql import SparkSession

#%%
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

#%%
spark = (SparkSession.builder
         .master("local[4]")
         .config(key="spark.sql.caseSensitive", value=True)
         .config(key="spark.sql.execution.arrow.pyspark.fallback.enabled", value=True)
         .config(key="spark.sql.execution.arrow.pyspark.enabled", value=True)
         .config(key="spark.sql.execution.arrow.pyspark.datetime64.enabled", value=True)
         .config(key="spark.jars", value="./jar/postgresql-42.6.0.jar")
         .getOrCreate())

#%%
baseline_data = spark.read.json("./datasets/baseline_data_predicted_with_timestamp.jsonl")
realtime_data = spark.read.json("./datasets/realtime_data_predicted_with_timestamp.jsonl")


#%%
def get_db_options(database_name, table_name):
    return dict(
        url=f"jdbc:postgresql://localhost:5432/{database_name}",
        dbtable=table_name,
        user="postgres",
        password=POSTGRES_PASSWORD,
        driver="org.postgresql.Driver"
    )


#%%
application_config = yaml.safe_load(open(sys.argv[1]))
application_id = application_config["application_metadata"]["id"]
application_name = application_config["application_metadata"]["name"]

#%%
for dashboard_config in application_config["dashboards"]:
    dashboard_name = dashboard_config["name"]
    for metric_config in dashboard_config["metrics"]:
        metric_name = metric_config["name"]
        metric_layout = metric_config["layout"]
        metric = MetricRegistry.get(metric_name)()
        df_dict = metric.transform_metric(
            baseline_data=baseline_data,
            realtime_data=realtime_data,
            spark=spark
        )
        for panel_name, metadata in metric.charts_metadata.items():
            (df_dict[panel_name]
             .write.format('jdbc')
             .options(**get_db_options(
                database_name=f"application_{application_id}",
                table_name=metadata["table_name"]
             ))
             .mode("append")
             .save())
