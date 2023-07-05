#%%
from src.metrics import MetricRegistry

import dotenv
dotenv.load_dotenv()

import os
import sys
import yaml
import pandas as pd
from sqlalchemy import create_engine, Column
from sqlalchemy import String, JSON
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import declarative_base
from sqlalchemy_utils import database_exists, create_database

#%%
POSTGRES_ENDPOINT = os.environ["POSTGRES_EXTERNAL_ENDPOINT"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_PSYCOPG_URL = f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_ENDPOINT}/"

#%%
application_config = yaml.safe_load(open(sys.argv[1]))
application_id = application_config["application_metadata"]["id"]
application_name = application_config["application_metadata"]["name"]

#%%
metadata_engine = create_engine(POSTGRES_PSYCOPG_URL + "postgres")

with metadata_engine.connect() as connection:
    if not connection.dialect.has_schema(connection, "metadata"):
        connection.execute(CreateSchema("metadata"))
        connection.commit()

#%%
application_engine = create_engine(POSTGRES_PSYCOPG_URL + f"application_{application_id}")

if not database_exists(application_engine.url):
    create_database(application_engine.url)

#%%
Base = declarative_base()

class MetaData(Base):
    __tablename__ = f"application_{application_id}"
    __table_args__ = dict(schema="metadata")

    metric_id = Column(String(length=128), primary_key=True)
    metric_type = Column(String(length=128))
    source_data = Column(JSON)

Base.metadata.create_all(metadata_engine)

#%%
Base = declarative_base()
metadata = pd.DataFrame(columns=["metric_id", "metric_type", "source_data"]).set_index("metric_id")

for dashboard_config in application_config["dashboards"]:
    dashboard_name = dashboard_config["name"]
    for metric_config in dashboard_config["metrics"]:
        metric_id = metric_config["id"]
        metric_type = metric_config["type"]
        metric_source_data = metric_config["source_data"]
        metric_layout = metric_config["layout"]
        metric = MetricRegistry.get(metric_type)()
        metric.get_table_definitions(Base)
        metadata.loc[metric_id] = pd.Series(dict(metric_type=metric_type, source_data=metric_source_data))

metadata.to_sql(f"application_{application_id}", metadata_engine,
                schema="metadata",
                if_exists="replace",
                index=True,
                dtype={"source_data": JSON})

Base.metadata.create_all(application_engine)
