#%%
from metrics import MetricRegistry

import dotenv
dotenv.load_dotenv()

import os
import sys
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy_utils import database_exists, create_database

#%%
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_USERNAME = "postgres"
POSTGRES_URL = f"postgresql+psycopg://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@localhost/"

#%%
application_config = yaml.safe_load(open(sys.argv[1]))
application_id = application_config["application_metadata"]["id"]
application_name = application_config["application_metadata"]["name"]

engine = create_engine(POSTGRES_URL + f"application_{application_id}")

#%%
if not database_exists(engine.url):
    create_database(engine.url)

#%%
Base = declarative_base()

for dashboard_config in application_config["dashboards"]:
    dashboard_name = dashboard_config["name"]
    for metric_config in dashboard_config["metrics"]:
        metric_name = metric_config["name"]
        metric_layout = metric_config["layout"]
        metric = MetricRegistry.get(metric_name)()
        metric.get_table_definitions(Base)

Base.metadata.create_all(engine)
