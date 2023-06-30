#%%
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

#%%
with DAG(
    "execute_spark_transformation",
    start_date=datetime.now(),
    description='A simple tutorial DAG',
    params={
        "application_id": "3",
        "metric_id": "model_acc"
    }
) as dag:
    task1 = BashOperator(
        task_id="ETL",
        bash_command="""
        cd /Users/chuhan/Workspaces/verizon-drift-monitoring-poc
        /opt/homebrew/anaconda3/bin/python spark_transformation.py {{ params.application_id }} {{ params.metric_id }}
        """,
    )

#%%
if __name__ == "__main__":
    dag.test()
