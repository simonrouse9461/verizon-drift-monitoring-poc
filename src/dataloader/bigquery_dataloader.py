from . import BaseDataLoader

from typing import Optional

from pyspark.sql import SparkSession


class BigQueryDataLoader(BaseDataLoader):
    def __init__(self, spark: SparkSession, *,
                 project: str,
                 temp_bucket: str,
                 dataset_name: str,
                 service_account_project: Optional[str] = None):
        super().__init__(spark)
        if service_account_project is None:
            service_account_project = project
        self.service_account_project = service_account_project
        self.temp_bucket = temp_bucket
        self.project = project
        self.dataset_name = dataset_name

    def configure_spark(self, spark: SparkSession) -> None:
        spark.conf.set('parentProject', self.service_account_project)
        spark.conf.set('temporaryGcsBucket', self.temp_bucket)

    @property
    def connection_format(self) -> str:
        return 'bigquery'

    def connection_options(self, table_name: str) -> Optional[dict]:
        return {
            'table': f'{self.project}:{self.dataset_name}.{table_name}'
        }

