from . import BaseDataLoader

from typing import Optional

from pyspark.sql import SparkSession


class JDBCDataLoader(BaseDataLoader):
    def __init__(self, spark: SparkSession, *,
                 url: str,
                 user: str,
                 password: str,
                 schema_name: Optional[str] = None,
                 driver: str = 'org.postgresql.Driver'):
        super().__init__(spark)
        self.url = url
        self.user = user
        self.password = password
        self.schema_name = schema_name
        self.driver = driver

    def configure_spark(self, spark: SparkSession) -> None:
        pass

    @property
    def connection_format(self) -> str:
        return 'jdbc'

    def connection_options(self, table_name: str) -> Optional[dict]:
        return {
            'url': self.url,
            'dbtable': f'{self.schema_name}.{table_name}' if self.schema_name is not None else table_name,
            'user': self.user,
            'password': self.password,
            'driver': self.driver
        }
