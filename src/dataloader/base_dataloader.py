from abc import ABC, abstractmethod

from typing import Optional

from pyspark.sql import SparkSession, DataFrameReader


class BaseDataLoader(ABC):

    def __init__(self, spark: SparkSession, **kwargs):
        self._spark = spark

    @abstractmethod
    def configure_spark(self, spark: SparkSession) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def connection_format(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def connection_options(self, table_name: str) -> Optional[dict]:
        raise NotImplementedError

    def from_table(self, table_name: str) -> DataFrameReader:
        self.configure_spark(self._spark)
        loader = self._spark.read.format(self.connection_format)
        if conn_opt := self.connection_options(table_name):
            loader = loader.options(**conn_opt)
        return loader

