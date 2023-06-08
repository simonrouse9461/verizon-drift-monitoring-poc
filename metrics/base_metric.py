from abc import ABC, abstractmethod
from sqlalchemy.orm import DeclarativeBase
from pyspark.sql import DataFrame, SparkSession


class BaseMetric(ABC):

    @property
    @abstractmethod
    def charts_metadata(self) -> dict[str, dict]:
        raise NotImplementedError

    @abstractmethod
    def get_table_definitions(self, base: DeclarativeBase) -> dict[str, type]:
        raise NotImplementedError

    @abstractmethod
    def get_grafana_panel_json(self, application_id: str, layout: dict[str, dict]) -> dict[str, dict]:
        raise NotImplementedError

    @abstractmethod
    def transform_metric(self,
                         baseline_data: DataFrame,
                         realtime_data: DataFrame,
                         spark: SparkSession) -> dict[str, DataFrame]:
        raise NotImplementedError
