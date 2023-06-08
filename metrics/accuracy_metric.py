from .base_metric import *
from .metric_registry import *

import pandas as pd
import torch
from torchmetrics import Accuracy
from sqlalchemy import Column, DateTime, Float
from sqlalchemy.orm import DeclarativeBase
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf, lit, to_timestamp, PandasUDFType
from pyspark.sql.types import FloatType


@MetricRegistry.register("Accuracy")
class AccuracyMetric(BaseMetric):

    @property
    def charts_metadata(self) -> dict[str, dict]:
        return {
            "accuracy": {
                "table_name": "acc_table"
            }
        }

    def get_table_definitions(self, base: DeclarativeBase) -> dict[str, type]:
        class AccuracyTable(base):
            __tablename__ = self.charts_metadata["accuracy"]["table_name"]

            timestamp = Column(DateTime, primary_key=True)
            baseline = Column(Float)
            realtime = Column(Float)

        return {
            "accuracy": AccuracyTable
        }

    def get_grafana_panel_json(self, application_id: str, layout: dict[str, dict]) -> dict[str, dict]:
        accuracy = {
            "datasource": {
                "type": "postgres",
                "uid": application_id
            },
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "palette-classic"
                    },
                    "custom": {
                        "axisCenteredZero": False,
                        "axisColorMode": "text",
                        "axisLabel": "",
                        "axisPlacement": "auto",
                        "barAlignment": 0,
                        "drawStyle": "line",
                        "fillOpacity": 0,
                        "gradientMode": "none",
                        "hideFrom": {
                            "legend": False,
                            "tooltip": False,
                            "viz": False
                        },
                        "lineInterpolation": "linear",
                        "lineWidth": 1,
                        "pointSize": 5,
                        "scaleDistribution": {
                            "type": "linear"
                        },
                        "showPoints": "auto",
                        "spanNulls": False,
                        "stacking": {
                            "group": "A",
                            "mode": "none"
                        },
                        "thresholdsStyle": {
                            "mode": "off"
                        }
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {
                                "color": "green",
                                "value": None
                            },
                            {
                                "color": "red",
                                "value": 80
                            }
                        ]
                    }
                },
                "overrides": []
            },
            "gridPos": layout["accuracy"],
            "options": {
                "legend": {
                    "calcs": [],
                    "displayMode": "list",
                    "placement": "bottom",
                    "showLegend": True
                },
                "tooltip": {
                    "mode": "single",
                    "sort": "none"
                }
            },
            "targets": [
                {
                    "datasource": {
                        "type": "postgres"
                    },
                    "editorMode": "code",
                    "format": "table",
                    "rawSql": """SELECT * FROM acc_table ORDER BY "timestamp" DESC;""",
                    "refId": "A",
                    "table": "acc_table"
                }
            ],
            "title": "Accuracy",
            "type": "timeseries"
        }

        return {
            "accuracy": accuracy
        }

    def transform_metric(self,
                         baseline_data: DataFrame,
                         realtime_data: DataFrame,
                         spark: SparkSession) -> dict[str, DataFrame]:
        @pandas_udf(FloatType(), PandasUDFType.GROUPED_AGG)
        def accuracy(score: pd.Series, label: pd.Series) -> float:
            return Accuracy(task="binary")(preds=torch.tensor(score), target=torch.tensor(label)).item()

        agg_expr = accuracy("score", "label").alias("accuracy")
        baseline_acc = baseline_data.agg(agg_expr).toPandas().iloc[0]
        realtime_acc = realtime_data.groupby("timestamp").agg(agg_expr)

        return {
            "accuracy": realtime_acc.select(
                to_timestamp("timestamp").alias("timestamp"),
                lit(baseline_acc["accuracy"]).alias("baseline"),
                realtime_acc["accuracy"].alias("realtime")
            )
        }
