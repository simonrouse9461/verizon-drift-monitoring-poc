from .base_metric import *
from .metric_registry import *

import pandas as pd
import numpy as np
import torch
from torchmetrics import Accuracy
from sqlalchemy import Column, DateTime, Float
from sqlalchemy.orm import DeclarativeBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, lit, to_timestamp, PandasUDFType
from pyspark.sql.types import FloatType


@MetricRegistry.register("PSI")
class PSIMetric(BaseMetric):

    @property
    def charts_metadata(self) -> dict[str, dict]:
        return {
            "psi": {
                "table_name": "psi_table"
            }
        }

    def get_table_definitions(self, base: DeclarativeBase) -> dict[str, type]:
        class PSITable(base):
            __tablename__ = self.charts_metadata["psi"]["table_name"]

            timestamp = Column(DateTime, primary_key=True)
            value = Column(Float)

        return {
            "psi": PSITable
        }

    def get_grafana_panel_json(self, application_id: str, layout: dict[str, dict]) -> dict[str, dict]:
        psi = {
            "datasource": {
                "type": "postgres",
                "uid": application_id
            },
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "fixedColor": "yellow",
                        "mode": "fixed",
                        "seriesBy": "last"
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
                            "mode": "area"
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
                                "value": 0.2
                            }
                        ]
                    }
                },
                "overrides": []
            },
            "gridPos": layout["psi"],
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
                    "rawSql": """SELECT * FROM psi_table ORDER BY "timestamp" DESC;""",
                    "refId": "A",
                    "table": "psi_table"
                }
            ],
            "title": "PSI",
            "type": "timeseries"
        }

        return {
            "psi": psi
        }

    def transform_metric(self,
                         baseline_data: DataFrame,
                         realtime_data: DataFrame,
                         spark: SparkSession) -> dict[str, DataFrame]:
        baseline_score = spark.sparkContext.broadcast(baseline_data.select("score").toPandas().to_numpy().flatten())
        @pandas_udf(FloatType(), PandasUDFType.GROUPED_AGG)
        def psi(score: pd.Series) -> float:
            return self.calculate_psi(baseline_score.value, np.array(score))

        agg_expr = psi("score").alias("psi")
        baseline_psi = baseline_data.agg(agg_expr).toPandas().iloc[0]
        realtime_psi = realtime_data.groupby("timestamp").agg(agg_expr)

        return {
            "psi": realtime_psi.select(
                to_timestamp("timestamp").alias("timestamp"),
                realtime_psi["psi"].alias("value")
            )
        }

    @staticmethod
    def calculate_psi(expected, actual, buckettype='bins', buckets=10, axis=0):
        def psi(expected_array, actual_array, buckets):
            def scale_range (input, min, max):
                input += -(np.min(input))
                input /= np.max(input) / (max - min)
                input += min
                return input

            breakpoints = np.arange(0, buckets + 1) / (buckets) * 100

            if buckettype == 'bins':
                breakpoints = scale_range(breakpoints, np.min(expected_array), np.max(expected_array))
            elif buckettype == 'quantiles':
                breakpoints = np.stack([np.percentile(expected_array, b) for b in breakpoints])

            expected_percents = np.histogram(expected_array, breakpoints)[0] / len(expected_array)
            actual_percents = np.histogram(actual_array, breakpoints)[0] / len(actual_array)

            def sub_psi(e_perc, a_perc):
                if a_perc == 0:
                    a_perc = 0.0001
                if e_perc == 0:
                    e_perc = 0.0001

                value = (e_perc - a_perc) * np.log(e_perc / a_perc)
                return value

            psi_value = np.sum(sub_psi(expected_percents[i], actual_percents[i]) for i in range(0, len(expected_percents)))

            return psi_value

        if len(expected.shape) == 1:
            psi_values = np.empty(len(expected.shape))
        else:
            psi_values = np.empty(expected.shape[axis])

        for i in range(0, len(psi_values)):
            if len(psi_values) == 1:
                psi_values = psi(expected, actual, buckets)
            elif axis == 0:
                psi_values[i] = psi(expected[:,i], actual[:,i], buckets)
            elif axis == 1:
                psi_values[i] = psi(expected[i,:], actual[i,:], buckets)

        return psi_values
