from .base_metric import *
from .metric_registry import *
from ..udfs import unpack_deciles, decile_value_baseline_diff

import pandas as pd
import numpy as np
import torch
from sqlalchemy import Column, DateTime, Float
from sqlalchemy.orm import DeclarativeBase
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, pandas_udf, lit, to_timestamp, PandasUDFType
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField


@MetricRegistry.register("Lift")
class LiftMetric(BaseMetric):

    @property
    def charts_metadata(self) -> dict[str, dict]:
        return {
            "lift": {
                "table_name": "lift_table"
            },
            "diff": {
                "table_name": "lift_diff_table"
            }
        }

    def get_table_definitions(self, base: DeclarativeBase) -> dict[str, type]:
        class LiftTable(base):
            __tablename__ = 'lift_table'

            timestamp = Column(DateTime, primary_key=True)

        for decile in range(1, 11):
            setattr(LiftTable, f"decile_{decile}", Column(Float))

        class LiftDiffTable(base):
            __tablename__ = 'lift_diff_table'

            timestamp = Column(DateTime, primary_key=True)

        for decile in range(1, 11):
            setattr(LiftDiffTable, f"decile_{decile}", Column(Float))

        return {
            "lift": LiftTable,
            "diff": LiftDiffTable
        }

    def get_grafana_panel_json(self, application_id: str, layout: dict[str, dict]) -> dict[str, dict]:
        lift = {
            "datasource": {
                "type": "postgres",
                "uid": application_id
            },
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "hideFrom": {
                            "legend": False,
                            "tooltip": False,
                            "viz": False
                        },
                        "scaleDistribution": {
                            "type": "linear"
                        }
                    }
                },
                "overrides": []
            },
            "gridPos": layout["lift"],
            "interval": "1h",
            "maxDataPoints": 10000,
            "options": {
                "calculate": False,
                "cellGap": 1,
                "color": {
                    "exponent": 0.5,
                    "fill": "dark-red",
                    "min": 1,
                    "mode": "scheme",
                    "reverse": False,
                    "scale": "exponential",
                    "scheme": "Inferno",
                    "steps": 64
                },
                "exemplars": {
                    "color": "rgba(255,0,255,0.7)"
                },
                "filterValues": {
                    "le": 1e-9
                },
                "legend": {
                    "show": False
                },
                "rowsFrame": {
                    "layout": "auto"
                },
                "tooltip": {
                    "show": True,
                    "yHistogram": False
                },
                "yAxis": {
                    "axisPlacement": "left",
                    "decimals": 0,
                    "reverse": False
                }
            },
            "targets": [
                {
                    "datasource": {
                        "type": "postgres"
                    },
                    "editorMode": "code",
                    "format": "table",
                    "rawQuery": True,
                    "rawSql": """SELECT * FROM lift_table ORDER BY "timestamp" DESC;""",
                    "refId": "A",
                    "table": "lift_table"
                }
            ],
            "title": "Lift",
            "transformations": [],
            "type": "heatmap"
        }

        diff = {
            "datasource": {
                "type": "postgres",
                "uid": application_id
            },
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "hideFrom": {
                            "legend": False,
                            "tooltip": False,
                            "viz": False
                        },
                        "scaleDistribution": {
                            "type": "linear"
                        }
                    }
                },
                "overrides": []
            },
            "gridPos": layout["diff"],
            "interval": "1h",
            "maxDataPoints": 10000,
            "options": {
                "calculate": False,
                "cellGap": 1,
                "color": {
                    "exponent": 0.5,
                    "fill": "dark-red",
                    "max": 0,
                    "mode": "scheme",
                    "reverse": True,
                    "scale": "exponential",
                    "scheme": "RdYlGn",
                    "steps": 10
                },
                "exemplars": {
                    "color": "rgba(255,0,255,0.7)"
                },
                "filterValues": {
                    "ge": 1,
                    "le": -1
                },
                "legend": {
                    "show": False
                },
                "rowsFrame": {
                    "layout": "auto"
                },
                "tooltip": {
                    "show": True,
                    "yHistogram": False
                },
                "yAxis": {
                    "axisPlacement": "left",
                    "decimals": 0,
                    "reverse": False
                }
            },
            "targets": [
                {
                    "datasource": {
                        "type": "postgres"
                    },
                    "editorMode": "code",
                    "format": "table",
                    "rawQuery": True,
                    "rawSql": """SELECT * FROM lift_diff_table ORDER BY "timestamp" DESC;""",
                    "refId": "A",
                    "table": "lift_diff_table"
                }
            ],
            "title": "Lift Drift",
            "transformations": [],
            "type": "heatmap"
        }

        return {
            "lift": lift,
            "diff": diff
        }

    def transform_metric(self, source_data: dict[str, DataFrame], spark: SparkSession) -> dict[str, DataFrame]:
        @pandas_udf(ArrayType(ArrayType(FloatType())), PandasUDFType.GROUPED_AGG)
        def liftcurve(score: pd.Series, label: pd.Series) -> list[float]:
            lift = self.lift_curve(y_val=torch.tensor(label), y_pred=torch.tensor(score), step=0.1)
            return [c.tolist() for c in lift]

        @udf(StructType([
            StructField("decile", ArrayType(FloatType())),
            StructField("lift", ArrayType(FloatType())),
        ]))
        def liftcurve_struct(roc: list) -> tuple:
            return tuple(roc)

        baseline_lift = (source_data["baseline"]
                         .groupby("timestamp")
                         .agg(liftcurve("score", "label").alias("liftcurve"))
                         .withColumn("liftcurve_struct", liftcurve_struct("liftcurve"))
                         .select("*", "liftcurve_struct.*")
                         .drop("liftcurve", "liftcurve_struct")
                         .toPandas().iloc[0])
        realtime_lift = (source_data["realtime"]
                         .groupby("timestamp")
                         .agg(liftcurve("score", "label").alias("liftcurve"))
                         .withColumn("liftcurve_struct", liftcurve_struct("liftcurve"))
                         .select("*", "liftcurve_struct.*")
                         .drop("liftcurve", "liftcurve_struct"))

        baseline_values = list(unpack_deciles.func(baseline_lift["decile"], baseline_lift["lift"]))

        lift_column = unpack_deciles("decile", "lift").alias("struct")
        diff_column = decile_value_baseline_diff(
            lit(baseline_values),
            *[f"decile_{i}" for i in range(1, 11)]
        ).alias("struct")

        lift = (realtime_lift
                .select(to_timestamp("timestamp").alias("timestamp"), lift_column)
                .select("timestamp", "struct.*")).cache()
        diff = (lift
                .select(to_timestamp("timestamp").alias("timestamp"), diff_column)
                .select("timestamp", "struct.*"))

        return {
            "lift": lift,
            "diff": diff
        }

    @staticmethod
    def lift_curve(y_val, y_pred, step=0.01):
        # Define an auxiliar dataframe to plot the curve
        aux_lift = pd.DataFrame()
        # Create a real and predicted column for our new DataFrame and assign values
        aux_lift['real'] = y_val
        aux_lift['predicted'] = y_pred
        # Order the values for the predicted probability column:
        aux_lift.sort_values('predicted',ascending=False,inplace=True)

        # Create the values that will go into the X axis of our plot
        x_val = np.arange(step,1+step,step)
        # Calculate the ratio of ones in our data
        ratio_ones = aux_lift['real'].sum() / len(aux_lift)
        # Create an empty vector with the values that will go on the Y axis our our plot
        y_v = []

        # Calculate for each x value its correspondent y value
        for x in x_val:
            # The ceil function returns the closest integer bigger than our number
            num_data = int(np.ceil(x*len(aux_lift)))
            data_here = aux_lift.iloc[:num_data,:]  # ie. np.ceil(1.4) = 2
            ratio_ones_here = data_here['real'].sum()/len(data_here)
            y_v.append(ratio_ones_here / ratio_ones)

        return x_val, np.array(y_v)
