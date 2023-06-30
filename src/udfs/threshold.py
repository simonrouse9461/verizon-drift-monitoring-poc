from pyspark.sql.functions import udf, pandas_udf, monotonically_increasing_id, lit, array, PandasUDFType
from pyspark.sql.types import IntegerType, TimestampType, FloatType, StructType, StructField, ArrayType, BooleanType


@udf(StructType([
    StructField(f"_{i}", FloatType())
    for i in range(100)
]))
def unpack_thresholds(thresholds: list, values: list) -> tuple:
    result = [None] * 100
    for t, v in zip(thresholds, values):
        result[round(t * 100)] = v
    return tuple(result)


@udf(StructType([
    StructField(f"_{i}", FloatType())
    for i in range(100)
]))
def threshold_value_baseline_diff(baseline: list, *values: list) -> tuple:
    return tuple(v - b for v, b in zip(values, baseline))
