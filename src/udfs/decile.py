from pyspark.sql.functions import udf, pandas_udf, monotonically_increasing_id, lit, array, PandasUDFType
from pyspark.sql.types import IntegerType, TimestampType, FloatType, StructType, StructField, ArrayType, BooleanType


@udf(StructType([
    StructField(f"decile_{i}", FloatType())
    for i in range(1, 11)
]))
def unpack_deciles(deciles: list, values: list) -> tuple:
    result = [None] * 10
    for d, v in zip(deciles, values):
        result[round(d * 10) - 1] = v
    return tuple(result)


@udf(StructType([
    StructField(f"decile_{i}", FloatType())
    for i in range(1, 11)
]))
def decile_value_baseline_diff(baseline: list, *values: list) -> tuple:
    return tuple(v - b for v, b in zip(values, baseline))