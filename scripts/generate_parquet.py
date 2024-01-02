import delta
import delta.pip_utils
import delta.tables
import pyspark
import pyarrow as pa
import polars as pl
import pyarrow.parquet as pq
import pyspark.pandas as ps


def get_spark_ts_64():
    builder = (
        pyspark.sql.SparkSession.builder.appName("lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.parquet.outputTimestampType",
            "TIMESTAMP_MICROS",
        )
    )
    return delta.pip_utils.configure_spark_with_delta_pip(builder).getOrCreate()


int_array = pa.array([1, 2, 3])
float_array = pa.array([1.0, None, 3.0])
struct_array = pa.array([None, {"a": 1, "b": [2, 3]}, {"a": 3, "b": [4], "c": 5}])
list_struct_array = pa.array(
    [None, [{"a": 1, "b": 2}, {"a": 3, "b": 4}], [{"a": 5}, {"a": 6, "b": 7}]]
)
table = pa.Table.from_arrays(
    [int_array, float_array, struct_array, list_struct_array],
    ["int", "float", "struct", "list_struct"],
)

pq.write_table(table, "pyarrow.parquet")

table_pl = pl.from_arrow(table)
table_pl.write_parquet("polars.parquet")

table_pd = table_pl.to_pandas()
table_pd.to_parquet("pandas.parquet")

spark = get_spark_ts_64()
spark.read.parquet("pyarrow.parquet").write.format("parquet").save("spark")
