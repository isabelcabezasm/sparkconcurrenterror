"""Sample for concurrent error."""
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
# from pyspark.sql import DataFrame
from delta import DeltaTable


def main_sample(source_path_sample: str,
                target_path: str) -> DeltaTable:
    """Description:

    Parameters:

    Return:
    :DeltaTable: the saved DeltaTable is returned by this method.
    """
    spark = SparkSession.builder \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                        .getOrCreate()

    # define schema
    schema = StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
    ])

    # read with schema
    data_frame_sample = (spark.read.format("JSON")
                         .schema(schema)
                         .option("multiline", True)
                         .load(source_path_sample)
                         ).cache()

    # Create one delta table with two partitions

    partition_by = ["country", "city"]

    delta_table = (DeltaTable.createIfNotExists()
                   .addColumns(schema)
                   .partitionedBy(partition_by)
                   .location(target_path)
                   .execute())

    # merge in delta table
    target_table_alias = "target"
    source_table_alias = "source"

    country = data_frame_sample.first()["country"]

    conditions = (f"{target_table_alias}.user_id = {source_table_alias}.user_id and \
                {target_table_alias}.city = {source_table_alias}.city and \
                {target_table_alias}.country = '{country}'")

    delta_merge_builder = (delta_table
                           .alias(target_table_alias)
                           .merge(
                               data_frame_sample.alias(source_table_alias),
                               condition=conditions
                           )
                           .whenMatchedUpdateAll()
                           .whenNotMatchedInsertAll()
                           .whenNotMatchedBySourceDelete()
                           )

    delta_merge_builder.execute()

    return delta_table
