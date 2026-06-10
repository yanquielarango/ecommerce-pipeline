from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from pyspark.sql.dataframe import DataFrame



SOURCE_PATH = "s3://ecommerce-dtb-dp1/data-store/locations"


@dp.table(
    name="ecommerce.bronze.locations",
    comment="Streaming ingestion of raw locations data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)

def locations_bronze() -> DataFrame:
    df = (
        spark.readStream.format("cloudFiles") 
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ";")
        .option("cloudFiles.schemaHints", "Postal_Code STRING, City STRING, State STRING, Region STRING, Country STRING")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue") 
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
        .select(
            "*",
            F.current_timestamp().alias("ingest_datetime"),
            F.col("_metadata.file_path").alias("file_name")
        )
    )
    return df
