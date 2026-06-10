from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from pyspark.sql.dataframe import DataFrame



SOURCE_PATH = "s3://ecommerce-dtb-dp1/data-store/products"


@dp.table(
    name="ecommerce.bronze.products",
    comment="Streaming ingestion of raw products data with Auto Loader",
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

def products_bronze() -> DataFrame:
    df = (
        spark.readStream.format("cloudFiles") 
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ";")
        .option("cloudFiles.schemaHints", "Product_ID STRING, Category STRING, Sub_Category STRING, Product_Name STRING")
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
