import pandas as pd
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("lookup_jurisdiction")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.createDataFrame(
    pd.read_csv("resources/lookup_jurisdiction_with_continents.csv", na_filter=False)
).write.format("delta").save("s3a://lakehouse-silver/lookup_jurisdiction")
