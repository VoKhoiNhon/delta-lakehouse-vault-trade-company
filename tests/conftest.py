import logging
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def logger():
    return logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark():
    print("init spark fixture")
    spark = (
        SparkSession.builder.appName("pytest")
        # .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.1")
        # .config("spark.jars", "/opt/workspace/jars/deequ-2.0.3-spark-3.3.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    return spark
    # yield spark
    # spark.sparkContext.stop()
