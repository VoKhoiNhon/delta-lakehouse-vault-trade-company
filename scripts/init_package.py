from pyspark.sql import SparkSession

SparkSession.builder.appName("init-packages").config(
    "spark.jars.packages",
    "io.delta:delta-spark_2.12:3.1.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.2,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.1,com.crealytics:spark-excel_2.12:0.14.0",
).getOrCreate()
