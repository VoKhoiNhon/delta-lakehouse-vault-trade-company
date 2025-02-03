from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField('Entity Type', StringType(), True),
    StructField('EntityName', StringType(), True),
    StructField('Name Status', StringType(), True),
    StructField('SL.', StringType(), True)
])