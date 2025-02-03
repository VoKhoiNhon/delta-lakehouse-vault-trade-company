from pyspark.sql.types import StructType, StructField, TimestampType, StringType
schema = StructType([
    StructField('dv_hashkey_person', StringType(), True),
    StructField('dv_recsrc', StringType(), True),
    StructField('dv_loaddts', TimestampType(), True),
    StructField('dv_source_version', StringType(), True),
    StructField('jurisdiction', StringType(), True),
    StructField('name', StringType(), True),
    StructField('full_address', StringType(), True)
])