from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType
schema = StructType([
    StructField('dv_hashkey_company', StringType(), True),
    StructField('dv_recsrc', StringType(), True),
    StructField('dv_loaddts', TimestampType(), True),
    StructField('dv_source_version', StringType(), True),
    StructField('jurisdiction', StringType(), True),
    StructField('registration_number', StringType(), True),
    StructField('name', StringType(), True),
    StructField('pure_name', StringType(), True),
    StructField('has_regnum', IntegerType(), True)
])