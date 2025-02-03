from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, IntegerType, FloatType
schema = StructType([
    StructField('dv_hashkey_company', StringType(), True),
    StructField('dv_recsrc', StringType(), True),
    StructField('dv_loaddts', TimestampType(), True),
    StructField('dv_hashdiff', StringType(), True),
    StructField('dv_source_version', StringType(), True),
    StructField('jurisdiction', StringType(), True),
    StructField('registration_number', StringType(), True),
    StructField('name', StringType(), True),
    StructField('pure_name', StringType(), True),
    StructField('full_address', StringType(), True),
    StructField('street', StringType(), True),
    StructField('city', StringType(), True),
    StructField('region', StringType(), True),
    StructField('state', StringType(), True),
    StructField('province', StringType(), True),
    StructField('country_code', StringType(), True),
    StructField('country_name', StringType(), True),
    StructField('postal_code', StringType(), True),
    StructField('latitude', FloatType(), True),
    StructField('longitude', FloatType(), True),
    StructField('type', IntegerType(), True),
    StructField('start_date', DateType(), True),
    StructField('end_date', DateType(), True)
])