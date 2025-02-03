from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType, IntegerType, DateType
schema = StructType([
    StructField('dv_hashkey_bol', StringType(), True),
    StructField('dv_recsrc', StringType(), True),
    StructField('dv_source_version', StringType(), True),
    StructField('dv_loaddts', TimestampType(), True),
    StructField('jurisdiction', StringType(), True),
    StructField('bol', StringType(), True),
    StructField('hs_code', StringType(), True),
    StructField('teu_number', StringType(), True),
    StructField('invoice_value', FloatType(), True),
    StructField('value_usd', FloatType(), True),
    StructField('exchange_rate', FloatType(), True),
    StructField('description', StringType(), True),
    StructField('actual_arrival_date', DateType(), True),
    StructField('estimated_arrival_date', DateType(), True),
    StructField('vessel_name', StringType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('quantity_unit', StringType(), True),
    StructField('weight', FloatType(), True),
    StructField('weight_unit', StringType(), True),
    StructField('pure_seller', StringType(), True),
    StructField('seller_country', StringType(), True),
    StructField('pure_buyer', StringType(), True),
    StructField('buyer_country', StringType(), True),
    StructField('dv_hashdiff', StringType(), True)
])