from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DateType
schema = StructType([
    StructField('dv_hashkey_l_bol', StringType(), True),
    StructField('dv_recsrc', StringType(), True),
    StructField('dv_loaddts', TimestampType(), True),
    StructField('jurisdiction', StringType(), True),
    StructField('dv_source_version', StringType(), True),
    StructField('dv_hashkey_bol', StringType(), True),
    StructField('buyer_dv_hashkey_company', StringType(), True),
    StructField('supplier_dv_hashkey_company', StringType(), True),
    StructField('shipper_dv_hashkey_company', StringType(), True),
    StructField('import_port', StringType(), True),
    StructField('export_port', StringType(), True),
    StructField('bol', StringType(), True),
    StructField('actual_arrival_date', DateType(), True)
])