from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField('entity_type', StringType(), True),
    StructField('name', StringType(), True),
    StructField('name_status', StringType(), True),
    StructField('sl_', StringType(), True),
    StructField('load_date', StringType(), True),
    StructField('pure_name', StringType(), True)
])