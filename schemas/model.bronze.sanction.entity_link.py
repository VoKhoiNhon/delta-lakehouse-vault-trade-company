from pyspark.sql.types import StructType, StructField, ArrayType, DateType, StringType
schema = StructType([
    StructField('datasets', ArrayType(), True),
    StructField('first_seen', StringType(), True),
    StructField('id', StringType(), True),
    StructField('last_change', StringType(), True),
    StructField('last_seen', StringType(), True),
    StructField('referents', ArrayType(), True),
    StructField('date', ArrayType(), True),
    StructField('raw_end_date', StringType(), True),
    StructField('modified_at', ArrayType(), True),
    StructField('from_entity_id', StringType(), True),
    StructField('relationship', ArrayType(), True),
    StructField('to_entity_id', StringType(), True),
    StructField('source_url', ArrayType(), True),
    StructField('raw_start_date', StringType(), True),
    StructField('load_date', StringType(), True),
    StructField('start_date', DateType(), True),
    StructField('end_date', DateType(), True)
])