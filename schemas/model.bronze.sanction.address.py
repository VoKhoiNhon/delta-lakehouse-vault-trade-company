from pyspark.sql.types import StructType, StructField, ArrayType, StringType
schema = StructType([
    StructField('full_address', StringType(), True),
    StructField('datasets', ArrayType(), True),
    StructField('first_seen', StringType(), True),
    StructField('id', StringType(), True),
    StructField('last_change', StringType(), True),
    StructField('last_seen', StringType(), True),
    StructField('referents', ArrayType(), True),
    StructField('city', StringType(), True),
    StructField('country_code', StringType(), True),
    StructField('full', ArrayType(), True),
    StructField('post_office_box', ArrayType(), True),
    StructField('postal_code', StringType(), True),
    StructField('region', StringType(), True),
    StructField('remarks', ArrayType(), True),
    StructField('state', StringType(), True),
    StructField('street', StringType(), True),
    StructField('summary', ArrayType(), True),
    StructField('load_date', StringType(), True)
])