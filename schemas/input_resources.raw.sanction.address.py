from pyspark.sql.types import StructType, StructField, BooleanType, ArrayType, StructType, StringType
schema = StructType([
    StructField('caption', StringType(), True),
    StructField('datasets', ArrayType(), True),
    StructField('first_seen', StringType(), True),
    StructField('id', StringType(), True),
    StructField('last_change', StringType(), True),
    StructField('last_seen', StringType(), True),
    StructField('properties', StructType([
        StructField('city', ArrayType(), True),
        StructField('country', ArrayType(), True),
        StructField('full', ArrayType(), True),
        StructField('postOfficeBox', ArrayType(), True),
        StructField('postalCode', ArrayType(), True),
        StructField('region', ArrayType(), True),
        StructField('remarks', ArrayType(), True),
        StructField('state', ArrayType(), True),
        StructField('street', ArrayType(), True),
        StructField('summary', ArrayType(), True)
    ]), True),
    StructField('referents', ArrayType(), True),
    StructField('schema', StringType(), True),
    StructField('target', BooleanType(), True)
])