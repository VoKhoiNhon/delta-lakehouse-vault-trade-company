from pyspark.sql.types import StructType, StructField, BooleanType, ArrayType, StructType, StringType
schema = StructType([
    StructField('caption', StringType(), True),
    StructField('datasets', ArrayType(), True),
    StructField('first_seen', StringType(), True),
    StructField('id', StringType(), True),
    StructField('last_change', StringType(), True),
    StructField('last_seen', StringType(), True),
    StructField('properties', StructType([
        StructField('date', ArrayType(), True),
        StructField('description', ArrayType(), True),
        StructField('endDate', ArrayType(), True),
        StructField('modifiedAt', ArrayType(), True),
        StructField('person', ArrayType(), True),
        StructField('relationship', ArrayType(), True),
        StructField('relative', ArrayType(), True),
        StructField('sourceUrl', ArrayType(), True),
        StructField('startDate', ArrayType(), True),
        StructField('summary', ArrayType(), True)
    ]), True),
    StructField('referents', ArrayType(), True),
    StructField('schema', StringType(), True),
    StructField('target', BooleanType(), True)
])