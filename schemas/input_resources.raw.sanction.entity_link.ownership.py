from pyspark.sql.types import StructType, StructField, BooleanType, ArrayType, StructType, StringType
schema = StructType([
    StructField('caption', StringType(), True),
    StructField('datasets', ArrayType(), True),
    StructField('first_seen', StringType(), True),
    StructField('id', StringType(), True),
    StructField('last_change', StringType(), True),
    StructField('last_seen', StringType(), True),
    StructField('properties', StructType([
        StructField('asset', ArrayType(), True),
        StructField('date', ArrayType(), True),
        StructField('description', ArrayType(), True),
        StructField('endDate', ArrayType(), True),
        StructField('modifiedAt', ArrayType(), True),
        StructField('owner', ArrayType(), True),
        StructField('percentage', ArrayType(), True),
        StructField('publisher', ArrayType(), True),
        StructField('recordId', ArrayType(), True),
        StructField('role', ArrayType(), True),
        StructField('sharesCount', ArrayType(), True),
        StructField('sharesCurrency', ArrayType(), True),
        StructField('sharesValue', ArrayType(), True),
        StructField('sourceUrl', ArrayType(), True),
        StructField('startDate', ArrayType(), True),
        StructField('status', ArrayType(), True),
        StructField('summary', ArrayType(), True)
    ]), True),
    StructField('referents', ArrayType(), True),
    StructField('schema', StringType(), True),
    StructField('target', BooleanType(), True)
])