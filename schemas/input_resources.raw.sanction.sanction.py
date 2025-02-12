from pyspark.sql.types import StructType, StructField, BooleanType, ArrayType, StructType, StringType
schema = StructType([
    StructField('caption', StringType(), True),
    StructField('datasets', ArrayType(), True),
    StructField('first_seen', StringType(), True),
    StructField('id', StringType(), True),
    StructField('last_change', StringType(), True),
    StructField('last_seen', StringType(), True),
    StructField('properties', StructType([
        StructField('authority', ArrayType(), True),
        StructField('authorityId', ArrayType(), True),
        StructField('country', ArrayType(), True),
        StructField('date', ArrayType(), True),
        StructField('description', ArrayType(), True),
        StructField('duration', ArrayType(), True),
        StructField('endDate', ArrayType(), True),
        StructField('entity', ArrayType(), True),
        StructField('listingDate', ArrayType(), True),
        StructField('modifiedAt', ArrayType(), True),
        StructField('program', ArrayType(), True),
        StructField('programId', ArrayType(), True),
        StructField('programUrl', ArrayType(), True),
        StructField('provisions', ArrayType(), True),
        StructField('publisher', ArrayType(), True),
        StructField('reason', ArrayType(), True),
        StructField('recordId', ArrayType(), True),
        StructField('sourceUrl', ArrayType(), True),
        StructField('startDate', ArrayType(), True),
        StructField('status', ArrayType(), True),
        StructField('summary', ArrayType(), True),
        StructField('unscId', ArrayType(), True)
    ]), True),
    StructField('referents', ArrayType(), True),
    StructField('schema', StringType(), True),
    StructField('target', BooleanType(), True)
])