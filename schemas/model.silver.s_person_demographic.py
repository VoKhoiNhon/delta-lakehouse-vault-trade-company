from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, IntegerType, BooleanType, FloatType
schema = StructType([
    StructField('dv_hashkey_person', StringType(), True),
    StructField('dv_recsrc', StringType(), True),
    StructField('dv_loaddts', TimestampType(), True),
    StructField('dv_hashdiff', StringType(), True),
    StructField('dv_source_version', StringType(), True),
    StructField('jurisdiction', StringType(), True),
    StructField('name', StringType(), True),
    StructField('full_address', StringType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('middle_name', StringType(), True),
    StructField('is_person', BooleanType(), True),
    StructField('image_url', StringType(), True),
    StructField('dob', StringType(), True),
    StructField('birthplace', StringType(), True),
    StructField('nationality', StringType(), True),
    StructField('country_of_residence', StringType(), True),
    StructField('accuracy_level', FloatType(), True),
    StructField('gender', IntegerType(), True),
    StructField('skills', ArrayType(), True),
    StructField('job_summary', StringType(), True),
    StructField('salary', StringType(), True),
    StructField('yoe', IntegerType(), True),
    StructField('industry', StringType(), True),
    StructField('phone_numbers', ArrayType(), True),
    StructField('emails', ArrayType(), True),
    StructField('linkedin_url', StringType(), True),
    StructField('twitter_url', StringType(), True),
    StructField('facebook_url', StringType(), True)
])