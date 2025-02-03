from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField('port_code', StringType(), True),
    StructField('port_name', StringType(), True),
    StructField('previous_port_name', StringType(), True),
    StructField('country_name', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('country_code', StringType(), True),
    StructField('jurisdiction', StringType(), True),
    StructField('load_date', StringType(), True)
])