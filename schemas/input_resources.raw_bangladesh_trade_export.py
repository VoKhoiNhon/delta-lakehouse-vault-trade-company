from pyspark.sql.types import StructType, StructField, StringType, StructType, LongType
schema = StructType([
    StructField('data', StructType([
        StructField('detail', StructType([
            StructField('bill_id', StringType(), True),
            StructField('bill_no', StringType(), True),
            StructField('billid', LongType(), True),
            StructField('buyer', StringType(), True),
            StructField('buyer_country', StringType(), True),
            StructField('cargo_value_local_currency', StringType(), True),
            StructField('customs_office_code', StringType(), True),
            StructField('customs_office_name', StringType(), True),
            StructField('date', StringType(), True),
            StructField('descript', StringType(), True),
            StructField('exporter_address', StringType(), True),
            StructField('g_weight_kg', StringType(), True),
            StructField('hs', StringType(), True),
            StructField('hs_code_desc', StringType(), True),
            StructField('n_weight_kg', StringType(), True),
            StructField('origin_country', StringType(), True),
            StructField('qty', StringType(), True),
            StructField('qty_unit', StringType(), True),
            StructField('seller', StringType(), True),
            StructField('seller_country', StringType(), True),
            StructField('seller_port', StringType(), True),
            StructField('weight', StringType(), True),
            StructField('weight_unit', StringType(), True)
        ]), True),
        StructField('raw', LongType(), True),
        StructField('title', ArrayType(), True)
    ]), True),
    StructField('message', StringType(), True),
    StructField('mmm', LongType(), True),
    StructField('state', LongType(), True)
])