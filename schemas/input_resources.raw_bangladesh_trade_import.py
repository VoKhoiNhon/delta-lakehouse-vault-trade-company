from pyspark.sql.types import StructType, StructField, StringType, StructType, LongType
schema = StructType([
    StructField('data', StructType([
        StructField('detail', StructType([
            StructField('amount', StringType(), True),
            StructField('assessed_unit_price_in_fc', StringType(), True),
            StructField('assessed_value_in_bdt', StringType(), True),
            StructField('be_date', StringType(), True),
            StructField('be_no', StringType(), True),
            StructField('bill_id', StringType(), True),
            StructField('billid', LongType(), True),
            StructField('bol_number', StringType(), True),
            StructField('buyer', StringType(), True),
            StructField('customs_office_code', StringType(), True),
            StructField('date', StringType(), True),
            StructField('declared_unit_price_in_fc', StringType(), True),
            StructField('descript', StringType(), True),
            StructField('exchange_rate', StringType(), True),
            StructField('foreign_currency', StringType(), True),
            StructField('g_weight_in_kg', StringType(), True),
            StructField('hs', StringType(), True),
            StructField('hs_codd_desc', StringType(), True),
            StructField('importer_address', StringType(), True),
            StructField('importer_id', StringType(), True),
            StructField('manifest_no', StringType(), True),
            StructField('n_weight_in_kg', StringType(), True),
            StructField('origin_country', StringType(), True),
            StructField('quantity', StringType(), True),
            StructField('seller', StringType(), True),
            StructField('total_customs_duties_cd_rd_sd_vat_fp_tk', StringType(), True),
            StructField('type', StringType(), True),
            StructField('type_of_package', StringType(), True),
            StructField('unit_of_quantity', StringType(), True)
        ]), True),
        StructField('raw', LongType(), True),
        StructField('title', ArrayType(), True)
    ]), True),
    StructField('message', StringType(), True),
    StructField('mmm', LongType(), True),
    StructField('state', LongType(), True)
])