from pyspark.sql.types import StructType, StructField, ArrayType, BooleanType, StringType
schema = StructType([
    StructField('jurisdiction', StringType(), True),
    StructField('other_names', ArrayType(), True),
    StructField('address', StringType(), True),
    StructField('date_struck_off', StringType(), True),
    StructField('date_incorporated', StringType(), True),
    StructField('inn_code', StringType(), True),
    StructField('lei_code', StringType(), True),
    StructField('kpp_code', StringType(), True),
    StructField('ogrn_code', StringType(), True),
    StructField('perm_id', StringType(), True),
    StructField('bik_code', StringType(), True),
    StructField('cage_code', StringType(), True),
    StructField('duns_code', StringType(), True),
    StructField('icij_id', StringType(), True),
    StructField('id_number', StringType(), True),
    StructField('npi_code', StringType(), True),
    StructField('okpo_code', StringType(), True),
    StructField('ric_code', StringType(), True),
    StructField('vat_code', StringType(), True),
    StructField('registration_number', StringType(), True),
    StructField('tax_number', StringType(), True),
    StructField('sector', StringType(), True),
    StructField('status', StringType(), True),
    StructField('program', StringType(), True),
    StructField('description', StringType(), True),
    StructField('notes', StringType(), True),
    StructField('summary', StringType(), True),
    StructField('sector_combined', StringType(), True),
    StructField('is_sanctioned', BooleanType(), True),
    StructField('emails', ArrayType(), True),
    StructField('phone_numbers', ArrayType(), True),
    StructField('websites', ArrayType(), True),
    StructField('main_country', StringType(), True),
    StructField('country', StringType(), True),
    StructField('company_name', StringType(), True),
    StructField('load_date', StringType(), True),
    StructField('country_name', StringType(), True),
    StructField('description_full', StringType(), True),
    StructField('pure_name', StringType(), True)
])