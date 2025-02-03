from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from pyspark.sql import DataFrame, functions as F, Window
from datetime import datetime, date
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from libs.utils.delta_utils import delta_upsert
import pytest
import os

from libs.utils.company_verifications import levenshtein_verify
from libs.utils.commons import add_pure_company_name

"""
case1: new_df null reg

hub_company: -> filter registration_number is not null
    - dv_hashkey_company
    - jurisdiction
    - registration_number
    - name
    - name_processed

hub_company_null_reg:
    - jurisdiction
    - name
    - verified_flag
    - verified_registration_number
    - verified_type: manual > levenshtein > ai
    - meta <string:string>


- filter company that have reg:
    - verified_flag: 1
    - verify_type:
        - levenshtein
        - data-source
"""


@pytest.fixture(scope="module")
def dfs(spark):

    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("registration_number", StringType(), True),
            StructField("jurisdiction", StringType(), True),
        ]
    )
    data = [
        (
            "ABC no123456, LLC.",
            "15",
            "a",
        ),
        (
            "ABC no1234 5 LLC.",
            "11",
            "a",
        ),
        (
            "ABC , LLC.",
            "16",
            "a",
        ),
        (
            "Casd Nonprofit LLC",
            "18",
            "a",
        ),
        (
            "AG D",
            2003,
            "a",
        ),
        (
            "sarl E",
            50,
            "a",
        ),
    ]
    data_test = [
        (
            "ABC",
            None,
            "a",
        ),
        (
            "sarl E  INC.",
            None,
            "a",
        ),
        (
            "ABC no 123456 LLC",
            None,
            "a",
        ),
        (
            "ABC no12345 LLC",
            None,
            "a",
        ),
        (
            "ABC no12345 LLC",
            "",
            "a",
        ),
    ]
    _df = (
        spark.createDataFrame(data, schema)
        .withColumn("verified", F.lit(1))
        .withColumn("verify_type", F.lit("levenshtein"))
        .withColumn(
            "dv_hashkey_company",
            F.expr("md5(concat_ws(';', jurisdiction, registration_number, name))"),
        )
        .withColumn("dv_recsrc", F.lit("source_0"))
        .withColumn("dv_source_version", F.lit("v1"))
        .withColumn("regexed_name", F.col("name"))
    )
    _df = add_pure_company_name(_df)
    _df_test_reg_null = (
        spark.createDataFrame(data_test, schema)
        .withColumn(
            "dv_hashkey_company",
            F.expr("md5(concat_ws(';', jurisdiction, registration_number, name))"),
        )
        .withColumn("dv_recsrc", F.lit("source_1"))
        .withColumn("dv_source_version", F.lit("v2"))
        .withColumn("regexed_name", F.col("name"))
    )
    _df_test_reg_null = add_pure_company_name(_df_test_reg_null)
    # _df_test_reg_null.show(10, False)

    return _df, _df_test_reg_null


def test_sub_layer(logger, dfs, spark):

    df, df_test_reg_null = dfs

    rs = levenshtein_verify(df, df_test_reg_null, threshold=90)
    rs.show(100, False)

    rs = levenshtein_verify(
        df,
        df_test_reg_null,
        base_col="regexed_name",
        threshold=60,
        verify_method="name60",
    )
    rs.show(100, False)
    # print(rs.schema.json())
    # print(rs.schema.simpleString())
