from pyspark.sql import DataFrame, functions as F, Window
import pytest
from libs.utils.company_verifications import (
    verify_regex_name_has_regis_vs_has_regis,
    verify_regex_name_no_regis_vs_no_regis,
    verify_regex_name_no_regis_vs_has_regis,
    verify_regex_name_flow,
)


@pytest.fixture(scope="module")
def df(spark):
    data = [
        (
            "src-x",
            "ver1",
            "hash1",
            "ABC Corp",
            "US",
            "ABC Company",
            "abc Company",
            "REG123",
            1,
        ),
        (
            "src-x",
            "ver1",
            "hash2",
            "ABC Corporation",
            "US",
            "ABC Company",
            "abc Company",
            "REG123",
            1,
        ),  # Longer name - higher priority
        (
            "src-x",
            "ver1",
            "hash3",
            "ABC Corporation LLC",
            "US",
            "ABC Company",
            "abc Company",
            "REG123",
            1,
        ),
        (
            "src-x",
            "ver2",
            "hash4",
            "ABC Corporation L,LC #",
            "US",
            "ABC Company",
            "abc Company",
            "REG1234",
            1,
        ),
        (
            "src-x",
            "ver2",
            "hash5",
            "ABC Company Ltd.",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
        ),
        (
            "src-x",
            "ver2",
            "hash6",
            "ABC Inc & Co.",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
        ),
        (
            "src-y",
            "ver3",
            "hash7",
            "ABC Inc #@$",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
        ),  # Special chars - lower priority
        (
            "src-y",
            "ver3",
            "hash8",
            "ABC Corporation L,LC",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
        ),
        (
            "src-z",
            "ver3",
            "hash9",
            "AC Corporation L,LC 12%",
            "US",
            "AC Company",
            "ac Company",
            None,
            0,
        ),
        (
            "src-z",
            "ver3",
            "hash10",
            "AC Corp",
            "US",
            "AC Company",
            "ac Company",
            None,
            0,
        ),
    ]
    # Create DataFrame
    df = spark.createDataFrame(
        data,
        [
            "dv_recsrc",
            "dv_source_version",
            "dv_hashkey_company",
            "name",
            "jurisdiction",
            "pure_name",
            "regexed_name",
            "registration_number",
            "has_regnum",
        ],
    )

    return df


def test_verify(df):
    rs_df_has_regis = verify_regex_name_has_regis_vs_has_regis(
        df.where("has_regnum = 1")
    )
    # rs_df_has_regis.show(5, False)

    rs_df_no_regis = verify_regex_name_no_regis_vs_no_regis(df.where("has_regnum = 0"))
    # rs_df_no_regis.show(10, False)

    rs_no_vs_has = verify_regex_name_no_regis_vs_has_regis(
        rs_df_has_regis.where("verify_method is null"),  # k bi verify - thang
        rs_df_no_regis.where("verify_method is null"),  # k bi verify - thang
    )

    # rs_no_vs_has.show(10, False)

    assert rs_df_has_regis.count() == 4
    assert rs_df_has_regis.where("verify_method is not null").count() == 2

    assert rs_df_no_regis.count() == 6
    assert rs_df_no_regis.where("verify_method is not null").count() == 4


def test_verify_regexed_name_flow(df):

    rs = verify_regex_name_flow(df)
    rs.show(20, False)
    assert rs.count() == 10
    assert (
        rs.where("verify_method = 'regexed_name_has_regis_vs_has_regis'").count() == 2
    )
    assert rs.where("verify_method = 'regexed_name_no_regis_vs_no_regis'").count() == 1
    assert (
        rs.where("verify_method = 'regexed_name_cross_no_regis_vs_has_regis'").count()
        == 4
    )
