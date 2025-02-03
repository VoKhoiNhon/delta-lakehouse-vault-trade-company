from dataclasses import dataclass
from typing import Optional
from pyspark.sql import DataFrame, functions as F, Window


@dataclass
class VerificationConfig:
    SPECIAL_CHARS_PATTERN = "[^a-zA-Z0-9&,. ]"
    SELECTED_COLUMNS = [
        "t.jurisdiction",
        "t.dv_hashkey_company",
        "t.regexed_name",
        "t.dv_recsrc",
        "t.dv_source_version",
        "t.registration_number",
        "t.name",
        "t.pure_name",
    ]
    VERIFIED_COLUMNS = [
        "v.verified_dv_hashkey_company",
        "v.verified_registration_number",
        "v.verified_regexed_name",
        "v.verified_name",
    ]


class CompanyVerification:
    def __init__(self, config: VerificationConfig = VerificationConfig()):
        self.config = config

    def _create_ranking_window(
        self, partition_cols: list, has_registration: bool = True
    ) -> Window:
        """Create window spec for ranking companies"""
        order_conditions = [
            F.when(
                F.regexp_replace("name", self.config.SPECIAL_CHARS_PATTERN, "")
                == F.col("name"),
                0,
            ).otherwise(1),
            F.length("name").desc(),
        ]

        if has_registration:
            order_conditions.insert(
                0, F.when(F.col("registration_number").isNull(), 1).otherwise(0)
            )

        return Window.partitionBy(partition_cols).orderBy(order_conditions)

    def _apply_verification(
        self,
        df: DataFrame,
        partition_cols: list,
        verify_method: str,
        has_registration: bool = True,
    ) -> DataFrame:
        """Apply verification logic and return matched records"""

        window = self._create_ranking_window(partition_cols, has_registration)

        # Get best matches
        best_matches = (
            df.withColumn("row_num", F.row_number().over(window))
            .where("row_num = 1")
            .drop("row_num")
        )

        # Join with original data
        join_conditions = [df[col] == best_matches[col] for col in partition_cols]

        result = df.join(
            best_matches.select(
                *[
                    F.col(c).alias(f"verified_{c}")
                    for c in [
                        "dv_hashkey_company",
                        "registration_number",
                        "regexed_name",
                        "name",
                    ]
                ]
            ),
            on=join_conditions,
            how="left",
        ).select(
            *self.config.SELECTED_COLUMNS,
            *self.config.VERIFIED_COLUMNS,
            F.when((F.col("name") != F.col("verified_name")), verify_method).alias(
                "verify_method"
            ),
        )

        return result

    def verify_has_registration(self, df: DataFrame) -> DataFrame:
        """Verify companies with registration numbers"""
        partition_cols = ["jurisdiction", "registration_number", "regexed_name"]
        return self._apply_verification(
            df,
            partition_cols=partition_cols,
            verify_method="regexed_name_has_regis_vs_has_regis",
            has_registration=True,
        )

    def verify_no_registration(self, df: DataFrame) -> DataFrame:
        """Verify companies without registration numbers"""
        partition_cols = ["jurisdiction", "regexed_name"]
        return self._apply_verification(
            df,
            partition_cols=partition_cols,
            verify_method="regexed_name_no_regis_vs_no_regis",
            has_registration=False,
        )

    def verify_cross_registration(
        self, trusted_df: DataFrame, to_verify_df: DataFrame
    ) -> DataFrame:
        """Cross verify between companies with and without registration"""
        partition_cols = ["jurisdiction", "regexed_name"]

        df = to_verify_df.join(trusted_df, on=partition_cols, how="inner")

        return self._apply_verification(
            df,
            partition_cols=partition_cols,
            verify_method="regexed_name_no_regis_vs_has_regis",
            has_registration=False,
        )

    def verify_flow(self, df: DataFrame) -> DataFrame:
        """Execute complete verification flow"""
        # Split data
        has_reg_df = df.where("registration_number is not null")
        no_reg_df = df.where("registration_number is null")

        # Verify each group
        verified_has_reg = self.verify_has_registration(has_reg_df)
        verified_no_reg = self.verify_no_registration(no_reg_df)

        # Cross verify unmatched records
        unverified_has_reg = verified_has_reg.where("verify_method is null")
        unverified_no_reg = verified_no_reg.where("verify_method is null")

        cross_verified = self.verify_cross_registration(
            unverified_has_reg, unverified_no_reg
        )

        # Combine no registration results
        final_no_reg = (
            verified_no_reg.alias("t")
            .join(
                cross_verified.alias("n"),
                F.col("t.verified_dv_hashkey_company") == F.col("n.dv_hashkey_company"),
                "left",
            )
            .select(
                *self.config.SELECTED_COLUMNS,
                F.coalesce(
                    "n.verified_dv_hashkey_company", "t.verified_dv_hashkey_company"
                ).alias("verified_dv_hashkey_company"),
                F.coalesce(
                    "n.verified_registration_number", "t.verified_registration_number"
                ).alias("verified_registration_number"),
                F.coalesce("n.verified_regexed_name", "t.verified_regexed_name").alias(
                    "verified_regexed_name"
                ),
                F.coalesce("n.verified_name", "t.verified_name").alias("verified_name"),
                F.when(
                    (F.col("t.verified_dv_hashkey_company").isNotNull())
                    & (F.col("n.verified_dv_hashkey_company").isNotNull()),
                    "regexed_name_cross_no_regis_vs_has_regis",
                )
                .otherwise(
                    F.when(
                        (F.col("t.verified_dv_hashkey_company").isNotNull())
                        & (F.col("n.verified_dv_hashkey_company").isNull()),
                        F.col("t.verify_method"),
                    )
                )
                .alias("verify_method"),
            )
        )

        return verified_has_reg.unionAll(final_no_reg)


import pytest
from pyspark.sql import DataFrame, functions as F

# from libs.utils.company_verifications import CompanyVerification, VerificationConfig


@pytest.fixture(scope="module")
def test_data(spark):
    data = [
        (
            "src-x",
            "hash1",
            "ABC Corp",
            "US",
            "ABC Company",
            "abc Company",
            "REG123",
            1,
            "v1",
        ),
        (
            "src-x",
            "hash2",
            "ABC Corporation",
            "US",
            "ABC Company",
            "abc Company",
            "REG123",
            1,
            "v1",
        ),  # Longer name - higher priority
        (
            "src-x",
            "hash3",
            "ABC Corporation LLC",
            "US",
            "ABC Company",
            "abc Company",
            "REG123",
            1,
            "v2",
        ),
        (
            "src-x",
            "hash4",
            "ABC Corporation L,LC #",
            "US",
            "ABC Company",
            "abc Company",
            "REG1234",
            1,
            "v2",
        ),
        (
            "src-x",
            "hash5",
            "ABC Company Ltd.",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
            "v1",
        ),
        (
            "src-x",
            "hash6",
            "ABC Inc & Co.",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
            "v1",
        ),
        (
            "src-y",
            "hash7",
            "ABC Inc #@$",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
            "v2",
        ),  # Special chars - lower priority
        (
            "src-y",
            "hash8",
            "ABC Corporation L,LC",
            "US",
            "ABC Company",
            "abc Company",
            None,
            0,
            "v2",
        ),
        (
            "src-z",
            "hash9",
            "AC Corporation L,LC 12%",
            "US",
            "AC Company",
            "ac Company",
            None,
            0,
            "v1",
        ),
        ("src-z", "hash10", "AC Corp", "US", "AC Company", "ac Company", None, 0, "v2"),
    ]

    return spark.createDataFrame(
        data,
        [
            "dv_recsrc",
            "dv_hashkey_company",
            "name",
            "jurisdiction",
            "pure_name",
            "regexed_name",
            "registration_number",
            "has_regnum",
            "dv_source_version",
        ],  # Added dv_source_version
    )


@pytest.fixture(scope="module")
def verifier():
    return CompanyVerification()


def test_verify_has_registration(test_data, verifier):
    # Test verification for companies with registration
    df_has_reg = test_data.where("registration_number is not null")
    result = verifier.verify_has_registration(df_has_reg)

    assert result.count() == 4
    assert result.where("verify_method is not null").count() == 2

    # Verify priority order (longer name should be preferred)
    verified_names = result.where("registration_number = 'REG123'").collect()
    assert any(row.verified_name == "ABC Corporation" for row in verified_names)


def test_verify_no_registration(test_data, verifier):
    # Test verification for companies without registration
    df_no_reg = test_data.where("registration_number is null")
    result = verifier.verify_no_registration(df_no_reg)

    assert result.count() == 6
    assert result.where("verify_method is not null").count() == 4

    # Verify special characters handling
    special_char_rows = result.where("name like '%#%'").collect()
    assert all(row.verify_method is not None for row in special_char_rows)


def test_verify_cross_registration(test_data, verifier):
    # Test cross verification between registered and unregistered
    df_has_reg = test_data.where("registration_number is not null")
    df_no_reg = test_data.where("registration_number is null")

    verified_has_reg = verifier.verify_has_registration(df_has_reg)
    verified_no_reg = verifier.verify_no_registration(df_no_reg)

    unverified_has_reg = verified_has_reg.where("verify_method is null")
    unverified_no_reg = verified_no_reg.where("verify_method is null")

    result = verifier.verify_cross_registration(unverified_has_reg, unverified_no_reg)

    assert result.where("verify_method is not null").count() > 0


def test_complete_verification_flow(test_data, verifier):
    # Test complete verification workflow
    result = verifier.verify_flow(test_data)

    assert result.count() == 10  # Should maintain all records

    # Verify different verification methods
    assert (
        result.where("verify_method = 'regexed_name_has_regis_vs_has_regis'").count()
        == 2
    )

    assert (
        result.where("verify_method = 'regexed_name_no_regis_vs_no_regis'").count() == 1
    )

    assert (
        result.where(
            "verify_method = 'regexed_name_cross_no_regis_vs_has_regis'"
        ).count()
        == 4
    )


def test_verification_edge_cases(spark, verifier):
    # Test empty DataFrame
    empty_df = spark.createDataFrame([], test_data.schema)
    result = verifier.verify_flow(empty_df)
    assert result.count() == 0

    # Test single record
    single_record = test_data.limit(1)
    result = verifier.verify_flow(single_record)
    assert result.count() == 1

    # Test all null registration numbers
    null_reg_df = test_data.withColumn("registration_number", F.lit(None))
    result = verifier.verify_flow(null_reg_df)
    assert result.count() == test_data.count()


def test_special_characters_handling(test_data, verifier):
    # Test handling of special characters in company names
    special_chars_df = test_data.withColumn(
        "name", F.concat(F.col("name"), F.lit(" #@$%"))
    )

    result = verifier.verify_flow(special_chars_df)

    # Verify special characters are handled correctly in ranking
    verified_records = result.where("verify_method is not null")
    assert verified_records.count() > 0

    # Check if clean names are preferred over names with special characters
    clean_name_count = verified_records.where(
        "regexp_replace(verified_name, '[^a-zA-Z0-9&,. ]', '') = verified_name"
    ).count()
    assert clean_name_count > 0
