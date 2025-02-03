from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from libs.utils.delta_utils import (
    delta_insert_for_hubnlink,
    print_last_delta_last_operation,
)
from libs.utils.connection import SparkSessionBuilder


def main(
    spark: SparkSession,
    l_company_verification_path="s3a://lakehouse-silver/l_company_verification",
    regex_path="s3a://lakehouse-silver/company_verification/regex",
    levenshtein_path="s3a://lakehouse-silver/company_verification/levenshtein",
):
    if not spark:
        spark = SparkSessionBuilder().create_spark_session()

    delta_table = DeltaTable.forPath(spark, l_company_verification_path)

    # step 1 - regex insert
    regex_df = spark.sql(f"""select * from delta.`{regex_path}`""")

    print("step 1 - regex insert")
    delta_insert_for_hubnlink(
        spark,
        df=regex_df,
        data_location=l_company_verification_path,
        base_cols=["jurisdiction", "dv_hashkey_company"],
        struct_type=delta_table.toDF().schema,
    )

    # step 2 - levenshtein update regex
    update_df = spark.sql(
        f"""
    select t.jurisdiction, t.dv_hashkey_company, t.dv_recsrc,
        t.dv_source_version, t.name, t.pure_name,
        v.verified_dv_hashkey_company, v.verified_registration_number,
        v.verified_pure_name, t.verified_regexed_name, v.verified_name
    from delta.`{levenshtein_path}` v
    inner join delta.`{regex_path}` t
        on v.dv_hashkey_company = t.verified_dv_hashkey_company
    where t.verify_method = 'regexed_name_no_regis_vs_no_regis'
    """
    )

    delta_table.alias("base").merge(
        update_df.alias("updates"),
        "base.dv_hashkey_company = updates.dv_hashkey_company and base.verify_method = 'regexed_name_no_regis_vs_no_regis'",
    ).whenMatchedUpdate(
        set={
            "verify_method": F.lit("cross_levenshtein_regexed_name"),
            "verified_dv_hashkey_company": "updates.verified_dv_hashkey_company",
            "verified_registration_number": "updates.verified_registration_number",
            "verified_pure_name": "updates.verified_pure_name",
            "verified_regexed_name": "updates.verified_regexed_name",
        }
    ).execute()
    print("step 2 - levenshtein update regex")
    print_last_delta_last_operation(delta_table)

    # step 3 - levenshtein insert
    insert_df = spark.sql(
        f"""
    select le.*
    from delta.`{levenshtein_path}` le
    where not exists (
        select 1 from delta.`{regex_path}` l
        where le.dv_hashkey_company = l.verified_dv_hashkey_company
    )
    """
    )

    print("step 3 - levenshtein insert")
    delta_insert_for_hubnlink(
        spark,
        df=insert_df,
        data_location=l_company_verification_path,
        base_cols=["jurisdiction", "dv_hashkey_company"],
        struct_type=delta_table.toDF().schema,
    )


if __name__ == "__main__":
    main(spark=None)
