from libs.meta import TableMeta
import pyspark.sql.functions as F


from jobs.silver.s_company_levenshtein_verification.s_company_levenshtein_verification import (
    SCompanyLevenshteinVerificationExecuter,
)


def test_run(spark):
    table_meta = TableMeta(
        from_files=["metas/silver/s_company_levenshtein_verification.yaml"], env="test"
    )
    print(table_meta)
    executer = SCompanyLevenshteinVerificationExecuter(
        "s_company_levenshtein_verification",
        table_meta.model,
        table_meta.input_resources,
        spark=spark,
    )

    # df = executer.input_dataframe_dict["silver.h_company"].dataframe
    # to_verify_df = df.limit(5).withColumn("registration_number", F.lit(None))
    # executer.input_dataframe_dict["silver.h_company"].dataframe = df.unionAll(to_verify_df)
    executer.execute()
