from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class HPersonExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.newfoundland_and_labrador"]
        df = source.dataframe
        # record_source = source.record_source
        df = df.filter(F.col("company_name").isNotNull()).orderBy(
            "company_name", ascending=True
        )
        df = df.filter(F.col("officer_name").isNotNull()).orderBy(
            "officer_name", ascending=True
        )
        df = df.filter(F.col("status") != "System Error")
        df = df.withColumnRenamed("officer_name", "name")
        df = df.withColumnRenamed("company_number", "registration_number")
        df = df.withColumn("full_address", F.lit(None).cast("string"))
        df = df.withColumn("jurisdiction", F.lit("CA.Newfoundland and Labrador"))

        df = df.filter(F.col("name").isNotNull())
        df = df.selectExpr(
            f"{DV_HASHKEY_PERSON} as dv_hashkey_person",
            "concat_ws(';', 'Newfoundland and Labrador', 'https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx') as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'Newfoundland and Labrador-20241231' as dv_source_version",
            "jurisdiction",
            "name",
            "NULL as full_address",
        )
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/h_person.yaml", env=env)
    executer = HPersonExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
    # import argparse
    # parser = argparse.ArgumentParser()
    # args = parser.parse_args()
    # env = args.env
    # params = {"dv_source_version": args.dv_source_version}
    # run(env=args, params=params)
    # from pyspark.sql import SparkSession

    # spark = (
    #     SparkSession.builder.appName("pytest")
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config(
    #         "spark.sql.catalog.spark_catalog",
    #         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     )
    #     .getOrCreate()
    # )

    # run(env="test", params={"dv_source_version": "init"}, spark=spark)
