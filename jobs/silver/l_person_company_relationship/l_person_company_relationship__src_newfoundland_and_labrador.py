from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import (
    DV_HASHKEY_FROM_PERSON,
    DV_HASHKEY_TO_COMPANY,
    DV_HASHKEY_L_PERSON_COMPANY_POSITION,
)


class LinkPositionExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.newfoundland_and_labrador"]
        df = source.dataframe
        df = df.filter(F.col("company_name").isNotNull()).orderBy(
            "company_name", ascending=True
        )
        df = df.withColumnRenamed("company_number", "to_registration_number")
        df = df.filter(F.col("officer_name").isNotNull()).orderBy(
            "officer_name", ascending=True
        )
        df = df.withColumnRenamed("officer_name", "from_name")
        df = df.filter(F.col("position").isNotNull())
        df = df.withColumn("from_full_address", F.lit(None).cast("string"))
        df = df.withColumnRenamed("pure_name", "to_pure_name")
        df = df.withColumn("from_jurisdiction", F.lit("CA.Newfoundland and Labrador"))
        df = df.withColumn("to_jurisdiction", F.lit("CA.Newfoundland and Labrador"))

        df = (
            df.selectExpr(
                f"{DV_HASHKEY_FROM_PERSON} as dv_hashkey_person",
                f"{DV_HASHKEY_TO_COMPANY} as dv_hashkey_company",
                "concat_ws(';', 'Newfoundland and Labrador', 'https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx') as dv_recsrc",
                "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
                "'Newfoundland and Labrador-20241231' as dv_source_version",
                "from_jurisdiction",
                "to_jurisdiction",
                "position",
                "position_code",
                "NULL as start_date",
                "NULL as end_date",
            )
            .withColumn(
                "dv_hashkey_l_person_company_relationship",
                F.expr(DV_HASHKEY_L_PERSON_COMPANY_POSITION),
            )
            .where("position is not null")
        )
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files="metas/silver/l_person_company_relationship.yaml", env=env
    )
    executer = LinkPositionExecuter(
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
