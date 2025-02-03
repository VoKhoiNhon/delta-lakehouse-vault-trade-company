import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.newyork"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")
        df = df.withColumn("jurisdiction", F.lit("New York"))
        df = add_pure_company_name(df, "name")

        return (
            df.selectExpr(
                f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
                f"'{record_source}' as dv_recsrc",
                "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
                f"'{dv_source_version}' as dv_source_version",
                "jurisdiction",
                "registration_number",
                "name",
                "pure_name",
                """
            case
                when registration_number is null then 0
            else 1 end as has_regnum
            """,
            )
            .withColumn(
                "row_number",
                F.expr(
                    "ROW_NUMBER() OVER (PARTITION BY dv_hashkey_company ORDER BY null)"
                ),
            )
            .where("row_number = 1")
            .drop("row_number")
        )


def run(env="pro", params={}):

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("pytest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    table_meta_link_position = TableMeta(
        from_files="metas/silver/h_company.yaml", env=env
    )
    executer_link_position = HCompanyExecuter(
        sys.argv[0],
        table_meta_link_position.model,
        table_meta_link_position.input_resources,
        params,
        spark,
    )
    executer_link_position.execute()


if __name__ == "__main__":
    # import argparse
    # parser = argparse.ArgumentParser()
    # args = parser.parse_args()
    # env = args.env
    # params = {"dv_source_version": args.dv_source_version}
    # run(env=args, params=params)
    run(env="test", params={})
