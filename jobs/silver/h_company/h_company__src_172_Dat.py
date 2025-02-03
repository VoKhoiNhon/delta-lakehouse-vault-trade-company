import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["172_Dat.bol"]
        df = source.dataframe
        record_source = source.record_source
        import_df = df.select(
            F.col("import_country").alias("jurisdiction"),
            F.col("importer").alias("name"),
            "dv_recsrc",
            "data_source",
        )

        export_df = df.select(
            F.col("export_country_or_area").alias("jurisdiction"),
            F.col("exporter").alias("name"),
            "dv_recsrc",
            "data_source",
        )
        df = import_df.union(export_df).dropDuplicates()
        df = df.withColumn(
            "jurisdiction",
            F.when(F.col("jurisdiction").isNull(), F.lit("unspecified")).otherwise(
                F.col("jurisdiction")
            ),
        )

        df = add_pure_company_name(df).withColumn("registration_number", F.lit(None))

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "concat_ws(';',data_source , '20240108') as dv_source_version",
            "jurisdiction",
            "name",
            "pure_name",
            """
            case
                when registration_number is null then 0
            else 1 end as has_regnum
            """,
        )

        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/h_company.yaml", env=env)
    executer = HCompanyExecuter(
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
