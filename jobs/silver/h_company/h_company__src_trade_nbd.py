import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["trade_nbd"]
        df = source.dataframe
        df = df.withColumn(
            "dv_source_version",
            F.concat_ws(";", F.col("data_source"), F.col("load_date")),
        )
        import_df = df.select(
            F.col("import_country").alias("jurisdiction"),
            F.col("importer").alias("name"),
            F.col("pure_importer").alias("pure_name"),
            F.col("buyer_dv_hashkey_company").alias("dv_hashkey_company"),
            "dv_recsrc",
            "dv_source_version",
        )

        export_df = df.select(
            F.col("export_country_or_area").alias("jurisdiction"),
            F.col("exporter").alias("name"),
            F.col("pure_exporter").alias("pure_name"),
            F.col("supplier_dv_hashkey_company").alias("dv_hashkey_company"),
            "dv_recsrc",
            "dv_source_version",
        )
        df = import_df.union(export_df).dropDuplicates()
        df = df.withColumn(
            "jurisdiction",
            F.when(F.col("jurisdiction").isNull(), F.lit("unspecified")).otherwise(
                F.col("jurisdiction")
            ),
        )

        df = df.selectExpr(
            "dv_hashkey_company",
            "dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "dv_source_version",
            "jurisdiction",
            "NULL as registration_number",
            "name",
            "pure_name",
            """
            case
                when registration_number is null then 0
            else 1 end as has_regnum
            """,
        )
        df.show(5)

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
