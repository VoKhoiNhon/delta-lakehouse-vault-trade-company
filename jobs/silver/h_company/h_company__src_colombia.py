import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.colombia"]
        df = source.dataframe
        # record_source = source.record_source
        df = df.filter(F.col("first_lastname").isNull())
        df = df.filter(
            F.col("business_name").isNotNull() | F.col("business_name").isNotNull()
        ).orderBy("business_name", ascending=True)
        df = df.withColumnRenamed("business_name", "name")
        df = df.withColumn("jurisdiction", F.lit("Colombia"))
        df = add_pure_company_name(df, "name")

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            "concat_ws(';', 'Colombia', 'https://www.datos.gov.co/en/Comercio-Industria-y-Turismo/Personas-Naturales-Personas-Jur-dicas-y-Entidades-/c82u-588k/about_data') as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'Colombia-20241231' as dv_source_version",
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
