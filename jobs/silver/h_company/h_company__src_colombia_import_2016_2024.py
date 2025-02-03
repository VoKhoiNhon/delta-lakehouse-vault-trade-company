import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["colombia.import_2016-2024"]
        df = source.dataframe
        record_source = source.record_source
        df = df.dropDuplicates()

        df_import = (
            df.select("name_import", "regis_import", "address_import", "tele_import")
            .withColumnsRenamed(
                {
                    "name_import": "name",
                    "regis_import": "registration_number",
                }
            )
            .withColumn("jurisdiction", F.lit("Colombia"))
        )

        df_export = df.select("name_export", "jurisdiction").withColumnsRenamed(
            {
                "name_export": "name",
            }
        )

        df_shipper = df.select("shipper_company").withColumnRenamed(
            "shipper_company", "name"
        )

        df_company = df_import.unionByName(df_export, allowMissingColumns=True)
        df_company = df_company.unionByName(df_shipper, allowMissingColumns=True)
        df_company = add_pure_company_name(df_company, "name")

        df_company = df_company.withColumn(
            "jurisdiction",
            F.expr(
                """
            case when jurisdiction is null then 'unspecified'
            else jurisdiction end
            """
            ),
        )

        df_company = df_company.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'gov_colombia_trade_import_2016_2024;20241227' as dv_source_version",
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

        return df_company


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/h_company.yaml", env=env)
    executer = HCompanyExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        # spark,
    )
    executer.execute()


if __name__ == "__main__":
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

    run(env="test", params={"dv_source_version": "init"})
