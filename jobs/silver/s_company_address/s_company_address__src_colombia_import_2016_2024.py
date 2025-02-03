from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
import sys
from libs.utils.commons import add_pure_company_name


class SCompanyExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["colombia.import_2016-2024"]
        df = source.dataframe
        record_source = source.record_source
        df_import = (
            df.select("name_import", "regis_import", "address_import")
            .withColumnsRenamed(
                {
                    "name_import": "name",
                    "regis_import": "registration_number",
                    "address_import": "full_address",
                }
            )
            .withColumn("jurisdiction", F.lit("Colombia"))
        )

        df_export = df.select(
            "name_export", "address_export", "jurisdiction"
        ).withColumnsRenamed({"name_export": "name", "address_export": "full_address"})

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

        df_company = df_company.withColumn("is_branch", F.lit(False))

        df_company = df_company.filter(F.col("full_address").isNotNull())
        df_company = df_company.dropDuplicates(["full_address"])

        df_company = df_company.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'gov_colombia_trade_import_2016_2024;20241227' as dv_source_version",
            "jurisdiction",
            "name",
            "pure_name",
            "registration_number",
            "full_address",
            # "street",
            # "city",
            # "region",
            # "state",
            # "province",
            # "country_code",
            # "country_name",
            # "postal_code",
            # "latitude",
            # "longitude",
            # "type",
        )

        return df_company


def run(env="pro", params={}, spark=None):
    table_meta_link_position = TableMeta(
        from_files="metas/silver/s_company_address.yaml", env=env
    )
    executer = SCompanyExecuter(
        sys.argv[0],
        table_meta_link_position.model,
        table_meta_link_position.input_resources,
        params,
    )
    executer.execute()


if __name__ == "__main__":
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

    run(
        env="test",
        params={"dv_source_version": "init"},
    )
