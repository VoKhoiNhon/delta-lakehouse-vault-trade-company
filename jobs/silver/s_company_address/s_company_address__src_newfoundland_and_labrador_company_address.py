from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
import sys
from libs.utils.commons import add_pure_company_name


class SCompanyExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.newfoundland_and_labrador"]
        df = source.dataframe
        # record_source = source.record_source
        df = df.filter(F.col("company_name").isNotNull()).orderBy(
            "company_name", ascending=True
        )
        df = df.withColumnRenamed("company_name", "name")
        df = df.withColumnRenamed("company_number", "registration_number")
        df = df.withColumn("jurisdiction", F.lit("CA.Newfoundland and Labrador"))
        df = df.withColumnRenamed("pure_company_name", "pure_name")
        df_address_main = df.select(
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            "main_full_address",
            "registered_office_city",
            "registered_office_province_state",
            "registered_office_postal_zip_code",
        )
        df_address_main = (
            df_address_main.withColumnRenamed("main_full_address", "full_address")
            .withColumnRenamed("registered_office_city", "city")
            .withColumnRenamed("registered_office_province_state", "state")
            .withColumnRenamed("registered_office_postal_zip_code", "postal_code")
            .withColumn("type", F.lit(1))
        )

        df_address_mailing = df.select(
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            "mailing_full_address",
            "registered_office_outside_nl_city",
            "registered_office_outside_nl_province_state",
            "registered_office_outside_nl_postal_zip_code",
        )
        df_address_mailing = (
            df_address_mailing.withColumnRenamed("mailing_full_address", "full_address")
            .withColumnRenamed("registered_office_outside_nl_city", "city")
            .withColumnRenamed("registered_office_outside_nl_province_state", "state")
            .withColumnRenamed(
                "registered_office_outside_nl_postal_zip_code", "postal_code"
            )
            .withColumn("type", F.lit(3))
        )
        df = df_address_main.union(df_address_mailing)
        df = df.filter(F.col("full_address").isNotNull())
        df = (
            df.withColumn("country_code", F.lit("CA"))
            .withColumn("country_name", F.lit("Canada"))
            .selectExpr(
                f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
                "concat_ws(';', 'Newfoundland and Labrador', 'https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx') as dv_recsrc",
                "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
                "'Newfoundland and Labrador-20241231' as dv_source_version",
                "jurisdiction",
                "registration_number",
                "name",
                "pure_name",
                "full_address",
                "NULL as street",
                "NULL as city",
                "NULL as region",
                "NULL as state",
                "NULL as province",
                "country_code",
                "country_name",
                "postal_code",
                "NULL as latitude",
                "NULL as longitude",
                "type",
                "NULL as start_date",
                "NULL as end_date",
            )
        )
        df = df.dropDuplicates()
        df.show(5)
        return df


def run(env="pro", params={}, spark=None):
    table_meta_link_position = TableMeta(
        from_files="metas/silver/s_company_address.yaml", env=env
    )
    executer = SCompanyExecuter(
        sys.argv[0],
        table_meta_link_position.model,
        table_meta_link_position.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
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
