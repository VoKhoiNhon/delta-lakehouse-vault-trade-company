from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY, DV_HASHKEY_INDUSTRY
from libs.utils.commons import add_pure_company_name


class SCompanyDemographicExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["manual_company"]
        df = source.dataframe
        # df = add_pure_company_name(df, "name")
        # try:
        #     parallelism = self.spark.sparkContext.defaultParallelism
        #     df = df.repartition(parallelism)
        #     print('repartition to ', parallelism)
        # except Exception as e:
        #     print(e)
        df = df.withColumn("category", F.array(F.col("category")))
        df = (
            df.withColumn("phone_numbers", F.array(F.col("phone_numbers")))
            .withColumn("emails", F.array(F.col("emails")))
            .withColumn("websites", F.array(F.col("websites")))
            .withColumn("fax_numbers", F.array(F.col("fax_numbers")))
            .withColumn("other_names", F.array(F.col("other_names")))
        )
        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            "concat_ws(';', data_source, data_source_link) as dv_recsrc",
            "'manual_company' as dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "jurisdiction",
            "name",
            "registration_number",
            "lei_code",
            "description",
            "country_code",
            "country_name",
            "date_incorporated",
            "date_struck_off",
            "legal_form",
            "category",
            "phone_numbers",
            "emails",
            "websites",
            "linkedin_url",
            "twitter_url",
            "facebook_url",
            "fax_numbers",
            "other_names",
            "no_of_employees",
            "image_url",
            "authorised_capital",
            "paid_up_capital",
            "currency_code",
            "status",
            "status_code",
            "status_desc",
            "is_branch",
            "is_sanctioned",
            # 'is_trade',
            f"{DV_HASHKEY_INDUSTRY} as dv_hashkey_industry",
        )
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files="metas/silver/s_company_demographic.yaml", env=env
    )
    executer = SCompanyDemographicExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
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

    # run(env="test", params={"dv_source_version": "init"}, spark=spark)
    run()
