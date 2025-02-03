from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta

# import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
import sys

# from libs.utils.commons import add_pure_company_name


class SCompanyExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["manual_company"]
        df = source.dataframe
        # df = add_pure_company_name(df, "name")

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            "concat_ws(';', data_source, data_source_link) as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'manual_company' as dv_source_version",
            "jurisdiction",
            "name",
            "pure_name",
            "registration_number",
            "full_address",
            "street",
            "city",
            "region",
            "state",
            "province",
            "country_code",
            "country_name",
            "postal_code",
            "latitude",
            "longitude",
            "type",
        )

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
