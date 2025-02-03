from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
import sys
from libs.utils.commons import add_pure_company_name


class SCompanyExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.colombia"]
        # record_source = source.record_source
        # dv_source_version = source.dv_source_version
        df = source.dataframe
        df = df.filter(F.col("first_lastname").isNull())
        df = df.filter(
            F.col("business_name").isNotNull() | F.col("business_name").isNotNull()
        ).orderBy("business_name", ascending=True)
        df = df.withColumnRenamed("business_name", "name")
        df = df.withColumn("jurisdiction", F.lit("Colombia"))
        df = add_pure_company_name(df, "name")
        df = (
            df.withColumn("country_code", F.lit("CO"))
            .withColumn("country_name", F.lit("Colombia"))
            .withColumnRenamed("chamber_commerce", "city")
            .selectExpr(
                f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
                "concat_ws(';', 'Colombia', 'https://www.datos.gov.co/en/Comercio-Industria-y-Turismo/Personas-Naturales-Personas-Jur-dicas-y-Entidades-/c82u-588k/about_data') as dv_recsrc",
                "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
                "'Colombia-20241231' as dv_source_version",
                "jurisdiction",
                "registration_number",
                "name",
                "pure_name",
                "NULL as full_address",
                "NULL as street",
                "city",
                "NULL as region",
                "NULL as state",
                "NULL as province",
                "country_code",
                "country_name",
                "NULL as postal_code",
                "NULL as latitude",
                "NULL as longitude",
                "NULL as type",
                "NULL as start_date",
                "NULL as end_date",
            )
        )
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
