from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON
import pandas as pd


class S1tmPersonExecuterAddress(SatelliteVaultExecuter):
    def transform(self):
        df = self.input_dataframe_dict["1tm_2412.person"].dataframe.filter(
            F.col("name").isNotNull()
        )

        df = df.withColumn("type", F.lit(1))

        df = df.selectExpr(
            f"{DV_HASHKEY_PERSON} as dv_hashkey_person",
            "concat_ws(';', data_source, data_source_link) as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'1tm' as dv_source_version",
            "jurisdiction",
            "name",
            "full_address",
            "type",
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
        )
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/s_person_address.yaml", env=env)
    executer = S1tmPersonExecuterAddress(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":

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

    run(env="test", params={"dv_source_version": "init"}, spark=spark)
