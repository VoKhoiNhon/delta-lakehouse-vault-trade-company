from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys


class SCompanyExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["2tm.company_demographic"]
        df = (
            source.dataframe.withColumn(
                "dv_hashkey_company", F.col("new_dv_hashkey_company")
            )
            .withColumn("pure_name", F.col("new_pure_name"))
            .drop("new_dv_hashkey_company", "new_pure_name")
        )

        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(
        from_files="metas/silver/s_company_demographic.yaml", env=env
    )
    executer = SCompanyExecuter(
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
