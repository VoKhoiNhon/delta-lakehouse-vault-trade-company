from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class HPersonExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["1tm_2412.person"]
        df = source.dataframe

        df = df.filter(F.col("name").isNotNull())
        df = df.selectExpr(
            f"{DV_HASHKEY_PERSON} as dv_hashkey_person",
            "concat_ws(';', data_source, data_source_link) as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'1tm' as dv_source_version",
            "jurisdiction",
            "name",
            "full_address",
        )
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/h_person.yaml", env=env)
    executer = HPersonExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    # import argparse
    # parser = argparse.ArgumentParser()
    # args = parser.parse_args()
    # env = args.env
    # params = {"dv_source_version": args.dv_source_version}
    # run(env=args, params=params)
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
