import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name

"""
don't apply pure_name regex logic here:
pure_name:
F.regexp_replace(
    F.regexp_replace(
        F.trim((F.lower(F.col('name')))),
        r"[^\p{L}\p{N}\s]",
        "",  # xóa bỏ ký tự đặc biệt
    ),
    r"\s+",
    " ",  # chuyển nhiều khoảng trắng thành 1 khoảng trắng duy nhất
),
"""


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["2tm.company"]
        df = source.dataframe

        df = df.selectExpr(
            "new_dv_hashkey_company as dv_hashkey_company",
            "dv_recsrc",
            "dv_loaddts",
            "dv_source_version",
            "jurisdiction",
            "registration_number",
            "name",
            "new_pure_name as pure_name",
            "has_regnum",
        )

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
