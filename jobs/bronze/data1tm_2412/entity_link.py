from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    clean_jurisdiciton_1tm,
    add_pure_company_name,
    add_load_date_columns,
)
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["1tm_2412.entity_link"].dataframe
        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(
        from_files="metas/bronze/data1tm_2412/entity_link.yaml", env=env
    )

    import sys

    executer = Executer(
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
