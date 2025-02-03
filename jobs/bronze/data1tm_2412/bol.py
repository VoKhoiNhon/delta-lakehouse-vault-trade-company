from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["1tm_2412.bol"].dataframe
        return df


def run(env="pro", params={}, spark=None):
    table_meta_link_position = TableMeta(
        from_files="metas/bronze/data1tm_2412/bol.yaml", env=env
    )

    import sys

    executer = Executer(
        sys.argv[0],
        table_meta_link_position.model,
        table_meta_link_position.input_resources,
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
