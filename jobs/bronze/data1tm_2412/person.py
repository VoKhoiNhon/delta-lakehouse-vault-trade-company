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


def normalize_name(df, column_name):
    df = df.withColumn(
        column_name,
        F.regexp_replace(
            F.lower(F.col(column_name)),
            "[^a-zA-Z .-]",
            "",
        ),
    )
    df = df.withColumn(column_name, F.initcap(F.col(column_name)))
    df = df.withColumn(
        column_name,
        F.when(
            F.col(column_name).contains("."),
            F.concat(
                F.split(F.col(column_name), "\\.").getItem(0),
                F.lit("."),
                F.initcap(F.regexp_replace(F.col(column_name), r"^[^\.]*\.", "")),
            ),
        ).otherwise(F.col(column_name)),
    )

    return df


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["1tm_2412.person"].dataframe
        df = normalize_name(df=df, column_name="name")
        # df = clean_column_names(df)
        # df = clean_string_columns(df)
        df = clean_jurisdiciton_1tm(self.spark, df)
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = df.filter(F.col("name").isNotNull())
        df = df.withColumn(
            "jurisdiction",
            F.expr(
                """
            case when jurisdiction = 'Georgia' and country_code = 'US' then 'US.Georgia'
                when jurisdiction in ('Global', 'European Union')  then 'Unknown'
                when jurisdiction is null then 'Unknown'
            else jurisdiction end"""
            ),
        )

        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(from_files="metas/bronze/data1tm_2412/person.yaml", env=env)

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
