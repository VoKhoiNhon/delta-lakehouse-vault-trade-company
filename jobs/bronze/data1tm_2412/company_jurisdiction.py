import os
from libs.executers.bronze_executer import BronzeExecuter
from pyspark.sql.functions import col, lit, lower, when, split, size
from libs.meta import TableMeta
from pyspark.sql import DataFrame

# from libs.utils.commons import (
#     add_load_date_columns,
# )


class Executer(BronzeExecuter):
    def load_resource_data(spark, resource_path: str) -> DataFrame:
        full_path = os.path.join("resources", resource_path)

        # Check if the file exists
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Resource file not found: {full_path}")

        resource = spark.read.csv(resource_path, header=True)
        for column_name in resource.columns:
            resource = resource.withColumnRenamed(column_name, f"res_{column_name}")
        resource = resource.withColumn(
            "last_part",
            split(col("res_jurisdiction"), "\\.").getItem(
                size(split(col("res_jurisdiction"), "\\.")) - 1
            ),
        )
        return resource

    def fill_jurisdiction(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "jurisdiction",
            when(
                (col("jurisdiction").isNull() | (col("jurisdiction") == "Unknown"))
                & col("country_name").isNotNull(),
                col("country_name"),
            ).otherwise(col("jurisdiction")),
        )

    def join_data(self, df: DataFrame, resource: DataFrame) -> DataFrame:
        df_joined = df.join(
            resource,
            (lower(df.jurisdiction) == lower(resource.last_part))
            & (lower(df.country_name) == lower(resource.res_country)),
            "left",
        )
        return df_joined

    def validate_data(self, df_joined: DataFrame) -> DataFrame:
        df_joined = df_joined.withColumn(
            "is_correct",
            when(
                col("res_jurisdiction").isNotNull()
                & (col("country_name") == col("res_country")),
                lit(True),
            ).otherwise(lit(False)),
        ).withColumn(
            "jurisdiction",
            when(col("is_correct") == True, col("res_jurisdiction")).otherwise(
                col("jurisdiction")
            ),
        )
        return df_joined

    def process_jurisdiction(self, df: DataFrame) -> DataFrame:
        df = (
            df.withColumn(
                "juris_in_address",
                when(
                    col("is_correct") == False,
                    lower(col("full_address")).contains(lower(col("jurisdiction"))),
                ).otherwise(None),
            )
            .withColumn(
                "country_in_address",
                when(
                    col("is_correct") == False,
                    lower(col("full_address")).contains(lower(col("country_name"))),
                ).otherwise(None),
            )
            .withColumn(
                "is_correct",
                when((col("juris_in_address") & ~col("country_in_address")), lit(True))
                .when((~col("juris_in_address") & col("country_in_address")), lit(True))
                .when((col("juris_in_address") & col("country_in_address")), lit(True))
                .otherwise(col("is_correct")),
            )
            .withColumn(
                "jurisdiction",
                when(
                    (~col("juris_in_address") & col("country_in_address")),
                    col("country_name"),
                ).otherwise(col("jurisdiction")),
            )
            .withColumn(
                "country_name",
                when(
                    (col("juris_in_address") & ~col("country_in_address")),
                    col("jurisdiction"),
                ).otherwise(col("country_name")),
            )
            .drop("juris_in_address", "country_in_address")
        )
        return df.withColumn(
            "is_correct",
            when(
                (col("jurisdiction") == col("country_name"))
                & (col("is_correct") == False),
                (lit(True)),
            ).otherwise(col("is_correct")),
        )

    def transform(self):
        df = self.input_dataframe_dict["1tm_2412.company"].dataframe
        # df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)

        df_select = df.limit(1000000)
        df_select.withColumn("original_jurisdiction", col("jurisdiction")).withColumn(
            "original_country_name", col("country_name")
        )

        df_resource = self.input_dataframe_dict["jurisdiction"].dataframe
        for column_name in df_resource.columns:
            df_resource = df_resource.withColumnRenamed(
                column_name, f"res_{column_name}"
            )
        df_resource = df_resource.withColumn(
            "last_part",
            split(col("res_jurisdiction"), "\\.").getItem(
                size(split(col("res_jurisdiction"), "\\.")) - 1
            ),
        )

        df_process = self.fill_jurisdiction(df_select)
        df_process = self.join_data(df_process, df_resource)
        df_process = self.validate_data(df_process)
        df_process = self.process_jurisdiction(df_process)
        return df_process


def run(env="pro", params={}, spark=None):
    table_meta_link_position = TableMeta(
        from_files="metas/bronze/data1tm_2412/company_jurisdiction.yaml", env=env
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
    # from delta import *
    # from pyspark.sql import SparkSession
    # builder = (
    #     SparkSession.builder.appName("MyApp")
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config(
    #         "spark.sql.catalog.spark_catalog",
    #         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     )
    # )

    # spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # spark = (
    #     SparkSession.builder.appName("pytest")
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config(
    #         "spark.sql.catalog.spark_catalog",
    #         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     )
    #     .getOrCreate()
    # )
    run(
        env="test",
        params={"dv_source_version": "init"},
    )
