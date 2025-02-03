from pyspark.sql import functions as F

from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta


class Executer(BronzeExecuter):
    def transform(self):
        industry = self.input_dataframe_dict["1tm_2412.industry"].dataframe
        company = self.input_dataframe_dict["bronze.company"].dataframe
        company_industry = self.input_dataframe_dict[
            "1tm_2412.company_industry"
        ].dataframe

        industry = (
            industry.alias("i")
            .join(
                company_industry.alias("ci"),
                industry.id == company_industry.industry_id,
                "inner",
            )
            .join(
                company.select("id", "industry_country_code").alias("c"),
                company_industry.company_id == company.id,
                "inner",
            )
            .withColumn(
                "country_code",
                F.when(
                    F.col("country_code").isNull(), F.col("industry_country_code")
                ).otherwise(F.col("country_code")),
            )
        ).select("i.*", "country_code")
        return industry


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(
        from_files="metas/bronze/data1tm_2412/industry.yaml", env=env
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
