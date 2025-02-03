from pyspark.sql import functions as F
from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    clean_jurisdiciton_1tm,
    add_load_date_columns,
    add_pure_company_name,
)


class Executer(BronzeExecuter):
    def transform(self):
        source = self.input_dataframe_dict["1tm_2412.company"]
        df = source.dataframe
        # try:
        #     parallelism = self.spark.sparkContext.defaultParallelism
        #     company_df = company_df.repartition(parallelism)
        # except Exception as e:
        #     print(e)

        # df = clean_column_names(df)
        # df = clean_string_columns(df)
        # df = clean_jurisdiciton_1tm(self.spark, df) # do it at raw
        bucket = source.data_location.replace("s3a://", "").split("/", 1)[0]
        tmp_path = source.data_location.replace(bucket, bucket + "/tmp", 1)
        df = add_pure_company_name(df, "name", tmp_path)
        # df = add_pure_company_name(df, "name")
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)

        import pandas as pd

        lookup_jurisdiction = self.spark.createDataFrame(
            pd.read_csv("resources/lookup_jurisdiction.csv", na_filter=False)
        )
        # lookup_jurisdiction = self.spark.read.format('csv').load(
        #     'resources/lookup_jurisdiction.csv',
        #     header=True
        # )

        df = (
            df.alias("c")
            .join(F.broadcast(lookup_jurisdiction).alias("l"), "jurisdiction", "left")
            .selectExpr(
                "c.*",
                "l.country_code as lookup_country_code",
                "l.country_name as lookup_country_name",
            )
            .withColumn("country_code", F.col("lookup_country_code"))
            .withColumn("country_name", F.col("lookup_country_name"))
            .withColumn(
                "jurisdiction",
                F.expr(
                    """
                case when jurisdiction in ('Global', 'European Union')  then 'unspecified'
                when jurisdiction is null then 'unspecified'
                else jurisdiction end
                """
                ),
            )
            .drop("lookup_country_code", "lookup_country_name")
        )

        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(from_files="metas/bronze/data1tm_2412/company.yaml", env=env)

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
