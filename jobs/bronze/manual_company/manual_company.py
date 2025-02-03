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
        df = self.input_dataframe_dict["raw_manual_company"].dataframe
        df = clean_column_names(df)
        df = clean_string_columns(df)
        df = clean_jurisdiciton_1tm(self.spark, df)
        df = add_pure_company_name(df, "name")
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df.show(n=5, truncate=False)
        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(
        from_files="metas/bronze/manual_company/manual_company.yaml", env=env
    )

    import sys

    executer = Executer(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute(
        options={
            "delta.autoOptimize.optimizeWrite": "true"
            # "delta.autoOptimize.autoCompact": "true"
        }
    )


if __name__ == "__main__":

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
    run()
