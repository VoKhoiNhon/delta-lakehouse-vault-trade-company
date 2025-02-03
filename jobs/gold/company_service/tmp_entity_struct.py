from libs.utils.support import compare_schema_objects
from libs.utils.commons import run_with_juris_splited
import pandas as pd


def write_entity_struct(df):
    lookup_jurisdiction = spark.createDataFrame(
        pd.read_csv(
            "resources/lookup_jurisdiction_with_continents.csv", na_filter=False
        )
    ).createOrReplaceTempView("lookup_jurisdiction")

    entity_struct = df.select(
        col("id"),
        col("jurisdiction"),
        struct(*[col(c) for c in df.columns]).alias("entity"),
    ).createOrReplaceTempView("entity_struct")
    # .withColumnRenamed("id", "to_id").withColumnRenamed("entity", "to_entity")
    # to_entity_with_struct = entity_df.select(
    #     col("id").alias("to_id"),
    #     col("jurisdiction"),
    #     struct(*[col(c) for c in entity_df.columns if c != "to_id"]).alias("to_entity")
    # )
    # compare_schema_objects(entity_struct.schema, to_entity_with_struct.schema )
    #
    spark.sql(
        """
    select a.*, l.continent from entity_struct a
    left join lookup_jurisdiction l on a.jurisdiction = l.jurisdiction
    """
    ).withColumn(
        "continent",
        F.expr(
            """
        case when continent is not null then continent else 'unspecified' end
        """
        ),
    ).write.format(
        "delta"
    ).option(
        "mergeSchema", "true"
    ).option(
        "delta.autoOptimize.optimizeWrite", "true"
    ).partitionBy(
        "continent", "jurisdiction"
    ).mode(
        "append"
    ).save(
        "s3a://lakehouse-gold/company_service/entity_struct"
    )


import time

start_time = time.time()
df = spark.read.format("delta").load("s3a://lakehouse-gold/company_service/entity")
df = df.drop("dv_recsrc", "dv_loaddts", "dv_source_version")
run_with_juris_splited(df, 15, write_entity_struct)
end_time = time.time()
execution_time = (end_time - start_time) / 60
print(f"Function  took {execution_time:.2f} minutes to execute")
