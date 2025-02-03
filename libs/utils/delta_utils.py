from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import time
from functools import wraps
from typing import Callable
from pprint import pprint
from datetime import datetime
import pytz


def timing_decorator(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = (end_time - start_time) / 60
        print(f"Function {func.__name__} took {execution_time:.2f} minutes to execute")
        return result

    return wrapper


def init_delta_folder_with_schema(
    spark,
    data_location: str,
    struct_type: StructType,
    partition_by: list = [],
    options={},
):

    if DeltaTable.isDeltaTable(spark, data_location):
        return False
    if not partition_by:
        partition_by = []

    _df = spark.createDataFrame([], struct_type)
    (
        _df.write.mode("overwrite")
        .format("delta")
        .partitionBy(*partition_by)
        .options(**options)
        .save(data_location)
    )
    return True


def merge_df_with_schema(
    df: DataFrame, schema: StructType, add_newcols: bool = False
) -> DataFrame:
    """force schema"""

    # for col in df.columns:
    for field in schema.fields:
        if str(field.name) in df.columns:
            df = df.withColumn(
                str(field.name), F.col(str(field.name)).cast(field.dataType)
            )
        else:
            df = df.withColumn(str(field.name), F.lit(None).cast(field.dataType))

    if add_newcols:
        adds = list(set(df.columns) - set(schema.fieldNames()))
        return df.select(list(schema.fieldNames()) + adds)
    return df.select(list(schema.fieldNames()))


@timing_decorator
def delta_insert(
    spark,
    df: DataFrame,
    data_location: str,
    partition_by: list = [],
    mode: str = "append",
    options: dict = {},
    max_retries: int = 2,
):
    """
    1, reformat update df with target schema if target table exists
    2, upsert by 1 column ! (hash_diff)
    """

    if DeltaTable.isDeltaTable(spark, data_location):
        delta_table = DeltaTable.forPath(spark, data_location)
        df = merge_df_with_schema(df, delta_table.toDF().schema)

    # Write to File with max_retries = 3
    attempt = 0
    delay = 5

    while attempt < max_retries:
        try:
            print(f"Attempt {attempt + 1}: Writing with mode {mode} to {data_location}")
            (
                df.write.format("delta")
                .mode(mode)
                .partitionBy(*partition_by)
                .option("mergeSchema", "true")
                # .option("delta.autoOptimize.optimizeWrite", "true")
                .options(**options)
                .save(data_location)
            )
            print(f"Successfully wrote to {data_location}")
            return True

        except Exception as e:
            attempt += 1
            print(
                f"Write failed on attempt {attempt}. Retrying in {delay} seconds... due to {e}"
            )
            if attempt < max_retries:
                time.sleep(delay)
            else:
                print(f"Write failed after {max_retries} attempts. Raising exception.")
                raise e


def print_last_delta_last_operation(delta_table):

    data = (
        delta_table.history()
        .limit(1)
        .select("version", "timestamp", "operationParameters", "operationMetrics")
        .toPandas()
        .to_dict(orient="records")[0]
    )

    try:
        data["operationMetrics"] = {
            k: v for k, v in data["operationMetrics"].items() if v not in (0, "0", "[]")
        }
        data["operationParameters"] = {
            k: v
            for k, v in data["operationParameters"].items()
            if v not in (0, "0", "[]")
        }

    except Exception as e:
        print(e)

    current_gmt = datetime.now(pytz.timezone("GMT"))
    if (current_gmt.timestamp() - data["timestamp"].timestamp()) / 60 <= 2:
        pprint(data)
        print("-" * 60)


@timing_decorator
def delta_upsert(
    spark,
    df: DataFrame,
    data_location: str,
    base_col: str,
    struct_type: StructType,
    partition_by: list = [],
):
    """
    1, create target table
    2, reformat update df with target schema
    3, upsert by 1 column ! (hash_diff)
    """

    init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)

    delta_table = DeltaTable.forPath(spark, data_location)
    update_df = merge_df_with_schema(df, struct_type)

    # spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "cycle-end")
    delta_table.alias("base").merge(
        update_df.alias("updates"), f"base.{base_col} = updates.{base_col}"
    ).whenNotMatchedInsertAll().execute()


@timing_decorator
def delta_insert_for_hubnlink(
    spark,
    df: DataFrame,
    data_location: str,
    base_cols: list,
    struct_type: StructType,
    partition_by: list = [],
):
    """
    1, create target table
    2, reformat update df with target schema
    """

    df = (
        df.withColumn(
            "row_number",
            F.expr(
                f"""
            ROW_NUMBER() OVER (
                PARTITION BY {', '.join(base_cols)}
                ORDER BY null
            )"""
            ),
        )
        .where("row_number = 1")
        .drop("row_number")
    )
    if not struct_type:
        struct_type = df.schema
        print("Warning: struct_type missing")

    # init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)
    if not DeltaTable.isDeltaTable(spark, data_location):
        init_delta_folder_with_schema(
            spark,
            data_location,
            struct_type,
            partition_by,
            {"delta.enableChangeDataFeed": "true"},
        )

    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    delta_table = DeltaTable.forPath(spark, data_location)
    update_df = merge_df_with_schema(df, struct_type)

    merge_conditions = " and ".join([f"base.{c} = updates.{c}" for c in base_cols])
    # spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "cycle-end")
    try:
        print("Merge_conditions:", merge_conditions)
        (
            delta_table.alias("base")
            .merge(update_df.alias("updates"), merge_conditions)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Successfully wrote to {data_location}")
        print_last_delta_last_operation(delta_table)

    except Exception as e:
        delta_table.vacuum()  # Clean up
        raise e


@timing_decorator
def delta_upsert_for_hubnlink(
    spark,
    df: DataFrame,
    data_location: str,
    base_cols: list,
    struct_type: StructType,
    partition_by: list = [],
):
    """
    1, create target table
    2, reformat update df with target schema
    """

    df = (
        df.withColumn(
            "row_number",
            F.expr(
                f"""
            ROW_NUMBER() OVER (
                PARTITION BY {', '.join(base_cols)}
                ORDER BY null
            )"""
            ),
        )
        .where("row_number = 1")
        .drop("row_number")
    )

    if not struct_type:
        struct_type = df.schema
        print("Warning: struct_type missing")

    # init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)
    if not DeltaTable.isDeltaTable(spark, data_location):
        init_delta_folder_with_schema(
            spark,
            data_location,
            struct_type,
            partition_by,
            {"delta.enableChangeDataFeed": "true"},
        )

    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    delta_table = DeltaTable.forPath(spark, data_location)
    update_df = merge_df_with_schema(df, struct_type)

    merge_conditions = " and ".join([f"base.{c} = updates.{c}" for c in base_cols])
    # spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "cycle-end")

    columns = delta_table.toDF().columns
    update_dict = {col: F.col("updates.{col}") for col in columns}

    try:
        print("Merge_conditions:", merge_conditions)
        (
            delta_table.alias("base")
            .merge(update_df.alias("updates"), merge_conditions)
            .whenMatchedUpdate(condition=F.col("updates.") == "M", set=update_dict)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Successfully wrote to {data_location}")
        print_last_delta_last_operation(delta_table)
    except Exception as e:
        delta_table.vacuum()  # Clean up
        raise e


@timing_decorator
def delta_upsert_for_gold(
    spark,
    df: DataFrame,
    data_location: str,
    base_cols: list,
    struct_type: StructType,
    partition_by: list = [],
):
    """
    1, create target table
    2, reformat update df with target schema
    """
    df = (
        df.withColumn(
            "row_number",
            F.expr(
                f"""
            ROW_NUMBER() OVER (
                PARTITION BY {', '.join(base_cols)}
                ORDER BY null
            )"""
            ),
        )
        .where("row_number = 1")
        .drop("row_number")
    )

    # init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)
    if not DeltaTable.isDeltaTable(spark, data_location):
        init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)

    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("delta.enableChangeDataFeed", "true")
    delta_table = DeltaTable.forPath(spark, data_location)
    update_df = merge_df_with_schema(df, struct_type)

    hashdiff_list = [
        f"cast(coalesce({col}, null) as string)"
        for col in update_df.columns
        if not col.startswith("dv_") and col != "created_at" and col != "updated_at"
    ]
    update_df = update_df.withColumn(
        "dv_hashdiff", F.expr(f"md5(concat_ws(';', {','.join(hashdiff_list)}))")
    )
    merge_conditions = " and ".join([f"base.{c} = updates.{c}" for c in base_cols])
    # spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "cycle-end")
    try:
        print(
            "Merge_conditions:",
            merge_conditions + " and base.dv_hashdiff != updates.dv_hashdiff",
        )
        (
            delta_table.alias("base")
            .merge(
                update_df.alias("updates"),
                merge_conditions + " and base.dv_hashdiff != updates.dv_hashdiff",
            )
            .whenMatchedUpdate(
                set={
                    f"base.{col}": f"updates.{col}"
                    for col in update_df.columns
                    if col != "created_at"
                }
            )
            # .whenNotMatchedInsertAll()
            # .option("mergeSchema", "true")
            .execute()
        )
        print("Merge_conditions:", merge_conditions)

        (
            delta_table.alias("base")
            .merge(update_df.alias("updates"), merge_conditions)
            .whenNotMatchedInsertAll()
            # .option("mergeSchema", "true")
            .execute()
        )

        print(f"Successfully wrote to {data_location}")
    except Exception as e:
        delta_table.vacuum()  # Clean up
        raise e


@timing_decorator
def delta_write_for_sat(
    spark,
    df: DataFrame,
    data_location: str,
    base_cols: list,
    struct_type: StructType,
    partition_by: list = [],
):
    """
    1, create target table
    2, reformat update df with target schema
    3, upsert by 1 column ! (hash_diff)
    """

    update_df = merge_df_with_schema(df, struct_type)
    hashdiff_list = [
        f"cast(coalesce({col}, null) as string)"
        for col in update_df.columns
        if not col.startswith("dv_")
    ]
    # update_df = merge_df_with_schema(update_df, struct_type)
    update_df = update_df.withColumn(
        "dv_hashdiff", F.expr(f"md5(concat_ws(';', {','.join(hashdiff_list)}))")
    )
    update_df = (
        update_df.withColumn(
            "row_number",
            F.expr("ROW_NUMBER() OVER (PARTITION BY dv_hashdiff ORDER BY null)"),
        )
        .where("row_number = 1")
        .drop("row_number")
    )

    if not DeltaTable.isDeltaTable(spark, data_location):
        print("delta insert for the fisrt time")
        (
            update_df.write.format("delta")
            .mode("append")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .option("delta.enableChangeDataFeed", "true")
            # .option("delta.autoOptimize.autoCompact", "true")
            .partitionBy(*partition_by)
            .save(data_location)
        )
        return True

    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    delta_table = DeltaTable.forPath(spark, data_location)
    # .option("delta.autoOptimize.autoCompact", "true")
    # spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "cycle-end")
    merge_conditions = " and ".join([f"base.{c} = updates.{c}" for c in base_cols])
    try:
        print("Merge_conditions:", merge_conditions)
        (
            delta_table.alias("base")
            .merge(update_df.alias("updates"), merge_conditions)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Successfully wrote to {data_location}")
        print_last_delta_last_operation(delta_table)
        return True
    except Exception as e:
        delta_table.vacuum()  # Clean up
        raise e


def delta_write_for_scd2sat(
    spark,
    df: DataFrame,
    data_location: str,
    base_cols: list,
    struct_type: StructType,
    partition_by: list = [],
    hashdiff: str = "dv_hashdiff",
    mode: str = "append",
    options: dict = {},
):
    """
    1, create target table
    2, reformat update df with target schema
    3, upsert by 1 column ! (hash_diff)
    """
    init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)

    delta_table = DeltaTable.forPath(spark, data_location)
    update_df = merge_df_with_schema(df, struct_type)

    hashdiff_list = [
        f"cast(coalesce({col}, null) as string)"
        for col in df.columns
        if not col.startswith("dv_")
    ]
    update_df = update_df.withColumn(
        "dv_hashdiff", F.expr(f"md5(concat_ws(';', {','.join(hashdiff_list)}))")
    )
    update_df = update_df.join(delta_table.toDF(), on=hashdiff, how="leftanti")
    merge_conditions = " and ".join([f"base.{c} = updates.{c}" for c in base_cols])
    if update_df.count() > 0:
        delta_table.alias("base").merge(
            update_df.alias("updates"),
            f"{merge_conditions} and base.{hashdiff} != updates.{hashdiff} and base.dv_status = 1",
        ).whenMatchedUpdate(
            set={
                "dv_status": "0",
                "dv_valid_to": F.expr(
                    "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1)"
                ),
            }
        ).execute()
        (
            update_df.write.format("delta")
            .mode(mode)
            .partitionBy(*partition_by)
            .options(**options)
            .save(data_location)
        )


@timing_decorator
def delta_upsert_for_bridge_key(
    spark,
    df: DataFrame,
    data_location: str,
    base_cols: list,
    struct_type: StructType,
    partition_by: list = [],
):
    """
    1, create target table
    2, reformat update df with target schema
    """

    df = (
        df.withColumn(
            "row_number",
            F.expr(
                f"""
            ROW_NUMBER() OVER (
                PARTITION BY {', '.join(base_cols)}
                ORDER BY null
            )"""
            ),
        )
        .where("row_number = 1")
        .drop("row_number")
    )

    # init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)
    if not DeltaTable.isDeltaTable(spark, data_location):
        init_delta_folder_with_schema(spark, data_location, struct_type, partition_by)

    # spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    delta_table = DeltaTable.forPath(spark, data_location)
    # old_df = (
    #     spark.read.format("delta").load(data_location).where("from_key is not null")
    # )
    update_df = merge_df_with_schema(df, struct_type)
    # new_rows = update_df.join(
    #     old_df, on=["from_key", "to_key", "jurisdiction"], how="left_anti"
    # )
    merge_conditions = " and ".join([f"base.{c} = updates.{c}" for c in base_cols])
    try:
        (
            delta_table.alias("base")
            .merge(update_df.alias("updates"), merge_conditions)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Successfully wrote to {data_location}")
        print_last_delta_last_operation(delta_table)
    except Exception as e:
        delta_table.vacuum()  # Clean up
        raise e


def summarize_df(df):
    # Total row count
    total_rows = df.count()

    # Count nulls for each column and get data types
    summary_data = []
    for column_name, column_type in df.dtypes:
        null_count = df.filter(F.col(column_name).isNull()).count()
        summary_data.append(
            {
                "column_name": column_name,
                "null_count": null_count,
                "total_rows": total_rows,
                "column_type": column_type,
            }
        )

    # Create a summary DataFrame
    schema = StructType()
    schema.add("column_name", StringType(), True)
    schema.add("null_count", IntegerType(), True)
    schema.add("count", IntegerType(), True)
    schema.add("data_type", StringType(), True)

    return spark.createDataFrame(summary_data, schema)
