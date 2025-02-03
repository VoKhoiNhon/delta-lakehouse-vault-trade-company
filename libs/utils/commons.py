import re
import json
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import os
from typing import Callable
import traceback


def str_to_snake_case(name: str) -> str:
    name = name.strip()
    name = re.sub("\\s+", "_", name)
    name = re.sub("\\.", "_", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    name = name.lower()
    return name


def clean_column_names(df: DataFrame):
    return df.toDF(*[str_to_snake_case(col) for col in df.columns])


def clean_string_columns(df: DataFrame) -> DataFrame:
    print(df.dtypes)

    for column_name, column_type in df.dtypes:
        # Strip whitespace and convert to snake case
        # Skip if not a string column
        if column_type != "string":
            continue

        #     # Replace newlines, tabs with spaces
        df = df.withColumn(
            column_name,
            F.regexp_replace(F.col(column_name), r"[\n\r\t]", " "),
        )

        #     # Remove double commas
        df = df.withColumn(
            column_name, F.regexp_replace(F.col(column_name), r", ,", ",")
        )

        #     # Add space after punctuation if not present
        # df = df.withColumn(new_column_name, F.regexp_replace(f.col(new_column_name), r"([.,!?])([^\s])", r"$1 $2"))
        #     # Replace multiple punctuation with single
        df = df.withColumn(
            column_name,
            F.regexp_replace(F.col(column_name), r"([.,!?;:])\1+", r"$1"),
        )

        #     # Remove double commas again
        df = df.withColumn(
            column_name,
            F.regexp_replace(F.col(column_name), r",,", r","),
        )

        #     # Remove spaces before punctuation
        df = df.withColumn(
            column_name,
            F.regexp_replace(F.col(column_name), r"\s+([.,!?;:])", r"$1"),
        )

        #     # Replace multiple spaces with single space
        df = df.withColumn(
            column_name,
            F.regexp_replace(F.col(column_name), r"\s{2,}", " "),
        )

        #     # Trim whitespace
        df = df.withColumn(column_name, F.trim(F.col(column_name)))

        #     # Replace empty strings with None
        df = df.withColumn(
            column_name,
            F.when(F.col(column_name) == "", None).otherwise(F.col(column_name)),
        )

    return df


def add_pure_company_name(df, name="name", return_col="pure_name"):
    df = df.withColumn(
        return_col,
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.trim((F.lower(F.col(name)))),
                    r"[^\p{L}\p{N}\s]",
                    "",  # xóa bỏ ký tự đặc biệt
                ),
                r"\s+",
                " ",  # chuyển nhiều khoảng trắng thành 1 khoảng trắng duy nhất
            )
        ),
    )
    return df


def clean_jurisdiciton_1tm(spark, df):
    """
    df must have jurisdiction and country_name
    """

    required_cols = ["jurisdiction", "country_name"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if len(missing_cols) > 0:
        raise ValueError(
            f"Required columns {missing_cols} are missing from the DataFrame. Available columns: {df.columns}"
        )

    # import pandas as pd
    # lookup_jurisdiction = spark.createDataFrame(
    #     pd.read_csv("resources/lookup_jurisdiction.csv")
    # )
    # verify_jurisdictions = spark.createDataFrame(
    #     pd.read_csv("resources/mapping_verify_jurisdictions.csv")
    # )

    lookup_jurisdiction = (
        spark.read.format("csv")
        .option("header", True)
        .load("resources/lookup_jurisdiction.csv")
    )

    verify_jurisdictions = (
        spark.read.format("csv")
        .option("header", True)
        .load("resources/mapping_verify_jurisdictions.csv")
    )

    df = (
        df.alias("c")
        .join(
            F.broadcast(lookup_jurisdiction).alias("l"),
            (
                F.col("l.jurisdiction").isNotNull()
                & (F.upper(F.col("l.country_name")) == F.upper(F.col("c.country_name")))
                & F.upper(F.col("l.jurisdiction")).contains(
                    F.upper(F.col("c.jurisdiction"))
                )  # Check if jurisdiction contains p_jurisdiction
            ),
            "left",
        )
        .join(
            F.broadcast(verify_jurisdictions).alias("v"),
            F.upper(F.col("c.jurisdiction")) == F.upper(F.col("v.jurisdiction")),
            "left",
        )
        .withColumn(
            "new_jurisdiction",
            F.expr(
                """
        case when l.jurisdiction is not null then l.jurisdiction
            when v.country is not null then v.country
            else Null end
        """
            ),
        )
        .selectExpr("c.*", "new_jurisdiction")
        .withColumn("jurisdiction", F.col("new_jurisdiction"))
        .drop("new_jurisdiction")
    )
    return df


def add_load_date_columns(df: DataFrame, date_value: str) -> DataFrame:
    df_with_date = df.withColumn("load_date", F.lit(date_value))
    return df_with_date


def format_date(df: DataFrame, column_name: str, from_format: str) -> DataFrame:
    if isinstance(df.schema[column_name].dataType, ArrayType):
        format_column = F.expr(
            f"transform({column_name}, x -> to_date(x, '{from_format}'))"
        )
    else:
        format_column = F.to_date(F.col(column_name), from_format)
    return df.withColumn(column_name, format_column)


def split_full_name(df: DataFrame, column_name: str) -> DataFrame:
    split_col = F.split(F.col(column_name), " ")
    num_parts = F.size(split_col)
    df = (
        df.withColumn("first_name", split_col.getItem(0))
        .withColumn("last_name", split_col.getItem(num_parts - 1))
        .withColumn(
            "middle_name",
            F.when(
                num_parts > 3,
                F.concat_ws(" ", F.slice(split_col, 2, num_parts - 2)),
            )
            .when(num_parts > 2, split_col.getItem(1))
            .otherwise(None),
        )
    )
    return df


def convert_country_code_to_country_name(country_code: str):

    if not country_code:
        return None

    import pycountry

    try:
        # Lookup country name using alpha_2 code
        country = pycountry.countries.get(alpha_2=country_code.upper())
        return country.name if country else None
    except Exception as e:
        print(
            f"FAILED to convert Country Code {country_code} to Country Name due to error: {e}"
        )
        return None


def get_n_group_jurisdiction(df, n_group=8):
    df.createOrReplaceTempView("main_table")
    spark = df.sparkSession
    return (
        spark.sql(
            f"""
    with grouped_data AS (
        select jurisdiction as jurisdiction, count(1) as cnt
        from main_table
        group by jurisdiction
    ),
    params AS (
        SELECT
            sum(cnt) as total_cnt,
            sum(cnt)/{n_group} as target_group_sum
        FROM grouped_data
    ),
    running_sum AS (
        SELECT
            jurisdiction,
            cnt,
            sum(cnt) over (order by cnt) as running_sum,
            (SELECT target_group_sum FROM params) as target_group_sum
        FROM grouped_data
    ),
    grouped AS (
        SELECT
            *,
            CASE
                WHEN floor(running_sum/target_group_sum) + 1 > {n_group} THEN {n_group}
                ELSE floor(running_sum/target_group_sum) + 1
            END as group_number
        FROM running_sum
    )
    SELECT
        group_number,
        count(*) as num_jurisdictions,
        sum(cnt) as total_cnt,
        collect_list(jurisdiction) as jurisdictions,
        concat_ws(',', collect_list(concat('"', jurisdiction, '"'))) as jurisdictions_str
    FROM grouped
    GROUP BY group_number
    ORDER BY group_number
    """
        )
        .toPandas()
        .to_dict("records")
    )


def filter_df_with_jurisdiction(df: DataFrame, jurisdictions: list) -> DataFrame:
    if len(jurisdictions) < 10:
        print("Using isin filter for jurisdictions")
        juris_df = df.where(F.col("jurisdiction").isin(jurisdictions))
    else:
        print("Using join for jurisdictions")
        schema = StructType([StructField("jurisdiction", StringType(), True)])
        spark = df.sparkSession
        jurisdiction_df = spark.createDataFrame(
            [[j] for j in jurisdictions], schema=schema
        )
        juris_df = df.join(jurisdiction_df, "jurisdiction", "inner")

    return juris_df


def run_with_juris_splited(
    df: DataFrame, n_group: int, func: Callable, *args, **kwargs
):
    """
    Chạy function theo nhóm jurisdiction với khả năng retry và debug

    Args:
        df: DataFrame gốc
        n_group: Số nhóm cần chia
        func: Function cần chạy
        *args: Additional positional arguments cho func
        **kwargs: Additional keyword arguments cho func

    Returns:
        dict: Kết quả chạy với status và failed_jurisdictions nếu có
    """
    results = {
        "success_groups": [],
        "failed_groups": [],
        "current_args": [],
        "current_kwargs": {},
    }

    try:
        groups = get_n_group_jurisdiction(df, n_group)

        for group in groups:
            group_number = group["group_number"]
            total_cnt = group["total_cnt"]
            jurisdictions = group["jurisdictions"]

            print(
                f"\nProcessing group {group_number}/{n_group} with {total_cnt} records"
            )
            print(f"Jurisdictions: {jurisdictions}")

            try:
                # Lọc DataFrame theo jurisdictions
                juris_df = filter_df_with_jurisdiction(df, jurisdictions)
                # Lưu current arguments để retry
                results["current_args"] = [
                    f"filter_df_with_jurisdiction(df, {jurisdictions})"
                ] + list(args)
                results["current_kwargs"] = kwargs

                # Chạy function
                func(juris_df, *args, **kwargs)

                results["success_groups"].append(
                    {
                        "group_number": group_number,
                        "jurisdictions": jurisdictions,
                        "total_cnt": total_cnt,
                    }
                )

                print(f"Successfully processed group {group_number}")

            except Exception as e:
                error_info = {
                    "group_number": group_number,
                    "jurisdictions": jurisdictions,
                    "total_cnt": total_cnt,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }
                results["failed_groups"].append(error_info)

                print(f"\nError processing group {group_number}:")
                print(f"Error message: {str(e)}")
                print("\nTo retry this group, use these parameters:")
                print(f"Jurisdictions: {jurisdictions}")
                print("Function call example:")
                args_str = ", ".join([str(arg) for arg in args])
                kwargs_str = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                params_str = ", ".join(filter(None, [args_str, kwargs_str]))

                print(
                    f"\nfunc(filter_df_with_jurisdiction(df, [{jurisdictions}])), {params_str})"
                )

                # Tạo file log
                log_filename = f"failed_group_{group_number}_{len(jurisdictions)}_jurisdictions.json"
                with open(log_filename, "w") as f:
                    json.dump(error_info, f, indent=2)
                print(f"\nDetailed error info saved to: {log_filename}")

        return results

    except Exception as e:
        print(f"Error in main function: {str(e)}")
        print(traceback.format_exc())
        return results


# import time
# from pyspark.sql.functions import col,explode, split, lit, collect_list, struct
# import pyspark.sql.functions as F
# def func_s_person_address(df):
#     start_time = time.time()
#     df.groupBy("jurisdiction", "dv_hashkey_person").agg(
#         collect_list(
#             struct(
#                 col("type"),
#                 lit(None).cast("timestamp").alias("start_date"),
#                 lit(None).cast("timestamp").alias("end_date"),
#                 col("full_address"),
#                 col("street"),
#                 col("city"),
#                 col("region"),
#                 col("state"),
#                 col("province"),
#                 col("country_code"),
#                 col("postal_code"),
#                 col("latitude"),
#                 col("longitude"))
#         ).alias("addresses")
#     ).write.format("delta").mode("append").save("s3a://lakehouse-gold/tmp/s_person_address")
#     end_time = time.time()
#     execution_time = (end_time - start_time) / 60
#     print(f"Function took {execution_time:.2f} minutes to execute")
# s_company_address = spark.read.format("delta").load("s3a://lakehouse-silver/s_person_address")
# run_with_juris_splited(s_company_address, 10, func_s_person_address)
