from pyspark.sql import DataFrame, functions as F
import time
import json
import os
from dataclasses import dataclass
from .boto3 import delete_s3_folder_with_spark_sesion


def add_pure_company_name_regex(df, column_name="name"):
    legal_forms = json.load(
        open(
            os.path.join("resources", "abbreviation_suffixes.json"),
            "r",
            encoding="utf-8",
        )
    )
    all_suffixes, all_prefixes = [], []
    for category in legal_forms:
        all_suffixes.extend(category["pure_suffix"])
        all_prefixes.extend(category.get("pure_prefix", []))
    all_suffixes = [i for i in all_suffixes if i != ""]
    all_prefixes = [i for i in all_prefixes if i != ""]

    result_col_name = f"regexed_{column_name}"
    non_special_col = f"pure_{column_name}"

    sorted_list_suffixes = sorted(list(set(all_suffixes)), key=lambda x: (-len(x), x))
    sorted_list_prefixes = sorted(list(set(all_prefixes)), key=lambda x: (-len(x), x))

    # Tạo pattern regex với điều kiện có space trước suffix
    suffix_pattern = "|".join(f" {suffix.lower()}$" for suffix in sorted_list_suffixes)
    prefix_pattern = "|".join(f"^{prefix.lower()} " for prefix in sorted_list_prefixes)

    result_df = (
        df.withColumn(
            result_col_name,
            F.regexp_replace(F.col(non_special_col), suffix_pattern, ""),
        )
        .withColumn(
            "pure_name_replaced_by",
            F.expr(
                f"case when {result_col_name} != {non_special_col} then 'suffix' else null end"
            ),
        )
        .withColumn(
            result_col_name,
            F.expr(
                f"""case when pure_name_replaced_by is null
                    then regexp_replace({non_special_col}, '{prefix_pattern}', '')
                    else {result_col_name} end
                """
            ),
        )
        .withColumn(
            "pure_name_replaced_by",
            F.expr(
                f"""case when {result_col_name} <> {non_special_col}
                        and pure_name_replaced_by is null then 'prefix'
                    else pure_name_replaced_by end"""
            ),
        )
    )
    return result_df


# ---------------------------pure_name-------------------------------------------- #


# def regexed_name_verify(trusted_company_df: DataFrame, to_verify_df: DataFrame):
#     df = (
#         to_verify_df.alias("t")
#         .join(
#             trusted_company_df.alias("m"),
#             (F.col("t.jurisdiction") == F.col("m.jurisdiction"))
#             & (F.col("t.regexed_name") == F.col("m.regexed_name")),
#             "inner",
#         )
#         .selectExpr(
#             *VerificationConfig.SELECTED_COLUMNS,
#             *VerificationConfig.VERIFIED_COLUMNS,
#             "'regexed_name' as verify_method",
#         )
#     )
#     return df


# ---------------------------regexed_name-------------------------------------------- #


@dataclass
class VerificationConfig:
    SELECTED_COLUMNS = [
        "t.jurisdiction",
        "t.dv_hashkey_company",
        "t.dv_recsrc",
        "t.dv_source_version",
        "t.registration_number",
        "t.name",
        "t.pure_name",
        "t.regexed_name",
    ]
    VERIFIED_COLUMNS = [
        "v.dv_hashkey_company as verified_dv_hashkey_company",
        "v.registration_number as verified_registration_number",
        "v.regexed_name as verified_regexed_name",
        "v.name as verified_name",
    ]
    ROW_NUMBER_JURIS_REGEXED_NAME = """
    ROW_NUMBER() OVER (
        PARTITION BY t.jurisdiction, t.regexed_name
        ORDER BY
            CASE WHEN regexp_replace(t.name, '[^a-zA-Z0-9&,. ]', '') = t.name THEN 0 ELSE 1 END,
            length(t.name) DESC
    )
    """

    ROW_NUMBER_JURIS_REGIS_REGEXED_NAME = """
    ROW_NUMBER() OVER (
        PARTITION BY jurisdiction, registration_number, regexed_name
        ORDER BY
            CASE WHEN registration_number IS NULL THEN 1 ELSE 0 END,
            -- Check special characters (exclude &,.)
            CASE WHEN regexp_replace(name, '[^a-zA-Z0-9&,. ]', '') = name THEN 0 ELSE 1 END,
            -- Length of name (DESC để tên dài hơn được ưu tiên)
            length(name) DESC
    )
    """


def verify_regex_name_has_regis_vs_has_regis(df):
    df.createOrReplaceTempView("base_df_has_regis")
    spark = df.sparkSession
    spark.sql(
        f"""
    SELECT
        name,
        jurisdiction,
        regexed_name,
        registration_number,
        dv_hashkey_company,
        {VerificationConfig.ROW_NUMBER_JURIS_REGIS_REGEXED_NAME} as row_num
    FROM base_df_has_regis
    -- ORDER BY jurisdiction, registration_number, regexed_name, row_num
    """
    ).createOrReplaceTempView("tbl_row_num")

    return spark.sql(
        f"""
    select
        {",".join(VerificationConfig.SELECTED_COLUMNS)},
        {",".join(VerificationConfig.VERIFIED_COLUMNS)},
        case when t.name <> v.name then 'regexed_name_has_regis_vs_has_regis'
            else null end as verify_method
    from base_df_has_regis t
    left join tbl_row_num v
        on t.jurisdiction = v.jurisdiction
        and t.registration_number = v.registration_number
        and t.regexed_name = v.regexed_name
    where v.row_num = 1
    -- ORDER BY t.jurisdiction,  t.registration_number,  t.regexed_name
    """
    )


def verify_regex_name_no_regis_vs_no_regis(df):
    spark = df.sparkSession
    df.createOrReplaceTempView("base_df_no_regis")
    spark.sql(
        f"""
    SELECT
        *,
        {VerificationConfig.ROW_NUMBER_JURIS_REGEXED_NAME} as row_num
    FROM base_df_no_regis t
    ORDER BY jurisdiction, regexed_name, row_num
    """
    ).createOrReplaceTempView("tbl_row_num_no_regis")

    return spark.sql(
        f"""
    select
        {",".join(VerificationConfig.SELECTED_COLUMNS)},
        {",".join(VerificationConfig.VERIFIED_COLUMNS)},
    case when t.name <> v.name then 'regexed_name_no_regis_vs_no_regis' else null end as verify_method
    from base_df_no_regis t
    left join tbl_row_num_no_regis v
        on t.jurisdiction = v.jurisdiction
        and t.regexed_name = v.regexed_name
        and v.row_num = 1
    -- ORDER BY jurisdiction, regexed_name
    """
    )


def verify_regex_name_no_regis_vs_has_regis(
    trusted_company_df: DataFrame, to_verify_df: DataFrame
):

    df = (
        to_verify_df.alias("t")
        .join(
            trusted_company_df.alias("v"),
            (F.col("t.jurisdiction") == F.col("v.jurisdiction"))
            & (F.col("t.regexed_name") == F.col("v.regexed_name")),
            "inner",
        )
        .withColumn("row_num", F.expr(VerificationConfig.ROW_NUMBER_JURIS_REGEXED_NAME))
        .where("row_num = 1")
        .selectExpr(
            *VerificationConfig.SELECTED_COLUMNS,
            *VerificationConfig.VERIFIED_COLUMNS,
            "'regexed_name_no_regis_vs_has_regis' as verify_method",
        )
    )
    return df


def verify_regex_name_flow(df, tmp_path=None):
    rs_df_has_regis = verify_regex_name_has_regis_vs_has_regis(
        df.where("registration_number is not null")
    )
    # rs_df_has_regis.show(5, False)

    rs_df_no_regis = verify_regex_name_no_regis_vs_no_regis(
        df.where("registration_number is null")
    )

    if tmp_path:
        from .boto3 import delete_s3_folder_with_spark_sesion

        spark = df.sparkSession
        delete_s3_folder_with_spark_sesion(spark, tmp_path)
        tmp_path_has_regis = tmp_path + "/has_regis"
        tmp_path_no_regis = tmp_path + "/no_regis"
        (
            rs_df_has_regis.write.mode("overwrite")
            .format("delta")
            .save(tmp_path_has_regis)
        )
        (rs_df_no_regis.write.mode("overwrite").format("delta").save(tmp_path_no_regis))
        rs_df_has_regis = spark.read.format("delta").load(tmp_path_has_regis)
        rs_df_no_regis = spark.read.format("delta").load(tmp_path_no_regis)
    # rs_df_no_regis.show(10, False)

    rs_no_vs_has = verify_regex_name_no_regis_vs_has_regis(
        rs_df_has_regis.where("verify_method is null"),  # k bi verify - thang
        rs_df_no_regis.where("verify_method is null"),  # k bi verify - thang
    )
    # rs_no_vs_has.show(10, False)

    rs_no_regis_final = (
        rs_df_no_regis.alias("t")
        .join(
            rs_no_vs_has.alias("n"),
            F.col("t.verified_dv_hashkey_company") == F.col("n.dv_hashkey_company"),
            "left",
        )
        .selectExpr(
            *VerificationConfig.SELECTED_COLUMNS,
            "coalesce(n.verified_dv_hashkey_company, t.verified_dv_hashkey_company) as verified_dv_hashkey_company",
            "coalesce(n.verified_registration_number, t.verified_registration_number) as verified_registration_number",
            "coalesce(n.verified_regexed_name, t.verified_regexed_name) as verified_regexed_name",
            "coalesce(n.verified_name, t.verified_name) as verified_name",
            """
            CASE
                WHEN t.verified_dv_hashkey_company is not null
                    and n.verified_dv_hashkey_company is not null
                THEN 'regexed_name_cross_no_regis_vs_has_regis'
                WHEN t.verified_dv_hashkey_company is not null
                    and n.verified_dv_hashkey_company is null
                THEN t.verify_method
                ELSE null
            END as verify_method
            """,
        )
    )
    return rs_df_has_regis.unionAll(rs_no_regis_final)


# ---------------------------levenshtein------------------------------------------ #
def prepare_to_join_levenshtein(
    df: DataFrame, base_col: str = "pure_name", num_firt_chars: int = 6
) -> DataFrame:

    if base_col not in df.columns:
        raise Exception(f"{base_col} column must exist in DataFrame")

    first_char_col = f"{base_col}_first_char"
    space_count_col = f"{base_col}_space_count"
    length_col = f"{base_col}_length"

    df = (
        df.withColumn(
            first_char_col,
            F.lower(F.substring(F.col(base_col), 1, num_firt_chars)),
        )
        .withColumn(space_count_col, F.size(F.split(base_col, " ")) - 1)
        .withColumn(length_col, F.length(F.col(base_col)))
    )
    return df


def levenshtein_verify(
    trusted_company_df: DataFrame,
    to_verify_df: DataFrame,
    base_col: str = "pure_name",
    verify_method: str = "levenshtein",
    threshold: int = 95,
    num_firt_chars: int = 6,
    num_diff_length: int = 3,
    num_diff_space: int = 3,
    tmp_path: str = None,
):
    """
    Pre-check conditions:
    - Same jurisdiction
    - Same first character

    trusted_company_df = hub_company: -> filter registration_number is not null
    - dv_hashkey_company
    - jurisdiction
    - registration_number
    - name
    - pure_name

    to_verify_df = hub_company: -> filter registration_number is null
    - dv_hashkey_company
    - jurisdiction
    - registration_number
    - name
    - pure_name
    - dv_recsrc
    """

    trusted_company_df = prepare_to_join_levenshtein(
        trusted_company_df, base_col, num_firt_chars
    )
    to_verify_df = prepare_to_join_levenshtein(to_verify_df, base_col, num_firt_chars)
    first_char_col = f"{base_col}_first_char"
    space_count_col = f"{base_col}_space_count"
    length_col = f"{base_col}_length"

    df_join = (
        to_verify_df.alias("t")
        .join(
            trusted_company_df.alias("m"),
            (F.col("t.jurisdiction") == F.col("m.jurisdiction"))
            & (F.col(f"t.{first_char_col}") == F.col(f"m.{first_char_col}"))
            & (
                F.abs(F.col(f"t.{length_col}") - F.col(f"m.{length_col}"))
                <= num_diff_length
            )
            & (
                F.abs(F.col(f"t.{space_count_col}") - F.col(f"m.{space_count_col}"))
                <= num_diff_space
            ),
            "left",
        )
        .withColumn(
            "max_length",
            F.greatest(
                F.length(F.col(f"m.{base_col}")),
                F.length(F.col(f"t.{base_col}")),
            ),
        )
        .selectExpr(
            "t.jurisdiction",
            "t.dv_hashkey_company",
            "t.registration_number",
            "t.name",
            f"t.{base_col}",
            "t.dv_recsrc",
            "t.dv_source_version",
            "m.dv_hashkey_company as verified_dv_hashkey_company",
            f"m.{base_col} as verified_{base_col}",
            "m.registration_number as verified_registration_number",
            "m.name as verified_name",
            "max_length",
        )
    )

    if tmp_path:
        start_time = time.time()
        spark = df_join.sparkSession
        tmp_path = "s3a://lakehouse-silver/tmp/company_levenshtein_prepare"
        delete_s3_folder_with_spark_sesion(spark, tmp_path)
        df_join.write.mode("overwrite").format("delta").save(tmp_path)
        end_time = time.time()
        execution_time = (end_time - start_time) / 60
        print(f"write to {tmp_path} took {execution_time:.2f} minutes to execute")
        df_join = spark.read.format("delta").load(tmp_path)

    rs_df = (
        df_join.withColumn(
            "distance",
            F.levenshtein(F.col(f"verified_{base_col}"), F.col(base_col)),
        )
        .withColumn(
            "similarity_percentage",
            (1 - F.col("distance") / F.col("max_length")) * 100,
        )
        .withColumn("verify_method", F.lit(verify_method))
        .where(f"similarity_percentage >= {threshold}")
    )
    # rs_df = rs_df.repartition(
    #     spark.sparkContext.defaultParallelism, "dv_hashkey_company"
    # )
    (
        rs_df.withColumn(
            "row_number",
            F.expr(
                "ROW_NUMBER() OVER (PARTITION BY dv_hashkey_company ORDER BY similarity_percentage desc)"
            ),
        )
        .where("row_number = 1")
        .drop("row_number")
    )
    return rs_df
