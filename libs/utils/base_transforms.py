from typing import Any, Dict
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as F


def flatten_df(df: DataFrame) -> DataFrame:
    complex_fields = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StructType)
    ]

    if not complex_fields:
        return df

    field = complex_fields[0]
    expanded = [
        F.col(f"{field}.{sub_field.name}").alias(f"{field}_{sub_field.name}")
        for sub_field in df.schema[field].dataType.fields
    ]

    df = df.select(*[col for col in df.columns if col != field], *expanded)

    return flatten_df(df)


def transform_column_from_json(
    df: DataFrame,
    column_name: str,
    trans_dict: Dict[Any, Any],
    miss_value: bool = False,
) -> DataFrame:
    tran_map = F.create_map(
        *[x for k, v in trans_dict.items() for x in (F.lit(k), F.lit(v))]
    )
    new_key = [
        key[column_name]
        for key in df.select(column_name).distinct().collect()
        if key[column_name] and key[column_name] not in trans_dict.keys()
    ]
    if new_key and not miss_value:
        print(new_key)
        raise ValueError("The translation dictionary is not enough")
    df = df.withColumn(
        column_name,
        F.when(
            (F.col(column_name).isNotNull()) & (~F.col(column_name).isin(new_key)),
            tran_map[F.col(column_name)],
        ).otherwise(F.col(column_name)),
    )
    return df


def fill_space_null(df: DataFrame) -> DataFrame:
    df = df.select(
        [
            (
                F.when(F.trim(F.col(c.name)) == "", None)
                .otherwise(F.col(c.name))
                .alias(c.name)
                if isinstance(c.dataType, StringType)
                else F.col(c.name)
            )
            for c in df.schema.fields
        ]
    )
    return df


def update_hashkey_company(
    df_original, df_mapping, id_col="id", from_key_col="from_key", to_key_col="to_key"
) -> DataFrame:
    """
    Updates IDs in the original dataframe by mapping them to new IDs based on a mapping dataframe.
    If no mapping exists, keeps the original ID.

    Args:
        df_original: Original Spark dataframe containing IDs to be updated
        df_mapping: Mapping dataframe containing old->new ID mappings
        id_col: Column name containing IDs in original dataframe (default: 'id')
        from_key_col: Column name in mapping df containing old IDs (default: 'from_key')
        to_key_col: Column name in mapping df containing new IDs (default: 'to_key')

    Returns:
        Spark dataframe with updated IDs
    """

    # Optimize by selecting only needed columns from mapping df
    df_mapping_filtered = df_mapping.select(from_key_col, to_key_col)

    return (
        df_original.alias("original").join(
            df_mapping_filtered.alias("mapping"),
            F.col(f"original.{id_col}") == F.col(f"mapping.{from_key_col}"),
            "left",
        )
        # Replace old ID with new ID if mapping exists, otherwise keep original
        .withColumn(
            id_col,
            F.coalesce(F.col(f"mapping.{to_key_col}"), F.col(f"original.{id_col}")),
        )
        # Clean up mapping columns
        .drop(f"mapping.{from_key_col}", f"mapping.{to_key_col}")
    )


def agg_consolidate_value(
    df, group_cols, custom_rules=None, include_cols=[], exclude_cols=[]
):
    """
    Deduplication with custom rules for different column types

    Parameters:
    - df: Input DataFrame
    - group_cols: Columns to group by
    - custom_rules: Dictionary of custom aggregation rules
    """
    # Default rules based on data types
    default_rules = {
        "array": lambda c: F.array_distinct(F.flatten(F.collect_list(c))),
        "boolean": lambda c: F.max(c),
        "string": lambda c: F.first(c, True),
        "numeric": lambda c: F.first(c, True),
    }

    # Combine with custom rules
    rules = {**default_rules, **(custom_rules or {})}

    # Build aggregation expressions
    agg_exprs = []

    for c, c_type in df.dtypes:
        if len(include_cols) > 0:
            if c not in include_cols:
                continue
        if c in exclude_cols:
            continue
        if c not in group_cols:
            if c_type.startswith("array"):
                expr = rules["array"](c)
            elif c_type == "boolean":
                expr = rules["boolean"](c)
            elif c_type in ("string", "varchar"):
                expr = rules["string"](c)
            else:
                expr = rules["numeric"](c)
            agg_exprs.append(expr.alias(c))

    # Apply groupBy and aggregation
    return df.groupBy(*group_cols).agg(*agg_exprs)


# data = [
#     # jurisdiciton, dv_hashkey_company, categories(array), is_active(bool), name, revenue(numeric), update_time
#     ("US", "hash1", ["retail", "tech"], True, "Company A", 1000000, "2023-01-01"),
#     ("US", "hash1", ["tech", "ecom"], True, "Company A Inc", 1200000, "2023-02-01"),
#     ("US", "hash1", ["retail"], False, None, 900000, "2023-03-01"),

#     ("SG", "hash2", ["finance"], True, "Company B", 500000, "2023-01-01"),
#     ("SG", "hash2", ["finance", "tech"], True, "Company B Pte", 600000, "2023-02-01"),

#     ("UK", "hash3", ["health"], True, "Company C", 800000, "2023-01-01"),
#     ("UK", "hash3", ["health", "tech"], False, "Company C Ltd", None, "2023-02-01")
# ]

# # Define schema
# schema = StructType([
#     StructField("jurisdiciton", StringType(), True),
#     StructField("dv_hashkey_company", StringType(), True),
#     StructField("categories", ArrayType(StringType()), True),
#     StructField("is_active", BooleanType(), True),
#     StructField("name", StringType(), True),
#     StructField("revenue", LongType(), True),
#     StructField("update_time", StringType(), True)
# ])
# df_deduped_default = agg_consolidate_value(
#     df,
#     ["jurisdiciton", "dv_hashkey_company"],
#     exclude_cols = ['update_time']
# )
# df_deduped_default.show(truncate=False)
