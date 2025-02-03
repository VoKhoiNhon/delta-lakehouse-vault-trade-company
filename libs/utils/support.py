from pyspark.sql import functions as F
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *
from typing import Dict
import time


def gen_yaml_schema(df, output="output.yaml"):
    from jinja2 import Environment, Template
    from ruamel.yaml import YAML
    import re

    def fill_yaml_template(template_str, data):
        template = Template(template_str)
        filled_yaml = template.render(**data)
        return filled_yaml

    # Example usage:
    model = """model:
    table_name: table_name
    database_name: database_name
    data_location: s3a//data_location
    unique_key: key
    partition_by: null
    data_format: delta
    columns:
      - name: dv_hashkey_
        type: string
        nullable: false
        tests:
          - check: isComplete
          - check: isUnique
        description: dv hashkey
      - name: dv_recsrc
        type: string
        nullable: false
        tests:
          - check: isComplete
        description: dv columns
      - name: dv_loaddts
        nullable: false
        type: timestamp
        description: dv columns
      {% for item in columns %}
      - name: {{ item.name }}
        type: {{ item.type }}{% endfor %}

input_resources:
  - table_name: table_input
    format: delta
    data_location: data/table_input
    record_source: source_a
    filter: null
"""
    data = {"columns": []}

    def to_snake_case(name: str) -> str:
        name = re.sub("\\s+", "_", name)
        name = re.sub("\\.", "_", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        name = name.lower()
        return name

    for field in df.schema:

        column_dict = {
            "name": to_snake_case(field.name),
            "type": field.dataType.simpleString(),
        }
        data["columns"].append(column_dict)

    filled_yaml = fill_yaml_template(model, data)

    yaml = YAML()
    with open(output, "w") as outfile:
        yaml.dump(yaml.load(filled_yaml), outfile)


def compare_schema_objects(
    schema1: StructType,
    schema2: StructType,
    schema1_name: str = "Schema1",
    schema2_name: str = "Schema2",
) -> Dict:
    """
    Compare two StructType schema objects and return differences

    Args:
        schema1: First StructType schema
        schema2: Second StructType schema
        schema1_name: Name of first schema
        schema2_name: Name of second schema

    Returns:
        Dict containing missing columns and type mismatches
    """

    def get_nested_fields(schema: StructType, prefix: str = "") -> Dict:
        fields = {}
        for field in schema.fields:
            full_name = f"{prefix}{field.name}"
            if isinstance(field.dataType, StructType):
                fields.update(get_nested_fields(field.dataType, f"{full_name}."))
            else:
                fields[full_name] = {"type": field.dataType, "nullable": field.nullable}
        return fields

    # Get flattened fields with types and nullable info
    fields1 = get_nested_fields(schema1)
    fields2 = get_nested_fields(schema2)

    # Find missing columns
    missing_in_schema2 = [col for col in fields1.keys() if col not in fields2]
    missing_in_schema1 = [col for col in fields2.keys() if col not in fields1]

    # Find type and nullable mismatches
    differences = []
    for col in set(fields1.keys()) & set(fields2.keys()):
        if fields1[col]["type"] != fields2[col]["type"]:
            differences.append(
                {
                    "column": col,
                    "difference_type": "data_type",
                    f"{schema1_name}_type": str(fields1[col]["type"]),
                    f"{schema2_name}_type": str(fields2[col]["type"]),
                }
            )
        if fields1[col]["nullable"] != fields2[col]["nullable"]:
            differences.append(
                {
                    "column": col,
                    "difference_type": "nullable",
                    f"{schema1_name}_nullable": fields1[col]["nullable"],
                    f"{schema2_name}_nullable": fields2[col]["nullable"],
                }
            )

    comparison_result = {
        f"missing_in_{schema2_name}": missing_in_schema2,
        f"missing_in_{schema1_name}": missing_in_schema1,
        "differences": differences,
    }
    print(f"\n=== Schema Comparison between {schema1_name} and {schema2_name} ===")

    # Print missing columns
    missing_in_schema2 = comparison_result[f"missing_in_{schema2_name}"]
    if missing_in_schema2:
        print(f"\nColumns in {schema1_name} but missing in {schema2_name}:")
        for col in missing_in_schema2:
            print(f"- {col}")

    missing_in_schema1 = comparison_result[f"missing_in_{schema1_name}"]
    if missing_in_schema1:
        print(f"\nColumns in {schema2_name} but missing in {schema1_name}:")
        for col in missing_in_schema1:
            print(f"- {col}")

    # Print differences
    differences = comparison_result["differences"]
    if differences:
        print("\nColumn differences:")
        for diff in differences:
            print(f"\nColumn: {diff['column']}")
            if diff["difference_type"] == "data_type":
                print(f"  {schema1_name} type: {diff[f'{schema1_name}_type']}")
                print(f"  {schema2_name} type: {diff[f'{schema2_name}_type']}")
            else:  # nullable
                print(f"  {schema1_name} nullable: {diff[f'{schema1_name}_nullable']}")
                print(f"  {schema2_name} nullable: {diff[f'{schema2_name}_nullable']}")

    if not (missing_in_schema1 or missing_in_schema2 or differences):
        print("\nNo differences found. Schemas are identical.")

    return comparison_result


def print_analysis(analysis: dict):
    """
    In kết quả phân tích theo format dễ đọc
    """

    def format_number(value, decimal_places=None):
        """Helper function để format số an toàn"""
        if value == "notfound":
            return "notfound"
        try:
            if decimal_places is not None:
                return f"{value:,.{decimal_places}f}"
            return f"{value:,}"
        except:
            return str(value)

    print("\n=== BASIC INFORMATION ===")
    basic = analysis.get("basic_info", {})
    print(f"Total rows: {format_number(basic.get('total_rows', 'notfound'))}")
    print(f"Total columns: {basic.get('total_columns', 'notfound')}")
    print(
        f"Execution time: {format_number(analysis.get('execution_time', 'notfound'), 2)}s"
    )

    print("\n=== COLUMNS ANALYSIS ===")
    for col_name, metrics in analysis.get("columns_analysis", {}).items():
        print(f"\n{col_name}:")
        print(f"Type: {metrics.get('type', 'notfound')}")
        print(
            f"Null count: {format_number(metrics.get('null_count', 'notfound'))} ({format_number(metrics.get('null_percentage', 'notfound'), 2)}%)"
        )

        if "numeric_stats" in metrics:
            ns = metrics.get("numeric_stats", {})
            print(f"Distinct values: {format_number(ns.get('distinct', 'notfound'))}")
            print(f"Mean: {format_number(ns.get('mean', 'notfound'), 2)}")
            print(f"StdDev: {format_number(ns.get('stddev', 'notfound'), 2)}")
            print(f"Min: {format_number(ns.get('min', 'notfound'))}")
            print(f"Max: {format_number(ns.get('max', 'notfound'))}")
            print(
                f"Zero values: {format_number(metrics.get('zero_count', 'notfound'))}"
            )
            print(
                f"Negative values: {format_number(metrics.get('negative_count', 'notfound'))}"
            )

        elif "string_stats" in metrics:
            ss = metrics.get("string_stats", {})
            print(f"Empty strings: {format_number(ss.get('empty_count', 'notfound'))}")
            print(
                f"Length - Min: {format_number(ss.get('min_length', 'notfound'))}, "
                f"Max: {format_number(ss.get('max_length', 'notfound'))}, "
                f"Avg: {format_number(ss.get('avg_length', 'notfound'), 1)}"
            )
            print("Top values:")
            for v in metrics.get("top_values", []):
                print(
                    f"  {v.get('value', 'notfound')}: {format_number(v.get('count', 'notfound'))}"
                )

        elif "true_count" in metrics:
            print(f"True count: {format_number(metrics.get('true_count', 'notfound'))}")
            print(
                f"False count: {format_number(metrics.get('false_count', 'notfound'))}"
            )

        elif "date_stats" in metrics:
            ds = metrics.get("date_stats", {})
            print(
                f"Distinct dates: {format_number(ds.get('distinct_dates', 'notfound'))}"
            )
            print(
                f"Date range: {ds.get('min_date', 'notfound')} to {ds.get('max_date', 'notfound')}"
            )

    print("\n=== SAMPLE DATA ===")
    for row in analysis.get("sample_data", []):
        print(row)


def analyze_df(df: DataFrame, sample_rows: int = 5) -> dict:
    """
    Phân tích chi tiết DataFrame

    Args:
        df: Spark DataFrame cần phân tích
        sample_rows: Số dòng mẫu cần hiển thị
    Returns:
        Dict chứa kết quả phân tích
    """
    start_time = time.time()

    # Basic info
    total_rows = df.count()

    # Tạo dict lưu kết quả phân tích
    analysis = {
        "basic_info": {
            "total_rows": total_rows,
            "total_columns": len(df.columns),
            "schema": str(df.schema.jsonValue()),
        },
        "columns_analysis": {},
        "sample_data": df.limit(sample_rows).toPandas().to_dict("records"),
    }

    # Phân tích từng cột
    for field in df.schema:
        col_name = field.name
        col_type = type(field.dataType).__name__

        # Base metrics cho mọi loại cột
        metrics = {
            "type": col_type,
            "null_count": df.filter(F.col(col_name).isNull()).count(),
        }

        # Metrics cho từng loại dữ liệu
        if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType)):
            numeric_stats = (
                df.select(
                    F.count(col_name).alias("count"),
                    F.countDistinct(col_name).alias("distinct"),
                    F.mean(col_name).alias("mean"),
                    F.stddev(col_name).alias("stddev"),
                    F.min(col_name).alias("min"),
                    F.max(col_name).alias("max"),
                )
                .collect()[0]
                .asDict()
            )
            metrics.update(
                {
                    "numeric_stats": numeric_stats,
                    "zero_count": df.filter(F.col(col_name) == 0).count(),
                    "negative_count": df.filter(F.col(col_name) < 0).count(),
                }
            )

        elif isinstance(field.dataType, StringType):
            string_stats = (
                df.select(
                    F.count(F.when(F.length(F.col(col_name)) == 0, True)).alias(
                        "empty_count"
                    ),
                    F.min(F.length(F.col(col_name))).alias("min_length"),
                    F.max(F.length(F.col(col_name))).alias("max_length"),
                    F.mean(F.length(F.col(col_name))).alias("avg_length"),
                )
                .collect()[0]
                .asDict()
            )

            # Top values
            top_values = (
                df.groupBy(col_name)
                .count()
                .orderBy("count", ascending=False)
                .limit(5)
                .collect()
            )

            metrics.update(
                {
                    "string_stats": string_stats,
                    "top_values": [
                        {"value": r[col_name], "count": r["count"]} for r in top_values
                    ],
                }
            )

        elif isinstance(field.dataType, BooleanType):
            bool_counts = (
                df.select(
                    F.count(F.when(F.col(col_name) == True, True)).alias("true_count"),
                    F.count(F.when(F.col(col_name) == False, True)).alias(
                        "false_count"
                    ),
                )
                .collect()[0]
                .asDict()
            )

            metrics.update(bool_counts)

        elif isinstance(field.dataType, (DateType, TimestampType)):
            date_stats = (
                df.select(
                    F.countDistinct(col_name).alias("distinct_dates"),
                    F.min(col_name).alias("min_date"),
                    F.max(col_name).alias("max_date"),
                )
                .collect()[0]
                .asDict()
            )

            metrics.update({"date_stats": date_stats})

        # Tính % null
        metrics["null_percentage"] = (metrics["null_count"] / total_rows) * 100

        # Add vào kết quả
        analysis["columns_analysis"][col_name] = metrics

    analysis["execution_time"] = time.time() - start_time

    print_analysis(analysis)
    return analysis


# results = analyze_df(df)


def compare_dataframes(
    df1: DataFrame,
    df2: DataFrame,
    join_columns: list = None,
    df1_name: str = "df1",
    df2_name: str = "df2",
):
    """
    So sánh 2 DataFrame và trả về thống kê khác biệt

    Args:
        df1: DataFrame thứ nhất
        df2: DataFrame thứ hai
        join_columns: List các cột dùng để join, nếu None sẽ tìm các cột chung
        df1_name: Tên cho DataFrame thứ nhất
        df2_name: Tên cho DataFrame thứ hai
    """

    # Tìm các cột chung
    common_columns = set(df1.columns).intersection(df2.columns)
    if not common_columns:
        return {"error": "Không có cột chung giữa 2 DataFrame"}

    # Nếu không chỉ định join columns, dùng tất cả cột chung
    if join_columns is None:
        join_columns = list(common_columns)

    # Kiểm tra join columns có tồn tại trong cả 2 df
    if not all(col in common_columns for col in join_columns):
        return {"error": "Một số join columns không tồn tại trong cả 2 DataFrame"}

    # Tính số dòng và số null của từng DataFrame
    df1_stats = {
        "name": df1_name,
        "total_rows": df1.count(),
        "null_counts": {
            col: df1.filter(F.col(col).isNull()).count() for col in common_columns
        },
    }

    df2_stats = {
        "name": df2_name,
        "total_rows": df2.count(),
        "null_counts": {
            col: df2.filter(F.col(col).isNull()).count() for col in common_columns
        },
    }

    # Join 2 DataFrame
    df1_prefixed = df1.select([F.col(c).alias(f"{df1_name}_{c}") for c in df1.columns])
    df2_prefixed = df2.select([F.col(c).alias(f"{df2_name}_{c}") for c in df2.columns])

    join_cond = [
        F.col(f"{df1_name}_{c}") == F.col(f"{df2_name}_{c}") for c in join_columns
    ]
    joined_df = df1_prefixed.join(df2_prefixed, join_cond, "full_outer")

    # So sánh từng cột
    column_comparisons = {}
    for column in common_columns:
        if column not in join_columns:
            # Tính số dòng giống và khác nhau
            comparison = joined_df.select(
                F.count("*").alias("total"),
                F.count(
                    F.when(
                        F.col(f"{df1_name}_{column}") == F.col(f"{df2_name}_{column}"),
                        True,
                    )
                ).alias("matching"),
                F.count(
                    F.when(
                        F.col(f"{df1_name}_{column}") != F.col(f"{df2_name}_{column}"),
                        True,
                    )
                ).alias("different"),
                F.count(
                    F.when(
                        F.col(f"{df1_name}_{column}").isNull()
                        & F.col(f"{df2_name}_{column}").isNotNull(),
                        True,
                    )
                ).alias(f"null_in_{df1_name}"),
                F.count(
                    F.when(
                        F.col(f"{df1_name}_{column}").isNotNull()
                        & F.col(f"{df2_name}_{column}").isNull(),
                        True,
                    )
                ).alias(f"null_in_{df2_name}"),
            ).collect()[0]

            # Lấy mẫu các dòng khác nhau
            sample_diff = (
                joined_df.filter(
                    F.col(f"{df1_name}_{column}") != F.col(f"{df2_name}_{column}")
                )
                .select(
                    *[F.col(f"{df1_name}_{jc}").alias(jc) for jc in join_columns],
                    F.col(f"{df1_name}_{column}").alias(f"{df1_name}_value"),
                    F.col(f"{df2_name}_{column}").alias(f"{df2_name}_value"),
                )
                .limit(5)
                .collect()
            )

            column_comparisons[column] = {
                "total_rows": comparison["total"],
                "matching_rows": comparison["matching"],
                "different_rows": comparison["different"],
                f"null_in_{df1_name}": comparison[f"null_in_{df1_name}"],
                f"null_in_{df2_name}": comparison[f"null_in_{df2_name}"],
                "sample_differences": [row.asDict() for row in sample_diff],
            }

    comparison = {
        "df1_stats": df1_stats,
        "df2_stats": df2_stats,
        "common_columns": list(common_columns),
        "join_columns": join_columns,
        "column_comparisons": column_comparisons,
    }

    if "error" in comparison:
        print(f"Error: {comparison['error']}")
        return

    df1_name = comparison["df1_stats"]["name"]
    df2_name = comparison["df2_stats"]["name"]

    print("\n=== BASIC INFORMATION ===")
    print(f"{df1_name} rows: {comparison['df1_stats']['total_rows']:,}")
    print(f"{df2_name} rows: {comparison['df2_stats']['total_rows']:,}")
    print(f"Common columns: {len(comparison['common_columns'])}")
    print(f"Join columns: {', '.join(comparison['join_columns'])}")

    print("\n=== COLUMN COMPARISONS ===")
    for col, stats in comparison["column_comparisons"].items():
        print(f"\n{col}:")
        print(f"Total rows: {stats['total_rows']:,}")
        print(f"Matching rows: {stats['matching_rows']:,}")
        print(f"Different rows: {stats['different_rows']:,}")
        print(f"Null in {df1_name}: {stats[f'null_in_{df1_name}']:,}")
        print(f"Null in {df2_name}: {stats[f'null_in_{df2_name}']:,}")

        if stats["sample_differences"]:
            print("Sample differences:")
            for diff in stats["sample_differences"]:
                keys = ", ".join(
                    f"{k}={v}"
                    for k, v in diff.items()
                    if k in comparison["join_columns"]
                )
                print(f"  Keys: {keys}")
                print(f"  {df1_name}: {diff[f'{df1_name}_value']}")
                print(f"  {df2_name}: {diff[f'{df2_name}_value']}")
                print()

    return comparison
