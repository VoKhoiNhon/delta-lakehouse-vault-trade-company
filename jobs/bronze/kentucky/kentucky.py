from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T


def split_cols(df: DataFrame, cols: str) -> DataFrame:
    df = df.withColumn(cols, F.split(F.col(cols), " - ").getItem(1))
    return df


def process_agent(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn(
            "agent_name",
            F.when(
                F.regexp_extract(F.col("registered_agent"), r"', '([^']+)", 1).rlike(
                    r"^\d|SUIT|BOX"
                ),
                F.regexp_extract(F.col("registered_agent"), r"\['([^']+)'", 1),
            ).otherwise(None),
        )
        .withColumn(
            "agent_address",
            F.regexp_extract(
                F.col("registered_agent"), r"', '([^']+(?:', '[^']+)*)'", 1
            ),
        )
        .withColumn(
            "agent_address", F.regexp_replace(F.col("agent_address"), r"', '", ", ")
        )
    )
    df = df.withColumn(
        "agent_name",
        F.when(F.col("agent_name").rlike("(?i)(ST\\.|STREET)$"), None).otherwise(
            F.col("agent_name")
        ),
    )
    return df


def process_address(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "principal_address",
        F.regexp_replace(F.col("principal_office"), r"[\'\[\]]", ""),
    )
    return df


def process_assumed_name(df: DataFrame) -> DataFrame:
    schema = T.ArrayType(
        T.StructType(
            [
                T.StructField("name", T.StringType(), True),
                T.StructField("status", T.StringType(), True),
                T.StructField("expiration_date", T.StringType(), True),
            ]
        )
    )

    df = df.withColumn(
        "assumed_names",
        F.explode(F.from_json(F.col("assumed_names").cast("string"), schema)),
    )

    df = df.select(F.col("organization_number"), F.col("assumed_names.name"))
    df = df.groupBy("organization_number").agg(
        F.collect_set("name").alias("other_names")
    )
    return df


def groupby_data(df: DataFrame) -> DataFrame:
    df = df.groupBy(
        "organization_number", "name", "company_type", "status", "state"
    ).agg(
        F.first("profit_or_non-profit", ignorenulls=True).alias("profit_or_non_profit"),
        F.first("standing", ignorenulls=True).alias("standing"),
        F.first("date_incorporated", ignorenulls=True).alias("date_incorporated"),
        F.first("last_annual_report", ignorenulls=True).alias("last_annual_report"),
        F.first("principal_address", ignorenulls=True).alias("principal_address"),
        F.first("agent_name", ignorenulls=True).alias("agent_name"),
        F.first("agent_address", ignorenulls=True).alias("agent_address"),
        F.first("industry", ignorenulls=True).alias("industry"),
        F.first("number_of_employees", ignorenulls=True).alias("number_of_employees"),
        F.first("primary_county", ignorenulls=True).alias("primary_county"),
        F.first("applicant_address", ignorenulls=True).alias("applicant_address"),
        F.first("renewal_date", ignorenulls=True).alias("renewal_date"),
        F.first("date_struck_off", ignorenulls=True).alias("date_struck_off"),
        F.first("managed_by", ignorenulls=True).alias("managed_by"),
        F.first("country", ignorenulls=True).alias("country"),
        F.first("authorized_shares", ignorenulls=True).alias("authorized_shares"),
        F.first("common_no_par_shares", ignorenulls=True).alias("common_no_par_shares"),
        F.first("common_par_shares", ignorenulls=True).alias("common_par_shares"),
        F.first("name_in_origin_state", ignorenulls=True).alias("name_in_origin_state"),
    )
    return df


def process_date(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "date_incorporated",
        F.when(
            F.col("file_date").isNull() & F.col("authority_date").isNull(),
            F.col("organization_date"),
        )
        .when(F.col("file_date").isNull(), F.col("authority_date"))
        .otherwise(F.col("file_date")),
    ).drop("authority_date", "file_date", "organization_date")

    df = df.withColumn(
        "date_incorporated", F.to_date(F.col("date_incorporated"), "M/d/yyyy")
    )
    df = df.withColumn(
        "date_struck_off", F.to_date(F.col("expiration_date"), "M/d/yyyy")
    ).drop("expiration_date")
    return df


def rename_cols(df: DataFrame) -> DataFrame:
    cols = {
        "company_type": "legal_form",
        "organization_number": "registration_number",
    }
    for old_name, new_name in cols.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def drop_cols(df: DataFrame) -> DataFrame:
    df = df.drop(
        "profit_or_non_profit",
        "standing",
        "applicant_address",
        "last_annual_report",
        "principal_office",
        "number_of_employees",
        "renewal_date",
        "managed_by",
        "reg_agent_resigned",
        "assumed_names",
        "current_officers",
    )

    df = df.select(
        "agent_name", "agent_address", "registration_number", "name"
    ).withColumnsRenamed({"agent_name": "full_name", "agent_address": "full_address"})
    df = df.withColumn("position", F.lit("Registered Agent"))
    df = df.withColumn("type", F.lit(1))
    df = df.withColumn("position_code", F.lit(4))
    return df


def process_other_names(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "other_names",
        F.split(
            F.concat_ws(
                ", ",
                F.split(
                    F.regexp_replace(F.col("other_names"), r"^\s*|\s*$", ""),
                    r"\s*,\s*",
                ),
            )
        ),
    )
    return df


def is_person(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "clean_name",
        F.when(
            F.col("full_name").isNotNull(),
            F.regexp_replace(F.upper(F.col("full_name")), "[.,]", ""),
        ).otherwise(None),
    )
    keywords = [
        "SYSTEM",
        "COMPANY",
        "CORPORATION",
        "SERVICE",
        "CORP",
        "STATE",
        "SERVICES",
        "INCORPORATING",
        "LTD",
        "LLC",
        "ASSOCIATION",
        "INC",
        "OF",
        "SECRETARY",
        "REGISTERED",
        "AGENTS",
        "AGENT",
        "SERVICES",
        "STATES",
        "CORPORATE",
        "PLLC",
        "GROUP",
        "HOLDINGS",
        "INCORPORATED",
        "LTD",
        "LIMITED",
        "PARTNERSHIP",
        "MMLK",
        "LLP",
        "TRUST",
        "LANDSCAPING",
        "INSURANCE",
    ]
    df = df.withColumn(
        "is_person",
        F.when(
            F.col("clean_name").rlike(r"\b(?:" + "|".join(keywords) + r")\b"), False
        ).otherwise(True),
    ).withColumn(
        "is_person",
        F.when(F.regexp_extract(F.col("clean_name"), r"\d", 0) != "", False)
        .when(
            (F.col("clean_name").contains("&"))
            | (F.col("clean_name").contains("|"))
            | (F.col("clean_name").contains("#")),
            False,
        )
        .otherwise(F.col("is_person")),
    )
    return df


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_kentucky"].dataframe
        df = df.limit(100)
        df = clean_column_names(df)
        df = df.withColumnRenamed("reg__agent_resigned", "reg_agent_resigned")
        # print(df.dtypes)
        # df = clean_string_columns(df)
        df = split_cols(df, "company_type")
        df = split_cols(df, "status")
        df = process_agent(df).drop("registered_agent")
        df = process_address(df)
        df = process_date(df)
        df = groupby_data(df)
        df = rename_cols(df)
        df = drop_cols(df)
        df = df.withColumn("load_date", lit("20240904"))
        df = clean_string_columns(df)
        df = (
            df.withColumn("jurisdiction", F.lit("Kentucky"))
            .withColumn("country_name", F.lit("United States"))
            .withColumn("country_code", F.lit("US"))
        )
        df = is_person(df)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/kentucky/kentucky.yaml")
    executer = Executer(
        app_name="kentucky_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
