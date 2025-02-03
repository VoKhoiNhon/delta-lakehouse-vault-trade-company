from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
    format_date,
    split_full_name,
)
from pyspark.sql import functions as F


def concat_columns(df, new_column: str, separator: str, *cols):
    df = df.withColumn(
        new_column,
        F.when(F.coalesce(*[F.col(c) for c in cols]).isNull(), F.lit(None)).otherwise(
            F.concat_ws(separator, *[F.col(c) for c in cols])
        ),
    )
    return df


class Executer(BronzeExecuter):
    def concat_address(self, df):
        columns = ["location", "dos_process", "ceo", "registered_agent"]
        for column in columns:
            name_without = column + "_name" if column != "location" else column
            df = concat_columns(
                df,
                f"{column}_full_address",
                ", ",
                *[c for c in df.columns if c.startswith(column) and c != name_without],
            )
        return df

    def transform(self):
        # dv_source_version = self.params.get("dv_source_version", "")
        df = self.input_dataframe_dict["raw.US.newyork"].dataframe
        df = add_load_date_columns(df)
        df = clean_column_names(df)
        df = clean_string_columns(df)

        # person
        # df.show(5)
        # company
        df = (
            df.withColumnRenamed("dos_id", "registration_number")
            .withColumnRenamed("current_entity_name", "name")
            .withColumnRenamed("initial_dos_filing_date", "date_incorporated")
            .withColumnRenamed("entity_type", "legal_form")
            .withColumnRenamed("county", "region")
        )
        # df.show(5)
        df = format_date(df, "date_incorporated", "MM/dd/yyyy")
        df = self.concat_address(df)
        df = df.withColumn(
            "is_branch",
            F.when(F.col("jurisdiction") == "New York", False).otherwise(True),
        )
        return df


def run(env="pro", params={"dv_source_version": "2024-06-11"}):
    table_meta_link_position = TableMeta(
        from_files="metas/bronze/us/newyork.yaml", env=env
    )
    executer_link_position = Executer(
        "bronze_newyork",
        table_meta_link_position.model,
        table_meta_link_position.input_resources,
        params,
    )
    executer_link_position.execute()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    env = args.env
    params = {"dv_source_version": args.dv_source_version}
    run(env=args, params=params)
