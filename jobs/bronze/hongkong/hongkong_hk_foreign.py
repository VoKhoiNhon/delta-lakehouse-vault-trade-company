from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_hongkong2"]
        df = df.select(
            f.col("Brn").alias("registration_number"),
            f.col("`Approved_name_for_carrying_on_business_in_H.K._Corporate`").alias(
                "approved_name_corporate"
            ),
            f.col(
                "`Approved_name_for_carrying_on_business_in_H.K._Other_Corporate`"
            ).alias("approved_name_other_corporate"),
            f.col("`Principal_Place_of_Business_in_H.K.`").alias("address"),
            f.col("Date_of_Registration").alias("regist_date"),
            *[
                f.col(c)
                for c in df.columns
                if c
                not in [
                    "Brn",
                    "Date_of_Registration",
                    "Approved_name_for_carrying_on_business_in_H.K._Corporate",
                    "Approved_name_for_carrying_on_business_in_H.K._Other_Corporate",
                    "Principal_Place_of_Business_in_H.K.",
                ]
            ]
        )
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df = clean_string_columns(df)
        # df.show(n=5)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/hongkong/hongkong_hk_foreign.yaml")
    print(table_meta.model)
    executer = Executer(
        "bronze_hongkong2",
        table_meta.model,
        table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
