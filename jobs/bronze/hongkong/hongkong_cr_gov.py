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
        df = self.input_dataframe_dict["raw_hongkong1"]
        df = df.select(
            f.col("BRN").alias("registration_number"),
            f.col("ENG_COY_NAME").alias("name"),
            f.col("Date BRN Updated").alias("regist_date"),
            "company_type",
        )
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df = clean_string_columns(df)
        df.show(n=5)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/hongkong/hongkong_cr_gov.yaml")
    executer = Executer(
        "bronze_hongkong1",
        table_meta.model,
        table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
