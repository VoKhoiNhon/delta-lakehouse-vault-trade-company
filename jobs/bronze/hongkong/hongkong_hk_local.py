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
        df = self.input_dataframe_dict["raw_hongkong3"]
        df.drop(f.col("Chinese_Company_Name"))
        df = df.select(
            f.col("Brn").alias("registration_number"),
            f.col("English_Company_Name").alias("corporate_name"),
            f.col("Address_of_Registered_Office").alias("address"),
            f.col("Date_of_Incorporation").alias("regist_date"),
            f.col("Company_Type").alias("Company_Type".lower()),
        )
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = clean_column_names(df)
        df = clean_string_columns(df)
        df.show(n=5)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/hongkong/hongkong_hk_local.yaml")
    executer = Executer(
        "bronze_hongkong3",
        table_meta.model,
        table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
