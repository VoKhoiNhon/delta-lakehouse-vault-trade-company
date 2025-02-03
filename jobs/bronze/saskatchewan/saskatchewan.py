from pyspark.sql.functions import current_date

from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import clean_column_names, clean_string_columns


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_saskatchewan"]
        df = clean_column_names(df)
        df = clean_string_columns(df)
        df = df.withColumn("load_date", current_date())
        df.show(n=5)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/saskatchewan/saskatchewan.yaml")
    executer = Executer(
        app_name="saskatchewan_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
