from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
    clean_jurisdiciton_1tm,
    add_pure_company_name,
)


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_bangladesh_company"].dataframe
        df = clean_column_names(df)
        df = df.withColumnRenamed("entity_name", "name")
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = add_pure_company_name(df=df, name="name", return_col="pure_name")
        df = clean_column_names(df)
        # df.show(n=5, vertical=True, truncate=False)
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files="metas/bronze/bangladesh/bangladesh_company.yaml", payload=payload
    )
    executer = Executer(
        app_name="bangladesh_company_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
