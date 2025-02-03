from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_singapore"]
        df = add_load_date_columns(df)
        df = clean_column_names(df)
        df = clean_string_columns(df)
        df.show(n=5)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/singapore/singapore.yaml")
    executer = Executer(
        app_name="singapore_raw_to_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
