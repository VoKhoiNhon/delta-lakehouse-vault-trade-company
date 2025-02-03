from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw.yukon"].dataframe
        df = add_load_date_columns(df)
        df = clean_column_names(df)
        df = clean_string_columns(df)
        # df.show(5)
        return df


def run(env="pro", params={}):
    table_meta_link_position = TableMeta(
        from_files="metas/bronze/yukon/yukon.yaml", env=env
    )
    executer_link_position = Executer(
        "bronze_yukon",
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
