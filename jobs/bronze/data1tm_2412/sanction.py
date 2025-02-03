from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["1tm_2412.sanction"].dataframe
        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(
        from_files="metas/bronze/data1tm_2412/sanction.yaml", env=env
    )

    import sys

    executer = Executer(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()
