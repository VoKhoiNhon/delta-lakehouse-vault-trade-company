import pyspark.sql.functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import libs.utils.vault_hashfuncs as H
from libs.utils.commons import add_pure_company_name


class Lbol1tmExecuter(RawVaultExecuter):
    def transform(self):
        bol = self.input_dataframe_dict["trade_nbd"].dataframe
        bol = bol.withColumn(
            "dv_source_version",
            F.concat_ws(";", F.col("data_source"), F.col("load_date")),
        )
        return bol.selectExpr(
            "dv_hashkey_bol",
            "dv_hashkey_l_bol",
            "dv_recsrc",
            "dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "buyer_dv_hashkey_company",
            "supplier_dv_hashkey_company",
            "import_port",
            "export_port",
            "bol",
            "actual_arrival_date",
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/l_bol.yaml", env=env)
    executer = Lbol1tmExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
