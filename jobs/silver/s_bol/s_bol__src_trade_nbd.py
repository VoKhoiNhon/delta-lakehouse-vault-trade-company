from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F

from libs.utils.vault_hashfuncs import DV_HASHKEY_BOL


class SBOLExecuter(RawVaultExecuter):
    def transform(self):
        data = self.input_dataframe_dict["trade_nbd"].dataframe
        data = data.withColumn(
            "dv_source_version",
            F.concat_ws(";", F.col("data_source"), F.col("load_date")),
        )
        return data.selectExpr(
            "dv_hashkey_bol",
            "dv_recsrc",
            "dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "bol",
            "hs_code",
            "teu_number",
            "invoice_value",
            "value_usd",
            "exchange_rate",
            "description",
            "actual_arrival_date",
            "estimated_arrival_date",
            "vessel_name",
            "quantity",
            "quantity_unit",
            "weight",
            "weight_unit",
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta_s_bol = TableMeta(from_files="metas/silver/s_bol.yaml", env=env)
    sbol = SBOLExecuter(
        sys.argv[0],
        table_meta_s_bol.model,
        table_meta_s_bol.input_resources,
        params,
        spark,
    )
    sbol.execute()


if __name__ == "__main__":
    run()
