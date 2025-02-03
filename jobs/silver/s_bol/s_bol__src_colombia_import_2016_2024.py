import pyspark.sql.functions as F
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_BOL


class SBOLExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["colombia.import_2016-2024"]
        df = source.dataframe
        record_source = source.record_source
        ls_column = [
            "bol",
            "teu_number",
            "invoice_value",
            "estimated_arrival_date",
            "vessel_name",
            "export_port",
            "import_port",
        ]

        for column_name in ls_column:
            df = df.withColumn(column_name, F.lit(None))

        return df.selectExpr(
            f"{DV_HASHKEY_BOL} as dv_hashkey_bol",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'gov_colombia_trade_import_2016_2024;20241227' as dv_source_version",
            *df.columns,
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
