from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_BOL


class SBOLExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["1tm_2412.bol"]
        record_source = source.record_source
        df = source.dataframe
        return df.selectExpr(
            f"{DV_HASHKEY_BOL} as dv_hashkey_bol",
            f"'{record_source}' as dv_recsrc",
            "'1tm' as dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
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
