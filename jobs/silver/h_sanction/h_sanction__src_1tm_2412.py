from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class HSanctionExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.sanction"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")

        return df.selectExpr(
            "md5(entity_id) as dv_hashkey_sanction",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "entity_id as id",
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/h_sanction.yaml", env=env)
    executer = HSanctionExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()
