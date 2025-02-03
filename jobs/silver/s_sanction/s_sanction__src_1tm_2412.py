from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta


class SSanctionExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.sanction"]
        record_source = source.record_source
        df = source.dataframe.withColumnRenamed("id", "sanction_id").withColumnRenamed(
            "entity_id", "id"
        )
        dv_source_version = self.params.get("dv_source_version", "")

        return df.selectExpr(
            "md5(id) as dv_hashkey_sanction",
            f"'{record_source}' as dv_recsrc",
            f"'{dv_source_version}' as dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            *df.columns,
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/s_sanction.yaml", env=env)
    executer = SSanctionExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()
