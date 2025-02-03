from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class LookupHscodeExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.hscode.table_lookup"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")

        return df.selectExpr(
            f"md5(code) as dv_hashkey_hscode",
            f"'{record_source}' as dv_recsrc",
            f"'{dv_source_version}' as dv_source_version",
            *df.columns,
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta_s_bol = TableMeta(from_files="metas/silver/lookup_hscode.yaml", env=env)
    sbol = LookupHscodeExecuter(
        sys.argv[0],
        table_meta_s_bol.model,
        table_meta_s_bol.input_resources,
        params,
        spark,
    )
    sbol.execute()


if __name__ == "__main__":
    run()
