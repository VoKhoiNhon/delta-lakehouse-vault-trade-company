from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class LSanctionCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.sanction"]
        record_source = source.record_source
        df = source.dataframe
        company = self.input_dataframe_dict["bronze.company"].dataframe.selectExpr(
            "DV_HASHKEY_COMPANY as dv_hashkey_company", "id"
        )
        dv_source_version = self.params.get("dv_source_version", "")

        df = (
            df.alias("s")
            .join(company.alias("c"), df.entity_id == company.id, "inner")
            .selectExpr("s.entity_id as id", "c.dv_hashkey_company")
        )
        return df.selectExpr(
            "md5(id) as dv_hashkey_sanction",
            f"'{record_source}' as dv_recsrc",
            f"'{dv_source_version}' as dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "dv_hashkey_company",
            f"md5(concat_ws(';', dv_hashkey_sanction, dv_hashkey_company)) as dv_hashkey_link_sanction",
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/l_sanction_company.yaml", env=env)
    executer = LSanctionCompanyExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()
