from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F

from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY, DV_HASHKEY_PERSON


class HSanctionExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.sanction"]
        dv_source_version = self.params.get("dv_source_version", "")
        record_source = source.record_source
        df = source.dataframe
        df = df.filter(F.col("is_sanction") == True)
        return df.selectExpr(
            "md5(id) as dv_hashkey_sanction",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "id",
            "jurisdiction",
        )


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/h_sanction.yaml"])

    executer_hub = HSanctionExecuter(
        "silver/h_sanction/h_sanction-demo.py",
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
