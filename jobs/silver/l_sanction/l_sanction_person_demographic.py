from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import flatten
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class LSanctionExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.sanction"]
        dv_source_version = self.params.get("dv_source_version", "")
        record_source = source.record_source
        df = source.dataframe

        df = df.filter(F.col("is_sanction") == True)
        df = df.filter(F.col("type") == "PERSON")
        df = df.selectExpr(
            f"md5(concat_ws(';', {DV_HASHKEY_PERSON}, id)) as dv_hashkey_link_sanction",
            f" {DV_HASHKEY_PERSON} as dv_hashkey_person",
            f"md5(id) as dv_hashkey_sanction",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "1 as dv_status",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            f"'{dv_source_version}' as dv_source_version",
            "jurisdiction",
        )
        return df


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/link_sanction_person.yaml"])
    executer_hub = LSanctionExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
