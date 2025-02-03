from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON
import sys


class SPersonAddressOpenSanctionExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.opensanction"]
        dv_source_version = self.params.get("dv_source_version", "")
        record_source = source.record_source
        df = source.dataframe

        df = df.filter(F.col("type") == "PERSON")

        df = df.selectExpr(
            f"{DV_HASHKEY_PERSON} as dv_hashkey_person",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "1 as dv_status",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            f"'{dv_source_version}' as dv_source_version",
            "jurisdiction",
            "full_name",
            "full_address",
            "type",
        )

        df.show(n=5, truncate=False)

        return df


def run():

    table_meta_hub = TableMeta(from_files=["metas/silver/s_person_address.yaml"])
    executer_hub = SPersonAddressOpenSanctionExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
