from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
import sys


class SCompanyAddressOpenSanctionExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.opensanction"]
        record_source = source.record_source
        df = source.dataframe
        df = df.filter(F.col("type") == "COMPANY")
        df = df.withColumn("postal_code", F.lit(None))
        dv_source_version = self.params.get("dv_source_version", "")

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "jurisdiction",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "1 as dv_status",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            f"'{dv_source_version}' as dv_source_version",
            "country_code",
            "country_name",
        )
        df.show()
        return df


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/s_company_address.yaml"])
    executer_hub = SCompanyAddressOpenSanctionExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
