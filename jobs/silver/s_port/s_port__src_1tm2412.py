from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys
import libs.utils.vault_hashfuncs as H


class SPortUSImportExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["1tm_2412.port"]
        record_source = source.record_source
        df = source.dataframe

        return df.selectExpr(
            f"'{record_source}' as dv_recsrc",
            f"{H.DV_HASHKEY_PORT} as dv_hashkey_port",
            "1 as dv_status",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            "port_code",
            "port_name",
            "emails",
            "address",
            "phone_number",
            "fax_number",
            "port_type",
            "port_size",
            "website",
            "country_code",
            "country_name",
            "terminal",
            "state",
            "locode",
            "status",
            "latitude",
            "longitude",
        )


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/s_port.yaml"])
    executer_hub = SPortUSImportExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
