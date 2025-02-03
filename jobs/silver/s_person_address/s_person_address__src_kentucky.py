from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class SPersonKentuckyExecuterAddress(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.kentucky"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")

        df = df.filter(F.col("full_name").isNotNull())
        df = df.filter(F.col("full_address").isNotNull())
        df = df.dropDuplicates(["full_name", "full_address"])

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
        df.show(truncate=False)
        return df


def run():
    table_meta_sat_address = TableMeta(
        from_files="metas/silver/s_person_address/s_person_address.yaml",
    )

    executer_sat_address = SPersonKentuckyExecuterAddress(
        "silver/s_person_address/s_person_address__src_kentucky",
        table_meta_sat_address.model,
        table_meta_sat_address.input_resources,
    )
    executer_sat_address.execute()


if __name__ == "__main__":
    run()
