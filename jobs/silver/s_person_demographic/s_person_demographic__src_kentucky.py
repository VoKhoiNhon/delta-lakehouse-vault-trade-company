from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class SPersonKentuckyExecuterDemographic(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.kentucky"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")

        df = df.filter(F.col("full_name").isNotNull())
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
            "type",
            "is_person",
        )
        return df


def run():

    table_meta_sat_graphic = TableMeta(
        from_files="metas/silver/s_person_demographic.yaml",
    )

    executer_sat_graphic = SPersonKentuckyExecuterDemographic(
        "silver/s_person_demographic/s_person_demographic__src_kentucky",
        table_meta_sat_graphic.model,
        table_meta_sat_graphic.input_resources,
    )
    executer_sat_graphic.execute()


if __name__ == "__main__":
    run()
