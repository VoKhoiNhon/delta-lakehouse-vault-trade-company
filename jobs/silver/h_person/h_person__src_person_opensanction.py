from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class HPersonExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.opensanction"]
        record_source = source.record_source
        df = source.dataframe
        df = df.filter(F.col("type") == "PERSON")
        dv_source_version = self.params.get("dv_source_version", "")

        df = df.filter(F.col("full_name").isNotNull())
        df = df.dropDuplicates(["full_name", "full_address"])
        df = df.selectExpr(
            f"{DV_HASHKEY_PERSON} as dv_hashkey_person",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "jurisdiction",
            "full_name",
            "full_address",
        )
        df.show(truncate=False)
        return df


def run():
    table_meta_hub = TableMeta(
        from_files=[
            "metas/silver/h_person/h_person.yaml",
        ]
    )

    executer_hub = HPersonExecuter(
        "silver/h_person/h_person__src_person_opensanction",
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
