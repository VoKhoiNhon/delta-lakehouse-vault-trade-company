from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class HPersonExecuter(RawVaultExecuter):
    def transform(self):
        source_person = self.input_dataframe_dict["bronze.sanction.person"]
        record_source = source_person.record_source.replace("'", "\\'")
        dv_source_version = source_person.dv_source_version.replace("'", "\\'")
        person_df = source_person.dataframe
        person_df.createOrReplaceTempView("sanction_person")
        df = self.spark.sql(
            f"""
            select
                {DV_HASHKEY_PERSON} as dv_hashkey_person,
                '{record_source}' as dv_recsrc,
                FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts,
                '{dv_source_version}' as dv_source_version,
                sp.*
            from (
                select
                    sp.*,
                    sp.country_name as jurisdiction,
                    sp.full_name as name,
                    sp.address as full_address
                from sanction_person sp
            ) sp
            """
        )
        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/h_person.yaml"], payload=payload)

    executer = HPersonExecuter(
        app_name="bronze.h_person.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.sanction.person"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
