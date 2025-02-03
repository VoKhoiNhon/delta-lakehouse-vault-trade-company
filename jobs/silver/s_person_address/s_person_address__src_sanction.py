from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class SPersonAddressExecuter(RawVaultExecuter):
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
                    sp.country_name as jurisdiction,
                    sp.full_name as name,
                    sp.address as full_address,
                    NULL as street,
                    NULL as city,
                    NULL as region,
                    NULL as state,
                    NULL as province,
                    NULL as country_code,
                    sp.country_name as country_name,
                    NULL as postal_code,
                    NULL as latitude,
                    NULL as longitude,
                    1 as type,
                    NULL as start_date,
                    NULL as end_date
                from sanction_person sp
            ) sp
            """
        )
        df.show(n=1, vertical =True, truncate = False)
        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/s_person_address.yaml"], payload=payload)

    executer = SPersonAddressExecuter(
        app_name="bronze.s_person_address.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.sanction.person"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
