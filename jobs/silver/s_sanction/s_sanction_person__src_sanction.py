from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_SANCTION


class SSanctionPersonExecuter(RawVaultExecuter):
    def transform(self):
        sources = self.input_dataframe_dict

        ####################################
        # Get source bronze.sanction.person
        ####################################
        source_sanction_person = sources["bronze.sanction.person"]
        record_source_person = str(
            getattr(source_sanction_person, "record_source")
        ).replace("'", "\\'")
        dv_source_version_person = str(
            getattr(source_sanction_person, "dv_source_version")
        ).replace("'", "\\'")
        sanction_person = source_sanction_person.dataframe
        sanction_person.createOrReplaceTempView("sanction_person")

        ####################################
        # Get source bronze.sanction.sanction
        ####################################
        source_sanction_sanction = sources["bronze.sanction.sanction"]
        record_source_sanction = str(
            getattr(source_sanction_sanction, "record_source")
        ).replace("'", "\\'")
        dv_source_version_sanction = str(
            getattr(source_sanction_sanction, "dv_source_version")
        ).replace("'", "\\'")
        sanction_sanction = source_sanction_sanction.dataframe
        sanction_sanction.createOrReplaceTempView("sanction_sanction")

        ###########
        # Query
        ###########
        df = self.spark.sql(
            f"""
            select
                {DV_HASHKEY_SANCTION} as dv_hashkey_sanction,
                dv_recsrc,
                FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts,
                dv_source_version,
                jurisdiction,
                id,
                sanction_id,
                authority,
                country,
                program,
                reason,
                start_date,
                end_date,
                source_link,
                source_schema
            from (
                select
                    dv_recsrc,
                    dv_source_version,
                    sp.jurisdiction as jurisdiction,
                    sp.id as id,
                    ss.sanction_id as sanction_id,
                    ss.authority as authority,
                    ss.country as country,
                    ss.program as program,
                    ss.reason as reason,
                    ss.start_date as start_date,
                    ss.end_date as end_date,
                    ss.source_link as source_link,
                    sp.source_schema as source_schema
                from (
                    select
                        '{record_source_person}' as dv_recsrc,
                        '{dv_source_version_person}' as dv_source_version,
                        country_name as jurisdiction,
                        CAST(id_number AS STRING) as id,
                        'person' as source_schema
                    from sanction_person
                    where is_sanctioned = 1
                ) sp left join (
                    select
                        country as jurisdiction,
                        id as sanction_id,
                        authority,
                        country,
                        program,
                        reason,
                        start_date,
                        end_date,
                        source_url as source_link
                    from sanction_sanction) ss
                on sp.id=ss.sanction_id
            )
            """
        )
        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/s_sanction.yaml"], payload=payload)

    executer = SSanctionPersonExecuter(
        app_name="bronze.s_sanction_person.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.sanction.person", "bronze.sanction.sanction"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
