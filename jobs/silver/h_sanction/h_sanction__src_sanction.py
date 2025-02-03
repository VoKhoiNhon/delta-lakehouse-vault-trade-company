from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_SANCTION


class HSanctionExecuter(RawVaultExecuter):
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
        # Get source bronze.sanction.company
        ####################################
        source_sanction_company = sources["bronze.sanction.company"]
        record_source_company = str(
            getattr(source_sanction_company, "record_source")
        ).replace("'", "\\'")
        dv_source_version_company = str(
            getattr(source_sanction_company, "dv_source_version")
        ).replace("'", "\\'")
        sanction_company = source_sanction_company.dataframe
        sanction_company.createOrReplaceTempView("sanction_company")

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
        h_sanction_person_df = self.spark.sql(
            f"""
            select
                {DV_HASHKEY_SANCTION} as dv_hashkey_sanction,
                dv_recsrc,
                FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts,
                dv_source_version,
                jurisdiction,
                id,
                sanction_id,
                source_schema
            from (
                select
                    sp.dv_recsrc as dv_recsrc,
                    sp.dv_source_version as dv_source_version,
                    sp.jurisdiction as jurisdiction,
                    sp.id as id,
                    ss.id as sanction_id,
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
                    ) sp
                left join sanction_sanction ss on sp.id=ss.id
            )
            """
        )
        h_sanction_person_df.createOrReplaceTempView("h_sanction_person_df")
        h_sanction_company_df = self.spark.sql(
            f"""
            select
                {DV_HASHKEY_SANCTION} as dv_hashkey_sanction,
                dv_recsrc,
                FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts,
                dv_source_version,
                jurisdiction,
                id,
                sanction_id,
                source_schema
            from (
                select
                    sc.dv_recsrc as dv_recsrc,
                    sc.dv_source_version as dv_source_version,
                    sc.jurisdiction as jurisdiction,
                    sc.id as id,
                    ss.id as sanction_id,
                    sc.source_schema as source_schema
                from (
                    select
                        '{record_source_company}' as dv_recsrc,
                        '{dv_source_version_company}' as dv_source_version,
                        country_name as jurisdiction,
                        CAST(id_number AS STRING) as id,
                        'company' as source_schema
                    from sanction_company
                    where is_sanctioned = 1
                    ) sc
                left join sanction_sanction ss on sc.id=ss.id
            )
            """
        )
        h_sanction_company_df.createOrReplaceTempView("h_sanction_company_df")
        df = self.spark.sql(
            f"""
            select
                dv_hashkey_sanction,
                dv_recsrc,
                dv_loaddts,
                dv_source_version,
                jurisdiction,
                id,
                sanction_id,
                source_schema
            from (
                select
                    dv_hashkey_sanction,
                    dv_recsrc,
                    dv_loaddts,
                    dv_source_version,
                    jurisdiction,
                    id,
                    sanction_id,
                    source_schema
                from h_sanction_person_df
                union all
                select
                    dv_hashkey_sanction,
                    dv_recsrc,
                    dv_loaddts,
                    dv_source_version,
                    jurisdiction,
                    id,
                    sanction_id,
                    source_schema
                from h_sanction_company_df
            )
            """
        )
        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/h_sanction.yaml"], payload=payload)

    executer = HSanctionExecuter(
        app_name="bronze.h_sanction.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=[
            "bronze.sanction.person",
            "bronze.sanction.company",
            "bronze.sanction.sanction",
        ],
    )
    executer.execute()


if __name__ == "__main__":
    run()
