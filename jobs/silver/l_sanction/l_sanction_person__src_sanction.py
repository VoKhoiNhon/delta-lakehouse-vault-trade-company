from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_L_SANCTION_PERSON


class SSanctionPersonExecuter(RawVaultExecuter):
    def transform(self):
        sources = self.input_dataframe_dict

        #####################################
        # Get source silver.s_sanction.person
        #####################################
        source_s_sanction_person = sources["silver.s_sanction.person"]
        record_source_person = str(
            getattr(source_s_sanction_person, "record_source")
        ).replace("'", "\\'")
        dv_source_version_person = str(
            getattr(source_s_sanction_person, "dv_source_version")
        ).replace("'", "\\'")
        s_sanction_person = source_s_sanction_person.dataframe
        s_sanction_person.createOrReplaceTempView("s_sanction_person")

        ##############################
        # Get source silver.h_sanction
        ##############################
        source_h_sanction = sources["silver.h_sanction"]
        h_sanction = source_h_sanction.dataframe
        h_sanction.createOrReplaceTempView("h_sanction")

        ###########
        # Query
        ###########
        df = self.spark.sql(
            f"""
            select
               {DV_HASHKEY_L_SANCTION_PERSON} as dv_hashkey_link_sanction,
               joined.dv_recsrc as dv_recsrc,
               joined.dv_source_version as dv_source_version,
               FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts,
               joined.dv_hashkey_person as dv_hashkey_person,
               joined.dv_hashkey_sanction as dv_hashkey_sanction
            from (
                select
                    '{record_source_person}' as dv_recsrc,
                    ssp.dv_hashkey_sanction as dv_hashkey_person,
                    '{dv_source_version_person}' as dv_source_version,
                    coalesce(hs.dv_hashkey_sanction,'CANT MAP') as dv_hashkey_sanction
                from s_sanction_person ssp
                left join h_sanction hs on ssp.sanction_id = hs.sanction_id
            ) joined
            """
        )
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files=["metas/silver/l_sanction_person.yaml"], payload=payload
    )

    executer = SSanctionPersonExecuter(
        app_name="silver.l_sanction_person",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["silver.s_sanction.person", "silver.h_sanction"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
