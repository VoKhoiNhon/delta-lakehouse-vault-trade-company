from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class SPersonDemographicExecuter(RawVaultExecuter):
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
                    sp.first_name as first_name,
                    sp.last_name as last_name,
                    sp.middle_name as middle_name,
                    1 as is_person,
                    NULL as image_url,
                    sp.dob as dob,
                    sp.birthplace as birthplace,
                    sp.nationality as nationality,
                    sp.country_name as country_of_residence,
                    NULL as accuracy_level,
                    sp.gender_numeric as gender,
                    NULL as skills,
                    NULL as job_summary,
                    NULL as salary,
                    NULL as yoe,
                    NULL as industry,
                    sp.phone_numbers as phone_numbers,
                    sp.emails as emails,
                    NULL as linkedin_url,
                    NULL as twitter_url,
                    NULL as facebook_url
                from sanction_person sp
            ) sp
            """
        )
        df.show(n=1, vertical =True, truncate = False)
        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/s_person_demographic.yaml"], payload=payload)

    executer = SPersonDemographicExecuter(
        app_name="bronze.s_person_demographic.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.sanction.person"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
