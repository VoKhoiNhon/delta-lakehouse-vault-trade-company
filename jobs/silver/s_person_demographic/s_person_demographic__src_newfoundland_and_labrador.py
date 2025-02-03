from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class SPersonNewfoundlandAndLabradorExecuterDemographic(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.newfoundland_and_labrador"]
        df = source.dataframe
        df = df.filter(F.col("officer_name").isNotNull()).orderBy(
            "officer_name", ascending=True
        )
        df = df.withColumnRenamed("officer_name", "name")
        df = df.withColumnRenamed("company_number", "registration_number")
        df = df.withColumn("jurisdiction", F.lit("CA.Newfoundland and Labrador"))
        df = df.withColumn("full_address", F.lit(None).cast("string"))
        df = df.selectExpr(
            f"{DV_HASHKEY_PERSON} as dv_hashkey_person",
            "concat_ws(';', 'Newfoundland and Labrador', 'https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx') as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'Newfoundland and Labrador-20241231' as dv_source_version",
            "jurisdiction",
            "name",
            "NULL as first_name",
            "NULL as last_name",
            "NULL as middle_name",
            "NULL as image_url",
            "NULL as dob",
            "NULL as birthplace",
            "NULL as nationality",
            "NULL as country_of_residence",
            "NULL as accuracy_level",
            "NULL as gender",
            "NULL as skills",
            "NULL as job_summary",
            "NULL as salary",
            "NULL as yoe",
            "NULL as industry",
            "NULL as phone_numbers",
            "NULL as emails",
            "NULL as linkedin_url",
            "NULL as twitter_url",
            "NULL as facebook_url",
        )
        return df


def run():

    table_meta_sat_graphic = TableMeta(
        from_files="metas/silver/s_person_demographic.yaml",
    )

    executer_sat_graphic = SPersonNewfoundlandAndLabradorExecuterDemographic(
        "silver/s_person_demographic/s_person_demographic__src_kentucky",
        table_meta_sat_graphic.model,
        table_meta_sat_graphic.input_resources,
    )
    executer_sat_graphic.execute()


if __name__ == "__main__":
    run()
