from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY


class SCompanyDemographicNewfoundlandAndLabradorExecuter(SatelliteVaultExecuter):
    def transform(self):
        # Transform DF
        source = self.input_dataframe_dict["bronze.newfoundland_and_labrador"]
        # record_source = source.record_source
        # dv_source_version = source.dv_source_version
        df = source.dataframe
        df = df.filter(F.col("company_name").isNotNull()).orderBy(
            "company_name", ascending=True
        )
        df = df.withColumnRenamed("company_name", "name")
        df = df.withColumnRenamed("company_number", "registration_number")
        df = df.withColumnRenamed("classification", "status_code")
        df = df.withColumn("jurisdiction", F.lit("CA.Newfoundland and Labrador"))
        df = df.withColumn("currency_code", F.lit("CAD"))
        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            "concat_ws(';', 'Newfoundland and Labrador', 'https://cado.eservices.gov.nl.ca/Company/CompanyNameNumberSearch.aspx') as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'Newfoundland and Labrador-20241231' as dv_source_version",
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            "NULL as lei_code",
            "NULL as description",
            "date_incorporated",
            "NULL as date_struck_off",
            "legal_form",
            "NULL as category",
            "NULL as phone_numbers",
            "NULL as emails",
            "NULL as websites",
            "NULL as linkedin_url",
            "NULL as twitter_url",
            "NULL as facebook_url",
            "NULL as fax_numbers",
            "NULL as other_names",
            "NULL as no_of_employees",
            "NULL as image_url",
            "NULL as authorised_capital",
            "NULL as paid_up_capital",
            "status",
            "is_branch",
            "currency_code",
            "NULL as status_desc",
            "status_code",
        )
        df = df.dropDuplicates(["dv_hashkey_company"])
        df.show(5)
        return df


def run():

    table_meta_sat_company_demographic = TableMeta(
        from_files=["metas/silver/s_company_demographic.yaml"]
    )
    executer_sat_company_demographic = SCompanyDemographicNewfoundlandAndLabradorExecuter(
        app_name="newfoundland_and_labrador_bronze_to_silver_sat_company_demographic",
        meta_table_model=table_meta_sat_company_demographic.model,
        meta_input_resource=table_meta_sat_company_demographic.input_resources,
    )
    executer_sat_company_demographic.execute()


if __name__ == "__main__":
    run()
