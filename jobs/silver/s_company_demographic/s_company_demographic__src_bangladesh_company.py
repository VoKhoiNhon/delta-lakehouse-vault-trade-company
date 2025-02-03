from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY, DV_HASHKEY_INDUSTRY

# Example run:
# python3 jobs/silver/s_company_demographic/s_company_demographic__src_bangladesh_company.py
# --payload '{"load_date":"20241128","dv_source_version": "bangladesh_company;20241128", "env":"/test_on_s3"}'


class SCompanyDemographicBangladeshExecuter(SatelliteVaultExecuter):
    def transform(self):

        # Transform DF
        source = self.input_dataframe_dict["bronze.bangladesh"]
        record_source = source.record_source.replace("'", "\\'")
        dv_source_version = source.dv_source_version.replace("'", "\\'")
        df = source.dataframe
        df = (
            df.withColumn("jurisdiction", F.lit("Bangladesh"))
            .withColumn("currency_code", F.lit("BDT"))
            .withColumn("is_branch", F.lit(False))
            .withColumn("country_code", F.lit("BD"))
            .withColumn("country_name", F.lit("Bangladesh"))
            .withColumn("industry_code", F.lit("NULL"))
            .withColumnRenamed("entity_type", "category")
            .withColumnRenamed("name_status", "registration_number")
            .withColumn("start_date", F.lit(None))
            .withColumn("end_date", F.lit(None))
        )

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "1 as dv_status",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            "jurisdiction",
            "registration_number",
            f"'{dv_source_version}' as dv_source_version",
            "name",
            "pure_name",
            "NULL as date_incorporated",
            "country_code",
            "country_name",
            "NULL as lei_code",
            "NULL as date_struck_off",
            "NULL as legal_form",
            "NULL as category",
            "NULL as description",
            "NULL as phone_numbers",
            "NULL as emails",
            "NULL as websites",
            "NULL as facebook_url",
            "NULL as other_names",
            "NULL as status",
            "NULL as no_of_employees",
            "NULL as authorised_capital",
            "is_branch",
            "currency_code",
            "1 as status_code",
        )
        # df.show(5)
        return df


def run(payload=None):

    table_meta = TableMeta(
        from_files=["metas/silver/s_company_demographic.yaml"], payload=payload
    )
    executer = SCompanyDemographicBangladeshExecuter(
        app_name="bangladesh_bronze_to_silver",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
