import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType
from libs.meta import TableMeta
from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY, DV_HASHKEY_INDUSTRY
from libs.utils.commons import add_pure_company_name

"""
Hiện tại, để chạy kịp các task hiện tại (trên Notebook), sử dụng bảng
`lakehouse-bronze/data4seo/_procompany`
join với `s3a://lakehouse-bronze/tmp/data4seo/company_add_pure_name`.
Về sau, các jobs cần chạy sẽ thực hiện trên bảng `lakehouse-bronze/data4seo/company`.
Phần Code này sẽ được apply với `lakehouse-bronze/data4seo/company`.
"""


class SCompanyDemographicData4seoExecuter(SatelliteVaultExecuter):
    def transform(self):

        # Transform DF
        source = self.input_dataframe_dict["bronze.data4seo"]
        record_source = source.record_source
        dv_source_version = source.dv_source_version
        df = source.dataframe

        df = (
            df.withColumn("is_branch", F.lit(False))
            .withColumn("industry_code", F.lit("NULL"))
            .withColumn("registration_number", F.lit(None))
            .withColumn(
                "phone_array", F.from_json(F.col("phone"), ArrayType(StringType()))
            )
            .withColumn(
                "mail_array", F.from_json(F.col("mail"), ArrayType(StringType()))
            )
            .withColumn(
                "website_array", F.from_json(F.col("website"), ArrayType(StringType()))
            )
        )

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
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
            "category_list as category",
            "description",
            "phone_array as phone_numbers",
            "mail_array as emails",
            "website_array as  websites",
            "facebook_url as facebook_url",
            "NULL as other_names",
            "NULL as status",
            "NULL as no_of_employees",
            "NULL as authorised_capital",
            "is_branch",
            "NULL as currency_code",
            "1 as status_code",
            f"{DV_HASHKEY_INDUSTRY} as dv_hashkey_industry",
        )
        return df


def run():

    table_meta_sat_company_demographic = TableMeta(
        from_files=["metas/silver/s_company_demographic.yaml"]
    )
    executer_sat_company_demographic = SCompanyDemographicData4seoExecuter(
        app_name="data4seo_bronze_to_silver_sat_company_demographic",
        meta_table_model=table_meta_sat_company_demographic.model,
        meta_input_resource=table_meta_sat_company_demographic.input_resources,
    )
    executer_sat_company_demographic.execute()


if __name__ == "__main__":
    run()
