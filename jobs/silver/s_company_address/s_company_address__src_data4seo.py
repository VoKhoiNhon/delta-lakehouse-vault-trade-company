from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
import sys
from libs.utils.commons import add_pure_company_name

"""
Hiện tại, để chạy kịp các task hiện tại (trên Notebook), sử dụng bảng
`lakehouse-bronze/data4seo/_procompany`
join với `s3a://lakehouse-bronze/tmp/data4seo/company_add_pure_name`.
Về sau, các jobs cần chạy sẽ thực hiện trên bảng `lakehouse-bronze/data4seo/company`.
Phần Code này sẽ được apply với `lakehouse-bronze/data4seo/company`.
"""


class SCompanyAddressData4seoExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.data4seo"]
        record_source = source.record_source
        dv_source_version = source.dv_source_version
        df = source.dataframe
        # df = add_pure_company_name(df, "name")
        df = (
            df.withColumn("registration_number", F.lit(None))
            .withColumn("state", F.lit(None))
            .withColumn("province", F.lit(None))
            .withColumn("type", F.lit(1))
        )

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "jurisdiction",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            f"'{dv_source_version}' as dv_source_version",
            "country_code",
            "country_name",
            "pure_name",
            "registration_number",
            "postal_code",
            "full_address",
            "street",
            "city",
            "region",
            "state",
            "province",
            "latitude",
            "longitude",
            "type",
            "NULL as start_date",
            "NULL as end_date",
        )
        # df.show()
        return df


def run():

    table_meta_sat_company_address = TableMeta(
        from_files=["metas/silver/s_company_address.yaml"]
    )
    executer_sat_company_address = SCompanyAddressData4seoExecuter(
        app_name="data4seo_bronze_to_silver_sat_company_address",
        meta_table_model=table_meta_sat_company_address.model,
        meta_input_resource=table_meta_sat_company_address.input_resources,
    )
    executer_sat_company_address.execute()


if __name__ == "__main__":
    run()
