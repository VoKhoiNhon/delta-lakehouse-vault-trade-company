import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name

"""
Hiện tại, để chạy kịp các task hiện tại (trên Notebook), sử dụng bảng
`lakehouse-bronze/data4seo/_procompany`
join với `s3a://lakehouse-bronze/tmp/data4seo/company_add_pure_name`.
Về sau, các jobs cần chạy sẽ thực hiện trên bảng `lakehouse-bronze/data4seo/company`.
Phần Code này sẽ được apply với `lakehouse-bronze/data4seo/company`.
"""


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.data4seo"]
        record_source = source.record_source
        dv_source_version = source.dv_source_version
        df = source.dataframe
        # df = add_pure_company_name(df, "name")
        df = df.withColumn("registration_number", F.lit(None))
        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            """
            case
                when registration_number is null then 'reg_num_not_exists'
            else 'reg_num_exists' end as type
            """,
        )
        return df


def run():
    table_meta_hub_company = TableMeta(from_files="metas/silver/h_company.yaml")
    executer_link_position = HCompanyExecuter(
        app_name="data4seo_bronze_to_silver",
        meta_table_model=table_meta_hub_company.model,
        meta_input_resource=table_meta_hub_company.input_resources,
    )
    executer_link_position.execute()


if __name__ == "__main__":

    run()
