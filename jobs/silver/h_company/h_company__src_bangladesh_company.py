import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name

# Example run:
# python3 jobs/silver/h_company/h_company__src_bangladesh_company.py
# --payload '{"load_date":"20241128","dv_source_version": "bangladesh_company;20241128", "env":"/test_on_s3"}'


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        # Transform DF
        source = self.input_dataframe_dict["bronze.bangladesh"]
        record_source = source.record_source.replace("'", "\\'")
        dv_source_version = source.dv_source_version.replace("'", "\\'")
        df = source.dataframe
        df = df.withColumn("jurisdiction", F.lit("Bangladesh")).withColumnRenamed(
            "name_status", "registration_number"
        )

        # Get DF for HubCompany
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
                when registration_number is null then 0
            else 1 end as has_regnum
            """,
        )
        # df.show(n=5)
        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/h_company.yaml"], payload=payload)

    executer = HCompanyExecuter(
        app_name="bronze_h_company_bangladesh",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.bangladesh"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
