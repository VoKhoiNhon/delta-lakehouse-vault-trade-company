import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.base_transforms import create_temp_column
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY


class HCompanyExecuter(RawVaultExecuter):
    def transform(self):
        df = self.input_dataframe_dict["bronze.taiwan"]
        df = df.withColumn("jurisdiction", F.lit("Taiwan"))
        df = create_temp_column(df, "name")
        return df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            "'https://data.gcis.nat.gov.tw/od/datacategory' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "jurisdiction",
            "registration_number",
            "name",
        ).drop("temp_name")


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/h_company.yaml"])

    executer_hub = HCompanyExecuter(
        "silver/h_company/h_company-s_company_taiwan.py",
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
