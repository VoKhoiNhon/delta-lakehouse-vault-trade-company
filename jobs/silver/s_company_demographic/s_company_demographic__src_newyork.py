from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY
from libs.utils.commons import add_pure_company_name
import sys


class SCompanyDemographicNewYorkExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.newyork"]
        dv_source_version = self.params.get("dv_source_version", "")
        record_source = source.record_source
        df = source.dataframe

        df = df.withColumn("jurisdiction", F.lit("New York")).withColumn(
            "is_branch", F.lit(False)
        )
        df = add_pure_company_name(df, "name")

        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "jurisdiction",
            "registration_number",
            f"'{dv_source_version}' as dv_source_version",
            "name",
            "pure_name",
            "is_branch",
            "'USD' as currency_code",
            "'Active' as status_code",
        )

        return df


def run():

    table_meta_hub = TableMeta(from_files=["metas/silver/s_company_demographic.yaml"])
    executer_hub = SCompanyDemographicNewYorkExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
