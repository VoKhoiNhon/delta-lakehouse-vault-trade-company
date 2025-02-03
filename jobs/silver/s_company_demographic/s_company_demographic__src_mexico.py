from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY, DV_HASHKEY_INDUSTRY
import sys


class SCompanyDemographicMexicoExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.mexico"]
        dv_source_version = self.params.get("dv_source_version", "")
        record_source = source.record_source
        df = source.dataframe

        df = df.dropDuplicates(["registration_number", "legal_name"])
        df = df.withColumnRenamed("legal_name", "name")
        df = df.filter(
            F.col("registration_number").isNotNull() & F.col("name").isNotNull()
        )

        df = (
            df.withColumn("jurisdiction", F.lit("Mexico"))
            .withColumn("is_branch", F.lit(False))
            .withColumn("country_code", F.lit("MX"))
            .withColumn("country_name", F.lit("Mexico"))
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
            "date_incorporated",
            "country_code",
            "country_name",
            "NULL as lei_code",
            "NULL as date_sturck_off",
            "legal_form",
            "NULL as category",
            "NULL as description",
            "phone_numbers",
            "emails",
            "websites",
            "facebook_url",
            "other_names",
            "status",
            "NULL as no_of_employees",
            "NULL as authorised_capital",
            "is_branch",
            "'MXN' as currency_code",
            "'Active' as status_code",
            f"{DV_HASHKEY_INDUSTRY} as dv_hashkey_industry",
        )
        df.show(5)
        return df


def run():
    import sys

    table_meta_hub = TableMeta(from_files=["metas/silver/s_company_demographic.yaml"])
    executer_hub = SCompanyDemographicMexicoExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
