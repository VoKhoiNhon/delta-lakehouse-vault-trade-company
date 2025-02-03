from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY

# Example run:
# python3 jobs/silver/s_company_address/s_company_address__src_sanction.py
# --payload '{"load_date":"20241128", "env":"/test_on_s3"}'


class SCompanyAddressExecuter(SatelliteVaultExecuter):
    def transform(self):

        # Transform DF
        source_sanction = self.input_dataframe_dict["bronze.sanction.company"]
        record_source = source_sanction.record_source.replace("'", "\\'")
        dv_source_version = source_sanction.dv_source_version.replace("'", "\\'")
        sanction_df = source_sanction.dataframe
        sanction_df = (
            sanction_df.withColumnRenamed("company_name", "name")
            .withColumnRenamed("address", "full_address")
            .withColumnRenamed("country", "country_code")
        )
        df = sanction_df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "jurisdiction",
            "registration_number",
            "name",
            "pure_name",
            "full_address",
            "NULL as street",
            "NULL as city",
            "NULL as region",
            "NULL as state",
            "NULL as province",
            "country_code",
            "country_name",
            "NULL as postal_code",
            "NULL as latitude",
            "NULL as longitude",
            "1 as type",
            "NULL as start_date",
            "NULL as end_date",
        )
        # df.show()
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files=["metas/silver/s_company_address.yaml"], payload=payload
    )
    executer = SCompanyAddressExecuter(
        app_name="silver.s_company_address.sanction",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.sanction.company"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
