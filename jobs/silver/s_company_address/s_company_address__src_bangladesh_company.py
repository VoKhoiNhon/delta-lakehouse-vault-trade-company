from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY

# Example run:
# python3 jobs/silver/s_company_address/s_company_address__src_bangladesh_company.py
# --payload '{"load_date":"20241128","dv_source_version": "bangladesh_company;20241128", "env":"/test_on_s3"}'


class SCompanyAddressBangladeshExecuter(SatelliteVaultExecuter):
    def transform(self):

        # Transform DF
        source = self.input_dataframe_dict["bronze.bangladesh"]
        record_source = source.record_source.replace("'", "\\'")
        dv_source_version = source.dv_source_version.replace("'", "\\'")
        df = source.dataframe
        df = (
            df.withColumn("jurisdiction", F.lit("Bangladesh"))
            .withColumn("country_name", F.lit("Bangladesh"))
            .withColumn("country_code", F.lit("BD"))
            .withColumn("full_address", F.lit(None))
            .withColumn("street", F.lit(None))
            .withColumn("city", F.lit(None))
            .withColumn("region", F.lit(None))
            .withColumn("state", F.lit(None))
            .withColumn("province", F.lit(None))
            .withColumn("postal_code", F.lit(None))
            .withColumn("latitude", F.lit(None))
            .withColumn("longitude", F.lit(None))
            .withColumn("type", F.lit(None))
            .withColumn("start_date", F.lit(None))
            .withColumn("end_date", F.lit(None))
            .withColumnRenamed("name_status", "registration_number")
        )
        df = df.selectExpr(
            f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
            f"'{record_source}' as dv_recsrc",
            "jurisdiction",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "1 as dv_status",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            f"'{dv_source_version}' as dv_source_version",
            "registration_number",
            "name",
            "pure_name",
            "country_code",
            "country_name",
            "full_address",
            "street",
            "city",
            "region",
            "state",
            "province",
            "postal_code",
            "latitude",
            "longitude",
            "type",
            "start_date",
            "end_date",
        )
        # df.show()
        return df


def run(payload=None):
    table_meta = TableMeta(
        from_files=["metas/silver/s_company_address.yaml"], payload=payload
    )
    executer = SCompanyAddressBangladeshExecuter(
        app_name="s_company_address_bangladesh",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
