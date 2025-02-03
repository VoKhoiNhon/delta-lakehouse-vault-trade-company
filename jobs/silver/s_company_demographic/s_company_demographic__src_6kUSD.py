from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F


class SCompanyDemographicExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["trade_nbd"]
        df = source.dataframe
        df = df.withColumn(
            "dv_source_version",
            F.concat_ws(";", F.col("data_source"), F.col("load_date")),
        )
        import_df = df.select(
            F.col("import_country").alias("jurisdiction"),
            F.col("importer").alias("name"),
            F.col("pure_importer").alias("pure_name"),
            F.col("buyer_dv_hashkey_company").alias("dv_hashkey_company"),
            "dv_recsrc",
            "dv_source_version",
        )

        export_df = df.select(
            F.col("export_country_or_area").alias("jurisdiction"),
            F.col("exporter").alias("name"),
            F.col("pure_exporter").alias("pure_name"),
            F.col("supplier_dv_hashkey_company").alias("dv_hashkey_company"),
            "dv_recsrc",
            "dv_source_version",
        )
        df = import_df.union(export_df).dropDuplicates()
        df = df.withColumn(
            "jurisdiction",
            F.when(F.col("jurisdiction").isNull(), F.lit("unspecified")).otherwise(
                F.col("jurisdiction")
            ),
        )
        df = df.selectExpr(
            "dv_hashkey_company",
            "dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "dv_source_version",
            "jurisdiction",
            "NULL as registration_number",
            "name",
            "pure_name",
            "NULL as lei_code",
            "NULL as description",
            "NULL as date_incorporated",
            "NULL as date_struck_off",
            "NULL as legal_form",
            "NULL as category",
            "NULL as phone_numbers",
            "NULL as emails",
            "NULL as websites",
            "NULL as linkedin_url",
            "NULL as twitter_url",
            "NULL as facebook_url",
            "NULL as fax_numbers",
            "NULL as other_names",
            "NULL as no_of_employees",
            "NULL as image_url",
            "NULL as authorised_capital",
            "NULL as paid_up_capital",
            "NULL as status",
            "NULL as is_branch",
            "NULL as currency_code",
            "NULL as status_desc",
            "NULL as status_code",
        )
        df.show(5)
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files="metas/silver/s_company_demographic.yaml", env=env
    )
    executer = SCompanyDemographicExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
