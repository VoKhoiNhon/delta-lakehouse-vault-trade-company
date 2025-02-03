from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.utils.commons import add_pure_company_name
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys
import libs.utils.vault_hashfuncs as H


class SBolBangladeshImportExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.bangladesh.import"]
        record_source = source.record_source.replace("'", "\\'")
        dv_source_version = source.dv_source_version.replace("'", "\\'")
        df = source.dataframe
        df = (
            df.withColumnRenamed("billid", "bol")
            .withColumnRenamed("hs", "hs_code")
            .withColumnRenamed("assessed_value_in_bdt", "invoice_value")
            .withColumnRenamed("descript", "description")
            .withColumnRenamed("unit_of_quantity", "quantity_unit")
            .withColumnRenamed("g_weight_in_kg", "weight")
        )
        df = (
            df.withColumn("value_usd", F.lit(None))
            .withColumn("teu_number", F.lit(None))
            .withColumn("vessel_name", F.lit(None))
            .withColumn("country_name", F.lit("Bangladesh"))
            .withColumn("country_code", F.lit("BD"))
            .withColumn("port_code", F.lit("Unspecified"))
            .withColumn("port_name", F.lit("Unspecified"))
            .withColumn("previous_port_name", F.lit("Unspecified"))
            .withColumn("latitude", F.lit(None))
            .withColumn("longitude", F.lit(None))
            .withColumn("jurisdiction", F.lit("Bangladesh"))
            .withColumn("weight_unit", F.lit("kg"))
            .withColumn("actual_arrival_date", F.col("date"))
            .withColumn("estimated_arrival_date", F.col("date"))
            .withColumn(
                "hs_code_ref",
                F.when(
                    F.length("hs_code") >= 8,
                    F.md5(F.concat("hs_code", F.lit("Bangladesh"))),
                ).otherwise(F.md5("hs_code")),
            )
            .withColumn("buyer_country", F.lit("Bangladesh"))
            .withColumn("seller_country", F.col("origin_country"))
        )
        df = add_pure_company_name(df=df, name="seller", return_col="pure_seller")
        df = add_pure_company_name(df=df, name="buyer", return_col="pure_buyer")
        df = df.selectExpr(
            f"{H.DV_HASHKEY_BOL} as dv_hashkey_bol",
            "1 as dv_status",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            f"'{record_source}' as dv_recsrc",
            "NULL as dv_valid_to",
            "bol",
            "hs_code",
            "hs_code_ref",
            "teu_number",
            "invoice_value",
            "value_usd",
            "exchange_rate",
            "description",
            "actual_arrival_date",
            "estimated_arrival_date",
            "vessel_name",
            "quantity",
            "quantity_unit",
            "weight",
            "weight_unit",
            # This is get from join with port_df
            "port_code",
            "port_name",
            # This is used for mapping with h_company -> l_bol
            "pure_seller",
            "seller_country",
            "pure_buyer",
            "buyer_country",
            # This is used for patitioning
            "jurisdiction",
        )
        # df.show(n=1, vertical=True, truncate=False)
        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/s_bol.yaml"], payload=payload)
    executer = SBolBangladeshImportExecuter(
        app_name="s_bol_bangladesh_import",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.bangladesh.import", "bronze.port"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
