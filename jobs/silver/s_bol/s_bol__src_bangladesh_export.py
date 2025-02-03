from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.utils.commons import add_pure_company_name
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys
import libs.utils.vault_hashfuncs as H


class SBolBangladeshexportExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.bangladesh.export"]
        record_source = source.record_source.replace("'", "\\'")
        dv_source_version = source.dv_source_version.replace("'", "\\'")
        df = source.dataframe
        port_df = self.input_dataframe_dict["bronze.port"].dataframe
        df = df.alias("bangladesh_trade_export").join(
            port_df.alias("port"),
            on=(
                (
                    F.col("bangladesh_trade_export.seller_port")
                    == F.col("port.port_name")
                )
                & (
                    F.col("bangladesh_trade_export.seller_country")
                    == F.col("port.country_name")
                )
            ),
            how="left",
        )
        df = (
            df.withColumnRenamed("billid", "bol")
            .withColumnRenamed("hs", "hs_code")
            .withColumnRenamed("n_weight_kg", "teu_number")
            .withColumnRenamed("cargo_value_local_currency", "invoice_value")
            .withColumnRenamed("descript", "description")
            .withColumnRenamed("date", "actual_arrival_date")
            .withColumnRenamed("qty", "quantity")
            .withColumnRenamed("qty_unit", "quantity_unit")
        )
        df = (
            df.withColumn(
                "hs_code_ref",
                F.when(
                    F.length("hs_code") >= 8,
                    F.md5(F.concat("hs_code", F.lit("Bangladesh"))),
                ).otherwise(F.md5("hs_code")),
            )
            .withColumn("estimated_arrival_date", F.lit(None))
            .withColumn("vessel_name", F.lit(None))
            .withColumn("value_usd", F.lit(None).cast("string"))
            .withColumn("exchange_rate", F.lit(None).cast("string"))
            .withColumn("jurisdiction", F.col("seller_country"))
        )
        df = add_pure_company_name(df=df, name="seller", return_col="pure_seller")
        df = add_pure_company_name(df=df, name="buyer", return_col="pure_buyer")
        df = df.selectExpr(
            f"{H.DV_HASHKEY_BOL} as dv_hashkey_bol",
            "1 as dv_status",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
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
            "port.port_code",
            "port.port_name",
            # This is used for mapping with h_company -> l_bol
            "pure_seller",
            "seller_country",
            "pure_buyer",
            "buyer_country",
            # This is used for patitioning
            "jurisdiction",
        )

        return df


def run(payload=None):
    table_meta = TableMeta(from_files=["metas/silver/s_bol.yaml"], payload=payload)
    executer = SBolBangladeshexportExecuter(
        app_name="s_bol_bangladesh_export",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        spark_resources=table_meta.spark_resources,
        filter_input_resources=["bronze.bangladesh.export", "bronze.port"],
    )
    executer.execute()


if __name__ == "__main__":
    run()
