from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys
import libs.utils.vault_hashfuncs as H


class SBolUSImportExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.us.import"]
        record_source = source.record_source
        df = source.dataframe
        sample = self.spark.read.format("delta").load(
            "tests/resource/lakehouse-bronze/port"
        )
        df = df.withColumn(
            "unloading_port_code", F.split(F.col("unloading_port"), ", ").getItem(0)
        )
        df = (
            df.alias("a")
            .join(
                sample.alias("b"),
                on=F.col("a.unloading_port_code") == F.col("b.port_code"),
                how="left",
            )
            .filter(F.col("b.port_name").isNotNull())
        )

        df = (
            df.drop("weight")
            .withColumn(
                "hs_code_ref",
                F.when(
                    F.length("hs_code") >= 8,
                    F.md5(F.concat("hs_code", F.lit("United States"))),
                ).otherwise(F.md5("hs_code")),
            )
            .withColumnRenamed("bill_of_lading", "bol")
            .withColumnRenamed("teu", "teu_number")
            .withColumnRenamed("product_desc", "description")
            .withColumnRenamed("weight_in_kg", "weight")
            .withColumn("invoice_value", F.lit(None).cast("string"))
            .withColumn("value_usd", F.lit(None).cast("string"))
            .withColumn("exchange_rate", F.lit(None).cast("string"))
        )

        return df.selectExpr(
            f"{H.DV_HASHKEY_BOL} as dv_hashkey_bol",  ## missing invoice_value, value_usd, exchange_rate
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
        )


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/s_port.yaml"])
    executer_hub = SBolUSImportExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
