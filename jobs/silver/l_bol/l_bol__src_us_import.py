import pyspark.sql.functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta

import libs.utils.vault_hashfuncs as H


class LbolUSImportExecuter(RawVaultExecuter):
    def transform(self):
        df = self.input_dataframe_dict["bronze.us.import"]
        df = df.dataframe
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
            .drop("weight")
            .withColumnRenamed("port_name", "unloading_port_name")
            .withColumn(
                "loading_port_name",
                F.concat_ws(
                    ", ",
                    F.when(
                        F.size(F.split(F.col("loading_port"), ", ")) > 1,
                        F.concat_ws(
                            ", ",
                            F.slice(
                                F.split(F.col("loading_port"), ", "),
                                2,
                                F.size(F.split(F.col("loading_port"), ", ")),
                            ),
                        ),
                    ).otherwise(F.lit("Unknown")),
                    F.concat_ws(
                        ", ",
                        F.slice(
                            F.split(F.col("Country"), ", "),
                            2,
                            F.size(F.split(F.col("Country"), ", ")),
                        ),
                    ),
                ),
            )
            .withColumn(
                "loading_port_code", F.split(F.col("loading_port"), ", ").getItem(0)
            )
            .withColumn(
                "hs_code_ref",
                F.when(
                    F.length("hs_code") >= 8,
                    F.md5(F.concat("hs_code", F.lit("United States"))),
                ).otherwise(F.md5("hs_code")),
            )
            .withColumn(
                "jurisdiction_supplier",
                F.when(
                    F.size(F.split(F.col("country"), ", ")) > 2,
                    F.concat_ws(
                        ", ",
                        F.slice(
                            F.split(F.col("country"), ", "),
                            2,
                            F.size(F.split(F.col("country"), ", ")),
                        ),
                    ),
                ).otherwise(F.lit("Unknown")),
            )
            .withColumn("jurisdiction_buyer", F.col("state"))
            .withColumn("weight_unit", F.lit("KG"))
            .withColumnRenamed("weight_in_kg", "weight")
            .withColumnRenamed("product_desc", "description")
            .withColumn("invoice_value", F.lit(None).cast("string"))
            .withColumn("value_usd", F.lit(None).cast("string"))
            .withColumn("exchange_rate", F.lit(None).cast("string"))
            .withColumn("registration_number_supplier", F.lit(None).cast("string"))
            .withColumn("registration_number_buyer", F.lit(None).cast("string"))
            .withColumnRenamed("bill_of_lading", "bol")
            .withColumnRenamed("teu", "teu_number")
            .withColumnRenamed("raw_shipper_name", "name_supplier")
            .withColumnRenamed("raw_consignee_name", "name_buyer")
        )
        return df.selectExpr(
            f"{H.DV_HASHKEY_BOL} as bol_id",  ## missing invoice_value, value_usd, exchange_rate
            f"{H.DV_HASHKEY_SUPPLIER} as supplier_id",
            f"{H.DV_HASHKEY_BUYER} as buyer_id",
            f"{H.DV_HASHKEY_EXPORT_PORT_ID} as export_port_id",
            f"{H.DV_HASHKEY_IMPORT_PORT_ID} as import_port_id",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "actual_arrival_date",
            f"{H.DV_HASHKEY_L_BOL} as dv_hashkey_l",
        )


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/l_bol.yaml"])

    executer_hub = LbolUSImportExecuter(
        "silver/l_bol/l_bol__src_us_import",
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
