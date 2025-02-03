from pyspark.sql import functions as F

from libs.executers.gold_executer import GoldTradeExecuter
from libs.meta import TableMeta


class TransactionExecuter(GoldTradeExecuter):
    def transform(self):
        source = self.input_dataframe_dict["silver.bol"]
        transaction = source.dataframe
        l_b_c = self.input_dataframe_dict["silver.l_bol"].dataframe
        bridge_company_key = self.input_dataframe_dict[
            "silver.bridge_company_key"
        ].dataframe
        hscode = self.input_dataframe_dict["silver.hscode"].dataframe

        l_b_c = (
            l_b_c.alias("bol")
            .join(
                bridge_company_key.alias("bckb"),
                (F.col("bckb.from_key") == F.col("bol.buyer_dv_hashkey_company")),
                "left",
            )
            .join(
                bridge_company_key.alias("bcks"),
                (F.col("bcks.from_key") == F.col("bol.supplier_dv_hashkey_company")),
                "left",
            )
            .join(
                bridge_company_key.alias("bcksh"),
                (F.col("bcksh.from_key") == F.col("bol.shipper_dv_hashkey_company")),
                "left",
            )
            .selectExpr(
                "dv_hashkey_l_bol",
                "case when bckb.to_key is not null then bckb.to_key else bol.buyer_dv_hashkey_company end as buyer_id",
                "case when bcks.to_key is not null then bcks.to_key else bol.supplier_dv_hashkey_company end as supplier_id",
                "case when bcksh.to_key is not null then bcksh.to_key else bol.shipper_dv_hashkey_company end as shipper_id",
                "dv_hashkey_bol as dv_hashkey_bol",
                "import_port",
                "export_port",
            )
            .filter(F.col("buyer_id").isNotNull())
            .filter(F.col("supplier_id").isNotNull())
        )

        transaction = (
            transaction.withColumn("yyyy", F.year("actual_arrival_date"))
            .withColumn(
                "yyyymm",
                F.expr("year(actual_arrival_date) * 100 + month(actual_arrival_date)"),
            )
            .withColumn(
                "yyyymmdd",
                F.expr(
                    "year(actual_arrival_date) * 10000 + month(actual_arrival_date) * 100 + day(actual_arrival_date)"
                ),
            )
            .alias("t")
            .join(l_b_c.alias("l"), "dv_hashkey_bol", "inner")
            .join(hscode.alias("c"), F.col("t.bol") == F.col("c.code"), "left")
            .withColumn(
                "hs_code_ref",
                F.when(
                    F.col("c.dv_hashkey_hscode").isNotNull(), F.col("t.bol")
                ).otherwise(F.lit("dummy")),
            )
            .select(
                "t.*",
                "l.buyer_id",
                "l.supplier_id",
                "l.shipper_id",
                "hs_code_ref",
                "l.import_port",
                "l.export_port",
            )
            .withColumnRenamed("dv_hashkey_bol", "id")
            .drop("dv_loaddts")
        )
        return transaction.selectExpr(
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as created_at",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as updated_at",
            *transaction.columns
        )


def run(env="pro", payload={}, spark=None):
    import sys

    table_meta_hub = TableMeta(
        from_files=["metas/gold/trade_service/transaction.yaml"], payload=payload
    )
    executer_hub = TransactionExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
        spark=spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
