from pyspark.sql import functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class PortExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["silver.lookup_port"]
        port = source.dataframe

        port = (
            port.withColumnRenamed("dv_hashkey_port", "id").withColumn(
                "previous_port_name", F.split("previous_port_name", ";")
            )
        ).drop("dv_loaddts")
        return port.selectExpr(
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            *port.columns,
        )


def run(env="pro", payload={}, params={}, spark=None):
    import sys

    table_meta_hub = TableMeta(
        from_files=["metas/gold/trade_service/port.yaml"], payload=payload
    )
    executer_hub = PortExecuter(
        sys.argv[0], table_meta_hub.model, table_meta_hub.input_resources, params, spark
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
