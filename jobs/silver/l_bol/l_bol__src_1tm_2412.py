import pyspark.sql.functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
import libs.utils.vault_hashfuncs as H


class Lbol1tmExecuter(RawVaultExecuter):
    def transform(self):
        bol = self.input_dataframe_dict["1tm_2412.bol"].dataframe
        company = self.input_dataframe_dict["1tm_2412.company_unique_id"].dataframe
        record_source = self.input_dataframe_dict["1tm_2412.bol"].record_source
        # .selectExpr(
        #     f"{H.DV_HASHKEY_COMPANY} as trade_id", "id"
        # )
        port = self.input_dataframe_dict["1tm_2412.port"].dataframe.selectExpr(
            f"{H.DV_HASHKEY_EXPORT_PORT_ID} as port_id", "id", "port_code", "port_name"
        )

        df_result = (
            bol.alias("bol")
            .join(
                company.alias("cs"),
                on=F.col("bol.supplier_id") == F.col("cs.id"),
                how="left",
            )
            .withColumn("supplier_dv_hashkey_company", F.col("cs.DV_HASHKEY_COMPANY"))
            .join(
                company.alias("cb"),
                on=F.col("bol.buyer_id") == F.col("cb.id"),
                how="left",
            )
            .withColumn("buyer_dv_hashkey_company", F.col("cb.DV_HASHKEY_COMPANY"))
            .join(
                company.alias("sh"),
                on=F.col("bol.shipper_id") == F.col("sh.id"),
                how="left",
            )
            .withColumn("shipper_dv_hashkey_company", F.col("sh.DV_HASHKEY_COMPANY"))
            .join(
                port.alias("ep"),
                on=F.col("bol.export_port") == F.col("ep.id"),
                how="inner",
            )
            .withColumn("export_port", F.col("ep.port_id"))
            .join(
                port.alias("ip"),
                on=F.col("bol.import_port") == F.col("ip.id"),
                how="inner",
            )
            .withColumn("import_port", F.col("ip.port_id"))
        )

        df_result = df_result.selectExpr(
            f"{H.DV_HASHKEY_BOL} as dv_hashkey_bol",
            f"{H.DV_HASHKEY_L_BOL} as dv_hashkey_l_bol",
            f"'{record_source}' as dv_recsrc",
            "'1tm' as dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "buyer_dv_hashkey_company",
            "supplier_dv_hashkey_company",
            "import_port",
            "export_port",
            "bol",
            "actual_arrival_date",
        )
        return df_result


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/l_bol.yaml", env=env)
    executer = Lbol1tmExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
