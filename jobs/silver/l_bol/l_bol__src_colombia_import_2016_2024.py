import pyspark.sql.functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta

from libs.utils.commons import add_pure_company_name
import libs.utils.vault_hashfuncs as H


class Lbol1tmExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["colombia.import_2016-2024"]
        df = source.dataframe
        record_source = source.record_source
        lookup_port = self.input_dataframe_dict["lookup_port"].dataframe

        # import company
        df = (
            df.withColumnsRenamed(
                {
                    "name_import": "name",
                    "regis_import": "registration_number",
                    "address_import": "full_address",
                    "tele_import": "phone_numbers",
                }
            )
            .withColumnRenamed("jurisdiction", "jurisdiction_export")
            .withColumn("jurisdiction", F.lit("Colombia"))
            .withColumn(
                "port_code_import",
                F.concat_ws(", ", F.lit("unspecified"), F.col("jurisdiction")),
            )
        )
        df = add_pure_company_name(df, "name")

        df = (
            df.selectExpr("*", f"{H.DV_HASHKEY_COMPANY} as buyer_dv_hashkey_company")
            .drop(
                "name",
                "registration_number",
                "full_address",
                "phone_numbers",
                "jurisdiction",
                "pure_name",
            )
            .withColumnRenamed("jurisdiction_export", "jurisdiction")
        )

        # export company
        df = (
            df.withColumnsRenamed(
                {
                    "name_export": "name",
                    "address_export": "full_address",
                }
            )
            .withColumn(
                "jurisdiction",
                F.when(F.col("jurisdiction").isNull(), F.lit("unspecified")).otherwise(
                    F.col("jurisdiction")
                ),
            )
            .withColumn(
                "port_code_export",
                F.concat_ws(", ", F.lit("unspecified"), F.col("jurisdiction")),
            )
        )
        df = add_pure_company_name(df, "name")

        df = df.selectExpr(
            "*",
            "NULL as registration_number",
            f"{H.DV_HASHKEY_COMPANY} as supplier_dv_hashkey_company",
        ).drop(
            "name", "full_address", "jurisdiction", "pure_name", "registration_number"
        )

        # shipper company
        df = df.withColumnRenamed("shipper_company", "name")
        df = add_pure_company_name(df, "name")

        df = df.selectExpr(
            "*",
            "'unspecified' as jurisdiction",
            "NULL as registration_number",
            f"{H.DV_HASHKEY_COMPANY} as shipper_dv_hashkey_company",
        ).drop("name", "jurisdiction", "pure_name", "registration_number")

        df = (
            df.alias("data")
            .join(
                lookup_port.select("dv_hashkey_port", "port_code").alias("import_port"),
                F.col("import_port.port_code") == F.col("data.port_code_import"),
                how="left",
            )
            .withColumnRenamed("dv_hashkey_port", "import_port")
            .drop("port_code")
            .join(
                lookup_port.select("dv_hashkey_port", "port_code").alias("export_port"),
                F.col("export_port.port_code") == F.col("data.port_code_export"),
                how="left",
            )
            .withColumnRenamed("dv_hashkey_port", "export_port")
            .drop("port_code")
        )

        df = df.filter(
            F.col("import_port").isNotNull() & F.col("export_port").isNotNull()
        )

        ls_column = [
            "bol",
            "teu_number",
            "invoice_value",
            "estimated_arrival_date",
            "vessel_name",
        ]

        for column_name in ls_column:
            if column_name not in df.columns:
                df = df.withColumn(column_name, F.lit(None))

        return df.selectExpr(
            f"{H.DV_HASHKEY_BOL} as dv_hashkey_bol",
            f"'{record_source}' as dv_recsrc",
            "'gov_colombia_trade_import_2016_2024;20241227' as dv_source_version",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "buyer_dv_hashkey_company",
            "supplier_dv_hashkey_company",
            "shipper_dv_hashkey_company",
            "import_port",
            "export_port",
            "actual_arrival_date",
            f"{H.DV_HASHKEY_L_BOL} as dv_hashkey_l_bol",
        )


def run(env="pro", params={}, spark=None):
    import sys

    table_meta_hub = TableMeta(from_files=["metas/silver/l_bol.yaml"], env=env)

    executer_hub = Lbol1tmExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
        params,
        spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
