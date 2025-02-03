import os
import json
from pyspark.sql import functions as F
from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.commons import (
    clean_column_names,
    add_load_date_columns,
    add_pure_company_name,
)
from libs.utils.vault_hashfuncs import DV_HASHKEY_BOL, DV_HASHKEY_L_BOL
from libs.utils.base_transforms import transform_column_from_json


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_data"].dataframe
        # jurisdiction = self.input_dataframe_dict["jurisdiction"].dataframe
        lookup_port = self.input_dataframe_dict["lookup_port"].dataframe

        with open(
            os.path.join("resources", "tradedata", "mapping_name.json"), "r"
        ) as f:
            mapping = json.load(f)
        with open(os.path.join("resources", "tradedata", "source.json"), "r") as f:
            translations = json.load(f)

        df = df.withColumn(
            "import_country", F.trim(F.initcap(F.col("import_country")))
        ).withColumn(
            "export_country_or_area", F.trim(F.initcap(F.col("export_country_or_area")))
        )
        df = transform_column_from_json(df, "import_country", mapping, False)
        df = transform_column_from_json(df, "export_country_or_area", mapping, False)
        df = transform_column_from_json(df, "data_source", translations, False)

        # jurisdiction_from_data = (
        #     df.select("import_country")
        #     .withColumnRenamed("import_country", "country_from_data")
        #     .union(
        #         df.select("export_country_or_area").withColumnRenamed(
        #             "export_country_or_area", "country_from_data"
        #         )
        #     )
        #     .distinct()
        # )
        # result = jurisdiction_from_data.alias("a").join(
        #     jurisdiction.alias("b"),
        #     on=F.col("a.country_from_data") == F.col("b.jurisdiction"),
        #     how="left",
        # )
        lookup_port_test = lookup_port.withColumn(
            "new_port_name", F.explode_outer(F.split("previous_port_name", "; "))
        )
        port_for_map = (
            lookup_port_test.select("port_name", "port_code")
            .union(
                lookup_port_test.select("new_port_name", "port_code").withColumnRenamed(
                    "new_port_name",
                    "port_name",
                )
            )
            .distinct()
        )
        df = (
            df.alias("data")
            .join(
                port_for_map.withColumnRenamed("port_name", "port_name_export")
                .withColumnRenamed("port_code", "port_code_export")
                .alias("export_port"),
                on=F.lower(F.trim(F.col("data.loading_port")))
                == F.lower(F.trim(F.col("export_port.port_name_export"))),
                how="left",
            )
            .join(
                port_for_map.withColumnRenamed("port_name", "port_name_import")
                .withColumnRenamed("port_code", "port_code_import")
                .alias("import_port"),
                on=F.lower(F.trim(F.col("data.unloading_port")))
                == F.lower(F.trim(F.col("import_port.port_name_import"))),
                how="left",
            )
        )
        df = (
            df.withColumn(
                "port_name_export",
                F.when(
                    F.col("port_code_export").isNull(),
                    F.concat_ws(
                        ", ", F.lit("unspecified"), F.col("export_country_or_area")
                    ),
                ).otherwise(F.col("port_name_export")),
            )
            .withColumn(
                "port_name_import",
                F.when(
                    F.col("port_code_import").isNull(),
                    F.concat_ws(", ", F.lit("unspecified"), F.col("import_country")),
                ).otherwise(F.col("port_name_import")),
            )
            .withColumn(
                "port_code_export",
                F.when(
                    F.col("port_code_export").isNull(),
                    F.concat_ws(
                        ", ", F.lit("unspecified"), F.col("export_country_or_area")
                    ),
                ).otherwise(F.col("port_code_export")),
            )
            .withColumn(
                "port_code_import",
                F.when(
                    F.col("port_code_import").isNull(),
                    F.concat_ws(", ", F.lit("unspecified"), F.col("import_country")),
                ).otherwise(F.col("port_code_import")),
            )
            .withColumn(
                "port_code_export",
                F.when(
                    F.col("port_code_export") == "unspecified, unspecified",
                    F.lit("unspecified"),
                ).otherwise(F.col("port_code_export")),
            )
            .withColumn(
                "port_code_import",
                F.when(
                    F.col("port_code_import") == "unspecified, unspecified",
                    F.lit("unspecified"),
                ).otherwise(F.col("port_code_import")),
            )
        )
        df = (
            df.alias("data")
            .join(
                lookup_port.select("dv_hashkey_port", "port_code")
                .withColumnRenamed("dv_hashkey_port", "export_port")
                .alias("export_port"),
                on=F.col("data.port_code_export") == F.col("export_port.port_code"),
                how="left",
            )
            .drop("port_code")
            .join(
                lookup_port.select("dv_hashkey_port", "port_code")
                .withColumnRenamed("dv_hashkey_port", "import_port")
                .alias("import_port"),
                on=F.col("data.port_code_import") == F.col("import_port.port_code"),
                how="left",
            )
            .drop("port_code")
        )
        df = df.filter(
            (F.col("import_country").isNotNull())
            & (F.col("export_country_or_area").isNotNull())
        )
        df = (
            df.withColumn("unit_of_weight", F.upper(F.col("unit_of_weight")))
            .withColumn(
                "actual_arrival_date",
                F.concat(
                    F.substring("date", 1, 4),  # yyyy
                    F.lit("-"),
                    F.substring("date", 5, 2),  # mm
                    F.lit("-"),
                    F.substring("date", 7, 2),  # dd
                ),
            )
            .withColumn(
                "dv_recsrc",
                F.lit(
                    "NBD Data;https://data.nbd.ltd/en/login?r=%2Fcn%2Ftransaction%2Fsearch"
                ),
            )
            .withColumn("bol", F.lit(None).cast("string"))
            .withColumn("teu_number", F.lit(None).cast("string"))
            .withColumn("invoice_value", F.lit(None).cast("string"))
            .withColumn("value_usd", F.lit(None).cast("string"))
            .withColumn("estimated_arrival_date", F.lit(None).cast("string"))
            .withColumn("vessel_name", F.lit(None).cast("string"))
            .withColumn("exchange_rate", F.lit(None).cast("string"))
            .withColumnRenamed("unit_of_weight", "weight_unit")
            .withColumnRenamed("unit_of_quantity", "quantity_unit")
            .withColumnRenamed("products", "description")
        )

        df = clean_column_names(df)
        df = add_pure_company_name(df, "importer", "pure_importer")
        df = add_pure_company_name(df, "exporter", "pure_exporter")
        # df = clean_string_columns(df)
        df = (
            df.withColumn(
                "buyer_dv_hashkey_company",
                F.md5(F.concat_ws(";", "import_country", F.lit(None), "pure_importer")),
            )
            .withColumn(
                "supplier_dv_hashkey_company",
                F.md5(
                    F.concat_ws(
                        ";", "export_country_or_area", F.lit(None), "pure_exporter"
                    )
                ),
            )
            .withColumn("shipper_dv_hashkey_company", F.lit(None))
            .filter(
                (F.col("pure_importer").isNotNull())
                & (F.col("pure_exporter").isNotNull())
            )
        )
        df = df.withColumn("dv_hashkey_bol", F.expr(DV_HASHKEY_BOL)).withColumn(
            "dv_hashkey_l_bol", F.expr(DV_HASHKEY_L_BOL)
        )
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        return df


def run():
    table_meta = TableMeta(from_files="metas/bronze/trade_dat_data/trade_dat_data.yaml")
    executer = Executer(
        app_name="trade_dat_bronze",
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
