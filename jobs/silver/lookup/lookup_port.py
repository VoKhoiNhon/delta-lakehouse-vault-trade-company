import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_PORT
from libs.utils.commons import (
    clean_column_names,
    clean_string_columns,
    add_load_date_columns,
)


class LookupPortExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.port"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")
        df = (
            df.withColumn(
                "port_code",
                F.when(
                    F.col("port_code").isin(
                        "99900",
                        "unspecified, Unknown",
                        "99930",
                        "99950",
                        "24899",
                        "62299",
                        "88888",
                        "24800",
                        "99940",
                        "99920",
                        "99910",
                        "68699",
                        "unspecified, Undisclosed",
                        "99999",
                        "56899",
                    ),
                    F.lit("unspecified"),
                ).otherwise(F.col("port_code")),
            )
            .withColumn(
                "port_name",
                F.when(
                    F.col("port_code") == "unspecified",
                    F.lit("unspecified"),
                ).otherwise(F.col("port_name")),
            )
            .withColumn(
                "country_name",
                F.when(
                    F.col("port_code") == "unspecified",
                    F.lit("unspecified"),
                ).otherwise(F.col("country_name")),
            )
        ).withColumn(
            "jurisdiction",
            F.when(
                F.col("port_code") == "unspecified",
                F.lit("unspecified"),
            ).otherwise(F.col("jurisdiction")),
        )

        df = (
            df.withColumn("email", F.lit(None))
            .withColumn("address", F.lit(None))
            .withColumn("phone_number", F.lit(None))
            .withColumn("fax_number", F.lit(None))
            .withColumn("port_type", F.lit(None))
            .withColumn("port_size", F.lit(None))
            .withColumn("website", F.lit(None))
            .withColumn("terminal", F.lit(None))
            .withColumn("locode", F.lit(None))
            .withColumn("status", F.lit(None))
        )
        df = add_load_date_columns(df=df, date_value=self.meta_table_model.load_date)
        df = df.withColumnRenamed("load_date", "dv_loaddts")
        df = df.selectExpr(
            f"{DV_HASHKEY_PORT} as dv_hashkey_port",
            f"'{record_source}' as dv_recsrc",
            "dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "port_code",
            "port_name",
            "previous_port_name",
            "email",
            "address",
            "phone_number",
            "fax_number",
            "port_type",
            "port_size",
            "website",
            "terminal",
            "jurisdiction",
            "country_code",
            "country_name",
            "locode",
            "status",
            "latitude",
            "longitude",
        ).dropDuplicates()

        df.show(n=1, vertical=True, truncate=False)
        return df


def run():
    table_meta_lookup = TableMeta(from_files=["metas/silver/lookup_port.yaml"])

    executer_hub = LookupPortExecuter(
        sys.argv[0],
        table_meta_lookup.model,
        table_meta_lookup.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
