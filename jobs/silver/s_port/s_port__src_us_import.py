from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys
import libs.utils.vault_hashfuncs as H


class SPortUSImportExecuter(SatelliteVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.us.import"]
        record_source = source.record_source
        df = source.dataframe
        sample = (
            self.spark.read.format("delta")
            .load("tests/resource/lakehouse-bronze/port")
            .withColumn("decimal", F.split("decimal", ","))
            .withColumn("latitude", F.trim(F.col("decimal").getItem(0)))
            .withColumn("longitude", F.trim(F.col("decimal").getItem(1)))
        )
        df = df.withColumn(
            "unloading_port_code", F.split(F.col("unloading_port"), ", ").getItem(0)
        )
        df_port_unloading = (
            df.alias("a")
            .join(
                sample.alias("b"),
                on=F.col("a.unloading_port_code") == F.col("b.port_code"),
                how="left",
            )
            .filter(F.col("b.port_name").isNotNull())
            .select(
                "b.port_code",
                "b.port_name",
                "b.emails",
                "b.address",
                "b.longitude",
                "b.latitude",
                "b.status",
                "b.locode",
                "b.state",
                "b.terminal",
                "b.website",
                "b.port_size",
                "b.port_type",
                "b.fax",
                "b.phone",
            )
        )
        df_port_loading = (
            df.withColumn(
                "port_name",
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
            .withColumn("port_code", F.split(F.col("loading_port"), ", ").getItem(0))
            .withColumn("emails", F.lit(None).cast("string"))
            .withColumn("address", F.lit(None).cast("string"))
            .withColumn("longitude", F.lit(None).cast("string"))
            .withColumn("latitude", F.lit(None).cast("string"))
            .withColumn("phone", F.lit(None).cast("string"))
            .withColumn("fax", F.lit(None).cast("string"))
            .withColumn("port_type", F.lit(None).cast("string"))
            .withColumn("port_size", F.lit(None).cast("string"))
            .withColumn("website", F.lit(None).cast("string"))
            .withColumn("terminal", F.lit(None).cast("string"))
            .withColumn("state", F.lit("Unknown"))
            .withColumn("locode", F.lit(None).cast("string"))
            .withColumn("status", F.lit(None).cast("string"))
            .select(
                "port_code",
                "port_name",
                "emails",
                "address",
                "longitude",
                "latitude",
                "status",
                "locode",
                "state",
                "terminal",
                "website",
                "port_size",
                "port_type",
                "fax",
                "phone",
            )
        )
        result = (
            df_port_unloading.unionByName(df_port_loading)
            .withColumnRenamed("fax", "fax_number")
            .withColumnRenamed("phone", "phone_number")
            .dropDuplicates()
        )

        return result.selectExpr(
            "md5(concat_ws(';', port_code, port_name,emails, address, phone_number, fax_number,port_type,port_size,\
            website,terminal,state,locode,status,latitude,longitude)) as dv_hashdiff",
            f"'{record_source}' as dv_recsrc",
            f"{H.DV_HASHKEY_PORT} as dv_hashkey_port",
            "1 as dv_status",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "DATE_SUB(CAST(FROM_UTC_TIMESTAMP(NOW(), 'UTC') AS DATE), 1) as dv_valid_from",
            "NULL as dv_valid_to",
            "port_code",
            "port_name",
            "emails",
            "address",
            "phone_number",
            "fax_number",
            "port_type",
            "port_size",
            "website",
            "terminal",
            "state",
            "locode",
            "status",
            "latitude",
            "longitude",
        )


def run():
    table_meta_hub = TableMeta(from_files=["metas/silver/s_port.yaml"])
    executer_hub = SPortUSImportExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
