import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class LookupHscodeExecuter(RawVaultExecuter):
    def transform(self):
        source1 = self.input_dataframe_dict["bronze.lookup_brazil_hs2"]
        source2 = self.input_dataframe_dict["bronze.lookup_brazil_hs6_8"]
        record_source = source1.record_source
        df1 = source1.dataframe
        df2 = source2.dataframe
        df1 = (
            df1.withColumn(
                "Chapter Code", F.regexp_replace(F.col("Chapter Code"), "Chapter ", "")
            )
            .withColumnRenamed("Chapter Code", "code")
            .withColumn(
                "Chapter Description",
                F.regexp_replace(F.col("Chapter Description"), "HS Code for ", ""),
            )
            .withColumnRenamed("Chapter Description", "description")
        )
        df2 = df2.select(
            F.col("NCM Code").alias("code"),
            F.col("Product Description").alias("description"),
        )
        df = df1.union(df2)
        df = df.withColumn(
            "description", F.regexp_replace(F.col("description"), r"[().]", "")
        )

        df_hs2 = (
            df.filter(F.length(df["code"]) == 2)
            .select(F.col("code").alias("code_2"), "description")
            .withColumnRenamed("description", "desc_2")
            .withColumn("code_2", F.trim(F.col("code_2")))
        )

        df_hs4 = (
            df.filter(F.length(df["code"]) == 4)
            .select(F.col("code").alias("code_4"), "description")
            .withColumnRenamed("description", "desc_4")
            .withColumn("code_4", F.trim(F.col("code_4")))
        )

        df_hs6 = (
            df.filter(F.length(df["code"]) == 6)
            .select(F.col("code").alias("code_6"), "description")
            .withColumnRenamed("description", "desc_6")
            .withColumn("code_6", F.trim(F.col("code_6")))
        )

        df_hs8 = (
            df.filter(F.length(df["code"]) == 8)
            .select(F.col("code").alias("code_8"), "description")
            .withColumnRenamed("description", "desc_8")
            .withColumn("code_8", F.trim(F.col("code_8")))
        )

        df_joined_2_4 = df_hs2.join(
            df_hs4, F.substring(df_hs4["code_4"], 1, 2) == df_hs2["code_2"], "left"
        )
        df_joined_4_6 = df_joined_2_4.join(
            df_hs6, F.substring(df_hs6["code_6"], 1, 4) == df_hs4["code_4"], "left"
        )
        lookup_hs8 = df_joined_4_6.join(
            df_hs8,
            F.when(
                F.col("code_6").isNull(),
                F.substring(df_hs8["code_8"], 1, 4) == df_hs4["code_4"],
            ).otherwise(F.substring(df_hs8["code_8"], 1, 6) == df_hs6["code_6"]),
            "left",
        )

        lookup_hs8 = (
            lookup_hs8.withColumn(
                "description_4",
                F.concat_ws("; ", lookup_hs8["desc_2"], lookup_hs8["desc_4"]),
            )
            .withColumn(
                "description_6",
                F.concat_ws(
                    "; ",
                    lookup_hs8["desc_2"],
                    lookup_hs8["desc_4"],
                    lookup_hs8["desc_6"],
                ),
            )
            .withColumn(
                "description_8",
                F.concat_ws(
                    "; ",
                    lookup_hs8["desc_2"],
                    lookup_hs8["desc_4"],
                    lookup_hs8["desc_6"],
                    lookup_hs8["desc_8"],
                ),
            )
        )

        df_hs2 = (
            lookup_hs8.select(
                F.col("code_2").alias("code"), F.col("desc_2").alias("description")
            )
            .filter((F.col("code").isNotNull()))
            .withColumn("type", F.lit("hs_2"))
            .dropDuplicates()
            .orderBy("code")
        )

        df_hs4 = (
            lookup_hs8.select(
                F.col("code_4").alias("code"),
                F.col("description_4").alias("description"),
            )
            .filter((F.col("code").isNotNull()))
            .withColumn("type", F.lit("hs_4"))
            .dropDuplicates()
            .orderBy("code")
        )

        df_hs6 = (
            lookup_hs8.select(
                F.col("code_6").alias("code"),
                F.col("description_6").alias("description"),
            )
            .filter((F.col("code").isNotNull()))
            .withColumn("type", F.lit("hs_6"))
            .dropDuplicates()
            .orderBy("code")
        )

        df_hs8 = (
            lookup_hs8.select(
                F.col("code_8").alias("code"),
                F.col("description_8").alias("description"),
            )
            .filter((F.col("code").isNotNull()))
            .withColumn("type", F.lit("hs_8"))
            .dropDuplicates()
            .orderBy("code")
        )

        df = (
            df_hs2.union(df_hs4)
            .union(df_hs6)
            .union(df_hs8)
            .withColumn("country_code", F.lit("BR"))
            .withColumn("country_name", F.lit("Brazil"))
            .orderBy("code")
        )
        df = df.withColumn(
            "parent_code",
            F.when(F.length(F.col("code")) == 4, F.col("code").substr(1, 2))
            .when(F.length(F.col("code")) == 6, F.col("code").substr(1, 4))
            .when(
                F.length(F.col("code")) == 8,
                F.when(
                    F.col("code")
                    .substr(1, 6)
                    .isin(
                        [
                            row[0]
                            for row in df.select("code")
                            .filter(F.length(F.col("code")) == 6)
                            .collect()
                        ]
                    ),
                    F.col("code").substr(1, 6),
                ).otherwise(F.col("code").substr(1, 4)),
            )
            .otherwise(None),
        )

        dv_source_version = self.params.get("dv_source_version", "")
        df = df.selectExpr(
            "md5(concat_ws(';',country_name, code)) as dv_hashkey_hscode",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "code",
            "description",
            "type",
            "country_code",
            "country_name",
            "parent_code",
        ).dropDuplicates()
        df.show(10)
        return df


def run():
    table_meta_lookup = TableMeta(from_files=["metas/silver/lookup_hscode.yaml"])

    executer_hub = LookupHscodeExecuter(
        sys.argv[0],
        table_meta_lookup.model,
        table_meta_lookup.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
