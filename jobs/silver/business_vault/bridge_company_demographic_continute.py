from pyspark.sql import functions as F

from libs.meta import TableMeta
from libs.executers.raw_vault_executer import SatelliteVaultExecuter


class BridgeCompanyDemographicContinute(SatelliteVaultExecuter):
    def transform(self):

        s_company_demographic = self.input_dataframe_dict[
            "silver.s_company_demographic"
        ].dataframe
        bridge_company_key = self.input_dataframe_dict[
            "silver.bridge_company_key"
        ].dataframe
        bridge_company_verified = self.input_dataframe_dict[
            "silver.bridge_company_verified"
        ].dataframe

        s_company_demographic = s_company_demographic.groupBy("dv_hashkey_company").agg(
            *[
                F.array_distinct(F.flatten(F.collect_list(c))).alias(c)
                if c_type.startswith("array")
                else F.max(c).alias(c)
                if c_type == "boolean"
                else F.first(c, ignorenulls=True).alias(c)
                for c, c_type in s_company_demographic.dtypes
                if c != "dv_hashkey_company"
            ]
        )

        bridge_company_key_old = (
            bridge_company_key.alias("a")
            .join(
                bridge_company_verified.alias("b"),
                (F.col("a.jurisdiction") == F.col("b.jurisdiction"))
                & (F.col("a.to_key") == F.col("b.dv_hashkey_company")),
                "inner",
            )
            .select(
                F.col("a.*"),
                *[
                    F.col(f"b.{c}").alias(f"prio_{c}")
                    for c in bridge_company_verified.columns
                ],
            )
        )

        bridge_company_key_new = (
            bridge_company_key.alias("a")
            .join(
                bridge_company_verified.alias("b"),
                (F.col("a.jurisdiction") == F.col("b.jurisdiction"))
                & (F.col("a.to_key") == F.col("b.dv_hashkey_company")),
                "leftanti",
            )
            .join(
                s_company_demographic.alias("c"),
                (F.col("a.jurisdiction") == F.col("c.jurisdiction"))
                & (F.col("a.to_key") == F.col("c.dv_hashkey_company")),
                "inner",
            )
            .select(
                F.col("a.*"),
                *[
                    F.col(f"c.{c}").alias(f"prio_{c}")
                    for c in bridge_company_verified.columns
                ],
            )
        )

        bridge_company_key = bridge_company_key_old.union(bridge_company_key_new)
        bridge_company_key = (
            bridge_company_key.alias("a")
            .join(
                s_company_demographic.alias("b"),
                (F.col("a.jurisdiction") == F.col("b.jurisdiction"))
                & (F.col("a.from_key") == F.col("b.dv_hashkey_company")),
                "left",
            )
            .select(
                F.col("a.from_key"),
                *[
                    F.when(F.col(f"a.prio_{c}").isNull(), F.col(f"b.{c}"))
                    .when(
                        F.col(f"a.prio_{c}").isNotNull()
                        & (F.col(f"b.{c}").isNotNull()),
                        F.array_union(F.col(f"a.prio_{c}"), F.col(f"b.{c}")),
                    )
                    .otherwise(F.col(f"a.prio_{c}"))
                    .alias(f"prio_{c}")
                    if c_type.startswith("array")
                    else F.when((F.col(f"a.prio_{c}").isNull()), F.col(f"b.{c}"))
                    .otherwise(F.col(f"a.prio_{c}"))
                    .alias(c)
                    if c_type != "boolean"
                    else F.when(F.col(f"b.{c}"), F.col(f"b.{c}"))
                    .otherwise(F.col(f"a.prio_{c}"))
                    .alias(c)
                    for c, c_type in s_company_demographic.dtypes
                ],
            )
        )

        bridge_company_key = (
            bridge_company_key.groupBy("dv_hashkey_company")
            .agg(
                *[
                    F.array_distinct(F.flatten(F.collect_list(c))).alias(c)
                    if c_type.startswith("array")
                    else F.max(c).alias(c)
                    if c_type == "boolean"
                    else F.first(c, ignorenulls=True).alias(c)
                    for c, c_type in bridge_company_key.dtypes
                    if c != "dv_hashkey_company" and c != "from_key"
                ]
            )
            .drop("dv_loaddts")
        )

        return bridge_company_key.selectExpr(
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            *bridge_company_key.columns,
        )


def run(env="pro", payload={}, spark=None):
    import sys

    table_meta_hub = TableMeta(
        from_files=["metas/silver/bridge/bridge_company_demographic.yaml"],
        payload=payload,
    )
    executer_hub = BridgeCompanyDemographicContinute(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
        spark=spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
