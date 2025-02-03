from pyspark.sql import DataFrame, functions as F

from libs.meta import TableMeta
from libs.executers.raw_vault_executer import BridgeVerifyExecuter


class BridgeCompanyVerifiedReg(BridgeVerifyExecuter):
    def group_prioritize(
        self,
        h_c: DataFrame,
        l_v: DataFrame,
    ) -> DataFrame:
        l_v = l_v.select(
            F.col("verified_dv_hashkey_company").alias("to_key"),
            F.col("dv_hashkey_company").alias("from_key"),
            F.col("jurisdiction").alias("jurisdiction"),
            F.col("dv_recsrc"),
        )
        h_c.createOrReplaceTempView("hc")
        l_v.createOrReplaceTempView("l_v")

        result = self.spark.sql(
            """
with a as (
select jurisdiction, from_key as dv_hashkey_company from l_v
union all
select jurisdiction, to_key as dv_hashkey_company from l_v
)
SELECT
    hc.dv_hashkey_company as to_key,
    hc.jurisdiction,
    hc.dv_recsrc,
    hc.dv_source_version
FROM
    hc
where  NOT EXISTS (
    SELECT 1
    FROM a
    WHERE hc.jurisdiction = a.jurisdiction and hc.dv_hashkey_company = a.dv_hashkey_company
)
"""
        )
        result = l_v.unionByName(result, allowMissingColumns=True)
        return result.selectExpr(
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            *result.columns,
        )

    def transform(self):
        h_company = self.input_dataframe_dict["silver.h_company"].dataframe
        s_company_levenshtein = self.input_dataframe_dict[
            "silver.l_company_verification"
        ].dataframe
        return self.group_prioritize(h_company, s_company_levenshtein)


def run(env="pro", payload=None, spark=None):
    import sys

    table_meta_hub = TableMeta(
        from_files=["metas/silver/bridge/bridge_company_verified_reg.yaml"],
        env=env,
        payload=payload,
    )
    executer_hub = BridgeCompanyVerifiedReg(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
        spark=spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
