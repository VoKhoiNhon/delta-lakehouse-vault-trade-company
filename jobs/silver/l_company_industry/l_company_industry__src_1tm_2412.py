from pyspark.sql import functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class LCompanyIndustryExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["1tm_2412.company_unique_id_industry"]
        record_source = source.record_source
        df = source.dataframe
        # dv_source_version = self.params.get("dv_source_version", "")
        import pandas as pd

        lookup_jurisdiction = self.spark.createDataFrame(
            pd.read_csv("resources/lookup_jurisdiction.csv", na_filter=False)
        )
        df = (
            df.alias("c")
            .join(F.broadcast(lookup_jurisdiction).alias("l"), "jurisdiction", "left")
            .selectExpr("c.*", "l.country_code  as lookup_country_code")
            .withColumn("country_code", F.col("lookup_country_code"))
        ).where("industry_code is not null and country_code is not null")

        return df.selectExpr(
            "jurisdiction",
            "DV_HASHKEY_COMPANY as dv_hashkey_company",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{record_source}' as dv_recsrc",
            "'1tm' dv_source_version",
            "md5(concat_ws(';', country_code, industry_code)) as dv_hashkey_industry",
            "type",
        ).withColumn(
            "dv_hashkey_l_c_i",
            F.expr("md5(concat_ws(';', dv_hashkey_company, dv_hashkey_industry))"),
        )


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(from_files="metas/silver/l_company_industry.yaml", env=env)
    import sys

    executer_link_relationship = LCompanyIndustryExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer_link_relationship.execute()
