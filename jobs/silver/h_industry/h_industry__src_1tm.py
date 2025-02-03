import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta

"""
company_unique_id = spark.read.format("delta").load("s3a://lakehouse-bronze/1tm_2412/company_unique_id")
company_industry_df = spark.read.format("delta").load("s3a://warehouse/data/all/company_industry")
industry_df = spark.read.format("delta").load("s3a://warehouse/data/all/industry")

all_company_joined = (
    company_unique_id.alias('e')
    .join(company_industry_df.alias('i'), [F.col('e.id') == F.col('i.company_id')], 'left')
    .join(industry_df.alias('id'), [F.col('i.industry_id') == F.col('id.id')], 'left')
).selectExpr("e.*", "i.industry_id", "i.type", 'id.industry_code',
             'id.country_code', 'id.standard_type', 'id.desc') \
    .write.format("delta").option("mergeSchema", "true") \
    .mode("overwrite").option("delta.autoOptimize.optimizeWrite", "true") \
    .save('s3a://lakehouse-raw/1tm_2412/company_unique_id_industry')
"""


class HIndustryExecuter(RawVaultExecuter):
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
        ).selectExpr(
            "md5(concat_ws(';',country_code, industry_code)) as dv_hashkey_industry",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'1tm' as dv_source_version",
            "industry_code",
            "desc",
            "country_code",
            "standard_type",
        )
        return df


def run(env="pro", params={}, spark=None):
    table_meta = TableMeta(from_files="metas/silver/h_industry.yaml", env=env)
    executer = HIndustryExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":
    run()
