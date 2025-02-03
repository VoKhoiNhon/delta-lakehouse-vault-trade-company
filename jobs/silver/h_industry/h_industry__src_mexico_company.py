import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import sys
from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta


class HIndustryExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.mexico"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")
        # manual : find decs of the country code
        df = df.withColumn("country_code", F.lit("MX")).withColumn(
            "standard_type", F.lit("SCIAN")
        )
        df = df.withColumnRenamed("industry_description", "desc")
        df = df.selectExpr(
            "md5(concat_ws(';',country_code, industry_code)) as dv_hashkey_industry",
            f"'{record_source}' as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            f"'{dv_source_version}' as dv_source_version",
            "industry_code",
            "desc",
            "country_code",
            "standard_type",
        ).dropDuplicates()
        df.show(5)
        return df


def run():
    table_meta_lookup = TableMeta(from_files=["metas/silver/hub_industry.yaml"])

    executer_hub = HIndustryExecuter(
        sys.argv[0],
        table_meta_lookup.model,
        table_meta_lookup.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
