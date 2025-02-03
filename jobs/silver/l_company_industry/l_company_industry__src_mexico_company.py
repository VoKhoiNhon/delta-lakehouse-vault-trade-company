import sys

import pyspark.sql.functions as F

from libs.executers.raw_vault_executer import RawVaultExecuter
from libs.meta import TableMeta
from libs.utils.vault_hashfuncs import DV_HASHKEY_COMPANY


class LinkCompanyIndustryExecuter(RawVaultExecuter):
    def transform(self):
        source = self.input_dataframe_dict["bronze.mexico"]
        record_source = source.record_source
        df = source.dataframe
        dv_source_version = self.params.get("dv_source_version", "")
        # manual : find decs of the country code
        df = (
            df.withColumn("country_code", F.lit("MX"))
            .withColumn("standard_type", F.lit("SCIAN"))
            .withColumn("jurisdiction", F.lit("Mexico"))
        )
        df = df.withColumnRenamed("industry_description", "desc")
        df = (
            df.selectExpr(
                "md5(concat_ws(';',country_code, industry_code)) as dv_hashkey_industry",
                f"{DV_HASHKEY_COMPANY} as dv_hashkey_company",
                f"'{record_source}' as dv_recsrc",
                "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
                f"'{dv_source_version}' as dv_source_version",
            )
            .withColumn("type", F.lit(True))
            .withColumn(
                "dv_hashkey_l_c_i",
                F.md5(F.concat_ws(";", "dv_hashkey_industry", "dv_hashkey_company")),
            )
        )
        return df


def run():
    table_meta_lookup = TableMeta(from_files=["metas/silver/l_company_industry.yaml"])

    executer_hub = LinkCompanyIndustryExecuter(
        sys.argv[0],
        table_meta_lookup.model,
        table_meta_lookup.input_resources,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
