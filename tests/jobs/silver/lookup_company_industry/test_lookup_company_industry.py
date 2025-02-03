from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys

from jobs.silver.lookup.lookup_industry_mexico import LookupCompanyIndustryExecuter

print(LookupCompanyIndustryExecuter)


def test_run(spark):
    table_meta = TableMeta(
        from_files=["metas/silver/lookup_company_industry.yaml"], env="test"
    )
    print(sys.argv[0])
    executer_hub = LookupCompanyIndustryExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-12"},
    )
    executer_hub.execute()
