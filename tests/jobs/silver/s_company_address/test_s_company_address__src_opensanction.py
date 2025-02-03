from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F


from jobs.silver.s_company_address.s_company_address__src_opensanction import (
    SCompanyAddressOpenSanctionExecuter,
)

print(SCompanyAddressOpenSanctionExecuter)


def test_run(spark):
    table_meta = TableMeta(
        from_files=["metas/silver/s_company_address.yaml"], env="test"
    )
    print(table_meta)
    executer_hub = SCompanyAddressOpenSanctionExecuter(
        "silver/s_company_demographic/s_company_address__src_opensanction",
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-17"},
    )
    executer_hub.execute()
