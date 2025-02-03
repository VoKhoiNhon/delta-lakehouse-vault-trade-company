from libs.meta import TableMeta
import pyspark.sql.functions as F


from jobs.gold.company_service.industry import (
    IndustryExecuter,
)


def test_run(spark):
    table_meta = TableMeta(
        from_files=["metas/gold/company_service/industry.yaml"], env="test"
    )
    print(table_meta)
    executer_hub = IndustryExecuter(
        "industry",
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-13"},
    )

    executer_hub.execute()
