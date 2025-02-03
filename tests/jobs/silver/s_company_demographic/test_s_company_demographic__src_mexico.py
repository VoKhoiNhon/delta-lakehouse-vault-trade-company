from libs.meta import TableMeta
import pyspark.sql.functions as F


from jobs.silver.s_company_demographic.s_company_demographic__src_mexico import (
    SCompanyDemographicMexicoExecuter,
)


def test_run(spark):
    table_meta = TableMeta(
        from_files=["metas/silver/s_company_demographic.yaml"], env="test"
    )
    print(table_meta)
    executer_hub = SCompanyDemographicMexicoExecuter(
        "s_company_demographic-src_mexico",
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-13"},
    )

    executer_hub.execute()

    # spark.read.format('delta').load(table_meta.model.data_location) \
    #     .select("dv_source_version").show(5, False)
