from libs.meta import TableMeta
import pyspark.sql.functions as F


from jobs.silver.l_sanction.link_sanction_person_demo import (
    LSanctionExecuter,
)


def test_run(spark):
    table_meta = TableMeta(
        from_files=["metas/silver/link_sanction_person.yaml"], env="test"
    )
    print(table_meta)
    executer_hub = LSanctionExecuter(
        "l_sanction_person_demo",
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-17"},
    )

    executer_hub.execute()

    # spark.read.format('delta').load(table_meta.model.data_location) \
    #     .select("dv_source_version").show(5, False)
