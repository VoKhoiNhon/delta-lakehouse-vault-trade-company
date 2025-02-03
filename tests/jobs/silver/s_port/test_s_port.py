from libs.meta import TableMeta
import pyspark.sql.functions as F


from jobs.silver.s_port.s_port__src_us_import import SPortUSImportExecuter


def test_run(spark):
    table_meta = TableMeta(from_files=["metas/silver/s_port.yaml"], env="test")
    print(table_meta)
    executer_hub = SPortUSImportExecuter(
        "s_port-src_us_import",
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-04"},
    )
    executer_hub.execute()

    spark.read.format("delta").load(table_meta.model.data_location).select(
        "dv_source_version"
    ).show(5, False)
