from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys

from jobs.silver.h_person.h_person__src_person_opensanction import HPersonExecuter


def test_run(spark):
    table_meta = TableMeta(from_files=["metas/silver/h_person.yaml"], env="test")
    print(sys.argv[0])
    executer_hub = HPersonExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-17"},
    )

    executer_hub.execute()
    print(table_meta.model.data_location)
    spark.read.format("delta").load(table_meta.model.data_location).show(5, False)
