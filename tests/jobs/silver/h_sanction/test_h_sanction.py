from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys

from jobs.silver.h_sanction.h_sanction_demo import HSanctionExecuter


def test_run(spark):
    table_meta = TableMeta(from_files=["metas/silver/h_sanction.yaml"], env="test")
    print(sys.argv[0])
    executer_hub = HSanctionExecuter(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        {"dv_source_version": "2024-12-17"},
    )

    executer_hub.execute()
    spark.read.format("delta").load(table_meta.model.data_location).show(5, False)
