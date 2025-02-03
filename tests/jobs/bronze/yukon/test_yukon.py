from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys

from jobs.bronze.yukon.yukon import Executer, run


def test_run(spark):
    run(env="test", params={"dv_source_version": "2024-12-04"})
    # table_meta = TableMeta(from_files=["metas/bronze/yukon/yukon.yaml"], env="test")
    # print(sys.argv[0])
    # executer = Executer(
    #     sys.argv[0],
    #     table_meta.model,
    #     table_meta.input_resources,
    #     {"dv_source_version": "2024-12-04"},
    # )
    # executer.execute()
