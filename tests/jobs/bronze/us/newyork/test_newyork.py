from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
import sys

from jobs.bronze.us.newyork import Executer, run


def test_run(spark):
    run(env="test", params={"dv_source_version": "2024-06-11"})
