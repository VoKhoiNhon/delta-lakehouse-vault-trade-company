import json
import pytest
from libs.executers.bronze_executer import BronzeExecuter
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from libs.utils.delta_utils import delta_upsert


@pytest.fixture(scope="module")
def df(spark, table_meta):
    schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("dv_recsrc", StringType(), True),
            StructField("dv_loaddts", StringType(), True),
            StructField("account_code", StringType(), True),
            StructField("count", IntegerType(), True),
        ]
    )
    data = [
        (0, "a1", "2023-01-03", "B", 15),
        (1, "a2", "2023-01-04", "C", 18),
        (2, "a1", "2023-01-05", "D", 20),
        (3, "a2", "2023-01-06", "E", 50),
    ]

    _df = spark.createDataFrame(data, schema)
    for i in range(len(table_meta.input_resources)):
        delta_upsert(
            spark, _df, table_meta.input_resources[i].data_location, "key", _df.schema
        )

    return _df


class Executer(BronzeExecuter):
    def transform(self, **kwargs) -> DataFrame:
        return self.input_dataframe_dict[self.meta_input_resource[0].dataframe]


def test_bronze_executer(table_meta, logger, df):

    # ddl_sql = table_meta.ddl_sql
    executer = Executer(
        "test_bronze_executer",
        table_meta.model,
        table_meta.input_resources,
    )

    executer.execute()

    logger.info("input_dataframe_dict: %s" % (executer.input_dataframe_dict))
