import pytest
from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DateType,
    TimestampType,
)
from libs.utils.delta_utils import delta_insert, delta_write_for_sat
from datetime import datetime, date
from libs.meta import TableMeta

table_meta = TableMeta(from_files="tests/libs/sat_account.yaml")


@pytest.fixture(scope="module")
def df(spark):
    schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("dv_hashdiff", StringType(), True),
            StructField("dv_recsrc", StringType(), True),
            StructField("dv_loaddts", TimestampType(), True),
            StructField("dv_status", IntegerType(), True),
            StructField("dv_valid_from", DateType(), True),
            StructField("dv_valid_to", DateType(), True),
            StructField("account_code", StringType(), True),
            StructField("count", IntegerType(), True),
        ]
    )
    data = [
        (
            0,
            "2",
            "a1",
            datetime(2023, 1, 3, 14, 30, 45),
            1,
            date(2023, 1, 3),
            None,
            "B",
            15,
        ),
        (
            1,
            "3",
            "a2",
            datetime(2023, 1, 4, 10, 0, 0),
            1,
            date(2023, 1, 4),
            None,
            "C",
            18,
        ),
        (
            2,
            "4",
            "a1",
            datetime(2023, 1, 5, 8, 15, 30),
            1,
            date(2023, 1, 5),
            None,
            "D",
            20,
        ),
        (
            3,
            "5",
            "a2",
            datetime(2023, 1, 6, 16, 45, 0),
            1,
            date(2023, 1, 6),
            None,
            "E",
            50,
        ),
    ]

    data_test = [
        (
            0,
            "1",
            "a1",
            datetime(2023, 1, 1, 14, 30, 45),
            1,
            date(2023, 1, 1),
            None,
            "B",
            10,
        ),
        (
            1,
            "2",
            "a2",
            datetime(2023, 1, 2, 10, 0, 0),
            1,
            date(2023, 1, 2),
            None,
            "C",
            12,
        ),
    ]
    _df = spark.createDataFrame(data, schema).withColumn(
        "dv_loaddts", f.col("dv_loaddts").cast(TimestampType())
    )
    _df_test = spark.createDataFrame(data_test, schema).withColumn(
        "dv_loaddts", f.col("dv_loaddts").cast(TimestampType())
    )
    for i in range(len(table_meta.input_resources)):
        delta_insert(
            spark,
            _df_test,
            table_meta.input_resources[i].data_location,
            _df_test.schema,
        )
        delta_write_for_sat(
            spark, _df, table_meta.input_resources[i].data_location, "key", _df.schema
        )

    return _df


class Executer(SatelliteVaultExecuter):
    def transform(self, **kwargs) -> DataFrame:

        return self.input_dataframe_dict[
            self.meta_input_resource[0].table_name
        ].dataframe


def test_satellite_vault_executer(logger, df, spark):

    executer = Executer(
        "test_executer",
        table_meta.model,
        table_meta.input_resources,
    )

    logger.info(
        executer.input_dataframe_dict[
            executer.meta_input_resource[0].table_name
        ].dataframe.show()
    )

    executer.execute()

    logger.info("input_dataframe_dict: %s" % (executer.input_dataframe_dict))

    spark.read.format("delta").load(table_meta.input_resources[0].data_location).show()
