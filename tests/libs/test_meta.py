from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    ArrayType,
    IntegerType,
)
import pytest
import json
from libs.meta import TableMeta


# Mock for command-line arguments
@pytest.fixture(scope="function")
def mock_args(monkeypatch):
    monkeypatch.setattr(
        "sys.argv", ["test_script.py", "--payload", '{"load_date": "20241205"}']
    )


@pytest.fixture(scope="function")
def table_meta(mock_args):
    # table_meta = TableMeta(from_text=meta_txt)
    # Mock command-line arguments
    table_meta = TableMeta(from_files="tests/libs/hub_account.yaml")
    assert (
        table_meta.input_resources[0].data_location
        == "data/test/src_g/load_date=20241205"
    )
    return table_meta


@pytest.fixture(scope="module")
def df(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("id2", StringType(), True),
            StructField("data_date", StringType(), True),
            StructField("test_array", ArrayType(StringType()), True),
            StructField("test_map", MapType(StringType(), StringType()), True),
        ]
    )
    data = [
        (0, "a", "2023-01-03", ["x", "y"], {"name": "test0", "age": "1"}),
        (12, "a", "2023-01-04", ["x", "y", "Z"], {"name": "test12", "age": "low"}),
        (1, "b", "2023-01-03", ["x", "y"], {"name": "test1", "age": "low"}),
        (2, "b", "2023-01-03", ["x", "y"], {"name": "test2", "age": "low"}),
        (2, "b", "2023-01-04", ["x", "y"], {"name": "test2", "age": "medium"}),
        (3, "c", "2023-01-03", ["x", "y"], {"name": "test3", "age": "old"}),
        (3, "d", "2023-01-04", ["x", "y"], {"name": "test3", "age": "medium"}),
        (4, "d", "2023-01-04", ["x", "y"], {"name": "test4", "age": "medium"}),
        (4, "d", "2023-01-03", None, {"name": "test4", "age": "medium"}),
    ]
    return spark.createDataFrame(data, schema)


def test_table_model(table_meta, logger, spark):

    # ddl_sql = table_meta.ddl_sql
    model = table_meta.model
    reconcile_suites = table_meta.reconciles
    deequ_quality_params = table_meta.deequ_quality_params
    input_resources = table_meta.input_resources
    # ge_quality_params = table_meta.ge_quality_params
    # spark_struct_type = table_meta.struct_type

    # logger.info(ddl_sql)
    # print(model.data_location)
    # print(model.partition_by)
    # print(model.data_format)
    # logger.info("model", model)

    logger.info("input_resources: %s" % (input_resources[0]))

    logger.info(
        "deequ_quality_params: %s" % (json.dumps(deequ_quality_params, indent=2))
    )

    # logger.info("ge_quality_params: %s" %(json.dumps(ge_quality_params, indent=2)))
    logger.info("reconcile_suites: %s" % (json.dumps(reconcile_suites, indent=2)))
    # logger.info(spark_struct_type)

    # table_meta.execute_create_delta_with_path(spark)

    # assert type(ddl_sql) == str
    assert type(deequ_quality_params) == list
    assert type(reconcile_suites) == list
    # assert type(spark_struct_type) == StructType
