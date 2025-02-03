from __future__ import annotations
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import argparse
import functools
import time
from delta.tables import DeltaTable


from libs.meta import TableMeta, TableMetaModel, TableMetaInputResource
from libs.utils.connection import SparkSessionBuilder
from libs.utils.delta_utils import timing_decorator


class BaseExecuter(ABC):
    def __init__(
        self,
        app_name: str,
        meta_table_model: TableMetaModel,
        meta_input_resource: TableMetaInputResource,
        spark_resources: dict = {},
        filter_input_resources: list = [],
        params: dict = {},
        spark: SparkSession = None,
        **kwargs,
    ):
        self.meta_table_model = meta_table_model
        self.meta_input_resource = self.filter_meta_input_resoures(
            meta_input_resource=meta_input_resource,
            filter_input_resources=filter_input_resources,
        )

        self.spark_resources = spark_resources
        self.params = params

        if spark:
            self.spark = spark
        else:
            user_config = {"spark.app.name": app_name}
            user_config.update(self.spark_resources)
            self.spark = SparkSessionBuilder(
                user_config=user_config
            ).create_spark_session()

    def filter_meta_input_resoures(
        self, meta_input_resource: list = [], filter_input_resources: list = []
    ):
        if filter_input_resources != []:
            filtered_meta_input_resources = []
            for input_resource in meta_input_resource:
                if getattr(input_resource, "table_name") in filter_input_resources:
                    filtered_meta_input_resources.append(input_resource)
            print(
                f"Choose only {len(filtered_meta_input_resources)} Input_Resources to load: {filter_input_resources}"
            )
            return filtered_meta_input_resources
        else:
            return meta_input_resource

    @property
    def input_dataframe_dict(self) -> dict:
        """
        load all input_resources to dict
        """
        df_dict = {}
        for source in self.meta_input_resource:
            try:
                _df = (
                    self.spark.read.format(source.format)
                    .options(**source.options)
                    .load(source.data_location)
                )
                print(f"Source: {source.data_location} loaded")
                if source.filter:
                    _df = _df.filter(source.filter)
                    print(f"filter: {source.filter} | df.count(): {_df.count():,}")

                source.dataframe = _df
                df_dict[source.table_name] = source
            except Exception as e:
                print(e)
                pass
        return df_dict

    def pre_transform(self, **kwargs):
        """
        Các xử lý trước transform như validation, cleaning data
        """
        pass

    @abstractmethod
    def transform(self, **kwargs) -> DataFrame:
        pass

    def post_transform(self, **kwargs):
        """
        Các xử lý sau transform như validation kết quả, logging
        """
        pass

    @abstractmethod
    @timing_decorator
    def execute(self, **kwargs):
        """
        execute all process
        self.pre_transform(**kwargs)
        df = self.transform(**kwargs)
        self.write(df, **kwargs)
        self.post_transform(**kwargs)
        """
        pass


def run(excuter: Basejob, params: dict):
    """
    _args = [
        "bin/spark-submit",
        "--master spark://spark:7077",
        "--driver-memory 1G",
        "--executor-memory 1G",
        "--executor-cores 2",
        "--total-executor-cores 2",
        "--num-executors 2",
        "--driver-cores 2",
        '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"',
        '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"',
        "--py-files /opt/workspace/spark/libs.zip",
        "/opt/workspace/spark/bronze/job_name.py",
        "--app_name", "job_name",
        "--dv_source_version 2024-12-04"
        ## custom
        "--source_uri", _mongo_uri,
        "--sink_path", _sink_path,
        "--start_point", f'{exec_day}',
        "--timebase_cols", "load_datetime",
        "--mode", "daily",
    ]

    _args_parsed = " ".join(_args)


    rm -rf /libs.zip
        cd /libs && \
        zip -r libs.zip \
        sdb/ -x '*__pycache__*' '*ipynb_checkpoints*'
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name", type=str)
    args = parser.parse_args()

    # table_meta = TableMeta(from_files='tests/libs/hub_account.yml')
    # Basejob(app_name='app', table_meta=table_meta)

    # _params = {
    #     "app_name": args.app_name,
    #     "start_point": args.start_point,
    #     "end_point": args.end_point,
    #     **params
    # }
    # print(_params)
    # excuter(**params).execute()
