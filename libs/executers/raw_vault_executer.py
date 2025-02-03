from .base_executer import BaseExecuter
from pyspark.sql import DataFrame
from libs.utils.delta_utils import (
    delta_insert_for_hubnlink,
    delta_write_for_scd2sat,
    delta_write_for_sat,
    delta_upsert_for_bridge_key,
)
from pyspark.sql import DataFrame, functions as F


def check_columns_exist(df, required_columns, warning_columns=None):
    missing = set(required_columns) - set(df.columns)
    if warning_columns:
        missing_warning = [col for col in warning_columns if col not in df.columns]
        if len(missing_warning) > 0:
            print(f"Warning: Optional columns missing: {missing_warning}")

    if missing:
        raise Exception(f"Missing columns: {list(missing)}")
    return True


class RawVaultExecuter(BaseExecuter):
    """'
    - HUB
    - lINK
    """

    def __init__(
        self,
        app_name,
        meta_table_model,
        meta_input_resource,
        spark_resources: dict = {},
        filter_input_resources: list = [],
        params={},
        spark=None,
        **kwargs,
    ):
        super().__init__(
            app_name,
            meta_table_model,
            meta_input_resource,
            spark_resources,
            filter_input_resources,
            params,
            spark,
            **kwargs,
        )
        self.query = params.get("query", None)

    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self):
        df = self.transform()
        check_columns_exist(
            df, ["dv_recsrc", "dv_loaddts"], ["dv_source_version", "jurisdiction"]
        )
        if df.count() > 0:
            print(
                f"Write to {self.meta_table_model.table_name} - {self.meta_table_model.data_location}"
            )
            delta_insert_for_hubnlink(
                spark=self.spark,
                df=df,
                data_location=self.meta_table_model.data_location,
                base_cols=self.meta_table_model.unique_key,
                struct_type=self.meta_table_model.struct_type,
                partition_by=self.meta_table_model.partition_by,
            )


class SatelliteVaultExecuter(BaseExecuter):
    """
    Stores the most recent value of an attribute.
    Only enrich data where values differ
    """

    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self):
        df = self.transform()
        check_columns_exist(
            df, ["dv_recsrc", "dv_loaddts"], ["dv_source_version", "jurisdiction"]
        )
        if df.count() > 0:
            print(
                f"Write to {self.meta_table_model.table_name} - {self.meta_table_model.data_location}"
            )

            delta_write_for_sat(
                spark=self.spark,
                df=df,
                data_location=self.meta_table_model.data_location,
                base_cols=self.meta_table_model.unique_key,
                struct_type=self.meta_table_model.struct_type,
                partition_by=self.meta_table_model.partition_by,
            )


class SatelliteSCD2Executer(BaseExecuter):
    """
    Stores historical values of an attribute.

    - name: dv_status
      nullable: false
      type: integer
      description: dv status
    - name: dv_valid_from
      nullable: false
      type: date
      description: dv valid from
    - name: dv_valid_to
      type: date
      description: dv valid to
    """

    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self):

        df = self.transform()

        if df.count() > 0:
            print("delta_write_for_sat")
            delta_write_for_scd2sat(
                spark=self.spark,
                df=df,
                data_location=self.meta_table_model.data_location,
                base_cols=self.meta_table_model.unique_key,
                struct_type=self.meta_table_model.struct_type,
                partition_by=self.meta_table_model.partition_by,
            )


class BridgeVerifyExecuter(BaseExecuter):
    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self):
        df = self.transform()
        if df.count() > 0:
            print(
                f"Write to {self.meta_table_model.table_name} - {self.meta_table_model.data_location}"
            )
            delta_upsert_for_bridge_key(
                spark=self.spark,
                df=df,
                data_location=self.meta_table_model.data_location,
                base_cols=self.meta_table_model.unique_key,
                struct_type=self.meta_table_model.struct_type,
                partition_by=self.meta_table_model.partition_by,
            )
