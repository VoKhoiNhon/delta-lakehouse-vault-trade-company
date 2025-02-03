from .base_executer import BaseExecuter

from pyspark.sql import DataFrame
from libs.utils.delta_utils import (
    delta_upsert_for_gold,
    delta_insert_for_hubnlink,
    delta_insert,
    print_last_delta_last_operation,
)


class GoldExecuter(BaseExecuter):
    def __init__(
        self,
        app_name,
        meta_table_model,
        meta_input_resource,
        params={},
        spark=None,
        **kwargs,
    ):
        super().__init__(
            app_name, meta_table_model, meta_input_resource, params, spark, **kwargs
        )
        self.query = params.get("query", None)

    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self, option={}):

        df = self.transform()
        delta_insert_for_hubnlink(
            spark=self.spark,
            df=df,
            data_location=self.meta_table_model.data_location,
            base_cols=self.meta_table_model.unique_key,
            struct_type=self.meta_table_model.struct_type,
            partition_by=self.meta_table_model.partition_by,
        )


class GoldCompanyExecuter(BaseExecuter):
    def __init__(
        self,
        app_name,
        meta_table_model,
        meta_input_resource,
        params={},
        spark=None,
        **kwargs,
    ):
        super().__init__(
            app_name, meta_table_model, meta_input_resource, params, spark, **kwargs
        )
        self.query = params.get("query", None)
        self.prerun_delete_sql = params.get("prerun_delete_sql", None)
        if self.prerun_delete_sql:
            self.execute_prerun_delete_sql(self.prerun_delete_sql)

    def execute_prerun_delete_sql(self):
        from delta.tables import DeltaTable

        DeltaTable.forPath(self.spark, self.meta_table_model.data_location).delete(
            self.prerun_delete_sql
        )
        print("executed prerun delete sql: ", self.prerun_delete_sql)

    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self):
        df = self.transform()
        # delta_insert(
        #     spark=self.spark,
        #     df=df,
        #     data_location=self.meta_table_model.data_location,
        #     partition_by=self.meta_table_model.partition_by,
        #     options=self.meta_table_model.options,
        # )
        delta_insert_for_hubnlink(
            spark=self.spark,
            df=df,
            data_location=self.meta_table_model.data_location,
            base_cols=self.meta_table_model.unique_key,
            struct_type=self.meta_table_model.struct_type,
            partition_by=self.meta_table_model.partition_by,
        )
        # from delta.tables import DeltaTable
        # delta_table = DeltaTable.forPath(
        #     self.spark, self.meta_table_model.data_location
        # )
        # print_last_delta_last_operation(delta_table)


class GoldTradeExecuter(BaseExecuter):
    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self, option={}):

        df = self.transform()
        delta_upsert_for_gold(
            spark=self.spark,
            df=df,
            data_location=self.meta_table_model.data_location,
            base_cols=self.meta_table_model.unique_key,
            struct_type=self.meta_table_model.struct_type,
            partition_by=self.meta_table_model.partition_by,
        )


class DBTradeServiceExecuter(BaseExecuter):
    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self, option={}):
        pass
