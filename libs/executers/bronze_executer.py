from pyspark.sql import DataFrame
from libs.utils.delta_utils import delta_insert
from .base_executer import BaseExecuter


class BronzeExecuter(BaseExecuter):
    def transform(self, **kwargs) -> DataFrame:
        pass

    def execute(self, options=None):
        df = self.transform()
        delta_insert(
            spark=self.spark,
            df=df,
            data_location=self.meta_table_model.data_location,
            partition_by=self.meta_table_model.partition_by,
            options=self.meta_table_model.options if not options else options,
        )
