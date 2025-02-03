import os
import json

from libs.executers.bronze_executer import BronzeExecuter
from libs.meta import TableMeta
from libs.utils.base_transforms import transform_column_from_json

from pyspark.sql import functions as F
from pyspark.sql.window import Window


class Executer(BronzeExecuter):
    def transform(self):
        df = self.input_dataframe_dict["raw_taiwan"]
        df = (
            df.withColumnRenamed("統一編號", "registration_number")
            .withColumnRenamed("公司名稱", "name")
            .withColumnRenamed("公司地址", "address")
            .withColumnRenamed("資本總額", "total_capital")
            .withColumnRenamed("實收資本額", "paid_in_capital")
            .withColumnRenamed("在境內營運資金", "domestic_working_capital")
            .withColumnRenamed("公司狀態", "status")
            .withColumnRenamed("產製日期", "production_date")
            .withColumnRenamed("負責人", "agent")
        )
        df = (
            df.withColumn(
                "rank",
                F.row_number().over(
                    Window.partitionBy("registration_number").orderBy(
                        F.col("production_date").desc()
                    )
                ),
            )
            .filter(F.col("rank") == 1)
            .drop("rank")
        )
        with open(os.path.join("resources", "taiwan", "status.json"), "r") as f:
            status = json.load(f)
        df = transform_column_from_json(df, "status", status)
        return df.repartition(1)


def run():
    table_meta = TableMeta(from_files="metas/bronze/taiwan/taiwan.yaml")
    # table_meta = TableMeta(from_files="yukon.yaml")
    executer = Executer(
        "bronze_taiwan",
        table_meta.model,
        table_meta.input_resources,
    )
    executer.execute()


if __name__ == "__main__":
    run()
